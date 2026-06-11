// Package raft implements the Raft consensus algorithm (Ongaro and
// Ousterhout, "In Search of an Understandable Consensus Algorithm").
// Section numbers in comments refer to that paper.
//
// A Raft value is a single-threaded state machine. The caller drives it with
// Tick, called at a regular interval, Step, called for each message received
// from another node, and Propose, called to add a command to the log on the
// leader. Committed entries come out of TakeCommitted, in log order.
//
// Raft persists state through Storage and sends messages through Transport
// synchronously from inside those calls, always persisting before it sends
// anything that depends on the persisted state. Raft never reads a clock and
// draws all randomness from Config.Rand, so tests can run it
// deterministically.
package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
)

// DefaultMaxEntriesPerMessage is used when Config.MaxEntriesPerMessage is 0.
const DefaultMaxEntriesPerMessage = 64

// Config configures a Raft node.
type Config struct {
	// ID is this node's ID. It must be nonzero and listed in Peers.
	ID uint64
	// Peers lists every member of the cluster, including ID.
	Peers []uint64

	// ElectionTick is the minimum number of ticks a follower waits without
	// hearing from a leader before starting an election. Each wait is chosen
	// at random from [ElectionTick, 2*ElectionTick) (§5.2).
	ElectionTick int
	// HeartbeatTick is the number of ticks between a leader's heartbeats. It
	// must be less than ElectionTick, or followers time out between
	// heartbeats.
	HeartbeatTick int

	// MaxEntriesPerMessage caps the entries in one AppendEntries request.
	MaxEntriesPerMessage int

	Storage   Storage
	Transport Transport
	// Rand is the source of randomness for election timeouts. If nil, a
	// randomly seeded source is used.
	Rand *rand.Rand
}

func (c *Config) validate() error {
	switch {
	case c.ID == None:
		return errors.New("raft: ID must be nonzero")
	case !slices.Contains(c.Peers, c.ID):
		return errors.New("raft: Peers must include ID")
	case slices.Contains(c.Peers, None):
		return errors.New("raft: peer IDs must be nonzero")
	case len(slices.Compact(slices.Sorted(slices.Values(c.Peers)))) != len(c.Peers):
		return errors.New("raft: duplicate peer ID")
	case c.HeartbeatTick <= 0 || c.ElectionTick <= c.HeartbeatTick:
		return errors.New("raft: need 0 < HeartbeatTick < ElectionTick")
	case c.MaxEntriesPerMessage < 0:
		return errors.New("raft: MaxEntriesPerMessage must not be negative")
	case c.Storage == nil || c.Transport == nil:
		return errors.New("raft: Storage and Transport are required")
	}
	return nil
}

// Raft is one node's consensus state machine. It is not safe for concurrent
// use.
type Raft struct {
	id        uint64
	peers     []uint64 // sorted, includes id
	storage   Storage
	transport Transport
	rng       *rand.Rand

	// Persistent state (Figure 2). These change only after Storage has
	// durably saved the new values; see persist and saveEntries.
	term uint64
	vote uint64
	log  []Entry // see log.go

	// Volatile state (Figure 2). Not persisted: after a restart the commit
	// index is relearned from the leader and entries are applied again
	// from the start.
	commit  uint64
	applied uint64 // highest index returned by TakeCommitted

	state State
	lead  uint64 // leader of the current term, if known
	// votes records the vote responses a candidate has received in the
	// current term. It is a set keyed by node, so a duplicated response is
	// counted once.
	votes map[uint64]bool

	// Leader only (Figure 2): for each node, the next index to send and the
	// highest index known to be replicated there. match includes the leader
	// itself, updated whenever it appends to its own log.
	next  map[uint64]uint64
	match map[uint64]uint64

	electionTick              int
	heartbeatTick             int
	maxMsgEntries             int
	randomizedElectionTimeout int
	electionElapsed           int // follower and candidate only
	heartbeatElapsed          int // leader only

	// err is set when Storage fails. Raft then stops: every later call
	// returns err and nothing more is sent. Continuing after a failed save
	// could mean replying based on state that was never persisted.
	err error
}

// New creates a node, restoring its term, vote, and log from cfg.Storage. A
// restarted node always begins as a follower.
func New(cfg Config) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hs, log, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading state: %w", err)
	}
	for i, e := range log {
		if e.Index != uint64(i)+1 {
			return nil, fmt.Errorf("raft: stored entry %d has index %d", i+1, e.Index)
		}
		if e.Term > hs.Term {
			return nil, fmt.Errorf("raft: stored entry %d has term %d, after current term %d", e.Index, e.Term, hs.Term)
		}
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	maxEntries := cfg.MaxEntriesPerMessage
	if maxEntries == 0 {
		maxEntries = DefaultMaxEntriesPerMessage
	}
	r := &Raft{
		id:            cfg.ID,
		peers:         slices.Sorted(slices.Values(cfg.Peers)),
		storage:       cfg.Storage,
		transport:     cfg.Transport,
		rng:           rng,
		term:          hs.Term,
		vote:          hs.Vote,
		log:           log,
		state:         Follower,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		maxMsgEntries: maxEntries,
	}
	r.resetElectionTimer()
	return r, nil
}

// Status returns a snapshot of the node's state.
func (r *Raft) Status() Status {
	return Status{
		ID:        r.id,
		Term:      r.term,
		Vote:      r.vote,
		Lead:      r.lead,
		State:     r.state,
		Commit:    r.commit,
		LastIndex: r.lastIndex(),
	}
}

// Propose appends a command to the log. It succeeds only on the leader, and
// returns the index and term the entry was given. A successful Propose does
// not mean the entry will commit: if leadership changes first, a later
// leader may overwrite it. The caller learns the outcome by watching
// TakeCommitted for an entry at that index with that term.
//
// data must be nonempty (empty entries are the leader's no-ops). Raft keeps
// its own copy.
func (r *Raft) Propose(data []byte) (index, term uint64, err error) {
	if r.err != nil {
		return 0, 0, r.err
	}
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if len(data) == 0 {
		return 0, 0, errors.New("raft: empty proposal")
	}
	e := Entry{Index: r.lastIndex() + 1, Term: r.term, Data: bytes.Clone(data)}
	if !r.appendAsLeader(e) {
		return 0, 0, r.err
	}
	return e.Index, e.Term, nil
}

// TakeCommitted returns the entries committed since the previous call, in
// log order, and marks them applied. The caller must apply them in exactly
// this order. After a restart, entries are returned again from index 1 as
// the node relearns the commit index.
func (r *Raft) TakeCommitted() []Entry {
	if r.commit <= r.applied {
		return nil
	}
	ents := slices.Clone(r.log[r.applied:r.commit])
	r.applied = r.commit
	return ents
}

// Tick advances the node's logical clock by one tick. A follower or
// candidate whose election timeout elapses starts an election; a leader
// sends AppendEntries, carrying any entries a follower is missing, every
// HeartbeatTick ticks.
func (r *Raft) Tick() error {
	if r.err != nil {
		return r.err
	}
	switch r.state {
	case Leader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTick {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
		}
	case Follower, Candidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.campaign()
		}
	}
	return r.err
}

// Step processes one message from another node. Messages not addressed to
// this node, or from a node outside the cluster, are ignored.
//
// If Step returns an error, Storage failed and the node has stopped. The
// caller should treat this as fatal.
func (r *Raft) Step(m Message) error {
	if r.err != nil {
		return r.err
	}
	if m.To != r.id || m.From == r.id || !slices.Contains(r.peers, m.From) {
		return nil
	}

	switch {
	case m.Term > r.term:
		// §5.1: a higher term in any message, request or response, means
		// our term is stale. Adopt it and become a follower before acting
		// on the message. A MsgApp in that term comes from its leader.
		lead := None
		if m.Type == MsgApp {
			lead = m.From
		}
		if !r.becomeFollower(m.Term, lead) {
			return r.err
		}
	case m.Term < r.term:
		// The sender is behind. Answer requests with our term so a deposed
		// leader or a stale candidate learns of it and steps down (§5.1).
		// Responses from an old term are ignored: a vote granted in an
		// earlier term must not count toward this one.
		switch m.Type {
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgApp:
			r.send(Message{Type: MsgAppResp, To: m.From})
		}
		return nil
	}

	// From here on, m.Term == r.term.
	switch m.Type {
	case MsgVote:
		r.handleVote(m)
	case MsgVoteResp:
		r.handleVoteResp(m)
	case MsgApp:
		r.handleAppend(m)
	case MsgAppResp:
		r.handleAppendResp(m)
	}
	return r.err
}

// campaign starts an election (§5.2): increment the term, vote for
// ourselves, and request votes from every peer.
func (r *Raft) campaign() {
	// The new term and our own vote must be durable before any MsgVote goes
	// out. Otherwise we could crash, restart in the same term with no vote
	// recorded, and vote for another candidate: two votes in one term.
	if !r.persist(r.term+1, r.id) {
		return
	}
	r.state = Candidate
	r.lead = None
	r.votes = map[uint64]bool{r.id: true}
	r.resetElectionTimer()
	if r.wonElection() {
		r.becomeLeader() // single-node cluster
		return
	}
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{
				Type:         MsgVote,
				To:           p,
				LastLogIndex: r.lastIndex(),
				LastLogTerm:  r.lastTerm(),
			})
		}
	}
}

func (r *Raft) handleVote(m Message) {
	// §5.2: at most one vote per term, first come first served. Granting
	// again to the candidate we already voted for is safe, and needed when
	// our first response was lost.
	canVote := r.vote == None || r.vote == m.From
	// §5.4.1: only vote for a candidate whose log is at least as up to date
	// as ours. Every committed entry is on a majority, and a candidate
	// needs votes from a majority, so a candidate missing a committed entry
	// cannot win.
	upToDate := isUpToDate(m.LastLogTerm, m.LastLogIndex, r.lastTerm(), r.lastIndex())
	if !canVote || !upToDate {
		// Rejecting does not reset the election timer (Figure 2).
		r.send(Message{Type: MsgVoteResp, To: m.From})
		return
	}
	if !r.persist(r.term, m.From) {
		return
	}
	r.resetElectionTimer()
	r.send(Message{Type: MsgVoteResp, To: m.From, VoteGranted: true})
}

func (r *Raft) handleVoteResp(m Message) {
	if r.state != Candidate {
		return // a late response after we already won or stepped down
	}
	r.votes[m.From] = m.VoteGranted
	if r.wonElection() {
		r.becomeLeader()
	}
	// Losing is not handled specially: if no candidate wins, election
	// timeouts start a new term.
}

// handleAppend is the receiver side of AppendEntries (Figure 2).
func (r *Raft) handleAppend(m Message) {
	switch r.state {
	case Leader:
		// Two leaders in one term would mean two majorities voted for
		// different candidates, which the vote rules make impossible. If it
		// happens, a safety invariant is already broken.
		panic(fmt.Sprintf("raft: node %d is leader in term %d but received MsgApp from %d", r.id, r.term, m.From))
	case Candidate:
		// §5.2: another node won this term's election.
		if !r.becomeFollower(r.term, m.From) {
			return
		}
	}
	r.lead = m.From
	r.resetElectionTimer()

	// Consistency check (§5.3): we must already have the entry just before
	// the new ones, with the same term. Log Matching then guarantees that
	// everything up to it matches the leader too.
	if m.PrevLogIndex > r.lastIndex() {
		r.send(Message{Type: MsgAppResp, To: m.From, RejectIndex: m.PrevLogIndex, HintIndex: r.lastIndex() + 1})
		return
	}
	if t, _ := r.termAt(m.PrevLogIndex); t != m.PrevLogTerm {
		// Suggest retrying from the first entry of the conflicting term, so
		// the leader skips the whole term in one round trip (end of §5.3).
		// Committed entries cannot conflict, so the hint stays above commit.
		hint := m.PrevLogIndex
		for hint > r.commit+1 {
			if prev, _ := r.termAt(hint - 1); prev != t {
				break
			}
			hint--
		}
		r.send(Message{Type: MsgAppResp, To: m.From, RejectIndex: m.PrevLogIndex, HintIndex: hint})
		return
	}

	// Skip entries we already have. Only the first entry whose term
	// differs, and everything after it, is replaced (Figure 2, receiver
	// step 3). Truncating at PrevLogIndex+1 unconditionally would be wrong:
	// this message may be a delayed duplicate, and our log may already hold
	// later entries from this leader that we have acknowledged.
	ents := m.Entries
	for len(ents) > 0 {
		if t, ok := r.termAt(ents[0].Index); !ok || t != ents[0].Term {
			break
		}
		ents = ents[1:]
	}
	if !r.saveEntries(ents) {
		return
	}

	// Only entries up to the last one in this message are known to match
	// the leader. Entries after it, if any, may be stale, so the commit
	// index must not pass it even if LeaderCommit is higher (Figure 2,
	// receiver step 5).
	lastNew := m.PrevLogIndex + uint64(len(m.Entries))
	if c := min(m.LeaderCommit, lastNew); c > r.commit {
		r.commit = c
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew})
}

func (r *Raft) handleAppendResp(m Message) {
	if r.state != Leader {
		return
	}
	p := m.From
	if m.Success {
		// Responses can arrive out of order. matchIndex only moves forward,
		// so an old success cannot undo a newer one.
		if m.MatchIndex > r.match[p] {
			r.match[p] = m.MatchIndex
			r.maybeCommit()
		}
		r.next[p] = max(r.next[p], m.MatchIndex+1)
		if r.next[p] <= r.lastIndex() {
			r.sendAppend(p) // more to send
		}
		return
	}
	// A rejection only counts if it answers the latest probe. An older one
	// refers to a PrevLogIndex we have already moved below.
	if m.RejectIndex != r.next[p]-1 {
		return
	}
	// The hint is at most RejectIndex, so next strictly decreases and the
	// search terminates. It never drops to or below match: those entries
	// are known to agree.
	r.next[p] = max(r.match[p]+1, min(m.HintIndex, m.RejectIndex))
	r.sendAppend(p)
}

// maybeCommit advances the commit index to the highest entry stored on a
// majority, but only if that entry is from the current term (§5.4.2).
//
// The term check is essential. An entry from an earlier term can be on a
// majority and still be overwritten by a later leader that never received
// it (Figure 8). Once an entry from the current term is on a majority, Log
// Matching means every earlier entry is too, and the election restriction
// means no future leader can lack them, so they all commit together.
func (r *Raft) maybeCommit() {
	matches := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		matches = append(matches, r.match[p])
	}
	slices.Sort(matches)
	// With n nodes, the entry at matches[(n-1)/2] is stored on at least
	// n/2+1 of them.
	n := matches[(len(matches)-1)/2]
	if n <= r.commit {
		return
	}
	if t, _ := r.termAt(n); t != r.term {
		return
	}
	r.commit = n
}

// appendAsLeader adds e to the leader's own log, then sends it out.
func (r *Raft) appendAsLeader(e Entry) bool {
	if !r.saveEntries([]Entry{e}) {
		return false
	}
	// The leader counts toward the majority only for entries it has
	// durably stored, which saveEntries just did.
	r.match[r.id] = e.Index
	r.maybeCommit()
	r.broadcastAppend()
	return true
}

func (r *Raft) sendAppend(to uint64) {
	prev := r.next[to] - 1
	prevTerm, _ := r.termAt(prev)
	end := min(r.lastIndex(), prev+uint64(r.maxMsgEntries))
	r.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		// A copy, so the message never aliases r.log, whose backing array
		// is overwritten in place if this node later truncates as a
		// follower.
		Entries:      slices.Clone(r.log[prev:end]),
		LeaderCommit: r.commit,
	})
}

func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

// becomeFollower moves to term as a follower. A higher term clears the
// vote. Returns false if persisting failed.
func (r *Raft) becomeFollower(term, lead uint64) bool {
	vote := r.vote
	if term > r.term {
		vote = None
	}
	if !r.persist(term, vote) {
		return false
	}
	if r.state == Leader {
		// A leader runs no election timer, so start a fresh one.
		r.resetElectionTimer()
	}
	r.state = Follower
	r.lead = lead
	r.votes = nil
	r.next, r.match = nil, nil
	return true
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.lead = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	r.next = make(map[uint64]uint64, len(r.peers))
	r.match = make(map[uint64]uint64, len(r.peers))
	for _, p := range r.peers {
		r.next[p] = r.lastIndex() + 1
	}
	r.match[r.id] = r.lastIndex()
	// §8: start the term with a no-op entry. Entries from earlier terms can
	// only commit once an entry from this term does (see maybeCommit), so
	// without it they could wait indefinitely for the next client command.
	// Sending it also announces leadership at once.
	r.appendAsLeader(Entry{Index: r.lastIndex() + 1, Term: r.term})
}

// wonElection reports whether granted votes form a majority of the cluster.
func (r *Raft) wonElection() bool {
	granted := 0
	for _, g := range r.votes {
		if g {
			granted++
		}
	}
	return granted >= len(r.peers)/2+1
}

// persist durably saves term and vote, then updates them in memory. Every
// change to term or vote goes through here, before any message that depends
// on it is sent. On failure the node stops.
func (r *Raft) persist(term, vote uint64) bool {
	if term == r.term && vote == r.vote {
		return true
	}
	if err := r.storage.Save(&HardState{Term: term, Vote: vote}, nil); err != nil {
		r.err = fmt.Errorf("raft: node %d stopped: saving term %d vote %d: %w", r.id, term, vote, err)
		return false
	}
	r.term, r.vote = term, vote
	return true
}

func (r *Raft) send(m Message) {
	m.From = r.id
	m.Term = r.term
	r.transport.Send(m)
}

// resetElectionTimer restarts the election countdown with a new random
// timeout. Re-randomizing on every reset is what makes repeated split votes
// unlikely (§5.2).
func (r *Raft) resetElectionTimer() {
	r.electionElapsed = 0
	r.randomizedElectionTimeout = r.electionTick + r.rng.IntN(r.electionTick)
}

// isUpToDate reports whether a log ending at (term, index) is at least as up
// to date as one ending at (myTerm, myIndex): the later last term wins, and
// with equal last terms the longer log wins (§5.4.1).
func isUpToDate(term, index, myTerm, myIndex uint64) bool {
	if term != myTerm {
		return term > myTerm
	}
	return index >= myIndex
}
