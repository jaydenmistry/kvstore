// Package raft implements the Raft consensus algorithm (Ongaro and
// Ousterhout, "In Search of an Understandable Consensus Algorithm").
// Section numbers in comments refer to that paper.
//
// A Raft value is a single-threaded state machine. The caller drives it with
// Tick, called at a regular interval, Step, called for each message received
// from another node, and Propose, called to add a command to the log on the
// leader. Committed entries come out of TakeCommitted, in log order.
// ReadIndex and TakeReadStates support linearizable reads that do not go
// through the log. Compact replaces applied entries with a snapshot of the
// application's state; a snapshot to restore, received from the leader or
// loaded at startup, comes out of TakeSnapshot.
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

// Defaults for Config fields left at zero.
const (
	DefaultMaxEntriesPerMessage = 64
	DefaultMaxBytesPerMessage   = 1 << 20
)

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
	// MaxBytesPerMessage caps the total Data size of the entries in one
	// AppendEntries request. A single larger entry is still sent, alone.
	// The follower saves a whole request in one Storage.Save, so this also
	// bounds the size of that write.
	MaxBytesPerMessage int

	// CheckQuorum makes a leader step down when it has not heard from a
	// majority of the cluster within ElectionTick ticks (thesis §6.2). It
	// does not affect safety, only how quickly clients of a leader cut off
	// in a minority learn to go elsewhere.
	CheckQuorum bool
	// PreVote makes a node whose election timeout elapses first ask
	// whether it could win, and start an election only if a majority says
	// yes (thesis §9.6). A node cut off from the cluster then keeps its
	// term, instead of raising it with every timeout and deposing a working
	// leader when it reconnects. With CheckQuorum also on, a node that has
	// heard from a leader within ElectionTick ticks says no.
	PreVote bool

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
	case c.MaxEntriesPerMessage < 0 || c.MaxBytesPerMessage < 0:
		return errors.New("raft: message limits must not be negative")
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
	log  logSuffix // see log.go
	// snapshot is the latest snapshot, kept to send to followers that need
	// entries it replaced.
	snapshot Snapshot

	// Volatile state (Figure 2). Not persisted: after a restart the commit
	// index is relearned from the leader and entries are applied again
	// from the start.
	commit  uint64
	applied uint64 // highest index returned by TakeCommitted or TakeSnapshot
	// pendingSnap is a snapshot the application must restore before it
	// applies anything more.
	pendingSnap *Snapshot

	state State
	lead  uint64 // leader of the current term, if known
	// votes records the vote responses a candidate, or pre-candidate, has
	// received in the current term. It is a set keyed by node, so a duplicated response is
	// counted once.
	votes map[uint64]bool

	// Leader only (Figure 2): for each node, the next index to send and the
	// highest index known to be replicated there. match includes the leader
	// itself, updated whenever it appends to its own log.
	next  map[uint64]uint64
	match map[uint64]uint64
	// probing holds the followers whose logs the leader has not yet
	// matched: see sendAppend.
	probing map[uint64]bool

	// Leader only, for ReadIndex: the current heartbeat round, the highest
	// round each peer has echoed back, and reads waiting for confirmation,
	// in the order they were requested.
	round uint64
	acked map[uint64]uint64
	reads []pendingRead
	// readStates holds outcomes not yet returned by TakeReadStates.
	readStates []ReadState
	// active records, for CheckQuorum, the peers a leader has heard from in
	// the current check window.
	active map[uint64]bool

	electionTick              int
	heartbeatTick             int
	maxMsgEntries             int
	maxMsgBytes               int
	checkQuorum               bool
	preVote                   bool
	randomizedElectionTimeout int
	// electionElapsed counts ticks toward the election timeout on a
	// follower or candidate, and toward the next CheckQuorum on a leader.
	electionElapsed  int
	heartbeatElapsed int // leader only

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
	hs, snap, ents, err := cfg.Storage.Load()
	if err != nil {
		return nil, fmt.Errorf("raft: loading state: %w", err)
	}
	if snap.Term > hs.Term {
		return nil, fmt.Errorf("raft: stored snapshot has term %d, after current term %d", snap.Term, hs.Term)
	}
	for i, e := range ents {
		if want := snap.Index + uint64(i) + 1; e.Index != want {
			return nil, fmt.Errorf("raft: stored entry %d has index %d", want, e.Index)
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
	maxBytes := cfg.MaxBytesPerMessage
	if maxBytes == 0 {
		maxBytes = DefaultMaxBytesPerMessage
	}
	r := &Raft{
		id:        cfg.ID,
		peers:     slices.Sorted(slices.Values(cfg.Peers)),
		storage:   cfg.Storage,
		transport: cfg.Transport,
		rng:       rng,
		term:      hs.Term,
		vote:      hs.Vote,
		log:       logSuffix{index: snap.Index, term: snap.Term, ents: ents},
		snapshot:  snap,
		// Everything in a snapshot was committed, so the commit index
		// starts there rather than at 0.
		commit:        snap.Index,
		applied:       snap.Index,
		state:         Follower,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
		maxMsgEntries: maxEntries,
		maxMsgBytes:   maxBytes,
		checkQuorum:   cfg.CheckQuorum,
		preVote:       cfg.PreVote,
	}
	if snap.Index > 0 {
		r.pendingSnap = &snap
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
		Snapshot:  r.log.index,
	}
}

// Propose appends commands to the log, one entry each, in order. It
// succeeds only on the leader, and returns the index of the first entry and
// the term they were all given. A successful Propose does not mean the
// entries will commit: if leadership changes first, a later leader may
// overwrite them. The caller learns the outcome by watching TakeCommitted
// for an entry at each index with that term.
//
// The entries are persisted in one storage write, so proposing many at once
// costs one fsync instead of one each (group commit). That write is
// crash-atomic, which is safe: none of the entries has been acknowledged
// before it completes. Keep batches small enough for one storage write.
//
// Each command must be nonempty (empty entries are the leader's no-ops).
// Raft keeps its own copies.
func (r *Raft) Propose(data ...[]byte) (index, term uint64, err error) {
	if r.err != nil {
		return 0, 0, r.err
	}
	if r.state != Leader {
		return 0, 0, ErrNotLeader
	}
	if len(data) == 0 {
		return 0, 0, errors.New("raft: nothing to propose")
	}
	ents := make([]Entry, len(data))
	for i, d := range data {
		if len(d) == 0 {
			return 0, 0, errors.New("raft: empty proposal")
		}
		ents[i] = Entry{Index: r.lastIndex() + 1 + uint64(i), Term: r.term, Data: bytes.Clone(d)}
	}
	if !r.appendAsLeader(ents) {
		return 0, 0, r.err
	}
	r.broadcastEntries()
	return ents[0].Index, r.term, nil
}

// TakeCommitted returns the entries committed since the previous call, in
// log order, and marks them applied. The caller must apply them in exactly
// this order. After a restart, entries are returned again from just after
// the snapshot, as the node relearns the commit index.
//
// If TakeSnapshot has a snapshot pending, it must be called first:
// TakeCommitted panics rather than hand out entries that follow a snapshot
// the application has not restored.
func (r *Raft) TakeCommitted() []Entry {
	if r.pendingSnap != nil {
		panic("raft: TakeCommitted called with a snapshot pending; call TakeSnapshot first")
	}
	if r.commit <= r.applied {
		return nil
	}
	ents := r.log.slice(r.applied+1, r.commit+1)
	r.applied = r.commit
	return ents
}

// TakeSnapshot returns a snapshot the application must restore, replacing
// its whole state, or nil if there is none. There is one after a restart
// from a snapshot and after a follower installs one from the leader.
// Entries from TakeCommitted then continue from just after it.
func (r *Raft) TakeSnapshot() *Snapshot {
	s := r.pendingSnap
	r.pendingSnap = nil
	return s
}

// Compact replaces the log up to and including index with a snapshot whose
// data is the application's state after applying index (§7). index must
// have been applied (returned by TakeCommitted), so the snapshot holds only
// committed state, and must be past the current snapshot. Raft keeps its
// own copy of data.
//
// Followers that still need entries before index are sent the snapshot
// instead (InstallSnapshot).
func (r *Raft) Compact(index uint64, data []byte) error {
	if r.err != nil {
		return r.err
	}
	if index <= r.log.index || index > r.applied {
		return fmt.Errorf("raft: cannot compact through %d: snapshot is at %d and applied at %d", index, r.log.index, r.applied)
	}
	term, _ := r.termAt(index)
	s := Snapshot{Index: index, Term: term, Data: bytes.Clone(data)}
	if err := r.storage.SaveSnapshot(s); err != nil {
		r.err = fmt.Errorf("raft: node %d stopped: saving snapshot at %d: %w", r.id, index, err)
		return r.err
	}
	r.log.compact(index, term)
	r.snapshot = s
	return nil
}

// ReadIndex starts a linearizable read on the leader (§8). The outcome is
// returned later by TakeReadStates as a ReadState carrying id. Reading the
// state machine is safe once it has applied every entry through the
// ReadState's Index; draining TakeCommitted first always achieves that.
//
// A leader cannot answer from its own state right away: it may have been
// deposed by a newer leader it has not heard from, which may already have
// committed writes it lacks. The read is confirmed once two things hold:
//
//  1. The leader has committed an entry from its current term (its no-op).
//     Before that its commit index may lag the cluster's, because entries
//     from earlier terms cannot be counted as committed (§5.4.2).
//  2. A majority has answered a heartbeat sent after the read was
//     requested. A newer leader needs a majority's votes, and each of those
//     voters had not yet left this term when it answered us, so the newer
//     leader was elected after the read began. Writes it commits are
//     concurrent with the read and may be left out. An answer to a
//     heartbeat sent before the read proves nothing, which is why the read
//     starts a new round.
func (r *Raft) ReadIndex(id uint64) error {
	if r.err != nil {
		return r.err
	}
	if r.state != Leader {
		return ErrNotLeader
	}
	rd := pendingRead{id: id}
	if r.committedCurrentTerm() {
		rd.index = r.commit
	}
	r.round++
	rd.round = r.round
	r.reads = append(r.reads, rd)
	r.broadcastAppend()
	r.advanceReads() // a single-node cluster confirms at once
	return r.err
}

// TakeReadStates returns the outcomes of ReadIndex requests resolved since
// the previous call.
func (r *Raft) TakeReadStates() []ReadState {
	rs := r.readStates
	r.readStates = nil
	return rs
}

// Tick advances the node's logical clock by one tick. A follower or
// candidate whose election timeout elapses starts an election; a leader
// sends AppendEntries, carrying any entries a follower is missing, every
// HeartbeatTick ticks, and with CheckQuorum checks every ElectionTick ticks
// that it can still reach a majority.
func (r *Raft) Tick() error {
	if r.err != nil {
		return r.err
	}
	switch r.state {
	case Leader:
		if r.checkQuorum {
			r.electionElapsed++
			if r.electionElapsed >= r.electionTick {
				r.electionElapsed = 0
				if !r.quorumActive() {
					// Stay in this term: stepping down is not evidence of
					// a newer leader. Our own election timer then runs,
					// and we campaign if no leader contacts us.
					r.becomeFollower(r.term, None)
					return r.err
				}
			}
		}
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTick {
			r.heartbeatElapsed = 0
			r.broadcastAppend()
		}
	case Follower, Candidate, PreCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			if r.preVote {
				r.preCampaign()
			} else {
				r.campaign()
			}
		}
	}
	return r.err
}

// Step processes messages from other nodes, in order. Messages not
// addressed to this node, or from a node outside the cluster, are ignored.
//
// A caller that has several messages waiting should pass them together:
// AppendEntries requests that continue one another are then merged, so a
// follower saves their entries in one storage write (see mergeAppend).
//
// If Step returns an error, Storage failed and the node has stopped. The
// caller should treat this as fatal.
func (r *Raft) Step(msgs ...Message) error {
	for i := 0; i < len(msgs) && r.err == nil; i++ {
		m := msgs[i]
		for i+1 < len(msgs) && r.mergeAppend(&m, msgs[i+1]) {
			i++
		}
		r.step(m)
	}
	return r.err
}

// mergeAppend merges next into m if both are AppendEntries requests from
// the same leader and term, and next carries the entries that follow m's
// last one (next's PrevLogIndex and PrevLogTerm are m's last entry). The
// merged request holds m's entries then next's, with the later commit index
// and heartbeat round.
//
// It is a request the leader could have sent itself: a leader never
// removes entries from its own log, so m's entries and next's are one
// contiguous run of it, and both commit indexes were committed when sent.
// The follower's consistency check (§5.3) applies to it as to any other.
// Handling it leaves the log as handling m and then next would, with one
// storage write and one response for both. If the check fails, the leader
// receives one rejection instead of two, as if next's had been lost. The
// commit index can end up higher than handling them in turn would leave
// it, if m was the later one sent: it is still a committed index, and no
// higher than the last entry known to match the leader's log.
//
// The merged entries stay within MaxBytesPerMessage, which bounds the
// follower's write as it does for a single request.
func (r *Raft) mergeAppend(m *Message, next Message) bool {
	if m.Type != MsgApp || next.Type != MsgApp || m.From != next.From || m.Term != next.Term || m.To != next.To {
		return false
	}
	lastIndex, lastTerm := m.PrevLogIndex, m.PrevLogTerm
	if n := len(m.Entries); n > 0 {
		lastIndex, lastTerm = m.Entries[n-1].Index, m.Entries[n-1].Term
	}
	if next.PrevLogIndex != lastIndex || next.PrevLogTerm != lastTerm {
		return false
	}
	if dataSize(m.Entries)+dataSize(next.Entries) > r.maxMsgBytes {
		return false
	}
	// A new slice: the caller's messages are left as they were.
	m.Entries = slices.Concat(m.Entries, next.Entries)
	m.LeaderCommit = max(m.LeaderCommit, next.LeaderCommit)
	m.Round = max(m.Round, next.Round)
	return true
}

func dataSize(ents []Entry) int {
	n := 0
	for _, e := range ents {
		n += len(e.Data)
	}
	return n
}

// step processes one message.
func (r *Raft) step(m Message) {
	if m.To != r.id || m.From == r.id || !slices.Contains(r.peers, m.From) {
		return
	}

	// A pre-vote request, and a grant, carry a term the sender has not
	// entered: the one it would campaign in. Adopting it would raise our
	// term with nobody campaigning, the very disruption PreVote prevents,
	// so these skip the term rules below. A refusal carries the voter's
	// own term and goes through them, so a pre-candidate that has fallen
	// behind learns the current term.
	if m.Type == MsgPreVote {
		r.handlePreVote(m)
		return
	}
	if m.Type == MsgPreVoteResp && m.VoteGranted {
		r.handlePreVoteResp(m)
		return
	}

	switch {
	case m.Term > r.term:
		// §5.1: a higher term in any message, request or response, means
		// our term is stale. Adopt it and become a follower before acting
		// on the message. A MsgApp or MsgSnap in that term comes from its
		// leader.
		lead := None
		if m.Type == MsgApp || m.Type == MsgSnap {
			lead = m.From
		}
		if !r.becomeFollower(m.Term, lead) {
			return
		}
	case m.Term < r.term:
		// The sender is behind. Answer requests with our term so a deposed
		// leader or a stale candidate learns of it and steps down (§5.1).
		// Responses from an old term are ignored: a vote granted in an
		// earlier term must not count toward this one.
		switch m.Type {
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From})
		case MsgApp, MsgSnap:
			r.send(Message{Type: MsgAppResp, To: m.From})
		}
		return
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
	case MsgSnap:
		r.handleSnapshot(m)
	case MsgPreVoteResp:
		r.handlePreVoteResp(m)
	}
}

// preCampaign asks every peer whether it would vote for us in the next term
// (PreVote, thesis §9.6). Nothing is persisted: our term and vote do not
// change. If a majority says yes, we start a real election with campaign.
func (r *Raft) preCampaign() {
	r.state = PreCandidate
	r.lead = None
	r.votes = map[uint64]bool{r.id: true}
	r.resetElectionTimer()
	if r.wonElection() {
		r.campaign() // single-node cluster
		return
	}
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{
				Type:         MsgPreVote,
				To:           p,
				Term:         r.term + 1,
				LastLogIndex: r.lastIndex(),
				LastLogTerm:  r.lastTerm(),
			})
		}
	}
}

// handlePreVote answers whether we would vote for the sender in m.Term. It
// changes nothing here: the answer is a prediction, and a real MsgVote is
// still decided by handleVote.
func (r *Raft) handlePreVote(m Message) {
	// A node already in m.Term or later has moved past the term the sender
	// would campaign in. It says no, with its own term, so the sender
	// catches up.
	grant := m.Term > r.term &&
		!r.inLeaderLease() &&
		isUpToDate(m.LastLogTerm, m.LastLogIndex, r.lastTerm(), r.lastIndex())
	if !grant {
		r.send(Message{Type: MsgPreVoteResp, To: m.From})
		return
	}
	r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term, VoteGranted: true})
}

// inLeaderLease reports whether, with CheckQuorum on, we are the leader or
// have heard from one within the last ElectionTick ticks. Such a node
// refuses pre-votes (leader stickiness, thesis §4.2.3): the leader looks
// alive, so a node that wants an election is the one cut off.
//
// Without CheckQuorum this could keep a cluster without a usable leader
// forever. Say the leader's messages reach follower A but nothing from A
// reaches it, and the leader is cut off from B entirely. The leader cannot
// commit, A keeps hearing from it and refuses B, and B can never win. With
// CheckQuorum, the leader steps down within two ElectionTick windows and A
// stops hearing from it.
func (r *Raft) inLeaderLease() bool {
	return r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTick
}

func (r *Raft) handlePreVoteResp(m Message) {
	if r.state != PreCandidate {
		return
	}
	// A grant for any other term answers an earlier pre-campaign, made
	// from an older term.
	if m.VoteGranted && m.Term != r.term+1 {
		return
	}
	r.votes[m.From] = m.VoteGranted
	if r.wonElection() {
		r.campaign()
	}
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

// followLeader records that m, a MsgApp or MsgSnap in the current term,
// comes from this term's leader. Returns false if persisting failed.
func (r *Raft) followLeader(m Message) bool {
	switch r.state {
	case Leader:
		// Two leaders in one term would mean two majorities voted for
		// different candidates, which the vote rules make impossible. If it
		// happens, a safety invariant is already broken.
		panic(fmt.Sprintf("raft: node %d is leader in term %d but received %v from %d", r.id, r.term, m.Type, m.From))
	case Candidate, PreCandidate:
		// §5.2: another node won this term's election, or, for a
		// pre-candidate, the leader we thought was gone is still there.
		if !r.becomeFollower(r.term, m.From) {
			return false
		}
	}
	r.lead = m.From
	r.resetElectionTimer()
	return true
}

// handleAppend is the receiver side of AppendEntries (Figure 2).
func (r *Raft) handleAppend(m Message) {
	if !r.followLeader(m) {
		return
	}

	// Entries up to our snapshot are committed, so by State Machine Safety
	// they match the leader's; only what follows needs checking. A delayed
	// request can start inside the snapshot.
	prev, prevTerm, ents := m.PrevLogIndex, m.PrevLogTerm, m.Entries
	if prev < r.log.index {
		for len(ents) > 0 && ents[0].Index <= r.log.index {
			ents = ents[1:]
		}
		prev, prevTerm = r.log.index, r.log.term
	}

	// Consistency check (§5.3): we must already have the entry just before
	// the new ones, with the same term. Log Matching then guarantees that
	// everything up to it matches the leader too.
	if prev > r.lastIndex() {
		r.send(Message{Type: MsgAppResp, To: m.From, RejectIndex: m.PrevLogIndex, HintIndex: r.lastIndex() + 1, Round: m.Round})
		return
	}
	if t, _ := r.termAt(prev); t != prevTerm {
		// Suggest retrying from the first entry of the conflicting term, so
		// the leader skips the whole term in one round trip (end of §5.3).
		// Committed entries cannot conflict, so the hint stays above commit.
		hint := prev
		for hint > r.commit+1 {
			if prev, _ := r.termAt(hint - 1); prev != t {
				break
			}
			hint--
		}
		r.send(Message{Type: MsgAppResp, To: m.From, RejectIndex: m.PrevLogIndex, HintIndex: hint, Round: m.Round})
		return
	}

	// Skip entries we already have. Only the first entry whose term
	// differs, and everything after it, is replaced (Figure 2, receiver
	// step 3). Truncating at PrevLogIndex+1 unconditionally would be wrong:
	// this message may be a delayed duplicate, and our log may already hold
	// later entries from this leader that we have acknowledged.
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
	// receiver step 5). Our snapshot, being committed, matches too.
	lastNew := max(m.PrevLogIndex+uint64(len(m.Entries)), r.log.index)
	if c := min(m.LeaderCommit, lastNew); c > r.commit {
		r.commit = c
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: lastNew, Round: m.Round})
}

// handleSnapshot is the receiver side of InstallSnapshot (Figure 13). The
// whole snapshot arrives in one message, so there are no offsets or done
// flag.
func (r *Raft) handleSnapshot(m Message) {
	if !r.followLeader(m) {
		return
	}
	s := m.Snapshot
	if s.Index == 0 || s.Term == 0 || s.Term > m.Term {
		return // malformed
	}
	if s.Index <= r.commit {
		// We already have everything it covers, committed. Installing it
		// would move the state machine backward. This happens when a
		// snapshot is delayed or duplicated.
		r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: s.Index, Round: m.Round})
		return
	}
	// Persisted before anything changes in memory, and before replying:
	// the leader counts our reply as replication of everything through
	// s.Index.
	if err := r.storage.SaveSnapshot(s); err != nil {
		r.err = fmt.Errorf("raft: node %d stopped: saving snapshot at %d: %w", r.id, s.Index, err)
		return
	}
	// Keep the entries after the snapshot only if our log holds its last
	// entry: they may be entries we acknowledged, which the leader may have
	// counted toward a commit. Otherwise none of our log can be trusted.
	r.log.compact(s.Index, s.Term)
	r.snapshot = s
	r.pendingSnap = &s // a copy, so later snapshots do not change it
	r.commit, r.applied = s.Index, s.Index
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true, MatchIndex: s.Index, Round: m.Round})
}

func (r *Raft) handleAppendResp(m Message) {
	if r.state != Leader {
		return
	}
	p := m.From
	// Any response in this term, success or rejection, shows the follower
	// accepted us as leader when it received that round.
	r.active[p] = true
	if m.Round > r.acked[p] {
		r.acked[p] = m.Round
		r.advanceReads()
	}
	if m.Success {
		// Responses can arrive out of order. matchIndex only moves forward,
		// so an old success cannot undo a newer one.
		if m.MatchIndex > r.match[p] {
			r.match[p] = m.MatchIndex
			r.maybeCommit()
		}
		if r.probing[p] {
			// The follower's log is now known to match up to match, so
			// replication resumes from there.
			r.probing[p] = false
			r.next[p] = r.match[p] + 1
		} else {
			r.next[p] = max(r.next[p], r.match[p]+1)
		}
		// Only entries not yet sent: a response is not a reason to send
		// again what is already on its way.
		if r.next[p] <= r.lastIndex() {
			r.sendAppend(p)
		}
		return
	}
	// A rejection counts only if it is newer than what we know. While
	// probing, it must answer the latest probe: an older one refers to a
	// PrevLogIndex we have already moved below. While replicating, several
	// messages may be in flight when one is lost, and each one after it is
	// rejected; the first rejection starts probing, and the rest then fail
	// this test.
	if r.probing[p] && m.RejectIndex != r.next[p]-1 ||
		m.RejectIndex <= r.match[p] || m.RejectIndex >= r.next[p] {
		return
	}
	// The hint is at most RejectIndex, which is below next, so next
	// strictly decreases and the search terminates. It never drops to or
	// below match: those entries are known to agree.
	r.next[p] = max(r.match[p]+1, min(m.HintIndex, m.RejectIndex))
	r.probing[p] = true
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
	r.advanceReads()
}

// quorumActive reports whether a majority, counting the leader, has
// answered since the last check, then starts a new window.
//
// This is not a lease. Followers on the majority side may elect a new
// leader after ElectionTick ticks without hearing from us, before we step
// down at the end of the window, so for a while two nodes act as leader in
// different terms. Only one of them can commit, and reads stay safe
// because ReadIndex still confirms each read with a heartbeat round. A lease
// would need bounds on clock drift between nodes (thesis §6.4.1).
func (r *Raft) quorumActive() bool {
	n := 1 + len(r.active) // active never holds ourselves
	clear(r.active)
	return n >= len(r.peers)/2+1
}

func (r *Raft) committedCurrentTerm() bool {
	t, _ := r.termAt(r.commit)
	return t == r.term
}

type pendingRead struct {
	id    uint64
	round uint64 // heartbeat round a majority must answer
	index uint64 // read index; 0 until an entry from this term commits
}

// advanceReads resolves the pending reads that are now confirmed (see
// ReadIndex).
func (r *Raft) advanceReads() {
	if len(r.reads) == 0 || !r.committedCurrentTerm() {
		return
	}
	// The highest round answered by a majority, counting ourselves as
	// having answered every round.
	rounds := make([]uint64, 0, len(r.peers))
	for _, p := range r.peers {
		if p == r.id {
			rounds = append(rounds, r.round)
		} else {
			rounds = append(rounds, r.acked[p])
		}
	}
	slices.Sort(rounds)
	confirmed := rounds[(len(rounds)-1)/2]
	// Rounds increase along r.reads, so the confirmed reads are a prefix.
	for len(r.reads) > 0 && r.reads[0].round <= confirmed {
		rd := r.reads[0]
		r.reads = r.reads[1:]
		if rd.index == 0 {
			// Requested before this term's first commit. The current commit
			// index is at least the commit index of the cluster when the
			// read arrived, and entries committed since are concurrent with
			// the read, so it is a valid read index.
			rd.index = r.commit
		}
		r.readStates = append(r.readStates, ReadState{ID: rd.id, Index: rd.index})
	}
}

// appendAsLeader adds ents, which follow the last entry, to the leader's
// own log in one storage write. The caller sends them out.
func (r *Raft) appendAsLeader(ents []Entry) bool {
	if !r.saveEntries(ents) {
		return false
	}
	// The leader counts toward the majority only for entries it has
	// durably stored, which saveEntries just did.
	r.match[r.id] = ents[len(ents)-1].Index
	r.maybeCommit()
	return true
}

// sendAppend sends to an AppendEntries request carrying the entries from
// next[to], up to the per-message limits. With none to carry, it is a
// heartbeat.
//
// next is only a guess at where the follower's log ends; the follower's
// consistency check (§5.3) is what keeps a wrong guess from doing harm, and
// next never drops below match+1, which is known to be right. How the guess
// moves depends on the follower's state:
//
//   - Probing: the leader has not matched the follower's log since it
//     was elected, or since a rejection or a snapshot. next stays put
//     until a response says where the log ends, and new entries are not
//     sent on their own (see broadcastEntries), so at most one message
//     per heartbeat or response is in flight. A success ends probing.
//   - Replicating: the follower's log matched at the last response, so the
//     leader sends entries as they come and moves next past them at once
//     (pipelining). Each entry is sent once rather than again with every
//     message until acknowledged. If one is lost, the follower rejects
//     the next message, and the follower is probed again.
func (r *Raft) sendAppend(to uint64) {
	prev := r.next[to] - 1
	if prev < r.log.index {
		r.sendSnapshot(to) // the entries it needs are gone
		return
	}
	prevTerm, _ := r.termAt(prev)
	end := min(r.lastIndex(), prev+uint64(r.maxMsgEntries))
	size := 0
	for i := prev + 1; i <= end; i++ {
		e, _ := r.log.entry(i)
		size += len(e.Data)
		if size > r.maxMsgBytes && i > prev+1 {
			end = i - 1
			break
		}
	}
	r.send(Message{
		Type:         MsgApp,
		To:           to,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		// A copy, so the message never aliases r.log, whose backing array
		// is overwritten in place if this node later truncates as a
		// follower.
		Entries:      r.log.slice(prev+1, end+1),
		LeaderCommit: r.commit,
		Round:        r.round,
	})
	if !r.probing[to] {
		r.next[to] = end + 1
	}
}

// sendSnapshot sends the latest snapshot to a follower that needs entries
// it replaced (§7).
//
// next moves past the snapshot at once, so the following heartbeats carry
// the entries after it rather than the snapshot again, and the follower is
// probed until it acknowledges the snapshot. If the snapshot is lost, the
// follower rejects those heartbeats, next falls back inside the snapshot,
// and it is sent again. Over a transport that keeps order, as TCP does, the
// snapshot arrives before the entries that follow it.
func (r *Raft) sendSnapshot(to uint64) {
	r.send(Message{Type: MsgSnap, To: to, Snapshot: r.snapshot, Round: r.round})
	r.next[to] = r.snapshot.Index + 1
	r.probing[to] = true
}

// broadcastAppend sends every follower an AppendEntries request, with any
// entries it is due: a heartbeat, which also starts a ReadIndex round.
func (r *Raft) broadcastAppend() {
	for _, p := range r.peers {
		if p != r.id {
			r.sendAppend(p)
		}
	}
}

// broadcastEntries sends newly appended entries to the followers being
// replicated to. Probed followers get them once a response shows where
// their logs end.
func (r *Raft) broadcastEntries() {
	for _, p := range r.peers {
		if p != r.id && !r.probing[p] {
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
		// Unconfirmed reads fail: we can no longer show that no newer
		// leader exists. The caller retries them on the new leader.
		for _, rd := range r.reads {
			r.readStates = append(r.readStates, ReadState{ID: rd.id, Err: ErrNotLeader})
		}
		r.reads, r.round = nil, 0
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
	r.probing = make(map[uint64]bool, len(r.peers))
	for _, p := range r.peers {
		r.next[p] = r.lastIndex() + 1
		r.probing[p] = p != r.id
	}
	r.match[r.id] = r.lastIndex()
	r.acked = make(map[uint64]uint64, len(r.peers))
	// A full window to hear from a majority, counting only this term.
	r.active = make(map[uint64]bool, len(r.peers))
	r.electionElapsed = 0
	// §8: start the term with a no-op entry. Entries from earlier terms can
	// only commit once an entry from this term does (see maybeCommit), so
	// without it they could wait indefinitely for the next client command.
	// Sending it also announces leadership at once.
	if r.appendAsLeader([]Entry{{Index: r.lastIndex() + 1, Term: r.term}}) {
		r.broadcastAppend()
	}
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

// send sends m from this node. m.Term defaults to our current term; only
// pre-vote messages set it explicitly.
func (r *Raft) send(m Message) {
	m.From = r.id
	if m.Term == 0 {
		m.Term = r.term
	}
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
