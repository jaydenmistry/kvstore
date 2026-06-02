// Package raft implements the Raft consensus algorithm (Ongaro and
// Ousterhout, "In Search of an Understandable Consensus Algorithm").
// Section numbers in comments refer to that paper.
//
// A Raft value is a single-threaded state machine. The caller drives it with
// two inputs: Tick, called at a regular interval, and Step, called for each
// message received from another node. Raft persists state through Storage
// and sends messages through Transport synchronously from inside those
// calls, always persisting before it sends anything that depends on the
// persisted state. Raft never reads a clock and draws all randomness from
// Config.Rand, so tests can run it deterministically.
package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
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

	// Persistent state (Figure 2). These fields change only after Storage
	// has durably saved the new values; see persist.
	term uint64
	vote uint64

	state State
	lead  uint64 // leader of the current term, if known
	// votes records the vote responses a candidate has received in the
	// current term. It is a set keyed by node, so a duplicated response is
	// counted once.
	votes map[uint64]bool

	electionTick              int
	heartbeatTick             int
	randomizedElectionTimeout int
	electionElapsed           int // follower and candidate only
	heartbeatElapsed          int // leader only

	// err is set when Storage fails. Raft then stops: every later call
	// returns err and nothing more is sent. Continuing after a failed save
	// could mean replying based on state that was never persisted.
	err error
}

// New creates a node, restoring its term and vote from cfg.Storage. A
// restarted node always begins as a follower.
func New(cfg Config) (*Raft, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	hs, err := cfg.Storage.HardState()
	if err != nil {
		return nil, fmt.Errorf("raft: loading hard state: %w", err)
	}
	rng := cfg.Rand
	if rng == nil {
		rng = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	r := &Raft{
		id:            cfg.ID,
		peers:         slices.Sorted(slices.Values(cfg.Peers)),
		storage:       cfg.Storage,
		transport:     cfg.Transport,
		rng:           rng,
		term:          hs.Term,
		vote:          hs.Vote,
		state:         Follower,
		electionTick:  cfg.ElectionTick,
		heartbeatTick: cfg.HeartbeatTick,
	}
	r.resetElectionTimer()
	return r, nil
}

// Status returns a snapshot of the node's state.
func (r *Raft) Status() Status {
	return Status{ID: r.id, Term: r.term, Vote: r.vote, Lead: r.lead, State: r.state}
}

// Tick advances the node's logical clock by one tick. A follower or
// candidate whose election timeout elapses starts an election; a leader
// sends heartbeats every HeartbeatTick ticks.
func (r *Raft) Tick() error {
	if r.err != nil {
		return r.err
	}
	switch r.state {
	case Leader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTick {
			r.heartbeatElapsed = 0
			r.broadcastHeartbeat()
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
		// Heartbeat responses carry nothing a leader needs until log
		// replication. A response with a higher term was handled above.
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
				LastLogIndex: r.lastLogIndex(),
				LastLogTerm:  r.lastLogTerm(),
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
	// as ours.
	upToDate := isUpToDate(m.LastLogTerm, m.LastLogIndex, r.lastLogTerm(), r.lastLogIndex())
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
	// Until log replication exists, every log is empty and the consistency
	// check trivially passes.
	r.send(Message{Type: MsgAppResp, To: m.From, Success: true})
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
	return true
}

func (r *Raft) becomeLeader() {
	r.state = Leader
	r.lead = r.id
	r.votes = nil
	r.heartbeatElapsed = 0
	// Announce leadership immediately so other candidates in this term
	// step down and followers reset their timers.
	r.broadcastHeartbeat()
}

func (r *Raft) broadcastHeartbeat() {
	for _, p := range r.peers {
		if p != r.id {
			r.send(Message{Type: MsgApp, To: p})
		}
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
	if err := r.storage.SaveHardState(HardState{Term: term, Vote: vote}); err != nil {
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

// Until log replication exists, every log is empty.
func (r *Raft) lastLogIndex() uint64 { return 0 }
func (r *Raft) lastLogTerm() uint64  { return 0 }

// isUpToDate reports whether a log ending at (term, index) is at least as up
// to date as one ending at (myTerm, myIndex): the later last term wins, and
// with equal last terms the longer log wins (§5.4.1).
func isUpToDate(term, index, myTerm, myIndex uint64) bool {
	if term != myTerm {
		return term > myTerm
	}
	return index >= myIndex
}
