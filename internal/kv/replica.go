package kv

import (
	"errors"
	"fmt"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// Errors a request can fail with before it is applied.
var (
	// ErrNotLeader means the request was not accepted: this node is not the
	// leader. Result.Leader names the leader, if known.
	ErrNotLeader = errors.New("kv: not the leader")
	// ErrLeadershipLost means this node stopped being leader while the
	// request was in progress. A write may or may not have been committed;
	// retrying it with the same sequence number is safe either way.
	ErrLeadershipLost = errors.New("kv: leadership lost; outcome unknown")
	// ErrTimeout means the request did not complete within the request
	// timeout. As with ErrLeadershipLost, a write's outcome is unknown.
	ErrTimeout = errors.New("kv: timed out; outcome unknown")
)

// Request is a client request.
type Request struct {
	Op    Op
	Key   string
	Value []byte // OpPut only
	// ClientID and Seq identify a write for duplicate detection (see
	// Command). Reads change nothing and need neither. OpRegister, which
	// returns a new client ID in Result.ClientID, takes no fields.
	ClientID uint64
	Seq      uint64
}

// DefaultMaxSessions is the default for Config.MaxSessions.
const DefaultMaxSessions = 10000

// Replica connects client requests to one Raft node and its state machine.
// Like raft.Raft it is single-threaded and driven from outside: Tick at a
// regular interval, Step for each message received, and Submit for each
// client request. It is not safe for concurrent use.
//
// Every node applies committed entries to its own state machine, but only
// the leader accepts requests. Writes go through the log. Reads use ReadIndex
// (§8), batched: all reads that arrive while one ReadIndex round is in flight
// share the next round, so the cost of confirming leadership is paid once
// per batch rather than once per read.
type Replica struct {
	raft          *raft.Raft
	sm            *StateMachine
	timeout       int // ticks
	snapshotEvery uint64
	maxSessions   uint64
	now           int // ticks since creation

	// term is the term in which this node accepted the pending requests as
	// leader, or 0 if it is not leader.
	term uint64

	writes  []pendingWrite // proposed, in log order
	queued  []pendingRead  // waiting for the next ReadIndex round
	batch   []pendingRead  // waiting for the round in flight
	batchID uint64         // ID of the round in flight, 0 if none
	lastID  uint64
}

type pendingWrite struct {
	index, term uint64
	deadline    int
	done        func(Result)
}

type pendingRead struct {
	key      string
	deadline int
	done     func(Result)
}

func (w pendingWrite) expiry() (int, func(Result)) { return w.deadline, w.done }
func (r pendingRead) expiry() (int, func(Result))  { return r.deadline, r.done }

// Config configures a Replica.
type Config struct {
	// RequestTimeout is how many ticks a request may wait before it fails
	// with ErrTimeout. It must be positive.
	RequestTimeout int
	// SnapshotEvery, if positive, makes the Replica snapshot the state
	// machine and compact the Raft log whenever that many entries have been
	// applied since the last snapshot (§7). Zero disables compaction.
	SnapshotEvery uint64
	// MaxSessions is how many client sessions the state machine keeps
	// (see OpRegister). Zero means DefaultMaxSessions. The leader writes it
	// into each registration, so a change takes effect when a node with
	// the new value leads.
	MaxSessions uint64
}

// NewReplica returns a Replica driving r, which must be newly created: the
// state machine starts empty and is rebuilt from r's snapshot, if it has
// one, and the entries r commits after it.
func NewReplica(r *raft.Raft, cfg Config) *Replica {
	if cfg.RequestTimeout <= 0 {
		panic("kv: RequestTimeout must be positive")
	}
	if cfg.MaxSessions == 0 {
		cfg.MaxSessions = DefaultMaxSessions
	}
	return &Replica{
		raft: r, sm: NewStateMachine(),
		timeout: cfg.RequestTimeout, snapshotEvery: cfg.SnapshotEvery, maxSessions: cfg.MaxSessions,
	}
}

// Raft returns the Raft node, for status and tests.
func (p *Replica) Raft() *raft.Raft { return p.raft }

// Applied returns the index of the last entry applied to the state machine.
func (p *Replica) Applied() uint64 { return p.sm.Applied() }

// Sessions returns how many client sessions the state machine holds.
func (p *Replica) Sessions() int { return len(p.sm.sessions) }

// Submit starts a request. done is called exactly once with the outcome,
// either before Submit returns or during a later call to Submit,
// SubmitBatch, Tick, or Step. done must not call back into the Replica.
//
// A non-nil error is fatal, as for raft.Raft: storage or the state machine
// failed and the node has stopped. Pending requests are then abandoned.
func (p *Replica) Submit(req Request, done func(Result)) error {
	return p.SubmitBatch([]Submission{{Req: req, Done: done}})
}

// Submission is a request and the function to call with its outcome.
type Submission struct {
	Req  Request
	Done func(Result)
}

// MaxBatch is the most writes SubmitBatch proposes together. They become
// one storage write: at MaxValueSize each, 32 MiB, well under the WAL's
// limit on one write. It is also as many entries as one AppendEntries
// request carries (raft.DefaultMaxEntriesPerMessage).
const MaxBatch = 64

// SubmitBatch starts several requests, as Submit does each of them. The
// writes among them are proposed together, up to MaxBatch at a time, so the
// leader persists them in one storage write and one fsync instead of one
// each (group commit). They are given log entries in the order of subs.
func (p *Replica) SubmitBatch(subs []Submission) error {
	p.checkLeadership()
	var cmds [][]byte
	var dones []func(Result)
	for _, s := range subs {
		req, done := s.Req, s.Done
		switch req.Op {
		case OpGet:
			if err := validateKV(req.Key, nil); err != nil {
				done(Result{Err: err})
				continue
			}
			if p.term == 0 {
				done(p.notLeader())
				continue
			}
			p.queued = append(p.queued, pendingRead{key: req.Key, deadline: p.now + p.timeout, done: done})
		case OpPut, OpDelete, OpRegister:
			c := Command{Op: req.Op, ClientID: req.ClientID, Seq: req.Seq, Key: req.Key, Value: req.Value}
			if req.Op == OpRegister {
				c.MaxSessions = p.maxSessions
			}
			if err := c.validate(); err != nil {
				done(Result{Err: err})
				continue
			}
			cmds, dones = append(cmds, c.Encode()), append(dones, done)
		default:
			done(Result{Err: fmt.Errorf("%w: unknown op %v", ErrInvalid, req.Op)})
		}
	}
	for len(cmds) > 0 {
		n := min(len(cmds), MaxBatch)
		index, term, err := p.raft.Propose(cmds[:n]...)
		switch {
		case errors.Is(err, raft.ErrNotLeader):
			for _, done := range dones[:n] {
				done(p.notLeader())
			}
		case err != nil:
			return err
		default:
			for i, done := range dones[:n] {
				p.writes = append(p.writes, pendingWrite{index: index + uint64(i), term: term, deadline: p.now + p.timeout, done: done})
			}
		}
		cmds, dones = cmds[n:], dones[n:]
	}
	return p.update()
}

// Tick advances time by one tick, for both Raft and request timeouts.
func (p *Replica) Tick() error {
	p.now++
	if err := p.raft.Tick(); err != nil {
		return err
	}
	p.expire()
	return p.update()
}

// Step processes a message from another node.
func (p *Replica) Step(m raft.Message) error {
	if err := p.raft.Step(m); err != nil {
		return err
	}
	return p.update()
}

func (p *Replica) notLeader() Result {
	return Result{Err: ErrNotLeader, Leader: p.raft.Status().Lead}
}

// update brings the Replica up to date with Raft after any call into it.
func (p *Replica) update() error {
	p.checkLeadership()
	for {
		if err := p.applyCommitted(); err != nil {
			return err
		}
		if err := p.finishReads(); err != nil {
			return err
		}
		started, err := p.startReads()
		if err != nil || !started {
			return err
		}
		// A new round can resolve at once (in a single-node cluster), so
		// go around again.
	}
}

// checkLeadership fails every pending request if this node is no longer
// the leader of the term in which it accepted them. A write may still
// commit under the next leader, but this node can no longer tell, so the
// client must retry; duplicate detection makes that safe.
func (p *Replica) checkLeadership() {
	var term uint64
	if st := p.raft.Status(); st.State == raft.Leader {
		term = st.Term
	}
	if term == p.term {
		return
	}
	p.term = term
	lost := Result{Err: ErrLeadershipLost}
	for _, w := range p.writes {
		w.done(lost)
	}
	for _, r := range p.batch {
		r.done(lost)
	}
	for _, r := range p.queued {
		r.done(lost)
	}
	p.writes, p.batch, p.queued, p.batchID = nil, nil, nil, 0
}

func (p *Replica) applyCommitted() error {
	if snap := p.raft.TakeSnapshot(); snap != nil {
		// Only a follower installs a snapshot, or a node at startup, and
		// neither has requests pending: checkLeadership failed them when
		// this node stopped leading.
		if len(p.writes) > 0 {
			return fmt.Errorf("kv: restoring a snapshot at %d with %d writes pending", snap.Index, len(p.writes))
		}
		if err := p.sm.Restore(*snap); err != nil {
			return err
		}
	}
	for _, e := range p.raft.TakeCommitted() {
		res, err := p.sm.Apply(e)
		if err != nil {
			return err
		}
		// A write is done when the entry at its index is applied. The index
		// alone would not prove that the entry is the one proposed: after a
		// leadership change, a newer leader may commit a different entry at
		// the same index (§5.3). That case cannot reach here, because
		// checkLeadership fails every pending write as soon as the term
		// changes, and a leader never overwrites its own entries. The term
		// check stops the node if that reasoning is ever wrong, rather than
		// report someone else's entry as this write's success.
		if len(p.writes) > 0 && p.writes[0].index == e.Index {
			w := p.writes[0]
			p.writes = p.writes[1:]
			if w.term != e.Term {
				return fmt.Errorf("kv: write proposed at index %d in term %d, but the entry applied there is from term %d", w.index, w.term, e.Term)
			}
			w.done(res)
		}
	}
	return p.maybeCompact()
}

// maybeCompact snapshots the state machine and compacts the log once enough
// entries have been applied since the last snapshot. Serializing the whole
// state here, on the only goroutine that runs the Replica, pauses request
// processing for a time that grows with the size of the state.
func (p *Replica) maybeCompact() error {
	if p.snapshotEvery == 0 {
		return nil
	}
	applied := p.sm.Applied()
	if applied < p.raft.Status().Snapshot+p.snapshotEvery {
		return nil
	}
	return p.raft.Compact(applied, p.sm.Snapshot())
}

func (p *Replica) finishReads() error {
	for _, rs := range p.raft.TakeReadStates() {
		if rs.ID != p.batchID {
			continue // a round from before leadership changed; already failed
		}
		batch := p.batch
		p.batch, p.batchID = nil, 0
		if rs.Err != nil {
			for _, r := range batch {
				r.done(Result{Err: ErrLeadershipLost})
			}
			continue
		}
		// The read index never exceeds the commit index, and every
		// committed entry was applied just before this, so the state
		// machine has already caught up to it.
		if p.sm.Applied() < rs.Index {
			return fmt.Errorf("kv: read index %d is past applied index %d", rs.Index, p.sm.Applied())
		}
		for _, r := range batch {
			v, ok := p.sm.Get(r.key)
			r.done(Result{Value: v, Found: ok})
		}
	}
	return nil
}

// startReads starts a ReadIndex round for the queued reads, unless one is
// already in flight. Returns true if it started one.
func (p *Replica) startReads() (bool, error) {
	if p.batchID != 0 || len(p.queued) == 0 || p.term == 0 {
		return false, nil
	}
	p.lastID++
	if err := p.raft.ReadIndex(p.lastID); err != nil {
		return false, err // not ErrNotLeader: checkLeadership just ran
	}
	p.batch, p.queued, p.batchID = p.queued, nil, p.lastID
	return true, nil
}

// expire fails requests whose deadline has passed. A write that times out
// may still commit later; the client cannot know and must retry.
func (p *Replica) expire() {
	p.writes = expired(p.writes, p.now)
	p.queued = expired(p.queued, p.now)
	p.batch = expired(p.batch, p.now)
}

// expired fails the requests whose deadline is at or before now, and
// returns the rest in their original order.
func expired[T interface{ expiry() (int, func(Result)) }](s []T, now int) []T {
	kept := s[:0]
	for _, x := range s {
		if deadline, done := x.expiry(); deadline <= now {
			done(Result{Err: ErrTimeout})
		} else {
			kept = append(kept, x)
		}
	}
	return kept
}
