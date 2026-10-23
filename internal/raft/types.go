package raft

import "errors"

// None is the zero node ID. It means "no node": no vote cast, or no known
// leader. Real node IDs must be nonzero.
const None uint64 = 0

// ErrNotLeader is returned by Propose and ReadIndex on a node that is not the
// leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Entry is one log entry. Indexes start at 1 and are contiguous.
//
// An entry with empty Data is a no-op: a new leader appends one at the start
// of its term (§8) and the application should skip it. Data must not be
// modified after it is handed to Raft or received from it.
type Entry struct {
	Index uint64
	Term  uint64
	Data  []byte
}

// State is a node's role (§5.1).
type State uint8

const (
	Follower State = iota
	Candidate
	Leader
	// PreCandidate is a node asking whether it could win an election
	// before starting one (PreVote, thesis §9.6). It keeps its term.
	PreCandidate
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	case PreCandidate:
		return "pre-candidate"
	}
	return "unknown"
}

// MessageType identifies an RPC request or response from Figure 2 of the
// Raft paper.
type MessageType uint8

const (
	// MsgVote is a RequestVote request (§5.2).
	MsgVote MessageType = iota + 1
	// MsgVoteResp is the response to MsgVote.
	MsgVoteResp
	// MsgApp is an AppendEntries request (§5.3). With no entries it is a
	// heartbeat.
	MsgApp
	// MsgAppResp is the response to MsgApp, and to MsgSnap.
	MsgAppResp
	// MsgSnap is an InstallSnapshot request (§7), sent instead of MsgApp
	// when a follower needs entries the leader has compacted. Its response
	// is a MsgAppResp whose MatchIndex is the snapshot's index.
	MsgSnap
	// MsgPreVote asks whether the receiver would vote for the sender in
	// the message's term, which is one past the sender's own (thesis §9.6).
	// Neither side changes its term or vote.
	MsgPreVote
	// MsgPreVoteResp is the response to MsgPreVote. A grant carries the
	// term asked about; a refusal carries the voter's own term.
	MsgPreVoteResp
)

func (t MessageType) String() string {
	switch t {
	case MsgVote:
		return "MsgVote"
	case MsgVoteResp:
		return "MsgVoteResp"
	case MsgApp:
		return "MsgApp"
	case MsgAppResp:
		return "MsgAppResp"
	case MsgSnap:
		return "MsgSnap"
	case MsgPreVote:
		return "MsgPreVote"
	case MsgPreVoteResp:
		return "MsgPreVoteResp"
	}
	return "MsgUnknown"
}

// Message is one RPC request or response. Fields that a message's Type does
// not use are zero.
//
// Responses report the positive outcome (VoteGranted, Success), so that a
// zero value or a malformed message reads as a refusal, never as a grant.
type Message struct {
	Type MessageType
	From uint64
	To   uint64
	// Term is the sender's current term. Every message carries it, and a
	// receiver that sees a higher term adopts it (§5.1). The exceptions are
	// MsgPreVote and a granted MsgPreVoteResp, which carry a term no one
	// has entered yet; see their types.
	Term uint64

	// MsgVote and MsgPreVote: the candidate's last log entry, for the
	// election restriction (§5.4.1).
	LastLogIndex uint64
	LastLogTerm  uint64

	// MsgVoteResp and MsgPreVoteResp: whether the vote was granted.
	VoteGranted bool

	// MsgApp: the entry just before Entries, which the follower must
	// already have for the request to be accepted (§5.3), the entries to
	// append, and the leader's commit index.
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []Entry
	LeaderCommit uint64

	// MsgAppResp: whether the follower accepted the request.
	Success bool
	// MsgAppResp, on success: the index of the last entry the request
	// covered (PrevLogIndex + len(Entries)). The follower's log is now known
	// to match the leader's up to here, and no further.
	MatchIndex uint64
	// MsgAppResp, on rejection: the PrevLogIndex that was rejected, so the
	// leader can recognize and ignore rejections of requests it has since
	// superseded, and a suggested next index to try.
	RejectIndex uint64
	HintIndex   uint64

	// MsgSnap: the leader's latest snapshot.
	Snapshot Snapshot

	// MsgApp and MsgSnap: the leader's heartbeat round, echoed in the MsgAppResp. A
	// response with round r proves the follower still accepted this leader
	// when it received a request sent at round r, which is how ReadIndex
	// confirms leadership.
	Round uint64
}

// Transport sends messages to other nodes. Delivery is best-effort: messages
// may be lost, delayed, duplicated, or reordered, and Raft tolerates all of
// these. Send must not block and must not call back into Raft.
type Transport interface {
	Send(m Message)
}

// ReadState is the outcome of a ReadIndex request.
type ReadState struct {
	// ID is the ID the caller passed to ReadIndex.
	ID uint64
	// Index is the read index: once every entry through Index has been
	// applied, the state machine reflects every write that completed
	// before the read was requested, and the read can be served from it.
	Index uint64
	// Err is ErrNotLeader if the node lost leadership before it could
	// confirm the read. The read must then be retried on the new leader.
	Err error
}

// Status is a snapshot of a node's state, for tests and monitoring.
type Status struct {
	ID        uint64
	Term      uint64
	Vote      uint64
	Lead      uint64
	State     State
	Commit    uint64 // highest index known to be committed
	LastIndex uint64 // index of the last entry in the log
	Snapshot  uint64 // last index covered by the latest snapshot
}
