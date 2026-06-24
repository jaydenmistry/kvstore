package kv

import (
	"errors"
	"fmt"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// ErrStaleSequence is the result of a write whose sequence number is below
// the latest one applied for its client. The client has already moved on
// from that request, so nobody is waiting for its result, and it is not
// kept.
var ErrStaleSequence = errors.New("kv: sequence number already superseded")

// Result is the outcome of a request.
type Result struct {
	// Value is the value read by a Get. It must not be modified.
	Value []byte
	// Found reports whether the key existed, for Get and Delete.
	Found bool
	// Err is nil on success. StateMachine sets it only to
	// ErrStaleSequence; Replica adds the errors that a request can fail
	// with before it is applied.
	Err error
	// Leader is, with ErrNotLeader, the leader this node knows of, or
	// raft.None.
	Leader uint64
}

// session is what the state machine remembers about one client: its latest
// applied write and that write's result.
type session struct {
	seq    uint64
	result Result
}

// StateMachine is the key-value map plus the client sessions used for
// duplicate detection. It changes only through Apply, which must see every
// committed entry exactly once, in log order, so every replica holds the
// same state after the same prefix of the log. It is not safe for concurrent
// use.
type StateMachine struct {
	data     map[string][]byte
	sessions map[uint64]session
	applied  uint64
}

// NewStateMachine returns an empty state machine, before index 1.
func NewStateMachine() *StateMachine {
	return &StateMachine{data: map[string][]byte{}, sessions: map[uint64]session{}}
}

// Applied returns the index of the last applied entry.
func (s *StateMachine) Applied() uint64 { return s.applied }

// Get returns the value stored under key. The value must not be modified.
func (s *StateMachine) Get(key string) ([]byte, bool) {
	v, ok := s.data[key]
	return v, ok
}

// Apply applies the next committed entry and returns its result. No-op
// entries change nothing but the applied index.
//
// An error means the entry is out of order or malformed. Every replica holds
// the same committed entry, so retrying or skipping it cannot help: the
// caller must stop.
func (s *StateMachine) Apply(e raft.Entry) (Result, error) {
	if e.Index != s.applied+1 {
		return Result{}, fmt.Errorf("kv: applying entry %d after %d", e.Index, s.applied)
	}
	if len(e.Data) == 0 {
		s.applied = e.Index
		return Result{}, nil
	}
	c, err := DecodeCommand(e.Data)
	if err != nil {
		return Result{}, fmt.Errorf("kv: committed entry %d: %w", e.Index, err)
	}
	s.applied = e.Index

	// Duplicate detection (§8). A client retries a write whose outcome it
	// never learned, so the same (client, seq) can be committed twice: say
	// the leader commits it and crashes before replying, and the retry goes
	// to the next leader. The second copy must not be applied again, or a
	// retried Put could overwrite a later write by the same client. It must
	// also return the first copy's result rather than a fresh one: a
	// retried Delete would otherwise report that the key was not found.
	sess := s.sessions[c.ClientID]
	switch {
	case c.Seq == sess.seq:
		return sess.result, nil
	case c.Seq < sess.seq:
		return Result{Err: ErrStaleSequence}, nil
	}

	var res Result
	switch c.Op {
	case OpPut:
		s.data[c.Key] = c.Value
	case OpDelete:
		_, res.Found = s.data[c.Key]
		delete(s.data, c.Key)
	}
	s.sessions[c.ClientID] = session{seq: c.Seq, result: res}
	return res, nil
}
