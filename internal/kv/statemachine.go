package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"slices"

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

// Snapshot encoding, with integers big-endian and keys and clients in
// sorted order, so equal states encode to equal bytes:
//
//	| version u8 = 1 |
//	| key count u64 | (key length u32 | key | value length u32 | value)... |
//	| session count u64 | (client u64 | seq u64 | found u8)... |
//
// The sessions are part of the replicated state. A node restored without
// them would apply a retried write a second time.
const snapshotVersion = 1

// Snapshot serializes the state machine: the data and the client sessions.
// It describes the state after Applied.
func (s *StateMachine) Snapshot() []byte {
	b := []byte{snapshotVersion}
	b = binary.BigEndian.AppendUint64(b, uint64(len(s.data)))
	for _, k := range slices.Sorted(maps.Keys(s.data)) {
		v := s.data[k]
		b = binary.BigEndian.AppendUint32(b, uint32(len(k)))
		b = append(b, k...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	b = binary.BigEndian.AppendUint64(b, uint64(len(s.sessions)))
	for _, c := range slices.Sorted(maps.Keys(s.sessions)) {
		sess := s.sessions[c]
		b = binary.BigEndian.AppendUint64(b, c)
		b = binary.BigEndian.AppendUint64(b, sess.seq)
		found := byte(0)
		if sess.result.Found {
			found = 1
		}
		b = append(b, found)
	}
	return b
}

// Restore replaces the state machine's state with a snapshot's. Like Apply,
// an error is fatal: the snapshot is committed state that every replica
// shares.
func (s *StateMachine) Restore(snap raft.Snapshot) error {
	d := decoder{b: snap.Data}
	if v := d.byte(); v != snapshotVersion {
		return fmt.Errorf("kv: snapshot at %d has version %d", snap.Index, v)
	}
	data := map[string][]byte{}
	for n := d.uint64(); n > 0 && d.err == nil; n-- {
		k := string(d.bytes(d.uint32()))
		v := d.bytes(d.uint32())
		if len(v) == 0 {
			v = nil
		}
		data[k] = v
	}
	sessions := map[uint64]session{}
	for n := d.uint64(); n > 0 && d.err == nil; n-- {
		c, seq := d.uint64(), d.uint64()
		sessions[c] = session{seq: seq, result: Result{Found: d.byte() == 1}}
	}
	if d.err == nil && len(d.b) > 0 {
		d.err = fmt.Errorf("%d bytes left over", len(d.b))
	}
	if d.err != nil {
		return fmt.Errorf("kv: snapshot at %d is malformed: %w", snap.Index, d.err)
	}
	s.data, s.sessions, s.applied = data, sessions, snap.Index
	return nil
}

// decoder reads a snapshot, recording the first error and returning zero
// values after it.
type decoder struct {
	b   []byte
	err error
}

func (d *decoder) bytes(n uint64) []byte {
	if d.err != nil {
		return nil
	}
	if n > uint64(len(d.b)) {
		d.err = errors.New("truncated")
		return nil
	}
	out := d.b[:n:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) byte() byte {
	if b := d.bytes(1); b != nil {
		return b[0]
	}
	return 0
}

func (d *decoder) uint32() uint64 {
	if b := d.bytes(4); b != nil {
		return uint64(binary.BigEndian.Uint32(b))
	}
	return 0
}

func (d *decoder) uint64() uint64 {
	if b := d.bytes(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}
