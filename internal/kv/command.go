// Package kv is the replicated key-value state machine and the layer that
// connects client requests to Raft.
//
// StateMachine applies committed commands deterministically, including the
// duplicate-detection table, so every replica reaches the same state.
// Replica drives one Raft node: it proposes writes, confirms reads with
// ReadIndex, applies committed entries, and completes each request once its
// outcome is known. Neither does any I/O or reads a clock.
package kv

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Limits on request sizes. MaxValueSize is kept well below
// raft.DefaultMaxBytesPerMessage, so a batch of entries always fits in one
// AppendEntries request and one storage write.
const (
	MaxKeySize   = 4 << 10
	MaxValueSize = 512 << 10
)

// Op is a request type.
type Op uint8

const (
	OpGet Op = iota + 1
	OpPut
	OpDelete
)

func (o Op) String() string {
	switch o {
	case OpGet:
		return "get"
	case OpPut:
		return "put"
	case OpDelete:
		return "delete"
	}
	return fmt.Sprintf("op(%d)", uint8(o))
}

// Command is a write as stored in the Raft log.
//
// ClientID and Seq identify the request for duplicate detection (§8): a
// client numbers its writes 1, 2, 3, ... and reuses the number when it
// retries, so a write that reaches the log twice is applied only once.
type Command struct {
	Op       Op // OpPut or OpDelete
	ClientID uint64
	Seq      uint64
	Key      string
	Value    []byte // OpPut only
}

// ErrInvalid wraps every validation error.
var ErrInvalid = errors.New("kv: invalid request")

func (c *Command) validate() error {
	switch {
	case c.Op != OpPut && c.Op != OpDelete:
		return fmt.Errorf("%w: %v is not a write", ErrInvalid, c.Op)
	case c.ClientID == 0 || c.Seq == 0:
		return fmt.Errorf("%w: client ID and sequence number must be nonzero", ErrInvalid)
	case c.Op == OpDelete && len(c.Value) > 0:
		return fmt.Errorf("%w: delete has a value", ErrInvalid)
	}
	return validateKV(c.Key, c.Value)
}

func validateKV(key string, value []byte) error {
	switch {
	case key == "":
		return fmt.Errorf("%w: empty key", ErrInvalid)
	case len(key) > MaxKeySize:
		return fmt.Errorf("%w: key is %d bytes, over the %d limit", ErrInvalid, len(key), MaxKeySize)
	case len(value) > MaxValueSize:
		return fmt.Errorf("%w: value is %d bytes, over the %d limit", ErrInvalid, len(value), MaxValueSize)
	}
	return nil
}

// Encoding:
//
//	| op u8 | client u64 | seq u64 | key length u32 | key | value |
//
// The value runs to the end. Integers are big-endian.
const commandHeaderSize = 1 + 8 + 8 + 4

// Encode returns c in its log encoding.
func (c *Command) Encode() []byte {
	b := make([]byte, commandHeaderSize, commandHeaderSize+len(c.Key)+len(c.Value))
	b[0] = byte(c.Op)
	binary.BigEndian.PutUint64(b[1:], c.ClientID)
	binary.BigEndian.PutUint64(b[9:], c.Seq)
	binary.BigEndian.PutUint32(b[17:], uint32(len(c.Key)))
	b = append(b, c.Key...)
	return append(b, c.Value...)
}

// DecodeCommand parses an encoded command and validates it. Value aliases b.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < commandHeaderSize {
		return Command{}, fmt.Errorf("%w: command is %d bytes, shorter than its header", ErrInvalid, len(b))
	}
	klen := binary.BigEndian.Uint32(b[17:])
	rest := b[commandHeaderSize:]
	if uint64(klen) > uint64(len(rest)) {
		return Command{}, fmt.Errorf("%w: key length %d exceeds the %d bytes left", ErrInvalid, klen, len(rest))
	}
	c := Command{
		Op:       Op(b[0]),
		ClientID: binary.BigEndian.Uint64(b[1:]),
		Seq:      binary.BigEndian.Uint64(b[9:]),
		Key:      string(rest[:klen]),
	}
	if v := rest[klen:]; len(v) > 0 {
		c.Value = v[:len(v):len(v)]
	}
	if err := c.validate(); err != nil {
		return Command{}, err
	}
	return c, nil
}
