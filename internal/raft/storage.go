package raft

import (
	"fmt"
	"slices"
	"sync"
)

// HardState is the state Raft must persist, besides the log, before
// responding to any RPC (Figure 2): currentTerm and votedFor.
type HardState struct {
	Term uint64
	Vote uint64
}

// Storage persists a node's hard state and log across restarts.
type Storage interface {
	// Load returns everything saved so far: the latest hard state (zero if
	// none) and the log.
	Load() (HardState, []Entry, error)

	// Save durably records hs, if it is not nil, and entries. Entries must
	// have contiguous indexes, and the first must be at most one past the
	// last stored entry. Saving them discards every stored entry at or after
	// entries[0].Index, which is how a follower replaces conflicting
	// entries (§5.3).
	//
	// Save must be atomic and durable: when it returns nil, all of it
	// survives a crash, and if a crash interrupts it, none of it does. Raft
	// sends messages that depend on the saved state as soon as it returns.
	// If it returns an error, Raft stops.
	Save(hs *HardState, entries []Entry) error
}

// checkSplice reports whether entries may be written over a log of length
// n, following the contiguity rules from Storage.Save.
func checkSplice(n int, entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	if first == 0 || first > uint64(n)+1 {
		return fmt.Errorf("raft: entries start at %d but log ends at %d", first, n)
	}
	for i, e := range entries {
		if e.Index != first+uint64(i) {
			return fmt.Errorf("raft: entry indexes not contiguous: %d follows %d", e.Index, first+uint64(i)-1)
		}
	}
	return nil
}

// spliceLog returns log with entries written over it starting at
// entries[0].Index. Callers must have checked them with checkSplice.
//
// Overwriting log's backing array in place is safe because no storage
// implementation lets that array escape (Load returns a copy).
func spliceLog(log, entries []Entry) []Entry {
	if len(entries) == 0 {
		return log
	}
	return append(log[:entries[0].Index-1], entries...)
}

// MemoryStorage is a Storage that keeps state in memory. Tests simulate a
// restart by building a new Raft on the same MemoryStorage. It is safe for
// concurrent use.
type MemoryStorage struct {
	mu  sync.Mutex
	hs  HardState
	log []Entry
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, slices.Clone(s.log), nil
}

func (s *MemoryStorage) Save(hs *HardState, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkSplice(len(s.log), entries); err != nil {
		return err
	}
	s.log = spliceLog(s.log, entries)
	if hs != nil {
		s.hs = *hs
	}
	return nil
}
