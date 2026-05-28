package raft

import "sync"

// HardState is the state Raft must persist before responding to any RPC
// (Figure 2): currentTerm and votedFor.
type HardState struct {
	Term uint64
	Vote uint64
}

// Storage persists a node's state across restarts.
//
// SaveHardState must be durable when it returns: Raft sends messages that
// depend on the saved state immediately afterwards. If it returns an error,
// Raft stops (see Raft.Step).
type Storage interface {
	// HardState returns the most recently saved state, or the zero value if
	// nothing has been saved.
	HardState() (HardState, error)
	SaveHardState(HardState) error
}

// MemoryStorage is a Storage that keeps state in memory. Tests simulate a
// restart by building a new Raft on the same MemoryStorage. It is safe for
// concurrent use.
type MemoryStorage struct {
	mu sync.Mutex
	hs HardState
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) HardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, nil
}

func (s *MemoryStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hs = hs
	return nil
}
