package raft

import (
	"bytes"
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

// Snapshot is the application's state after applying every entry through
// Index (§7). It replaces those entries: once it is saved, they can be
// discarded.
type Snapshot struct {
	// Index and Term identify the last entry the snapshot covers. The term
	// is kept because the entry after it still needs the AppendEntries
	// consistency check (§5.3), which compares terms at the previous index.
	Index uint64
	Term  uint64
	// Data is the application's serialized state. It must not be modified.
	Data []byte
}

// Storage persists a node's hard state, snapshot, and log across restarts.
type Storage interface {
	// Load returns everything saved so far: the latest hard state (zero if
	// none), the latest snapshot (zero if none), and the entries after it.
	Load() (HardState, Snapshot, []Entry, error)

	// Save durably records hs, if it is not nil, and entries. Entries must
	// have contiguous indexes, and the first must come after the snapshot
	// and be at most one past the last stored entry. Saving them discards
	// every stored entry at or after entries[0].Index, which is how a
	// follower replaces conflicting entries (§5.3).
	//
	// Save must be atomic and durable: when it returns nil, all of it
	// survives a crash, and if a crash interrupts it, none of it does. Raft
	// sends messages that depend on the saved state as soon as it returns.
	// If it returns an error, Raft stops.
	Save(hs *HardState, entries []Entry) error

	// SaveSnapshot durably replaces the stored snapshot with snap, which
	// must be newer, and discards the entries it covers. If the log holds
	// the snapshot's last entry (same index and term), the entries after it
	// are kept; otherwise the whole log is discarded, since none of it can
	// be trusted to follow the snapshot (Figure 13, receiver steps 6 and 7).
	//
	// Like Save, it is atomic and durable: after a crash, Load returns
	// either the old snapshot and log or the new ones.
	SaveSnapshot(snap Snapshot) error
}

// logSuffix is a log whose entries up to and including (index, term) have
// been replaced by a snapshot. With no snapshot, index and term are 0: the
// empty position before the first entry, whose term 0 makes the consistency
// check for the first entry always pass.
type logSuffix struct {
	index, term uint64
	ents        []Entry // ents[i] has index index+1+i
}

func (l *logSuffix) lastIndex() uint64 { return l.index + uint64(len(l.ents)) }

// termAt returns the term of the entry at i. ok is false if i is past the
// end of the log, or before the snapshot, where the term is no longer known.
func (l *logSuffix) termAt(i uint64) (term uint64, ok bool) {
	switch {
	case i == l.index:
		return l.term, true
	case i < l.index || i > l.lastIndex():
		return 0, false
	}
	return l.ents[i-l.index-1].Term, true
}

// entry returns the entry at i, if it is in the log and not compacted.
func (l *logSuffix) entry(i uint64) (Entry, bool) {
	if i <= l.index || i > l.lastIndex() {
		return Entry{}, false
	}
	return l.ents[i-l.index-1], true
}

// slice returns a copy of the entries with indexes in [lo, hi). They must
// all be in the log.
func (l *logSuffix) slice(lo, hi uint64) []Entry {
	return slices.Clone(l.ents[lo-l.index-1 : hi-l.index-1])
}

// check reports whether entries may be spliced into the log, following the
// rules of Storage.Save.
func (l *logSuffix) check(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}
	first := entries[0].Index
	if first <= l.index || first > l.lastIndex()+1 {
		return fmt.Errorf("raft: entries start at %d but the log holds %d to %d", first, l.index+1, l.lastIndex())
	}
	for i, e := range entries {
		if e.Index != first+uint64(i) {
			return fmt.Errorf("raft: entry indexes not contiguous: %d follows %d", e.Index, first+uint64(i)-1)
		}
	}
	return nil
}

// splice writes entries over the log starting at entries[0].Index. Callers
// must have checked them with check.
//
// It overwrites the backing array in place. That is safe because no copy of
// it escapes: Load and slice return copies.
func (l *logSuffix) splice(entries []Entry) {
	if len(entries) > 0 {
		l.ents = append(l.ents[:entries[0].Index-l.index-1], entries...)
	}
}

// compact discards the entries covered by a snapshot whose last entry is
// (index, term), as Storage.SaveSnapshot describes.
func (l *logSuffix) compact(index, term uint64) {
	if t, ok := l.termAt(index); ok && t == term {
		// A copy, so the compacted entries' memory can be freed.
		l.ents = slices.Clone(l.ents[index-l.index:])
	} else {
		l.ents = nil
	}
	l.index, l.term = index, term
}

// checkSnapshot reports whether snap may replace a snapshot ending at
// index.
func checkSnapshot(snap Snapshot, index uint64) error {
	if snap.Index <= index || snap.Term == 0 {
		return fmt.Errorf("raft: snapshot at index %d term %d does not follow the one at %d", snap.Index, snap.Term, index)
	}
	return nil
}

// MemoryStorage is a Storage that keeps state in memory. Tests simulate a
// restart by building a new Raft on the same MemoryStorage. It is safe for
// concurrent use.
type MemoryStorage struct {
	mu   sync.Mutex
	hs   HardState
	snap Snapshot
	log  logSuffix
}

var _ Storage = (*MemoryStorage)(nil)

func NewMemoryStorage() *MemoryStorage { return &MemoryStorage{} }

func (s *MemoryStorage) Load() (HardState, Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.snap
	snap.Data = bytes.Clone(snap.Data)
	return s.hs, snap, slices.Clone(s.log.ents), nil
}

func (s *MemoryStorage) Save(hs *HardState, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.check(entries); err != nil {
		return err
	}
	s.log.splice(entries)
	if hs != nil {
		s.hs = *hs
	}
	return nil
}

func (s *MemoryStorage) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := checkSnapshot(snap, s.log.index); err != nil {
		return err
	}
	snap.Data = bytes.Clone(snap.Data)
	s.log.compact(snap.Index, snap.Term)
	s.snap = snap
	return nil
}
