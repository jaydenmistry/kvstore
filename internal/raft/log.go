package raft

import "fmt"

// The log is held in memory in r.log, where r.log[i] has index i+1. Index 0
// is the empty position before the first entry and has term 0, which makes
// the consistency check for the first entry (PrevLogIndex 0) always pass.

func (r *Raft) lastIndex() uint64 { return uint64(len(r.log)) }

func (r *Raft) lastTerm() uint64 {
	t, _ := r.termAt(r.lastIndex())
	return t
}

// termAt returns the term of the entry at index i. Index 0 has term 0. ok is
// false if the log does not extend to i.
func (r *Raft) termAt(i uint64) (term uint64, ok bool) {
	switch {
	case i == 0:
		return 0, true
	case i > r.lastIndex():
		return 0, false
	}
	return r.log[i-1].Term, true
}

// saveEntries durably writes entries over the log starting at
// entries[0].Index, discarding whatever was there and after it, then makes
// the same change in memory. Returns false if storage failed.
//
// In-flight messages never alias r.log (see sendAppend), so overwriting its
// backing array in place is safe.
func (r *Raft) saveEntries(entries []Entry) bool {
	if len(entries) == 0 {
		return true
	}
	if first := entries[0].Index; first <= r.commit {
		// Committed entries are permanent (§5.4). Reaching this line means a
		// safety invariant is already broken somewhere.
		panic(fmt.Sprintf("raft: node %d would overwrite committed index %d (commit %d)", r.id, first, r.commit))
	}
	if err := r.storage.Save(nil, entries); err != nil {
		r.err = fmt.Errorf("raft: node %d stopped: saving entries from index %d: %w", r.id, entries[0].Index, err)
		return false
	}
	r.log = append(r.log[:entries[0].Index-1], entries...)
	return true
}

// Entry returns the entry at index i, if the log has one. It is meant for
// tests and debugging.
func (r *Raft) Entry(i uint64) (Entry, bool) {
	if i == 0 || i > r.lastIndex() {
		return Entry{}, false
	}
	return r.log[i-1], true
}
