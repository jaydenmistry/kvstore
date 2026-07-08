package raft

import "fmt"

// The log is held in memory in r.log, a logSuffix (see storage.go): the
// entries after the latest snapshot, plus the index and term of the last
// entry the snapshot covers.

func (r *Raft) lastIndex() uint64 { return r.log.lastIndex() }

func (r *Raft) lastTerm() uint64 {
	t, _ := r.log.termAt(r.lastIndex())
	return t
}

// termAt returns the term of the entry at index i. ok is false if the log
// does not extend to i, or if i is covered by the snapshot and its term is
// no longer known. The snapshot's own last index still has a term.
func (r *Raft) termAt(i uint64) (term uint64, ok bool) { return r.log.termAt(i) }

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
	r.log.splice(entries)
	return true
}

// Entry returns the entry at index i, if the log has one and it has not
// been compacted into a snapshot. It is meant for tests and debugging.
func (r *Raft) Entry(i uint64) (Entry, bool) { return r.log.entry(i) }
