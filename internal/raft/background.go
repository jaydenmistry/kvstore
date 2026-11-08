package raft

import (
	"slices"
	"sync"
)

// maxBackgroundWrite caps the entry data BackgroundStorage combines into
// one write. A single larger SaveAsync is still written whole.
const maxBackgroundWrite = 1 << 20

// BackgroundStorage adds SaveAsync to a Storage, making it an
// AsyncStorage. One goroutine at a time writes the queued entries through
// the wrapped Storage's Save. Writes queued while one is in progress are
// combined into the next, so they share one fsync: under load, the
// leader's writes batch themselves the way the server loop batches client
// requests.
//
// After each background write, Written receives a notice, and LastWritten
// reports the last entry written. Notices are combined too: one notice may
// stand for several writes. The caller passes the position to
// Raft.Persisted, which needs only the latest.
//
// It is safe for concurrent use, but Raft's calls must come from one
// goroutine, as they always do.
type BackgroundStorage struct {
	inner   Storage
	written chan struct{}

	mu      sync.Mutex
	idle    sync.Cond // signaled when writing becomes false
	queue   [][]Entry // SaveAsync calls not yet written, in order
	writing bool      // a goroutine is writing the queue
	index   uint64    // last entry written in the background
	term    uint64
	err     error // a background write failed; nothing more is written

	// onWait, if set, is called with mu held whenever a caller starts
	// waiting for background writes. Tests use it to finish a write only
	// once something waits for it.
	onWait func()
}

var _ AsyncStorage = (*BackgroundStorage)(nil)

// NewBackgroundStorage returns s with background writes added.
func NewBackgroundStorage(s Storage) *BackgroundStorage {
	b := &BackgroundStorage{inner: s, written: make(chan struct{}, 1)}
	b.idle.L = &b.mu
	return b
}

// Load waits for background writes, then loads from the wrapped Storage.
func (b *BackgroundStorage) Load() (HardState, Snapshot, []Entry, error) {
	if err := b.Wait(); err != nil {
		return HardState{}, Snapshot{}, nil, err
	}
	return b.inner.Load()
}

// Save waits for background writes, then saves through the wrapped
// Storage, so it never takes effect before a write started earlier.
func (b *BackgroundStorage) Save(hs *HardState, entries []Entry) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.wait(); err != nil {
		return err
	}
	return b.inner.Save(hs, entries)
}

// SaveSnapshot waits for background writes, like Save.
func (b *BackgroundStorage) SaveSnapshot(snap Snapshot) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.wait(); err != nil {
		return err
	}
	return b.inner.SaveSnapshot(snap)
}

// SaveAsync queues entries to be saved in the background. After a write
// fails, nothing more is written: LastWritten reports the error.
func (b *BackgroundStorage) SaveAsync(entries []Entry) {
	if len(entries) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return
	}
	b.queue = append(b.queue, entries)
	if !b.writing {
		b.writing = true
		go b.writeQueued()
	}
}

// Written receives a notice after background writes finish or fail.
func (b *BackgroundStorage) Written() <-chan struct{} { return b.written }

// LastWritten returns the index and term of the last entry written in the
// background, or the error that stopped background writes.
func (b *BackgroundStorage) LastWritten() (index, term uint64, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.index, b.term, b.err
}

// Wait blocks until no background write is queued or in progress, and
// returns the error of one that failed. Call it before closing the wrapped
// Storage.
func (b *BackgroundStorage) Wait() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.wait()
}

func (b *BackgroundStorage) wait() error {
	for b.writing {
		if b.onWait != nil {
			b.onWait()
		}
		b.idle.Wait()
	}
	return b.err
}

// writeQueued writes the queue until it is empty. The lock is released
// while writing, so SaveAsync never waits for the disk.
func (b *BackgroundStorage) writeQueued() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.queue) > 0 {
		ents := b.takeQueued()
		b.mu.Unlock()
		err := b.inner.Save(nil, ents)
		b.mu.Lock()
		if err != nil {
			b.err, b.queue = err, nil
		} else {
			last := ents[len(ents)-1]
			b.index, b.term = last.Index, last.Term
		}
		select {
		case b.written <- struct{}{}:
		default: // a notice is already waiting
		}
	}
	b.writing = false
	b.idle.Broadcast()
}

// takeQueued removes whole SaveAsync calls from the queue, up to
// maxBackgroundWrite bytes of entry data but at least one, and returns
// them as one Save. Saving the result has the same effect as saving the
// calls in turn: each later call replaces the entries from its first
// index on, as Save would. Whole calls only, so each stays atomic.
func (b *BackgroundStorage) takeQueued() []Entry {
	ents := slices.Clone(b.queue[0])
	size := dataSize(ents)
	n := 1
	for ; n < len(b.queue); n++ {
		next := b.queue[n]
		if size += dataSize(next); size > maxBackgroundWrite {
			break
		}
		keep := 0
		for keep < len(ents) && ents[keep].Index < next[0].Index {
			keep++
		}
		ents = append(ents[:keep], next...)
	}
	b.queue = b.queue[n:]
	return ents
}
