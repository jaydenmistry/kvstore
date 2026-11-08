package raft

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

// gatedStorage records the entries of each Save, and can hold the first
// one until the test releases it, so writes queue up behind it.
type gatedStorage struct {
	*MemoryStorage
	entered chan struct{} // closed when the first Save starts, if gated
	release chan struct{} // the first Save waits for it to close, if gated
	fail    error

	mu    sync.Mutex
	saves []string
}

func newGatedStorage(gated bool) *gatedStorage {
	g := &gatedStorage{MemoryStorage: NewMemoryStorage()}
	if gated {
		g.entered, g.release = make(chan struct{}), make(chan struct{})
	}
	return g
}

func (g *gatedStorage) Save(hs *HardState, entries []Entry) error {
	g.mu.Lock()
	first := len(g.saves) == 0
	span := "none"
	if len(entries) > 0 {
		span = fmt.Sprintf("%d-%d", entries[0].Index, entries[len(entries)-1].Index)
	}
	g.saves = append(g.saves, span)
	g.mu.Unlock()
	if first && g.release != nil {
		close(g.entered)
		<-g.release
	}
	if g.fail != nil {
		return g.fail
	}
	return g.MemoryStorage.Save(hs, entries)
}

func (g *gatedStorage) savesSoFar() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return slices.Clone(g.saves)
}

func checkLastWritten(t *testing.T, b *BackgroundStorage, wantIndex, wantTerm uint64) {
	t.Helper()
	index, term, err := b.LastWritten()
	if err != nil || index != wantIndex || term != wantTerm {
		t.Fatalf("LastWritten = %d, %d, %v; want %d, %d, nil", index, term, err, wantIndex, wantTerm)
	}
}

func TestBackgroundWriteIsSavedAndReported(t *testing.T) {
	inner := newGatedStorage(false)
	b := NewBackgroundStorage(inner)
	b.SaveAsync(ents(1, 1, 2, 1))
	<-b.Written()
	checkLastWritten(t, b, 2, 1)
	checkLoad(t, b, HardState{}, ents(1, 1, 2, 1))
}

func TestWritesQueuedBehindOneAreCombined(t *testing.T) {
	inner := newGatedStorage(true)
	b := NewBackgroundStorage(inner)
	b.SaveAsync(ents(1, 1, 2, 1))
	<-inner.entered
	// Queued while the first write runs. The second replaces entry 4, as
	// a Save of it would: the combined write must have the same effect,
	// and must leave the callers' slices as they were.
	first := ents(3, 1, 4, 1)
	b.SaveAsync(first)
	b.SaveAsync(ents(4, 2))
	b.SaveAsync(ents(5, 2))
	close(inner.release)
	if err := b.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, want := inner.savesSoFar(), []string{"1-2", "3-5"}; !slices.Equal(got, want) {
		t.Fatalf("saves %q, want %q", got, want)
	}
	checkLastWritten(t, b, 5, 2)
	checkLoad(t, b, HardState{}, ents(1, 1, 2, 1, 3, 1, 4, 2, 5, 2))
	if !equalEntries(first, ents(3, 1, 4, 1)) {
		t.Fatalf("combining changed a caller's entries to %+v", first)
	}
}

func TestCombinedWritesStayWithinByteLimit(t *testing.T) {
	inner := newGatedStorage(true)
	b := NewBackgroundStorage(inner)
	b.SaveAsync(ents(1, 1))
	<-inner.entered
	big := func(i uint64) []Entry {
		return []Entry{{Index: i, Term: 1, Data: bytes.Repeat([]byte("x"), maxBackgroundWrite*2/3)}}
	}
	b.SaveAsync(big(2))
	b.SaveAsync(big(3))
	b.SaveAsync(ents(4, 1))
	close(inner.release)
	if err := b.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, want := inner.savesSoFar(), []string{"1-1", "2-2", "3-4"}; !slices.Equal(got, want) {
		t.Fatalf("saves %q, want %q", got, want)
	}
}

func TestSaveWaitsForBackgroundWrites(t *testing.T) {
	// Each save depends on the background write coming first: the entry
	// saved follows its entries, and the snapshot replaces them. Out of
	// order, the save fails, or the background write fails after it.
	for _, tc := range []struct {
		name string
		save func(*BackgroundStorage) error
		hs   HardState
		log  []Entry
	}{
		{"Save", func(b *BackgroundStorage) error { return b.Save(&HardState{Term: 2}, ents(3, 2)) },
			HardState{Term: 2}, ents(1, 1, 2, 1, 3, 2)},
		{"SaveSnapshot", func(b *BackgroundStorage) error {
			return b.SaveSnapshot(Snapshot{Index: 2, Term: 1, Data: []byte("snap")})
		},
			HardState{}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner := newGatedStorage(true)
			b := NewBackgroundStorage(inner)
			b.SaveAsync(ents(1, 1, 2, 1))
			<-inner.entered
			// Finish the background write only once the save waits for it.
			b.onWait = func() { close(inner.release) }
			if err := tc.save(b); err != nil {
				t.Fatalf("saving after a background write: %v", err)
			}
			if err := b.Wait(); err != nil {
				t.Fatalf("background write: %v", err)
			}
			checkLoad(t, b, tc.hs, tc.log)
		})
	}
}

func TestBackgroundWriteFailureStopsWrites(t *testing.T) {
	inner := newGatedStorage(false)
	inner.fail = errors.New("disk on fire")
	b := NewBackgroundStorage(inner)
	b.SaveAsync(ents(1, 1))
	<-b.Written()
	if _, _, err := b.LastWritten(); !errors.Is(err, inner.fail) {
		t.Fatalf("LastWritten error %v, want %v", err, inner.fail)
	}
	if err := b.Save(&HardState{Term: 1}, nil); !errors.Is(err, inner.fail) {
		t.Fatalf("Save after a failed background write: %v, want %v", err, inner.fail)
	}
	b.SaveAsync(ents(2, 1))
	if err := b.Wait(); !errors.Is(err, inner.fail) {
		t.Fatalf("Wait: %v, want %v", err, inner.fail)
	}
	if got := inner.savesSoFar(); !slices.Equal(got, []string{"1-1"}) {
		t.Fatalf("saves %q: wrote again after a failure", got)
	}
}
