package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/vfs"
	"github.com/jaydenmistry/kvstore/internal/wal"
)

func ents(spec ...uint64) []Entry {
	// ents(i, t, i, t, ...) builds entries with the given index and term,
	// and data naming both so that different entries are distinguishable.
	var out []Entry
	for k := 0; k < len(spec); k += 2 {
		i, t := spec[k], spec[k+1]
		out = append(out, Entry{Index: i, Term: t, Data: fmt.Appendf(nil, "%d@%d", i, t)})
	}
	return out
}

func equalEntries(a, b []Entry) bool {
	return slices.EqualFunc(a, b, func(x, y Entry) bool {
		return x.Index == y.Index && x.Term == y.Term && string(x.Data) == string(y.Data)
	})
}

// storageImpls runs fn against every Storage implementation. reopen
// simulates a clean restart and returns the storage as a new process would
// see it.
func storageImpls(t *testing.T, fn func(t *testing.T, s Storage, reopen func() Storage)) {
	t.Run("memory", func(t *testing.T) {
		s := NewMemoryStorage()
		fn(t, s, func() Storage { return s })
	})
	t.Run("disk", func(t *testing.T) {
		m := vfs.NewMem()
		s := mustOpenDisk(t, m, "/data")
		fn(t, s, func() Storage {
			s.Close()
			s = mustOpenDisk(t, m, "/data")
			return s
		})
	})
}

func checkLoad(t *testing.T, s Storage, wantHS HardState, want []Entry) {
	t.Helper()
	hs, _, log, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if hs != wantHS {
		t.Fatalf("hard state %+v, want %+v", hs, wantHS)
	}
	if !equalEntries(log, want) {
		t.Fatalf("log %+v, want %+v", log, want)
	}
}

func TestStorageAppendAndReplace(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		hs := HardState{Term: 2, Vote: 1}
		if err := s.Save(&hs, ents(1, 1, 2, 1, 3, 2)); err != nil {
			t.Fatal(err)
		}
		if err := s.Save(nil, ents(4, 2, 5, 2)); err != nil {
			t.Fatal(err)
		}
		checkLoad(t, reopen(), hs, ents(1, 1, 2, 1, 3, 2, 4, 2, 5, 2))

		// Writing at index 3 discards 3, 4, and 5: a follower replacing a
		// conflicting suffix.
		s = reopen()
		if err := s.Save(nil, ents(3, 3)); err != nil {
			t.Fatal(err)
		}
		checkLoad(t, reopen(), hs, ents(1, 1, 2, 1, 3, 3))
	})
}

func TestStorageRejectsGapsWithoutWriting(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		if err := s.Save(nil, ents(1, 1)); err != nil {
			t.Fatal(err)
		}
		hs := HardState{Term: 9}
		for name, bad := range map[string][]Entry{
			"starts past end": ents(3, 1),
			"index zero":      ents(0, 1),
			"not contiguous":  ents(2, 1, 4, 1),
			"duplicate index": ents(2, 1, 2, 1),
		} {
			if err := s.Save(&hs, bad); err == nil {
				t.Fatalf("%s: Save accepted %+v", name, bad)
			}
		}
		// A rejected Save must not have written anything, not even hs.
		checkLoad(t, reopen(), HardState{}, ents(1, 1))
	})
}

func TestStorageLoadReturnsCopy(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		if err := s.Save(nil, ents(1, 1, 2, 1)); err != nil {
			t.Fatal(err)
		}
		_, _, log, _ := s.Load()
		log[0].Term = 99
		if err := s.Save(nil, ents(3, 1)); err != nil {
			t.Fatal(err)
		}
		checkLoad(t, s, HardState{}, ents(1, 1, 2, 1, 3, 1))
	})
}

func TestDiskStorageSaveIsAtomicAcrossCrashes(t *testing.T) {
	// A hard state and many entries saved together must survive a crash
	// together or not at all, even when they span many pages.
	before := ents(1, 1)
	hs := HardState{Term: 3, Vote: 2}
	var batch []Entry
	for i := uint64(2); i <= 40; i++ {
		batch = append(batch, ents(i, 3)...)
	}
	for seed := range uint64(200) {
		m := vfs.NewMem()
		m.SetPageSize(32)
		s := mustOpenDisk(t, m, "/data")
		if err := s.Save(&HardState{Term: 1}, before); err != nil {
			t.Fatal(err)
		}
		m.SetFault(func(op vfs.Op, _ string) error {
			if op == vfs.OpSync {
				return errors.New("crash before fsync")
			}
			return nil
		})
		s.Save(&hs, batch)
		m.SetFault(nil)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))

		gotHS, _, log, _ := mustOpenDisk(t, m, "/data").Load()
		switch {
		case gotHS == (HardState{Term: 1}) && equalEntries(log, before):
		case gotHS == hs && equalEntries(log, append(slices.Clone(before), batch...)):
		default:
			t.Fatalf("seed %d: partial save recovered: hs %+v, %d entries", seed, gotHS, len(log))
		}
	}
}

func TestDiskStorageRejectsGapInWAL(t *testing.T) {
	m := vfs.NewMem()
	w, _, err := wal.Open(m, "/data/raft.wal")
	if err != nil {
		t.Fatal(err)
	}
	e := ents(2, 1)[0] // index 2 with no index 1
	if err := w.Append(wal.Record{Type: recEntry, Data: encodeEntry(e)}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := OpenDiskStorage(m, "/data"); err == nil {
		t.Fatal("opened storage whose log starts at index 2")
	}
}

func TestEntryEncoding(t *testing.T) {
	for _, e := range []Entry{{Index: 1, Term: 1}, {Index: 1 << 40, Term: 7, Data: []byte("payload")}} {
		got, err := decodeEntry(encodeEntry(e))
		if err != nil || !equalEntries([]Entry{got}, []Entry{e}) {
			t.Fatalf("round trip of %+v gave %+v, %v", e, got, err)
		}
	}
	if _, err := decodeEntry(make([]byte, 15)); err == nil {
		t.Fatal("decoded a truncated entry")
	}
}
