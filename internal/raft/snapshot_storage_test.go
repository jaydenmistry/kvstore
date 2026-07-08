package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/vfs"
	"github.com/jaydenmistry/kvstore/internal/wal"
)

func snap(index, term uint64) Snapshot {
	return Snapshot{Index: index, Term: term, Data: fmt.Appendf(nil, "state@%d", index)}
}

func checkSnapshotLoad(t *testing.T, s Storage, wantSnap Snapshot, want []Entry) {
	t.Helper()
	_, got, log, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != wantSnap.Index || got.Term != wantSnap.Term || string(got.Data) != string(wantSnap.Data) {
		t.Fatalf("snapshot %d@%d %q, want %d@%d %q", got.Index, got.Term, got.Data, wantSnap.Index, wantSnap.Term, wantSnap.Data)
	}
	if !equalEntries(log, want) {
		t.Fatalf("log %+v, want %+v", log, want)
	}
}

func TestStorageSnapshotKeepsMatchingSuffix(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		if err := s.Save(&HardState{Term: 1}, ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1)); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveSnapshot(snap(3, 1)); err != nil {
			t.Fatal(err)
		}
		checkSnapshotLoad(t, s, snap(3, 1), ents(4, 1, 5, 1))
		checkSnapshotLoad(t, reopen(), snap(3, 1), ents(4, 1, 5, 1))
		checkLoad(t, reopen(), HardState{Term: 1}, ents(4, 1, 5, 1))
	})
}

func TestStorageSnapshotDropsLogThatDoesNotFollowIt(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		if err := s.Save(&HardState{Term: 2}, ents(1, 1, 2, 1, 3, 1)); err != nil {
			t.Fatal(err)
		}
		// The snapshot's last entry is index 2 with term 2, but we hold term
		// 1 there: entry 3 cannot be trusted to follow it.
		if err := s.SaveSnapshot(snap(2, 2)); err != nil {
			t.Fatal(err)
		}
		s = reopen()
		checkSnapshotLoad(t, s, snap(2, 2), nil)
		// A snapshot past the end of the log leaves nothing either.
		if err := s.Save(nil, ents(3, 2)); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveSnapshot(snap(9, 2)); err != nil {
			t.Fatal(err)
		}
		checkSnapshotLoad(t, reopen(), snap(9, 2), nil)
	})
}

func TestStorageSnapshotRules(t *testing.T) {
	storageImpls(t, func(t *testing.T, s Storage, reopen func() Storage) {
		if err := s.Save(&HardState{Term: 1}, ents(1, 1, 2, 1, 3, 1)); err != nil {
			t.Fatal(err)
		}
		if err := s.SaveSnapshot(snap(2, 1)); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []Snapshot{snap(2, 1), snap(1, 1), {Index: 3}} {
			if err := s.SaveSnapshot(bad); err == nil {
				t.Fatalf("accepted snapshot %d@%d after one at 2", bad.Index, bad.Term)
			}
		}
		if err := s.Save(nil, ents(2, 1)); err == nil {
			t.Fatal("accepted an entry the snapshot covers")
		}
		// Appending and replacing after the snapshot work as before.
		if err := s.Save(&HardState{Term: 2}, ents(3, 2, 4, 2)); err != nil {
			t.Fatal(err)
		}
		checkSnapshotLoad(t, reopen(), snap(2, 1), ents(3, 2, 4, 2))
		checkLoad(t, reopen(), HardState{Term: 2}, ents(3, 2, 4, 2))
	})
}

func TestDiskSnapshotDetectsCorruption(t *testing.T) {
	m := vfs.NewMem()
	s := mustOpenDisk(t, m, "/data")
	s.Save(&HardState{Term: 1}, ents(1, 1, 2, 1))
	if err := s.SaveSnapshot(snap(2, 1)); err != nil {
		t.Fatal(err)
	}
	s.Close()
	f, err := m.OpenFile("/data/snapshot", os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, 64)
	n, _ := f.ReadAt(b, 0)
	b[n-1] ^= 1
	f.Truncate(0)
	f.Write(b[:n])
	f.Close()
	if _, err := OpenDiskStorage(m, "/data"); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("opened storage with a corrupt snapshot: %v", err)
	}
}

func TestDiskSnapshotIsAtomicAcrossCrashes(t *testing.T) {
	// SaveSnapshot writes two files. Crash at every filesystem operation it
	// makes, then lose power: recovery must give either the old snapshot and
	// log or the new ones. Afterwards the storage must still accept
	// appends that survive a restart, which fails if recovery left the WAL
	// ending before the snapshot.
	hs := HardState{Term: 3, Vote: 1}
	old := ents(3, 1, 4, 1, 5, 2, 6, 2) // after an earlier snapshot at 2
	cases := []struct {
		name    string
		snap    Snapshot
		wantLog []Entry
	}{
		{"keeps suffix", snap(5, 2), ents(6, 2)},
		{"drops conflicting log", snap(5, 3), nil},
		{"past the log", snap(9, 3), nil},
	}
	for _, tc := range cases {
		for seed := range uint64(30) {
			for crashAt := 0; ; crashAt++ {
				m := vfs.NewMem()
				m.SetPageSize(16)
				s := mustOpenDisk(t, m, "/data")
				if err := s.Save(&hs, ents(1, 1, 2, 1)); err != nil {
					t.Fatal(err)
				}
				if err := s.SaveSnapshot(snap(2, 1)); err != nil {
					t.Fatal(err)
				}
				if err := s.Save(nil, old); err != nil {
					t.Fatal(err)
				}
				ops := 0
				m.SetFault(func(vfs.Op, string) error {
					if ops++; ops > crashAt {
						return errors.New("crashed")
					}
					return nil
				})
				err := s.SaveSnapshot(tc.snap)
				m.SetFault(nil)
				m.Crash(rand.New(rand.NewPCG(seed, uint64(crashAt))))

				s = mustOpenDisk(t, m, "/data")
				gotHS, gotSnap, log, err2 := s.Load()
				if err2 != nil {
					t.Fatalf("%s seed %d crash at op %d: Load: %v", tc.name, seed, crashAt, err2)
				}
				var wantSnap Snapshot
				var want []Entry
				switch {
				case gotSnap.Index == 2:
					wantSnap, want = snap(2, 1), old
				case gotSnap.Index == tc.snap.Index:
					wantSnap, want = tc.snap, tc.wantLog
				default:
					t.Fatalf("%s seed %d crash at op %d: recovered snapshot at %d", tc.name, seed, crashAt, gotSnap.Index)
				}
				if gotHS != hs || gotSnap.Term != wantSnap.Term || string(gotSnap.Data) != string(wantSnap.Data) || !equalEntries(log, want) {
					t.Fatalf("%s seed %d crash at op %d: recovered hs %+v snapshot %d@%d %q log %+v; want snapshot %d@%d log %+v",
						tc.name, seed, crashAt, gotHS, gotSnap.Index, gotSnap.Term, gotSnap.Data, log, wantSnap.Index, wantSnap.Term, want)
				}
				next := wantSnap.Index + uint64(len(want)) + 1
				if err := s.Save(nil, ents(next, 3)); err != nil {
					t.Fatalf("%s seed %d crash at op %d: append after recovery: %v", tc.name, seed, crashAt, err)
				}
				checkSnapshotLoad(t, reopenDisk(t, m, s), wantSnap, append(want, ents(next, 3)...))
				if err == nil {
					break // SaveSnapshot finished before the crash point
				}
			}
		}
	}
}

func reopenDisk(t *testing.T, m *vfs.Mem, s *DiskStorage) *DiskStorage {
	t.Helper()
	s.Close()
	return mustOpenDisk(t, m, "/data")
}
