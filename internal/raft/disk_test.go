package raft

import (
	"errors"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/vfs"
	"github.com/jaydenmistry/kvstore/internal/wal"
)

func mustOpenDisk(t *testing.T, fsys vfs.FS, dir string) *DiskStorage {
	t.Helper()
	s, err := OpenDiskStorage(fsys, dir)
	if err != nil {
		t.Fatalf("OpenDiskStorage: %v", err)
	}
	return s
}

func TestDiskStorageRoundTrip(t *testing.T) {
	for name, fsys := range map[string]vfs.FS{"mem": vfs.NewMem(), "os": vfs.OS{}} {
		t.Run(name, func(t *testing.T) {
			dir := "/data"
			if name == "os" {
				dir = t.TempDir()
			}
			s := mustOpenDisk(t, fsys, dir)
			if hs, _, _ := s.Load(); hs != (HardState{}) {
				t.Fatalf("new storage has state %+v", hs)
			}
			for _, hs := range []HardState{{Term: 1, Vote: 1}, {Term: 2}, {Term: 2, Vote: 3}} {
				if err := s.Save(&hs, nil); err != nil {
					t.Fatal(err)
				}
			}
			s.Close()
			s = mustOpenDisk(t, fsys, dir)
			defer s.Close()
			if hs, _, _ := s.Load(); hs != (HardState{Term: 2, Vote: 3}) {
				t.Fatalf("after reopen: %+v, want {2 3}", hs)
			}
		})
	}
}

func TestDiskStorageSurvivesCrash(t *testing.T) {
	// Once Save returns, the state must survive a power loss. A
	// node that forgot its vote could vote twice in one term.
	for seed := range uint64(100) {
		m := vfs.NewMem()
		m.SetPageSize(16)
		s := mustOpenDisk(t, m, "/data")
		for term := uint64(1); term <= 5; term++ {
			if err := s.Save(&HardState{Term: term, Vote: term % 3}, nil); err != nil {
				t.Fatal(err)
			}
		}
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		s = mustOpenDisk(t, m, "/data")
		if hs, _, _ := s.Load(); hs != (HardState{Term: 5, Vote: 2}) {
			t.Fatalf("seed %d: after crash: %+v, want {5 2}", seed, hs)
		}
	}
}

func TestDiskStorageFailedSaveKeepsOldState(t *testing.T) {
	m := vfs.NewMem()
	s := mustOpenDisk(t, m, "/data")
	if err := s.Save(&HardState{Term: 1, Vote: 1}, nil); err != nil {
		t.Fatal(err)
	}
	m.SetFault(func(op vfs.Op, _ string) error {
		if op == vfs.OpSync {
			return errors.New("disk on fire")
		}
		return nil
	})
	if err := s.Save(&HardState{Term: 2, Vote: 2}, nil); err == nil {
		t.Fatal("Save succeeded despite fsync failure")
	}
	if hs, _, _ := s.Load(); hs != (HardState{Term: 1, Vote: 1}) {
		t.Fatalf("in-memory state changed after failed save: %+v", hs)
	}
}

func TestDiskStorageRejectsUnknownRecords(t *testing.T) {
	m := vfs.NewMem()
	w, _, err := wal.Open(m, "/data/raft.wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(wal.Record{Type: 99, Data: []byte("?")}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := OpenDiskStorage(m, "/data"); err == nil {
		t.Fatal("opened storage containing an unknown record type")
	}
}

func TestDiskStorageRejectsMalformedHardState(t *testing.T) {
	m := vfs.NewMem()
	w, _, err := wal.Open(m, "/data/raft.wal")
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(wal.Record{Type: recHardState, Data: make([]byte, 15)}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	if _, err := OpenDiskStorage(m, "/data"); err == nil {
		t.Fatal("opened storage containing a malformed hard state")
	}
}

func TestDiskStorageMissingDir(t *testing.T) {
	if _, err := OpenDiskStorage(vfs.OS{}, t.TempDir()+"/missing"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
}
