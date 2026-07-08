package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"os"
	"testing"
)

const rw = os.O_RDWR | os.O_CREATE

func mustOpen(t *testing.T, m *Mem, name string, flag int) File {
	t.Helper()
	f, err := m.OpenFile(name, flag, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustWrite(t *testing.T, f File, p string) {
	t.Helper()
	if _, err := f.Write([]byte(p)); err != nil {
		t.Fatal(err)
	}
}

func contents(t *testing.T, m *Mem, name string) []byte {
	t.Helper()
	f := mustOpen(t, m, name, os.O_RDONLY)
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, 0); err != nil && err != io.EOF {
		t.Fatal(err)
	}
	return buf
}

func TestMemOpenFlags(t *testing.T) {
	m := NewMem()
	if _, err := m.OpenFile("/d/f", os.O_RDWR, 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("open missing file: err = %v, want ErrNotExist", err)
	}
	f := mustOpen(t, m, "/d/f", rw|os.O_EXCL)
	mustWrite(t, f, "abc")
	f.Close()
	if _, err := m.OpenFile("/d/f", rw|os.O_EXCL, 0); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("O_EXCL on existing file: err = %v, want ErrExist", err)
	}

	f = mustOpen(t, m, "/d/f", os.O_RDWR|os.O_APPEND)
	mustWrite(t, f, "de")
	f.Close()
	if got := contents(t, m, "/d/f"); string(got) != "abcde" {
		t.Fatalf("after append: %q, want %q", got, "abcde")
	}

	f = mustOpen(t, m, "/d/f", os.O_RDWR)
	mustWrite(t, f, "X")
	f.Close()
	if got := contents(t, m, "/d/f"); string(got) != "Xbcde" {
		t.Fatalf("after positional write: %q, want %q", got, "Xbcde")
	}

	f = mustOpen(t, m, "/d/f", os.O_RDONLY)
	if _, err := f.Write([]byte("no")); err == nil {
		t.Fatal("write to read-only handle succeeded")
	}
	f.Close()

	f = mustOpen(t, m, "/d/f", os.O_RDWR|os.O_TRUNC)
	f.Close()
	if got := contents(t, m, "/d/f"); len(got) != 0 {
		t.Fatalf("after O_TRUNC: %q, want empty", got)
	}
}

func TestMemSyncedDataSurvivesCrash(t *testing.T) {
	for seed := range uint64(200) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		mustWrite(t, f, "durable")
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := m.SyncDir("/d"); err != nil {
			t.Fatal(err)
		}
		mustWrite(t, f, "-volatile")
		m.Crash(rand.New(rand.NewPCG(seed, 0)))

		got := contents(t, m, "/d/f")
		if !bytes.HasPrefix(got, []byte("durable")) {
			t.Fatalf("seed %d: synced prefix lost: %q", seed, got)
		}
		if len(got) > len("durable-volatile") {
			t.Fatalf("seed %d: file grew past what was written: %q", seed, got)
		}
	}
}

func TestMemCrashOutcomesVary(t *testing.T) {
	// The crash model is only useful if it actually produces torn tails, so
	// check that both full loss and partial survival of unsynced data occur.
	var sawNone, sawPartial bool
	for seed := range uint64(200) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		m.SyncDir("/d")
		mustWrite(t, f, "0123456789")
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		switch n := len(contents(t, m, "/d/f")); {
		case n == 0:
			sawNone = true
		case n < 10:
			sawPartial = true
		}
	}
	if !sawNone || !sawPartial {
		t.Fatalf("crash outcomes: none=%v partial=%v, want both", sawNone, sawPartial)
	}
}

func TestMemCrashCanReorderPages(t *testing.T) {
	// Pages of an unsynced write may reach disk out of order, leaving a hole
	// of zeros followed by data that did make it.
	for seed := range uint64(200) {
		m := NewMem()
		m.SetPageSize(4)
		f := mustOpen(t, m, "/d/f", rw)
		m.SyncDir("/d")
		mustWrite(t, f, "aaaabbbbccccdddd")
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		got := contents(t, m, "/d/f")
		if len(got) >= 8 && bytes.Equal(got[:4], make([]byte, 4)) && string(got[4:8]) == "bbbb" {
			return
		}
	}
	t.Fatal("no crash produced a lost page followed by a surviving one")
}

func TestMemCrashKeepsSyncedPartOfPage(t *testing.T) {
	// A page that holds both synced and unsynced bytes must never lose the
	// synced ones, whether or not the page was written back.
	for seed := range uint64(200) {
		m := NewMem()
		m.SetPageSize(8)
		f := mustOpen(t, m, "/d/f", rw)
		m.SyncDir("/d")
		mustWrite(t, f, "abc")
		f.Sync()
		mustWrite(t, f, "defghijklmn")
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		if got := contents(t, m, "/d/f"); !bytes.HasPrefix(got, []byte("abc")) {
			t.Fatalf("seed %d: synced bytes lost: %q", seed, got)
		}
	}
}

// exists reports whether name can be opened.
func exists(t *testing.T, m *Mem, name string) bool {
	t.Helper()
	f, err := m.OpenFile(name, os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	return true
}

func TestMemNewFileNeedsDirSync(t *testing.T) {
	// Without a directory sync, a new file may or may not survive, even if
	// its contents were synced. After one, it always does.
	var sawKept, sawLost bool
	for seed := range uint64(50) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		mustWrite(t, f, "data")
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		if exists(t, m, "/d/f") {
			sawKept = true
			if got := contents(t, m, "/d/f"); string(got) != "data" {
				t.Fatalf("seed %d: kept file holds %q", seed, got)
			}
		} else {
			sawLost = true
		}

		f = mustOpen(t, m, "/d/f", rw|os.O_TRUNC)
		mustWrite(t, f, "data")
		f.Sync()
		if err := m.SyncDir("/d"); err != nil {
			t.Fatal(err)
		}
		m.Crash(rand.New(rand.NewPCG(seed, 1)))
		if got := contents(t, m, "/d/f"); string(got) != "data" {
			t.Fatalf("seed %d: after dir sync and crash: %q, want %q", seed, got, "data")
		}
	}
	if !sawKept || !sawLost {
		t.Fatalf("unsynced create: kept=%v lost=%v, want both outcomes", sawKept, sawLost)
	}
}

func TestMemSyncDirOnlyAffectsThatDir(t *testing.T) {
	var sawLost bool
	for seed := range uint64(50) {
		m := NewMem()
		mustOpen(t, m, "/a/f", rw).Close()
		mustOpen(t, m, "/b/f", rw).Close()
		m.SyncDir("/a")
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		if !exists(t, m, "/a/f") {
			t.Fatalf("seed %d: /a/f lost although /a was synced", seed)
		}
		sawLost = sawLost || !exists(t, m, "/b/f")
	}
	if !sawLost {
		t.Fatal("/b/f always survived without its dir being synced")
	}
}

func TestMemRenameIsAtomic(t *testing.T) {
	// The write-to-temp-then-rename pattern. Before the directory sync, a
	// crash leaves either the old file or the new one under the name,
	// never a mix; after it, always the new one.
	var sawOld, sawNew bool
	for seed := range uint64(50) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		mustWrite(t, f, "old")
		f.Sync()
		m.SyncDir("/d")
		tmp := mustOpen(t, m, "/d/f.tmp", rw)
		mustWrite(t, tmp, "new contents")
		tmp.Sync()
		if err := m.Rename("/d/f.tmp", "/d/f"); err != nil {
			t.Fatal(err)
		}
		if got := contents(t, m, "/d/f"); string(got) != "new contents" || exists(t, m, "/d/f.tmp") {
			t.Fatalf("before crash: f = %q, tmp exists = %v", got, exists(t, m, "/d/f.tmp"))
		}
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		switch got := string(contents(t, m, "/d/f")); got {
		case "old":
			sawOld = true // the temporary file may or may not exist
		case "new contents":
			sawNew = true
			if exists(t, m, "/d/f.tmp") {
				t.Fatalf("seed %d: rename kept but the temporary file is still there", seed)
			}
		default:
			t.Fatalf("seed %d: f = %q", seed, got)
		}

		tmp = mustOpen(t, m, "/d/g.tmp", rw)
		mustWrite(t, tmp, "g")
		tmp.Sync()
		m.Rename("/d/g.tmp", "/d/g")
		m.SyncDir("/d")
		m.Crash(rand.New(rand.NewPCG(seed, 1)))
		if got := contents(t, m, "/d/g"); string(got) != "g" || exists(t, m, "/d/g.tmp") {
			t.Fatalf("seed %d: after dir sync: g = %q, tmp exists = %v", seed, got, exists(t, m, "/d/g.tmp"))
		}
	}
	if !sawOld || !sawNew {
		t.Fatalf("unsynced rename: old=%v new=%v, want both outcomes", sawOld, sawNew)
	}
}

func TestMemRemoveNeedsDirSync(t *testing.T) {
	var sawBack bool
	for seed := range uint64(50) {
		m := NewMem()
		mustOpen(t, m, "/d/f", rw).Close()
		m.SyncDir("/d")
		if err := m.Remove("/d/f"); err != nil {
			t.Fatal(err)
		}
		if exists(t, m, "/d/f") {
			t.Fatal("removed file still visible")
		}
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		sawBack = sawBack || exists(t, m, "/d/f")
		if exists(t, m, "/d/f") {
			m.Remove("/d/f")
		}
		m.SyncDir("/d")
		m.Crash(rand.New(rand.NewPCG(seed, 1)))
		if exists(t, m, "/d/f") {
			t.Fatalf("seed %d: removed file came back after dir sync", seed)
		}
	}
	if !sawBack {
		t.Fatal("unsynced remove always survived the crash")
	}
}

func TestMemRenameErrors(t *testing.T) {
	m := NewMem()
	if err := m.Rename("/d/missing", "/d/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("rename of missing file: %v", err)
	}
	mustOpen(t, m, "/d/f", rw).Close()
	if err := m.Rename("/d/f", "/e/f"); err == nil {
		t.Fatal("rename across directories succeeded")
	}
	if err := m.Remove("/d/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("remove of missing file: %v", err)
	}
}

func TestMemUnsyncedTruncateMayRevert(t *testing.T) {
	var sawOld, sawNew bool
	for seed := range uint64(100) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		mustWrite(t, f, "0123456789")
		f.Sync()
		m.SyncDir("/d")
		f.Truncate(5)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		switch got := string(contents(t, m, "/d/f")); got {
		case "0123456789":
			sawOld = true
		case "01234":
			sawNew = true
		default:
			t.Fatalf("seed %d: unexpected contents %q", seed, got)
		}
	}
	if !sawOld || !sawNew {
		t.Fatalf("truncate outcomes: old=%v new=%v, want both", sawOld, sawNew)
	}
}

func TestMemCrashInvalidatesHandles(t *testing.T) {
	m := NewMem()
	f := mustOpen(t, m, "/d/f", rw)
	m.SyncDir("/d")
	m.Crash(rand.New(rand.NewPCG(1, 0)))
	if _, err := f.Write([]byte("x")); !errors.Is(err, ErrStaleHandle) {
		t.Fatalf("write after crash: err = %v, want ErrStaleHandle", err)
	}
	if err := f.Sync(); !errors.Is(err, ErrStaleHandle) {
		t.Fatalf("sync after crash: err = %v, want ErrStaleHandle", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close after crash: %v", err)
	}
}

func TestMemCrashIsDeterministic(t *testing.T) {
	run := func() []byte {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		m.SyncDir("/d")
		mustWrite(t, f, "some unsynced bytes that may be torn")
		m.Crash(rand.New(rand.NewPCG(42, 7)))
		return contents(t, m, "/d/f")
	}
	if a, b := run(), run(); !bytes.Equal(a, b) {
		t.Fatalf("same seed gave different results: %q vs %q", a, b)
	}
}

func TestMemFaultInjection(t *testing.T) {
	errBoom := errors.New("boom")
	failSync := func(op Op, name string) error {
		if op == OpSync {
			return errBoom
		}
		return nil
	}
	// A failed sync must have no effect: if it had made "-lost" durable,
	// every crash would preserve it. Some seed must drop it.
	for seed := range uint64(50) {
		m := NewMem()
		f := mustOpen(t, m, "/d/f", rw)
		mustWrite(t, f, "ok")
		f.Sync()
		m.SyncDir("/d")
		mustWrite(t, f, "-lost")
		m.SetFault(failSync)
		if err := f.Sync(); !errors.Is(err, errBoom) {
			t.Fatalf("sync: err = %v, want injected error", err)
		}
		m.SetFault(nil)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		if string(contents(t, m, "/d/f")) == "ok" {
			return
		}
	}
	t.Fatal("data written before a failed sync survived every crash")
}
