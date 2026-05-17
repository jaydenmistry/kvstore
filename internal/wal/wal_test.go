package wal

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/vfs"
)

// forEachFS runs fn against the in-memory filesystem and the real one.
func forEachFS(t *testing.T, fn func(t *testing.T, fsys vfs.FS, path string)) {
	t.Run("mem", func(t *testing.T) { fn(t, vfs.NewMem(), "/data/wal") })
	t.Run("os", func(t *testing.T) { fn(t, vfs.OS{}, filepath.Join(t.TempDir(), "wal")) })
}

func mustOpen(t *testing.T, fsys vfs.FS, path string) (*WAL, []Record) {
	t.Helper()
	w, recs, err := Open(fsys, path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return w, recs
}

func mustAppend(t *testing.T, w *WAL, recs ...Record) {
	t.Helper()
	if err := w.Append(recs...); err != nil {
		t.Fatalf("Append: %v", err)
	}
}

func reopen(t *testing.T, w *WAL, fsys vfs.FS, path string) (*WAL, []Record) {
	t.Helper()
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return mustOpen(t, fsys, path)
}

func equalRecords(a, b []Record) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || !bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

func checkRecords(t *testing.T, got, want []Record) {
	t.Helper()
	if !equalRecords(got, want) {
		t.Fatalf("records:\n got %s\nwant %s", fmtRecords(got), fmtRecords(want))
	}
}

func fmtRecords(recs []Record) string {
	var b bytes.Buffer
	for _, r := range recs {
		fmt.Fprintf(&b, "{%d %q} ", r.Type, r.Data)
	}
	return fmt.Sprintf("%d records: %s", len(recs), b.String())
}

// testRecords returns n distinct records of varying sizes.
func testRecords(n int) []Record {
	recs := make([]Record, n)
	for i := range recs {
		recs[i] = Record{Type: uint8(i%3 + 1), Data: bytes.Repeat([]byte{byte('a' + i%26)}, i*7%40)}
	}
	return recs
}

func readRaw(t *testing.T, fsys vfs.FS, path string) []byte {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, size)
	if n, err := f.ReadAt(b, 0); n < len(b) {
		t.Fatal(err)
	}
	return b
}

// writeRaw replaces the file's contents with b, durably.
func writeRaw(t *testing.T, fsys vfs.FS, path string, b []byte) {
	t.Helper()
	f, err := fsys.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
}

func TestOpenEmpty(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		w, recs := mustOpen(t, fsys, path)
		defer w.Close()
		if len(recs) != 0 {
			t.Fatalf("new log has %d records", len(recs))
		}
	})
}

func TestAppendAndReopen(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		want := testRecords(20)
		w, _ := mustOpen(t, fsys, path)
		mustAppend(t, w, want[0])
		mustAppend(t, w, want[1:5]...)
		mustAppend(t, w, want[5:]...)

		w, got := reopen(t, w, fsys, path)
		checkRecords(t, got, want)

		// Appending after recovery continues the same log.
		more := Record{Type: 9, Data: []byte("after reopen")}
		mustAppend(t, w, more)
		w, got = reopen(t, w, fsys, path)
		defer w.Close()
		checkRecords(t, got, append(want, more))
	})
}

func TestAppendRejectsInvalidRecords(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		w, _ := mustOpen(t, fsys, path)
		good := Record{Type: 1, Data: []byte("ok")}
		if err := w.Append(good, Record{Type: 0}); !errors.Is(err, errBadType) {
			t.Fatalf("type 0: err = %v, want errBadType", err)
		}
		if err := w.Append(Record{Type: 1, Data: make([]byte, MaxRecordSize+1)}); !errors.Is(err, errBadSize) {
			t.Fatalf("oversized: err = %v, want errBadSize", err)
		}
		// Validation errors are the caller's mistake, not an I/O failure:
		// nothing was written, and the log stays usable.
		mustAppend(t, w, good)
		w, got := reopen(t, w, fsys, path)
		defer w.Close()
		checkRecords(t, got, []Record{good})
	})
}

func TestEmptyAppendIsNoop(t *testing.T) {
	m := vfs.NewMem()
	w, _ := mustOpen(t, m, "/data/wal")
	defer w.Close()
	var ops int
	m.SetFault(func(vfs.Op, string) error { ops++; return nil })
	mustAppend(t, w)
	if ops != 0 {
		t.Fatalf("empty Append performed %d I/O operations", ops)
	}
}

func TestClosed(t *testing.T) {
	w, _ := mustOpen(t, vfs.NewMem(), "/data/wal")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Type: 1}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close: err = %v, want ErrClosed", err)
	}
	if err := w.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close: err = %v, want ErrClosed", err)
	}
}

func TestAcknowledgedAppendSurvivesCrash(t *testing.T) {
	// Every successful Append must survive any crash. This covers both the
	// file fsync and the directory fsync Open does for a new file.
	want := testRecords(5)
	for seed := range uint64(100) {
		m := vfs.NewMem()
		w, _ := mustOpen(t, m, "/data/wal")
		mustAppend(t, w, want...)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))

		w, got := mustOpen(t, m, "/data/wal")
		checkRecords(t, got, want)
		w.Close()
	}
}

func TestOpenSyncsDirectory(t *testing.T) {
	m := vfs.NewMem()
	var synced []string
	m.SetFault(func(op vfs.Op, name string) error {
		if op == vfs.OpSyncDir {
			synced = append(synced, name)
		}
		return nil
	})
	w, _ := mustOpen(t, m, "/data/wal")
	w.Close()
	if len(synced) != 1 || synced[0] != "/data" {
		t.Fatalf("directories synced = %q, want [/data]", synced)
	}
}

func TestOpenFailsOnReadError(t *testing.T) {
	_, _, err := recoverLog(vfs.NewMem(), "/data/wal", shortReader{})
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
	}
}

// shortReader claims to be 10 bytes long but returns nothing.
type shortReader struct{ vfs.File }

func (shortReader) Size() (int64, error)              { return 10, nil }
func (shortReader) ReadAt([]byte, int64) (int, error) { return 0, nil }
