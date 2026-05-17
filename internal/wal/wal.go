// Package wal implements an append-only write-ahead log of checksummed
// records.
//
// The log knows nothing about Raft. Callers give each record a type and
// interpret the contents themselves. The only promises are:
//
//   - Append returns nil only after its records are fsynced.
//   - Open returns exactly the records whose Append succeeded, possibly
//     followed by some records from a final Append that failed or was
//     interrupted by a crash, in the order they were appended.
//   - Open discards a torn final write, which was never acknowledged, but
//     refuses to discard damaged data that has valid records after it.
package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/jaydenmistry/kvstore/internal/vfs"
)

// ErrClosed is returned by operations on a closed WAL.
var ErrClosed = errors.New("wal: closed")

// WAL is an open write-ahead log. It is safe for concurrent use, though
// Appends are serialized.
type WAL struct {
	mu sync.Mutex
	f  vfs.File

	// err is set when a write or fsync fails and is returned by every later
	// call. After a failed fsync on Linux the kernel may have dropped the
	// dirty pages and marked them clean, so a retried fsync can report
	// success without the data being on disk. Callers should treat this as
	// fatal and restart the process.
	err    error
	closed bool
}

// Open opens the log at path, creating it if needed, and returns the records
// it contains. If the file ends in a torn write, the torn bytes are truncated
// away (and the truncation fsynced) before Open returns. If the file is
// damaged in the middle, Open returns an error matching ErrCorrupt and leaves
// the file untouched.
func Open(fsys vfs.FS, path string) (*WAL, []Record, error) {
	f, err := fsys.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, nil, fmt.Errorf("wal: open: %w", err)
	}
	w, recs, err := recoverLog(fsys, path, f)
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return w, recs, nil
}

func recoverLog(fsys vfs.FS, path string, f vfs.File) (*WAL, []Record, error) {
	// Sync the directory on every open, not only when the file is created.
	// If a previous process created the file and died before syncing the
	// directory, the name may still exist only in the page cache, and records
	// appended now would be lost along with it if the machine then crashed.
	if err := fsys.SyncDir(filepath.Dir(path)); err != nil {
		return nil, nil, fmt.Errorf("wal: sync dir: %w", err)
	}
	size, err := f.Size()
	if err != nil {
		return nil, nil, fmt.Errorf("wal: stat: %w", err)
	}
	buf := make([]byte, size)
	if n, err := f.ReadAt(buf, 0); n < len(buf) {
		if err == nil {
			err = io.ErrUnexpectedEOF
		}
		return nil, nil, fmt.Errorf("wal: read: %w", err)
	}
	recs, valid, err := decodeAll(buf)
	if err != nil {
		return nil, nil, err
	}
	if int64(valid) < size {
		// Truncation must be durable before any new append. Otherwise a
		// crash could bring back the torn bytes, now followed by new valid
		// records, and the next recovery would report corruption.
		if err := f.Truncate(int64(valid)); err != nil {
			return nil, nil, fmt.Errorf("wal: truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			return nil, nil, fmt.Errorf("wal: sync after truncate: %w", err)
		}
	}
	return &WAL{f: f}, recs, nil
}

// Append writes recs to the end of the log as one write and fsyncs it. It
// returns nil only once all of recs are durable. Callers that need several
// records to persist together (for example a Raft term update and the
// entries that depend on it) should pass them in one call: the cost is a
// single fsync.
//
// A crash during Append may leave any prefix of recs on disk, so all-or-
// nothing atomicity across records is not guaranteed.
//
// If the write or fsync fails, the WAL becomes unusable: this and every later
// call returns the same error.
func (w *WAL) Append(recs ...Record) error {
	var buf []byte
	for _, r := range recs {
		if err := validate(r); err != nil {
			return fmt.Errorf("wal: append: %w", err)
		}
		buf = appendRecord(buf, r)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.err != nil {
		return w.err
	}
	if len(buf) == 0 {
		return nil
	}
	if _, err := w.f.Write(buf); err != nil {
		w.err = fmt.Errorf("wal: write failed, log must be reopened: %w", err)
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: fsync failed, log must be reopened: %w", err)
		return w.err
	}
	return nil
}

// Close closes the log. Records whose Append succeeded are already durable.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	w.closed = true
	return w.f.Close()
}
