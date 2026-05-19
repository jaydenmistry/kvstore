// Package wal implements an append-only write-ahead log of checksummed
// records, written in atomic batches.
//
// The log knows nothing about Raft. Callers give each record a type and
// interpret the contents themselves. The only promises are:
//
//   - Append returns nil only after its records are fsynced.
//   - Append is atomic across crashes: after recovery, either all of its
//     records are present or none are.
//   - Open returns the records of every Append that succeeded, possibly
//     followed by those of a final Append that failed or was interrupted by
//     a crash, in the order they were appended.
//   - Open discards a torn final batch, which was never acknowledged, but
//     refuses to discard damaged data that has a valid batch after it.
//
// One case is inherently ambiguous: damage confined to the final batch looks
// exactly like a torn write, so it is discarded even if that batch was
// acknowledged and later hit by bit rot. Telling the two apart would need
// information from outside this file.
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
// it contains, all of which are durable when Open returns. If the file ends
// in a torn write, the torn bytes are truncated away before Open returns. If the file is
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
		if err := f.Truncate(int64(valid)); err != nil {
			return nil, nil, fmt.Errorf("wal: truncate torn tail: %w", err)
		}
	}
	// Sync even if nothing was truncated. The bytes just read may exist only
	// in the page cache: a previous process can be killed between writing a
	// record and fsyncing it. Returning such a record as part of the log
	// would let Raft acknowledge an entry that a power loss could still
	// erase. After this fsync, everything returned is durable.
	//
	// The same fsync makes a truncation durable before any new append.
	// Otherwise a crash could bring back the torn bytes, now followed by new
	// valid records, and the next recovery would report corruption.
	if err := f.Sync(); err != nil {
		return nil, nil, fmt.Errorf("wal: sync: %w", err)
	}
	return &WAL{f: f}, recs, nil
}

// Append writes recs to the end of the log as one checksummed frame and
// fsyncs it. It returns nil only once all of recs are durable, and a crash
// never leaves only some of them on disk. Callers that need several records
// to persist together (for example a Raft term update and the entries that
// depend on it) should pass them in one call.
//
// If the write or fsync fails, the WAL becomes unusable: this and every later
// call returns the same error.
func (w *WAL) Append(recs ...Record) error {
	var buf []byte
	if len(recs) > 0 {
		var err error
		if buf, err = appendFrame(nil, recs); err != nil {
			return fmt.Errorf("wal: append: %w", err)
		}
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
		w.err = fmt.Errorf("wal: write failed, log is unusable: %w", err)
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		w.err = fmt.Errorf("wal: fsync failed, log is unusable: %w", err)
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
