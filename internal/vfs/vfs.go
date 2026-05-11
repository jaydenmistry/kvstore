// Package vfs abstracts the filesystem operations the storage layer needs.
//
// Production code uses OS. Tests use Mem, which tracks which bytes have been
// fsynced and can simulate a crash that loses or tears everything else.
package vfs

import (
	"io"
	"os"
)

// File is an open file. It is the subset of *os.File the storage layer uses.
type File interface {
	io.Writer
	io.ReaderAt
	// Sync flushes the file's contents to stable storage (fsync). It does not
	// make the file's directory entry durable; see FS.SyncDir.
	Sync() error
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

// FS is a filesystem.
type FS interface {
	// OpenFile opens name with the same flag semantics as os.OpenFile.
	OpenFile(name string, flag int, perm os.FileMode) (File, error)
	// SyncDir fsyncs the directory dir. A newly created file's name is not
	// guaranteed to survive a crash until its parent directory is synced,
	// even if the file itself was synced.
	SyncDir(dir string) error
}
