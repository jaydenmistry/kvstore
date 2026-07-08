package vfs

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// ErrStaleHandle is returned by operations on a file handle that was open
// when Mem.Crash was called. The process that owned it is considered dead.
var ErrStaleHandle = errors.New("vfs: file handle invalidated by crash")

// Op identifies an operation passed to a fault hook.
type Op int

const (
	OpWrite Op = iota
	OpSync
	OpTruncate
	OpSyncDir
	OpRename
	OpRemove
)

func (o Op) String() string {
	switch o {
	case OpWrite:
		return "write"
	case OpSync:
		return "sync"
	case OpTruncate:
		return "truncate"
	case OpSyncDir:
		return "syncdir"
	case OpRename:
		return "rename"
	case OpRemove:
		return "remove"
	}
	return "unknown"
}

// Mem is an in-memory filesystem that models what survives a crash.
//
// Each file has two versions of its contents: what the running process sees,
// and what has been made durable by Sync. The namespace is modeled the same
// way: names created, renamed, or removed in a directory become durable when
// the directory is synced with SyncDir. Until then, a crash keeps either all
// of that directory's unsynced name changes or none of them. (A real
// filesystem journals metadata changes in order, so this is a
// simplification, but it produces both outcomes a caller must handle.)
//
// Crash discards everything that is not durable, except that unsynced
// appended bytes may partly survive. Like a real kernel, Mem writes dirty
// pages back in no particular order: the file may grow by any amount up to
// what was written, each page of that growth independently either reached
// disk or reads back as zeros, and the last few bytes may be garbage. That
// models torn writes, including ones with holes in the middle.
//
// Mem is safe for concurrent use.
type Mem struct {
	mu       sync.Mutex
	files    map[string]*inode // namespace as the running process sees it
	durable  map[string]*inode // namespace as it would be after a crash
	epoch    uint64            // incremented by Crash; invalidates open handles
	fault    func(op Op, name string) error
	pageSize int
}

// DefaultPageSize is the page size Crash uses unless SetPageSize changes it.
const DefaultPageSize = 4096

var _ FS = (*Mem)(nil)

type inode struct {
	data   []byte // contents as the running process sees them
	synced []byte // contents guaranteed to survive a crash
}

// NewMem returns an empty in-memory filesystem.
func NewMem() *Mem {
	return &Mem{
		files:    make(map[string]*inode),
		durable:  make(map[string]*inode),
		pageSize: DefaultPageSize,
	}
}

// SetPageSize sets the granularity at which Crash decides whether unsynced
// data reached disk. Tests with small records use a small page size so that
// a single write spans several pages.
func (m *Mem) SetPageSize(n int) {
	if n <= 0 {
		panic("vfs: page size must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pageSize = n
}

// SetFault installs a hook called before every write, sync, truncate,
// directory sync, rename, and remove. If it returns an error, the operation fails with that error
// and has no effect. The hook runs with Mem's lock held and must not call
// back into Mem. Pass nil to remove it.
func (m *Mem) SetFault(fn func(op Op, name string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fault = fn
}

func (m *Mem) injected(op Op, name string) error {
	if m.fault == nil {
		return nil
	}
	return m.fault(op, name)
}

func (m *Mem) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = filepath.Clean(name)
	ino, ok := m.files[name]
	switch {
	case ok && flag&os.O_CREATE != 0 && flag&os.O_EXCL != 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrExist}
	case !ok && flag&os.O_CREATE == 0:
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	case !ok:
		ino = &inode{}
		m.files[name] = ino
	}
	if flag&os.O_TRUNC != 0 {
		ino.data = nil
	}
	return &memFile{fs: m, ino: ino, name: name, flag: flag, epoch: m.epoch}, nil
}

func (m *Mem) SyncDir(dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = filepath.Clean(dir)
	if err := m.injected(OpSyncDir, dir); err != nil {
		return err
	}
	for name := range m.durable {
		if filepath.Dir(name) == dir && m.files[name] == nil {
			delete(m.durable, name)
		}
	}
	for name, ino := range m.files {
		if filepath.Dir(name) == dir {
			m.durable[name] = ino
		}
	}
	return nil
}

func (m *Mem) Rename(oldpath, newpath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	oldpath, newpath = filepath.Clean(oldpath), filepath.Clean(newpath)
	if filepath.Dir(oldpath) != filepath.Dir(newpath) {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: errors.New("vfs: Mem renames only within a directory")}
	}
	ino, ok := m.files[oldpath]
	if !ok {
		return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: fs.ErrNotExist}
	}
	if err := m.injected(OpRename, oldpath); err != nil {
		return err
	}
	delete(m.files, oldpath)
	m.files[newpath] = ino
	return nil
}

func (m *Mem) Remove(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name = filepath.Clean(name)
	if _, ok := m.files[name]; !ok {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	if err := m.injected(OpRemove, name); err != nil {
		return err
	}
	delete(m.files, name)
	return nil
}

// Crash simulates a power failure followed by a restart. Every open handle
// becomes stale. Each directory keeps either its durable names or its
// current ones, and each file's contents are replaced by what could have
// survived on disk. All randomness comes from rng, so a given seed always
// produces the same outcome.
func (m *Mem) Crash(rng *rand.Rand) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	byDir := func(ns map[string]*inode) map[string]map[string]*inode {
		out := map[string]map[string]*inode{}
		for name, ino := range ns {
			d := filepath.Dir(name)
			if out[d] == nil {
				out[d] = map[string]*inode{}
			}
			out[d][name] = ino
		}
		return out
	}
	durable, current := byDir(m.durable), byDir(m.files)
	dirs := maps.Clone(durable)
	maps.Copy(dirs, current)
	names := map[string]*inode{}
	// Sorted so rng is consumed in a deterministic order.
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		ns := durable[d]
		if !maps.Equal(durable[d], current[d]) && rng.IntN(2) == 0 {
			ns = current[d]
		}
		maps.Copy(names, ns)
	}
	files := make(map[string]*inode, len(names))
	for _, name := range slices.Sorted(maps.Keys(names)) {
		ino := names[name]
		ino.data = survivingContents(ino, m.pageSize, rng)
		ino.synced = bytes.Clone(ino.data)
		files[name] = ino
	}
	m.files = files
	m.durable = maps.Clone(files)
}

func survivingContents(ino *inode, pageSize int, rng *rand.Rand) []byte {
	synced, data := ino.synced, ino.data
	if !bytes.HasPrefix(data, synced) {
		// The file was truncated or overwritten since the last sync. Model
		// the change as either fully reaching disk or not at all.
		if rng.IntN(2) == 0 {
			return bytes.Clone(synced)
		}
		return bytes.Clone(data)
	}
	// Only appends since the last sync. The new size is metadata written
	// independently of the data, so pick any length between synced and
	// current. Then each page overlapping the new region is either on disk
	// or not. A page that was not written back keeps only its synced bytes.
	tail := data[len(synced):]
	k := rng.IntN(len(tail) + 1)
	out := append(bytes.Clone(synced), tail[:k]...)
	for page := len(synced) / pageSize * pageSize; page < len(out); page += pageSize {
		if rng.IntN(3) == 0 {
			clear(out[max(page, len(synced)):min(page+pageSize, len(out))])
		}
	}
	if k > 0 && rng.IntN(4) == 0 {
		torn := out[len(out)-1-rng.IntN(min(k, 16)):]
		if rng.IntN(2) == 0 {
			clear(torn)
		} else {
			for i := range torn {
				torn[i] = byte(rng.Uint32())
			}
		}
	}
	return out
}

type memFile struct {
	fs     *Mem
	ino    *inode
	name   string
	flag   int
	epoch  uint64
	off    int64
	closed bool
}

// check reports whether the handle can be used. Caller holds fs.mu.
func (f *memFile) check() error {
	if f.closed {
		return os.ErrClosed
	}
	if f.epoch != f.fs.epoch {
		return ErrStaleHandle
	}
	return nil
}

func (f *memFile) writable() bool {
	return f.flag&(os.O_WRONLY|os.O_RDWR) != 0
}

func (f *memFile) Write(p []byte) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if !f.writable() {
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: fs.ErrPermission}
	}
	if err := f.fs.injected(OpWrite, f.name); err != nil {
		return 0, err
	}
	if f.flag&os.O_APPEND != 0 {
		f.off = int64(len(f.ino.data))
	}
	if end := f.off + int64(len(p)); end > int64(len(f.ino.data)) {
		f.ino.data = append(f.ino.data, make([]byte, end-int64(len(f.ino.data)))...)
	}
	n := copy(f.ino.data[f.off:], p)
	f.off += int64(n)
	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	if off < 0 {
		return 0, &fs.PathError{Op: "readat", Path: f.name, Err: fs.ErrInvalid}
	}
	if off >= int64(len(f.ino.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.ino.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) Sync() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if err := f.fs.injected(OpSync, f.name); err != nil {
		return err
	}
	f.ino.synced = bytes.Clone(f.ino.data)
	return nil
}

func (f *memFile) Truncate(size int64) error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return err
	}
	if !f.writable() || size < 0 {
		return &fs.PathError{Op: "truncate", Path: f.name, Err: fs.ErrInvalid}
	}
	if err := f.fs.injected(OpTruncate, f.name); err != nil {
		return err
	}
	if size <= int64(len(f.ino.data)) {
		f.ino.data = f.ino.data[:size]
	} else {
		f.ino.data = append(f.ino.data, make([]byte, size-int64(len(f.ino.data)))...)
	}
	return nil
}

func (f *memFile) Size() (int64, error) {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if err := f.check(); err != nil {
		return 0, err
	}
	return int64(len(f.ino.data)), nil
}

// Close always succeeds on a stale handle, so code that cleans up after a
// simulated crash does not have to special-case it.
func (f *memFile) Close() error {
	f.fs.mu.Lock()
	defer f.fs.mu.Unlock()
	if f.closed {
		return os.ErrClosed
	}
	f.closed = true
	return nil
}
