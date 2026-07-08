package raft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/jaydenmistry/kvstore/internal/vfs"
	"github.com/jaydenmistry/kvstore/internal/wal"
)

// WAL record types used by DiskStorage.
const (
	recHardState uint8 = 1 // term u64 | vote u64
	recEntry     uint8 = 2 // index u64 | term u64 | data
	recBase      uint8 = 3 // index u64 | term u64: the log starts after this entry
)

// File names in a DiskStorage directory.
const (
	walName      = "raft.wal"
	snapName     = "snapshot"
	tmpSuffix    = ".tmp"
	maxWALRecord = 1 << 20 // rewrite batches stay well under wal.MaxBatchSize
)

// DiskStorage is a Storage backed by a write-ahead log and a snapshot file
// in a directory.
//
// Each Save is one WAL Append: a hard state record and one record per entry,
// in a single crash-atomic frame with a single fsync. Recovery replays the
// records in order. An entry record at index i first discards any entries at
// or after i, so replay reproduces both appends and conflict truncations
// exactly as Save performed them.
//
// SaveSnapshot takes two steps, each made atomic by writing a temporary
// file, fsyncing it, renaming it into place, and fsyncing the directory:
//
//  1. Write the new snapshot file.
//  2. Rewrite the WAL to hold only the hard state and the entries after the
//     snapshot, starting with a base record naming the snapshot's last
//     entry.
//
// No filesystem makes two files change together, so a crash can fall
// between the steps, leaving a snapshot newer than the WAL's base. Open
// detects that, applies the snapshot to the old log by the same rule
// SaveSnapshot used (Storage.SaveSnapshot), and finishes step 2. The result
// is what SaveSnapshot would have produced.
type DiskStorage struct {
	mu   sync.Mutex
	fsys vfs.FS
	dir  string
	w    *wal.WAL
	hs   HardState
	log  logSuffix
	// snapIndex and snapTerm describe the snapshot file. Its data is read
	// from disk by Load rather than kept in memory.
	snapIndex, snapTerm uint64
	// err is set when an operation failed partway. The storage then refuses
	// further changes, like the WAL does after a failed write.
	err error
}

var _ Storage = (*DiskStorage)(nil)

// OpenDiskStorage opens or creates the storage in dir, which must exist.
func OpenDiskStorage(fsys vfs.FS, dir string) (*DiskStorage, error) {
	s := &DiskStorage{fsys: fsys, dir: dir}
	// Leftovers from an interrupted SaveSnapshot. The renames had not
	// happened, so these were never in use.
	for _, name := range []string{snapName + tmpSuffix, walName + tmpSuffix} {
		if err := fsys.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}
	}
	snap, err := readSnapshot(fsys, filepath.Join(dir, snapName))
	if err != nil {
		return nil, err
	}
	w, recs, err := wal.Open(fsys, filepath.Join(dir, walName))
	if err != nil {
		return nil, err
	}
	s.w = w
	if err := s.replay(recs); err != nil {
		w.Close()
		return nil, err
	}
	switch {
	case snap.Index < s.log.index:
		w.Close()
		return nil, fmt.Errorf("raft: log starts after index %d, past the snapshot at %d", s.log.index, snap.Index)
	case snap.Index == s.log.index && snap.Term != s.log.term:
		w.Close()
		return nil, fmt.Errorf("raft: snapshot at index %d has term %d, but the log's base has term %d", snap.Index, snap.Term, s.log.term)
	case snap.Index > s.log.index:
		// Crashed between the two steps of SaveSnapshot: finish it.
		s.log.compact(snap.Index, snap.Term)
		if err := s.rewriteWAL(); err != nil {
			s.w.Close()
			return nil, err
		}
	}
	s.snapIndex, s.snapTerm = snap.Index, snap.Term
	return s, nil
}

func (s *DiskStorage) replay(recs []wal.Record) error {
	for i, r := range recs {
		switch r.Type {
		case recHardState:
			hs, err := decodeHardState(r.Data)
			if err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			s.hs = hs
		case recBase:
			if i != 0 {
				return fmt.Errorf("raft: wal record %d: base record is not first", i)
			}
			index, term, err := decodePair(r.Data)
			if err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			s.log = logSuffix{index: index, term: term}
		case recEntry:
			e, err := decodeEntry(r.Data)
			if err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			if err := s.log.check([]Entry{e}); err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			s.log.splice([]Entry{e})
		default:
			return fmt.Errorf("raft: wal record %d: unknown type %d", i, r.Type)
		}
	}
	return nil
}

func (s *DiskStorage) Load() (HardState, Snapshot, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, err := readSnapshot(s.fsys, filepath.Join(s.dir, snapName))
	if err != nil {
		return HardState{}, Snapshot{}, nil, err
	}
	if snap.Index != s.snapIndex || snap.Term != s.snapTerm {
		return HardState{}, Snapshot{}, nil, fmt.Errorf("raft: snapshot file changed underneath storage")
	}
	return s.hs, snap, slices.Clone(s.log.ents), nil
}

func (s *DiskStorage) Save(hs *HardState, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	// Validate before writing, so a bad call never reaches the disk.
	if err := s.log.check(entries); err != nil {
		return err
	}
	var recs []wal.Record
	if hs != nil {
		recs = append(recs, wal.Record{Type: recHardState, Data: encodeHardState(*hs)})
	}
	for _, e := range entries {
		recs = append(recs, wal.Record{Type: recEntry, Data: encodeEntry(e)})
	}
	if err := s.w.Append(recs...); err != nil {
		return err
	}
	s.log.splice(entries)
	if hs != nil {
		s.hs = *hs
	}
	return nil
}

func (s *DiskStorage) SaveSnapshot(snap Snapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	if err := checkSnapshot(snap, s.log.index); err != nil {
		return err
	}
	// Step 1. Until the rename is durable, a crash leaves the old snapshot,
	// and the old WAL is untouched.
	if err := s.replaceFile(snapName, func(f vfs.File) error {
		_, err := f.Write(encodeSnapshot(snap))
		return err
	}); err != nil {
		s.err = fmt.Errorf("raft: storage unusable after failed snapshot: %w", err)
		return s.err
	}
	s.snapIndex, s.snapTerm = snap.Index, snap.Term
	// Step 2.
	s.log.compact(snap.Index, snap.Term)
	if err := s.rewriteWAL(); err != nil {
		s.err = fmt.Errorf("raft: storage unusable after failed WAL rewrite: %w", err)
		return s.err
	}
	return nil
}

// rewriteWAL replaces the WAL with one holding only s.log and s.hs.
func (s *DiskStorage) rewriteWAL() error {
	tmp := filepath.Join(s.dir, walName+tmpSuffix)
	if err := s.fsys.Remove(tmp); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	nw, _, err := wal.Open(s.fsys, tmp)
	if err != nil {
		return err
	}
	// The base and hard state go first, then the entries in batches that
	// fit in WAL frames. Atomicity comes from the rename, not the frames.
	batch := []wal.Record{
		{Type: recBase, Data: encodePair(s.log.index, s.log.term)},
		{Type: recHardState, Data: encodeHardState(s.hs)},
	}
	size := 0
	for _, e := range s.log.ents {
		rec := wal.Record{Type: recEntry, Data: encodeEntry(e)}
		if size+len(rec.Data) > maxWALRecord && len(batch) > 0 {
			if err := nw.Append(batch...); err != nil {
				nw.Close()
				return err
			}
			batch, size = nil, 0
		}
		batch = append(batch, rec)
		size += len(rec.Data)
	}
	if err := nw.Append(batch...); err != nil {
		nw.Close()
		return err
	}
	if err := nw.Close(); err != nil {
		return err
	}
	if err := s.w.Close(); err != nil && !errors.Is(err, wal.ErrClosed) {
		return err
	}
	if err := s.fsys.Rename(tmp, filepath.Join(s.dir, walName)); err != nil {
		return err
	}
	if err := s.fsys.SyncDir(s.dir); err != nil {
		return err
	}
	w, _, err := wal.Open(s.fsys, filepath.Join(s.dir, walName))
	if err != nil {
		return err
	}
	s.w = w
	return nil
}

// replaceFile atomically replaces dir/name with contents written by write.
func (s *DiskStorage) replaceFile(name string, write func(vfs.File) error) error {
	path := filepath.Join(s.dir, name)
	f, err := s.fsys.OpenFile(path+tmpSuffix, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if err := write(f); err != nil {
		f.Close()
		return err
	}
	// The contents must be durable before the name points at them, or a
	// crash could leave the new name on an empty or torn file.
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := s.fsys.Rename(path+tmpSuffix, path); err != nil {
		return err
	}
	return s.fsys.SyncDir(s.dir)
}

// Close closes the underlying log.
func (s *DiskStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Close()
}

// Snapshot file format:
//
//	| crc u32 | index u64 | term u64 | data |
//
// The CRC-32C covers everything after it. The file is fsynced before it is
// renamed into place, so a mismatch means corruption, not a torn write.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

const snapHeaderSize = 4 + 8 + 8

func encodeSnapshot(snap Snapshot) []byte {
	b := make([]byte, 4, snapHeaderSize+len(snap.Data))
	b = binary.LittleEndian.AppendUint64(b, snap.Index)
	b = binary.LittleEndian.AppendUint64(b, snap.Term)
	b = append(b, snap.Data...)
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
	return b
}

// readSnapshot reads the snapshot file at path, returning a zero Snapshot
// if there is none.
func readSnapshot(fsys vfs.FS, path string) (Snapshot, error) {
	f, err := fsys.OpenFile(path, os.O_RDONLY, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return Snapshot{}, nil
	}
	if err != nil {
		return Snapshot{}, err
	}
	defer f.Close()
	size, err := f.Size()
	if err != nil {
		return Snapshot{}, err
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && err != io.EOF {
		return Snapshot{}, err
	}
	if len(b) < snapHeaderSize || binary.LittleEndian.Uint32(b) != crc32.Checksum(b[4:], castagnoli) {
		return Snapshot{}, fmt.Errorf("raft: snapshot file %s is corrupt: %w", path, wal.ErrCorrupt)
	}
	snap := Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:12]),
		Term:  binary.LittleEndian.Uint64(b[12:20]),
	}
	if len(b) > snapHeaderSize {
		snap.Data = b[snapHeaderSize:]
	}
	return snap, nil
}

func encodeHardState(hs HardState) []byte { return encodePair(hs.Term, hs.Vote) }

func decodeHardState(b []byte) (HardState, error) {
	term, vote, err := decodePair(b)
	return HardState{Term: term, Vote: vote}, err
}

// encodePair encodes two uint64s, for base and hard state records.
func encodePair(a, b uint64) []byte {
	buf := make([]byte, 0, 16)
	buf = binary.LittleEndian.AppendUint64(buf, a)
	return binary.LittleEndian.AppendUint64(buf, b)
}

func decodePair(b []byte) (uint64, uint64, error) {
	if len(b) != 16 {
		return 0, 0, fmt.Errorf("record is %d bytes, want 16", len(b))
	}
	return binary.LittleEndian.Uint64(b[0:8]), binary.LittleEndian.Uint64(b[8:16]), nil
}

func encodeEntry(e Entry) []byte {
	b := make([]byte, 0, 16+len(e.Data))
	b = binary.LittleEndian.AppendUint64(b, e.Index)
	b = binary.LittleEndian.AppendUint64(b, e.Term)
	return append(b, e.Data...)
}

func decodeEntry(b []byte) (Entry, error) {
	if len(b) < 16 {
		return Entry{}, fmt.Errorf("entry record is %d bytes, want at least 16", len(b))
	}
	e := Entry{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
	}
	if len(b) > 16 {
		e.Data = b[16:]
	}
	return e, nil
}
