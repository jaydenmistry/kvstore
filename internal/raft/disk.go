package raft

import (
	"encoding/binary"
	"fmt"
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
)

// DiskStorage is a Storage backed by a write-ahead log in a directory.
//
// Each Save is one WAL Append: a hard state record and one record per entry,
// in a single crash-atomic frame with a single fsync. Recovery replays the
// records in order. An entry record at index i first discards any entries at
// or after i, so replay reproduces both appends and conflict truncations
// exactly as Save performed them.
//
// The WAL grows without bound until log compaction is implemented.
type DiskStorage struct {
	mu  sync.Mutex
	w   *wal.WAL
	hs  HardState
	log []Entry
}

var _ Storage = (*DiskStorage)(nil)

// OpenDiskStorage opens or creates the storage in dir, which must exist.
func OpenDiskStorage(fsys vfs.FS, dir string) (*DiskStorage, error) {
	w, recs, err := wal.Open(fsys, filepath.Join(dir, "raft.wal"))
	if err != nil {
		return nil, err
	}
	s := &DiskStorage{w: w}
	if err := s.replay(recs); err != nil {
		w.Close()
		return nil, err
	}
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
		case recEntry:
			e, err := decodeEntry(r.Data)
			if err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			if err := checkSplice(len(s.log), []Entry{e}); err != nil {
				return fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			s.log = spliceLog(s.log, []Entry{e})
		default:
			return fmt.Errorf("raft: wal record %d: unknown type %d", i, r.Type)
		}
	}
	return nil
}

func (s *DiskStorage) Load() (HardState, []Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, slices.Clone(s.log), nil
}

func (s *DiskStorage) Save(hs *HardState, entries []Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Validate before writing, so a bad call never reaches the disk.
	if err := checkSplice(len(s.log), entries); err != nil {
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
	s.log = spliceLog(s.log, entries)
	if hs != nil {
		s.hs = *hs
	}
	return nil
}

// Close closes the underlying log.
func (s *DiskStorage) Close() error {
	return s.w.Close()
}

func encodeHardState(hs HardState) []byte {
	b := make([]byte, 0, 16)
	b = binary.LittleEndian.AppendUint64(b, hs.Term)
	return binary.LittleEndian.AppendUint64(b, hs.Vote)
}

func decodeHardState(b []byte) (HardState, error) {
	if len(b) != 16 {
		return HardState{}, fmt.Errorf("hard state record is %d bytes, want 16", len(b))
	}
	return HardState{
		Term: binary.LittleEndian.Uint64(b[0:8]),
		Vote: binary.LittleEndian.Uint64(b[8:16]),
	}, nil
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
