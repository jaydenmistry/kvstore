package raft

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/jaydenmistry/kvstore/internal/vfs"
	"github.com/jaydenmistry/kvstore/internal/wal"
)

// WAL record types used by DiskStorage.
const (
	recHardState uint8 = 1
)

// DiskStorage is a Storage backed by a write-ahead log in a directory.
//
// Every SaveHardState appends a record and fsyncs it. Recovery replays the
// log and keeps the last HardState. The log grows with every term change and
// vote until compaction is implemented, which is acceptable because each
// record is 16 bytes.
type DiskStorage struct {
	mu sync.Mutex
	w  *wal.WAL
	hs HardState
}

var _ Storage = (*DiskStorage)(nil)

// OpenDiskStorage opens or creates the storage in dir, which must exist.
func OpenDiskStorage(fsys vfs.FS, dir string) (*DiskStorage, error) {
	w, recs, err := wal.Open(fsys, filepath.Join(dir, "raft.wal"))
	if err != nil {
		return nil, err
	}
	s := &DiskStorage{w: w}
	for i, r := range recs {
		switch r.Type {
		case recHardState:
			hs, err := decodeHardState(r.Data)
			if err != nil {
				w.Close()
				return nil, fmt.Errorf("raft: wal record %d: %w", i, err)
			}
			s.hs = hs
		default:
			w.Close()
			return nil, fmt.Errorf("raft: wal record %d: unknown type %d", i, r.Type)
		}
	}
	return s, nil
}

func (s *DiskStorage) HardState() (HardState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hs, nil
}

func (s *DiskStorage) SaveHardState(hs HardState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.w.Append(wal.Record{Type: recHardState, Data: encodeHardState(hs)}); err != nil {
		return err
	}
	s.hs = hs
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
