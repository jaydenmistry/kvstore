package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk record layout, all integers little-endian:
//
//	| crc u32 | length u32 | type u8 | payload [length]byte |
//
// crc is CRC-32C (Castagnoli) over length, type, and payload. Covering the
// length means a corrupted length is detected instead of silently making the
// decoder read the wrong bytes.
const (
	headerSize = 9

	// MaxRecordSize bounds a record's payload. Besides keeping records sane,
	// it lets the decoder reject a garbage length without trusting it.
	MaxRecordSize = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one entry in the log. Type is chosen by the caller and must be
// nonzero; zero is reserved so that a zero-filled region never decodes as a
// valid record.
type Record struct {
	Type uint8
	Data []byte
}

var (
	errShort   = errors.New("record extends past end of data")
	errBadSize = errors.New("record length exceeds MaxRecordSize")
	errBadCRC  = errors.New("record checksum mismatch")
	errBadType = errors.New("record type is zero")
)

// ErrCorrupt matches any *CorruptError via errors.Is.
var ErrCorrupt = errors.New("wal: corrupt log")

// CorruptError reports damage in the middle of the log: an invalid record
// followed later by a valid one. A crash can only tear the final write, so
// this means acknowledged data was damaged after the fact (bad disk, bit rot,
// or a bug). Recovery refuses to continue rather than discard it.
type CorruptError struct {
	Offset    int64 // where the invalid record starts
	NextValid int64 // where the next valid record was found
	Reason    error // why the record at Offset is invalid
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal: corrupt record at offset %d (%v), valid record follows at offset %d",
		e.Offset, e.Reason, e.NextValid)
}

func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }

func validate(r Record) error {
	if r.Type == 0 {
		return errBadType
	}
	if len(r.Data) > MaxRecordSize {
		return errBadSize
	}
	return nil
}

// appendRecord appends the encoding of r to dst. r must be valid.
func appendRecord(dst []byte, r Record) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, 0) // crc placeholder
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.Data)))
	dst = append(dst, r.Type)
	dst = append(dst, r.Data...)
	crc := crc32.Checksum(dst[start+4:], castagnoli)
	binary.LittleEndian.PutUint32(dst[start:], crc)
	return dst
}

// decodeRecord decodes the record at the start of b and returns it with its
// encoded length. The returned Data aliases b.
func decodeRecord(b []byte) (Record, int, error) {
	if len(b) < headerSize {
		return Record{}, 0, errShort
	}
	length := binary.LittleEndian.Uint32(b[4:8])
	if length > MaxRecordSize {
		return Record{}, 0, errBadSize
	}
	end := headerSize + int(length)
	if end > len(b) {
		return Record{}, 0, errShort
	}
	if crc32.Checksum(b[4:end], castagnoli) != binary.LittleEndian.Uint32(b[0:4]) {
		return Record{}, 0, errBadCRC
	}
	if b[8] == 0 {
		return Record{}, 0, errBadType
	}
	// The three-index slice caps capacity so appending to Data can never
	// overwrite the next record in b.
	return Record{Type: b[8], Data: b[headerSize:end:end]}, end, nil
}

// decodeAll decodes records from the start of b and returns them along with
// the length of the valid prefix.
//
// Decoding stops at the first invalid record. If no valid record can be found
// anywhere after that point, the damage is confined to the tail and is
// treated as a torn write: the caller may truncate b to valid. If a valid
// record does appear later, the damage is in the middle of the log, which a
// crash cannot cause, so decodeAll returns a *CorruptError and the caller
// must not truncate.
func decodeAll(b []byte) (recs []Record, valid int, err error) {
	for valid < len(b) {
		r, n, err := decodeRecord(b[valid:])
		if err != nil {
			if off, ok := findRecord(b, valid+1); ok {
				return recs, valid, &CorruptError{Offset: int64(valid), NextValid: int64(off), Reason: err}
			}
			return recs, valid, nil
		}
		recs = append(recs, r)
		valid += n
	}
	return recs, valid, nil
}

// findRecord returns the offset of the first position at or after start where
// a valid record decodes.
//
// This is a byte-by-byte scan, but it is cheap in practice: most garbage is
// rejected by the length check before any checksum is computed.
func findRecord(b []byte, start int) (int, bool) {
	for off := start; off+headerSize <= len(b); off++ {
		if _, _, err := decodeRecord(b[off:]); err == nil {
			return off, true
		}
	}
	return 0, false
}
