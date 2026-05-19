package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

// On-disk layout. The file is a sequence of frames, one per Append call.
// All integers are little-endian.
//
//	frame:  | crc u32 | length u32 | body [length]byte |
//	body:   one or more records
//	record: | type u8 | size u32 | data [size]byte |
//
// crc is CRC-32C (Castagnoli) over length and body. Covering the length means
// a corrupted length is detected instead of silently making the decoder read
// the wrong bytes. Records inside a frame have no checksum of their own: the
// frame is the unit of both integrity checking and crash atomicity.
//
// Why frames rather than a checksum per record: the kernel writes dirty
// pages back in no particular order, so a crash during an Append can leave a
// hole in the middle of what it wrote with later bytes intact. With one
// checksum per record, the valid records after such a hole would look like
// mid-log corruption. Because Append fsyncs before returning, and Open
// fsyncs before any new Append, only the final frame can ever be partially
// written, so damage confined to the final frame is always safe to discard.
const (
	frameHeaderSize  = 8
	recordHeaderSize = 5

	// MaxBatchSize bounds the encoded size of one Append call. Besides
	// keeping frames sane, it lets the decoder reject a garbage length
	// without trusting it.
	MaxBatchSize = 64 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Record is one entry in the log. Type is chosen by the caller and must be
// nonzero.
type Record struct {
	Type uint8
	Data []byte
}

var (
	errShort   = errors.New("frame extends past end of data")
	errBadSize = errors.New("frame length out of range")
	errBadCRC  = errors.New("frame checksum mismatch")
	errBadBody = errors.New("frame body is malformed")
	errBadType = errors.New("record type is zero")
	errTooBig  = errors.New("batch exceeds MaxBatchSize")
)

// ErrCorrupt matches any *CorruptError via errors.Is.
var ErrCorrupt = errors.New("wal: corrupt log")

// CorruptError reports damage in the middle of the log: an invalid frame
// followed later by a valid one. A crash can only damage the final frame, so
// this means acknowledged data was damaged after the fact (bad disk, bit rot,
// or a bug). Recovery refuses to continue rather than discard it.
type CorruptError struct {
	Offset    int64 // where the invalid frame starts
	NextValid int64 // where the next valid frame was found
	Reason    error // why the frame at Offset is invalid
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal: corrupt frame at offset %d (%v), valid frame follows at offset %d",
		e.Offset, e.Reason, e.NextValid)
}

func (e *CorruptError) Is(target error) bool { return target == ErrCorrupt }

// appendFrame appends a frame holding recs to dst. recs must be nonempty.
func appendFrame(dst []byte, recs []Record) ([]byte, error) {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, 0) // crc placeholder
	dst = binary.LittleEndian.AppendUint32(dst, 0) // length placeholder
	for _, r := range recs {
		if r.Type == 0 {
			return nil, errBadType
		}
		if len(dst)-start-frameHeaderSize+recordHeaderSize+len(r.Data) > MaxBatchSize {
			return nil, errTooBig
		}
		dst = append(dst, r.Type)
		dst = binary.LittleEndian.AppendUint32(dst, uint32(len(r.Data)))
		dst = append(dst, r.Data...)
	}
	binary.LittleEndian.PutUint32(dst[start+4:], uint32(len(dst)-start-frameHeaderSize))
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst, nil
}

// decodeFrame decodes the frame at the start of b and returns its records
// and encoded length. The returned Data slices alias b.
func decodeFrame(b []byte) ([]Record, int, error) {
	if len(b) < frameHeaderSize {
		return nil, 0, errShort
	}
	// A frame holds at least one record, so a zero length (as in a
	// zero-filled region) is never valid.
	length := binary.LittleEndian.Uint32(b[4:8])
	if length < recordHeaderSize || length > MaxBatchSize {
		return nil, 0, errBadSize
	}
	end := frameHeaderSize + int(length)
	if end > len(b) {
		return nil, 0, errShort
	}
	if crc32.Checksum(b[4:end], castagnoli) != binary.LittleEndian.Uint32(b[0:4]) {
		return nil, 0, errBadCRC
	}
	// The checksum matched, so the body is exactly what some writer
	// produced. A malformed body here means a bug or a checksum collision,
	// not a torn write; it is still reported as an invalid frame.
	var recs []Record
	for off := frameHeaderSize; off < end; {
		if end-off < recordHeaderSize {
			return nil, 0, errBadBody
		}
		typ := b[off]
		size := int(binary.LittleEndian.Uint32(b[off+1 : off+5]))
		off += recordHeaderSize
		if typ == 0 || size > end-off {
			return nil, 0, errBadBody
		}
		// The three-index slice caps capacity so appending to Data can
		// never overwrite the bytes that follow it in b.
		recs = append(recs, Record{Type: typ, Data: b[off : off+size : off+size]})
		off += size
	}
	return recs, end, nil
}

// decodeAll decodes frames from the start of b and returns their records
// along with the length of the valid prefix.
//
// Decoding stops at the first invalid frame. If no valid frame can be found
// anywhere after that point, the damage is confined to the final frame and
// is treated as a torn write: the caller may truncate b to valid. If a valid
// frame does appear later, the damage is in the middle of the log, which a
// crash cannot cause, so decodeAll returns a *CorruptError and the caller
// must not truncate.
func decodeAll(b []byte) (recs []Record, valid int, err error) {
	for valid < len(b) {
		frame, n, err := decodeFrame(b[valid:])
		if err != nil {
			if off, ok := findFrame(b, valid+1); ok {
				return recs, valid, &CorruptError{Offset: int64(valid), NextValid: int64(off), Reason: err}
			}
			return recs, valid, nil
		}
		recs = append(recs, frame...)
		valid += n
	}
	return recs, valid, nil
}

// findFrame returns the offset of the first position at or after start where
// a valid frame decodes.
//
// This is a byte-by-byte scan that runs only during recovery, over the bytes
// after the damage. It is cheap in practice: most positions are rejected by
// the length check before any checksum is computed.
func findFrame(b []byte, start int) (int, bool) {
	for off := start; off+frameHeaderSize <= len(b); off++ {
		if _, _, err := decodeFrame(b[off:]); err == nil {
			return off, true
		}
	}
	return 0, false
}
