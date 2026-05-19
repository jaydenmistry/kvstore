package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

// encodeFrames encodes each batch as one frame, as successive Appends would.
func encodeFrames(t testing.TB, batches ...[]Record) []byte {
	t.Helper()
	var b []byte
	for _, batch := range batches {
		var err error
		if b, err = appendFrame(b, batch); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// frameEnds returns the byte offset at which each batch's frame ends.
func frameEnds(batches [][]Record) []int {
	ends := make([]int, len(batches))
	n := 0
	for i, batch := range batches {
		n += frameHeaderSize
		for _, r := range batch {
			n += recordHeaderSize + len(r.Data)
		}
		ends[i] = n
	}
	return ends
}

func flatten(batches [][]Record) []Record {
	var recs []Record
	for _, b := range batches {
		recs = append(recs, b...)
	}
	return recs
}

// frameWithBody builds a frame with a correct checksum around an arbitrary
// body, to test what happens when the checksum passes but the body is bad.
func frameWithBody(body []byte) []byte {
	b := binary.LittleEndian.AppendUint32(nil, 0)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(body)))
	b = append(b, body...)
	binary.LittleEndian.PutUint32(b, crc32.Checksum(b[4:], castagnoli))
	return b
}

func TestFrameRoundTrip(t *testing.T) {
	for _, batch := range [][]Record{
		{{Type: 1}},
		{{Type: 2, Data: []byte("x")}, {Type: 3}, {Type: 4, Data: []byte("yz")}},
		{{Type: 255, Data: bytes.Repeat([]byte{0xab}, 4096)}},
	} {
		b := encodeFrames(t, batch)
		if want := frameEnds([][]Record{batch})[0]; len(b) != want {
			t.Fatalf("encoded length %d, want %d", len(b), want)
		}
		got, n, err := decodeFrame(b)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if n != len(b) {
			t.Fatalf("decoded length %d, want %d", n, len(b))
		}
		checkRecords(t, got, batch)
	}
}

func TestAppendFrameValidation(t *testing.T) {
	if _, err := appendFrame(nil, []Record{{Type: 1}, {Type: 0}}); !errors.Is(err, errBadType) {
		t.Fatalf("type 0: err = %v, want errBadType", err)
	}
	limit := []Record{{Type: 1, Data: make([]byte, MaxBatchSize-recordHeaderSize)}}
	if _, err := appendFrame(nil, limit); err != nil {
		t.Fatalf("batch of exactly MaxBatchSize: %v", err)
	}
	over := []Record{{Type: 1, Data: make([]byte, MaxBatchSize-recordHeaderSize)}, {Type: 1}}
	if _, err := appendFrame(nil, over); !errors.Is(err, errTooBig) {
		t.Fatalf("batch over MaxBatchSize: err = %v, want errTooBig", err)
	}
}

func TestDecodedDataCannotClobberNextRecord(t *testing.T) {
	b := encodeFrames(t, []Record{{Type: 1, Data: []byte("ab")}, {Type: 1, Data: []byte("cd")}})
	recs, _, err := decodeAll(b)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(recs[0].Data, "XXXXXXXXXXXXXXXX"...)
	if string(recs[1].Data) != "cd" {
		t.Fatalf("appending to one record's Data changed the next: %q", recs[1].Data)
	}
}

func TestDecodeFrameErrors(t *testing.T) {
	good := encodeFrames(t, []Record{{Type: 1, Data: []byte("payload")}})

	flipped := bytes.Clone(good)
	flipped[len(flipped)-1] ^= 0x01

	withLength := func(n uint32) []byte {
		b := bytes.Clone(good)
		binary.LittleEndian.PutUint32(b[4:8], n)
		return b
	}

	for _, tc := range []struct {
		name string
		b    []byte
		want error
	}{
		{"empty", nil, errShort},
		{"short header", good[:frameHeaderSize-1], errShort},
		{"short body", good[:len(good)-1], errShort},
		{"bit flip", flipped, errBadCRC},
		{"zero length", withLength(0), errBadSize},
		{"length below one record", withLength(recordHeaderSize - 1), errBadSize},
		{"length above max", withLength(MaxBatchSize + 1), errBadSize},
		{"length max uint32", withLength(0xffffffff), errBadSize},
		{"all zeros", make([]byte, 64), errBadSize},
		// The checksum passes but the body cannot have come from appendFrame.
		{"record overruns body", frameWithBody([]byte{1, 10, 0, 0, 0, 'x'}), errBadBody},
		{"trailing partial record", frameWithBody([]byte{1, 0, 0, 0, 0, 1, 0}), errBadBody},
		{"zero record type", frameWithBody([]byte{0, 0, 0, 0, 0}), errBadBody},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := decodeFrame(tc.b); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeAllStopsAtTornFrame(t *testing.T) {
	first := []Record{{Type: 1, Data: []byte("a")}}
	b := encodeFrames(t, first)
	boundary := len(b)
	b = append(b, encodeFrames(t, []Record{{Type: 1, Data: []byte("torn")}})[:10]...)
	recs, valid, err := decodeAll(b)
	if err != nil {
		t.Fatal(err)
	}
	checkRecords(t, recs, first)
	if valid != boundary {
		t.Fatalf("valid = %d, want %d", valid, boundary)
	}
}

func TestDecodeAllHoleInFinalFrameIsTorn(t *testing.T) {
	// The case that motivated frames: pages written back out of order leave
	// a hole in the final batch with intact records after it. With one
	// checksum per record those records would look like mid-log corruption.
	first := []Record{{Type: 1, Data: []byte("synced")}}
	last := []Record{
		{Type: 1, Data: bytes.Repeat([]byte("a"), 20)},
		{Type: 1, Data: bytes.Repeat([]byte("b"), 20)},
		{Type: 1, Data: bytes.Repeat([]byte("c"), 20)},
	}
	b := encodeFrames(t, first, last)
	boundary := frameEnds([][]Record{first})[0]
	clear(b[boundary+frameHeaderSize : boundary+frameHeaderSize+20]) // lose part of "aaa..."

	recs, valid, err := decodeAll(b)
	if err != nil {
		t.Fatalf("hole in final frame reported as corruption: %v", err)
	}
	checkRecords(t, recs, first)
	if valid != boundary {
		t.Fatalf("valid = %d, want %d", valid, boundary)
	}
}

func TestDecodeAllReportsMidLogCorruption(t *testing.T) {
	batches := [][]Record{
		{{Type: 1, Data: []byte("first")}},
		{{Type: 1, Data: []byte("second")}, {Type: 2, Data: []byte("second-b")}},
		{{Type: 1, Data: []byte("third")}},
	}
	b := encodeFrames(t, batches...)
	ends := frameEnds(batches)
	b[ends[0]+frameHeaderSize+recordHeaderSize] ^= 0xff // damage the second frame's body
	recs, valid, err := decodeAll(b)
	var ce *CorruptError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want *CorruptError", err)
	}
	if ce.Offset != int64(ends[0]) || ce.NextValid != int64(ends[1]) || !errors.Is(ce.Reason, errBadCRC) {
		t.Fatalf("CorruptError = %+v", ce)
	}
	checkRecords(t, recs, batches[0])
	if valid != ends[0] {
		t.Fatalf("valid = %d, want %d", valid, ends[0])
	}
}

// FuzzDecodeAll checks that for arbitrary bytes the decoder never panics, the
// valid prefix is exactly a sequence of frames that re-encode to the same
// bytes, and the torn-versus-corrupt decision matches its definition.
func FuzzDecodeAll(f *testing.F) {
	valid := encodeFrames(f, []Record{{Type: 1, Data: []byte("a")}}, []Record{{Type: 2, Data: []byte("bc")}, {Type: 3}})
	f.Add([]byte{})
	f.Add(valid)
	f.Add(valid[:len(valid)-1])
	f.Add(append(bytes.Clone(valid), make([]byte, 32)...))
	f.Add(append(make([]byte, 5), valid...))
	f.Fuzz(func(t *testing.T, b []byte) {
		recs, n, err := decodeAll(b)
		if n < 0 || n > len(b) {
			t.Fatalf("valid prefix %d out of range [0, %d]", n, len(b))
		}
		var all []Record
		for off := 0; off < n; {
			frame, size, err := decodeFrame(b[off:n])
			if err != nil {
				t.Fatalf("valid prefix does not decode as frames at offset %d: %v", off, err)
			}
			enc, err := appendFrame(nil, frame)
			if err != nil || !bytes.Equal(enc, b[off:off+size]) {
				t.Fatalf("frame at offset %d does not re-encode to the same bytes", off)
			}
			all = append(all, frame...)
			off += size
		}
		if !equalRecords(all, recs) {
			t.Fatalf("returned records differ from the frames in the valid prefix")
		}

		_, laterValid := findFrame(b, n+1)
		var ce *CorruptError
		switch {
		case err == nil:
			if n < len(b) && laterValid {
				t.Fatalf("tail treated as torn but a valid frame follows")
			}
		case errors.As(err, &ce):
			if ce.Offset != int64(n) || !laterValid || ce.NextValid <= ce.Offset {
				t.Fatalf("inconsistent CorruptError %+v (valid=%d)", ce, n)
			}
		default:
			t.Fatalf("unexpected error type: %v", err)
		}
	})
}
