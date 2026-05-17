package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func encodeAll(recs ...Record) []byte {
	var b []byte
	for _, r := range recs {
		b = appendRecord(b, r)
	}
	return b
}

func TestRecordRoundTrip(t *testing.T) {
	for _, r := range []Record{
		{Type: 1, Data: nil},
		{Type: 2, Data: []byte("x")},
		{Type: 255, Data: bytes.Repeat([]byte{0xab}, 4096)},
	} {
		b := appendRecord(nil, r)
		if len(b) != headerSize+len(r.Data) {
			t.Fatalf("encoded length %d, want %d", len(b), headerSize+len(r.Data))
		}
		got, n, err := decodeRecord(b)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if n != len(b) || got.Type != r.Type || !bytes.Equal(got.Data, r.Data) {
			t.Fatalf("decoded %+v (n=%d), want %+v (n=%d)", got, n, r, len(b))
		}
	}
}

func TestDecodedDataCannotClobberNextRecord(t *testing.T) {
	b := encodeAll(Record{Type: 1, Data: []byte("ab")}, Record{Type: 1, Data: []byte("cd")})
	recs, _, err := decodeAll(b)
	if err != nil {
		t.Fatal(err)
	}
	_ = append(recs[0].Data, "XXXXXXXXXXXXXXXX"...)
	if string(recs[1].Data) != "cd" {
		t.Fatalf("appending to one record's Data changed the next: %q", recs[1].Data)
	}
}

func TestDecodeRecordErrors(t *testing.T) {
	good := appendRecord(nil, Record{Type: 1, Data: []byte("payload")})

	flipped := bytes.Clone(good)
	flipped[len(flipped)-1] ^= 0x01

	huge := bytes.Clone(good)
	binary.LittleEndian.PutUint32(huge[4:8], MaxRecordSize+1)

	// A record whose checksum is valid but whose type is zero cannot have
	// been written by Append, so it must be rejected.
	zeroType := appendRecord(nil, Record{Type: 1})
	zeroType[8] = 0
	binary.LittleEndian.PutUint32(zeroType[0:4], crcOf(zeroType[4:]))

	for _, tc := range []struct {
		name string
		b    []byte
		want error
	}{
		{"empty", nil, errShort},
		{"short header", good[:headerSize-1], errShort},
		{"short payload", good[:len(good)-1], errShort},
		{"bit flip", flipped, errBadCRC},
		{"length too large", huge, errBadSize},
		{"length max uint32", append([]byte{0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff}, 1), errBadSize},
		{"all zeros", make([]byte, 64), errBadCRC},
		{"zero type", zeroType, errBadType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := decodeRecord(tc.b); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestDecodeAllStopsAtTornTail(t *testing.T) {
	b := encodeAll(Record{Type: 1, Data: []byte("a")}, Record{Type: 1, Data: []byte("b")})
	boundary := len(b)
	b = append(b, appendRecord(nil, Record{Type: 1, Data: []byte("torn")})[:7]...)
	recs, valid, err := decodeAll(b)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 2 || valid != boundary {
		t.Fatalf("got %d records, valid=%d; want 2, %d", len(recs), valid, boundary)
	}
}

func TestDecodeAllReportsMidLogCorruption(t *testing.T) {
	first := appendRecord(nil, Record{Type: 1, Data: []byte("first")})
	b := encodeAll(Record{Type: 1, Data: []byte("first")}, Record{Type: 1, Data: []byte("second")}, Record{Type: 1, Data: []byte("third")})
	b[len(first)+headerSize] ^= 0xff // damage the second record's payload
	recs, valid, err := decodeAll(b)
	var ce *CorruptError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v, want *CorruptError", err)
	}
	if ce.Offset != int64(len(first)) || !errors.Is(ce.Reason, errBadCRC) {
		t.Fatalf("CorruptError = %+v", ce)
	}
	if len(recs) != 1 || valid != len(first) {
		t.Fatalf("got %d records, valid=%d; want 1, %d", len(recs), valid, len(first))
	}
}

func crcOf(b []byte) uint32 {
	return crc32.Checksum(b, castagnoli)
}

// FuzzDecodeAll checks that for arbitrary bytes the decoder never panics, the
// valid prefix is exactly the encoding of the returned records, and the
// torn-versus-corrupt decision matches its definition.
func FuzzDecodeAll(f *testing.F) {
	valid := encodeAll(Record{Type: 1, Data: []byte("a")}, Record{Type: 2, Data: []byte("bc")})
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
		if enc := encodeAll(recs...); !bytes.Equal(enc, b[:n]) {
			t.Fatalf("records do not re-encode to the valid prefix")
		}
		_, laterValid := findRecord(b, n+1)
		var ce *CorruptError
		switch {
		case err == nil:
			if n < len(b) && laterValid {
				t.Fatalf("tail treated as torn but a valid record follows")
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
