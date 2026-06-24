package kv

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestCommandRoundTrip(t *testing.T) {
	for _, c := range []Command{
		{Op: OpPut, ClientID: 1, Seq: 1, Key: "k", Value: []byte("v")},
		{Op: OpPut, ClientID: 1<<64 - 1, Seq: 1<<64 - 1, Key: "key/with/slashes", Value: nil},
		{Op: OpPut, ClientID: 2, Seq: 3, Key: string([]byte{0, 0xff}), Value: bytes.Repeat([]byte{0}, MaxValueSize)},
		{Op: OpDelete, ClientID: 9, Seq: 4, Key: strings.Repeat("k", MaxKeySize)},
	} {
		got, err := DecodeCommand(c.Encode())
		if err != nil {
			t.Fatalf("decode %v %q: %v", c.Op, c.Key, err)
		}
		if got.Op != c.Op || got.ClientID != c.ClientID || got.Seq != c.Seq || got.Key != c.Key || !bytes.Equal(got.Value, c.Value) {
			t.Fatalf("round trip changed %+v into %+v", c, got)
		}
	}
}

func TestDecodeRejectsMalformed(t *testing.T) {
	good := (&Command{Op: OpPut, ClientID: 1, Seq: 1, Key: "key", Value: []byte("v")}).Encode()
	cases := map[string][]byte{
		"empty":        nil,
		"short header": good[:commandHeaderSize-1],
		"key past end": good[:commandHeaderSize+2],
		"get op":       append([]byte{byte(OpGet)}, good[1:]...),
		"unknown op":   append([]byte{99}, good[1:]...),
		"zero client":  (&Command{Op: OpPut, Seq: 1, Key: "k"}).Encode(),
		"zero seq":     (&Command{Op: OpPut, ClientID: 1, Key: "k"}).Encode(),
		"empty key":    (&Command{Op: OpPut, ClientID: 1, Seq: 1}).Encode(),
		"delete value": (&Command{Op: OpDelete, ClientID: 1, Seq: 1, Key: "k", Value: []byte("v")}).Encode(),
		"long key":     (&Command{Op: OpPut, ClientID: 1, Seq: 1, Key: strings.Repeat("k", MaxKeySize+1)}).Encode(),
		"long value":   (&Command{Op: OpPut, ClientID: 1, Seq: 1, Key: "k", Value: make([]byte, MaxValueSize+1)}).Encode(),
	}
	for name, b := range cases {
		if _, err := DecodeCommand(b); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}
}

func TestDecodedValueDoesNotGrowIntoBuffer(t *testing.T) {
	// Value aliases the encoded bytes. Appending to it must copy, not
	// write past the end of the buffer it came from.
	b := (&Command{Op: OpPut, ClientID: 1, Seq: 1, Key: "k", Value: []byte("v")}).Encode()
	c, err := DecodeCommand(b)
	if err != nil {
		t.Fatal(err)
	}
	if cap(c.Value) != len(c.Value) {
		t.Fatalf("value has spare capacity %d", cap(c.Value)-len(c.Value))
	}
}

func FuzzDecodeCommand(f *testing.F) {
	f.Add((&Command{Op: OpPut, ClientID: 1, Seq: 1, Key: "k", Value: []byte("v")}).Encode())
	f.Add((&Command{Op: OpDelete, ClientID: 7, Seq: 2, Key: "key"}).Encode())
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := DecodeCommand(b)
		if err != nil {
			return
		}
		// Anything accepted must re-encode to the same bytes.
		if got := c.Encode(); !bytes.Equal(got, b) {
			t.Fatalf("decoded %+v re-encodes to %x, not %x", c, got, b)
		}
	})
}
