package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"os"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/vfs"
)

// testBatches groups distinct records into batches of varying sizes.
func testBatches() [][]Record {
	r := testRecords(7)
	return [][]Record{{r[0]}, {r[1], r[2]}, {r[3], r[4], r[5]}, {r[6]}}
}

// completeBatches returns how many batches fit entirely within the first
// cut bytes, and where the last of them ends.
func completeBatches(ends []int, cut int) (n, boundary int) {
	for n < len(ends) && ends[n] <= cut {
		boundary = ends[n]
		n++
	}
	return n, boundary
}

func TestTornWriteAtEveryOffset(t *testing.T) {
	// Cut the log at every byte. Recovery must return exactly the batches
	// that fit entirely before the cut, truncate the rest, and leave a log
	// that accepts new appends.
	batches := testBatches()
	full := encodeFrames(t, batches...)
	ends := frameEnds(batches)
	extra := Record{Type: 7, Data: []byte("appended after recovery")}

	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		for cut := 0; cut <= len(full); cut++ {
			writeRaw(t, fsys, path, full[:cut])
			n, boundary := completeBatches(ends, cut)
			want := flatten(batches[:n])

			w, got := mustOpen(t, fsys, path)
			checkRecords(t, got, want)
			if size := len(readRaw(t, fsys, path)); size != boundary {
				t.Fatalf("cut %d: file size after recovery = %d, want %d", cut, size, boundary)
			}
			mustAppend(t, w, extra)
			w, got = reopen(t, w, fsys, path)
			checkRecords(t, got, append(want, extra))
			w.Close()
		}
	})
}

func TestHoleInFinalBatchIsTorn(t *testing.T) {
	// Out-of-order page writeback can lose any part of the final batch while
	// later parts of it survive. Wherever the hole is, recovery must discard
	// the whole final batch and keep everything before it.
	batches := testBatches()
	full := encodeFrames(t, batches...)
	ends := frameEnds(batches)
	lastStart := ends[len(ends)-2]
	want := flatten(batches[:len(batches)-1])

	check := func(t *testing.T, fsys vfs.FS, path string, b []byte) {
		t.Helper()
		writeRaw(t, fsys, path, b)
		w, got, err := Open(fsys, path)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		w.Close()
		checkRecords(t, got, want)
		if size := len(readRaw(t, fsys, path)); size != lastStart {
			t.Fatalf("file size after recovery = %d, want %d", size, lastStart)
		}
	}

	m := vfs.NewMem()
	for _, holeLen := range []int{1, 4, 16} {
		for start := lastStart; start+holeLen <= len(full); start++ {
			b := bytes.Clone(full)
			clear(b[start : start+holeLen])
			if bytes.Equal(b, full) {
				continue // the bytes were already zero: no damage
			}
			check(t, m, "/data/wal", b)
		}
	}
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		b := bytes.Clone(full)
		clear(b[lastStart+frameHeaderSize : lastStart+frameHeaderSize+8])
		check(t, fsys, path, b)
	})
}

func TestJunkTailIsTruncated(t *testing.T) {
	batches := testBatches()
	good := encodeFrames(t, batches...)
	want := flatten(batches)
	rng := rand.New(rand.NewPCG(1, 2))
	random := func(n int) []byte {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte(rng.Uint32())
		}
		return b
	}
	// A header claiming a 4 GiB body must be treated as torn without trying
	// to read or allocate that much.
	hugeLength := binary.LittleEndian.AppendUint32(make([]byte, 4), 0xffffffff)
	hugeLength = append(hugeLength, 1, 'x', 'y')

	tails := map[string][]byte{
		"1 zero":       make([]byte, 1),
		"8 zeros":      make([]byte, frameHeaderSize),
		"4096 zeros":   make([]byte, 4096),
		"7 random":     random(7),
		"100 random":   random(100),
		"4096 random":  random(4096),
		"huge length":  hugeLength,
		"max length+1": binary.LittleEndian.AppendUint32(make([]byte, 4), MaxBatchSize+1),
	}
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		for name, tail := range tails {
			writeRaw(t, fsys, path, append(bytes.Clone(good), tail...))
			w, got := mustOpen(t, fsys, path)
			w.Close()
			if !equalRecords(got, want) {
				t.Fatalf("%s: got %s", name, fmtRecords(got))
			}
			if size := len(readRaw(t, fsys, path)); size != len(good) {
				t.Fatalf("%s: file size = %d, want %d", name, size, len(good))
			}
		}
	})
}

func TestBitFlipInLastBatchIsTreatedAsTorn(t *testing.T) {
	// Damage confined to the final batch is indistinguishable from a torn
	// write, so it is discarded. See the package docs for why this is the
	// best a single log can do.
	batches := testBatches()
	full := encodeFrames(t, batches...)
	ends := frameEnds(batches)
	want := flatten(batches[:len(batches)-1])
	m := vfs.NewMem()
	for bit := ends[len(ends)-2] * 8; bit < len(full)*8; bit++ {
		b := bytes.Clone(full)
		b[bit/8] ^= 1 << (bit % 8)
		writeRaw(t, m, "/data/wal", b)
		w, got := mustOpen(t, m, "/data/wal")
		w.Close()
		if !equalRecords(got, want) {
			t.Fatalf("bit %d: got %s", bit, fmtRecords(got))
		}
	}
}

func TestBitFlipInEarlierBatchIsCorrupt(t *testing.T) {
	// Damage followed by a valid frame cannot be a torn write. Open must
	// refuse to start and must not modify the file, so an operator can
	// inspect or repair it.
	batches := testBatches()
	full := encodeFrames(t, batches...)
	ends := frameEnds(batches)
	m := vfs.NewMem()
	for bit := 0; bit < ends[len(ends)-2]*8; bit++ {
		b := bytes.Clone(full)
		b[bit/8] ^= 1 << (bit % 8)
		writeRaw(t, m, "/data/wal", b)

		_, _, err := Open(m, "/data/wal")
		var ce *CorruptError
		if !errors.As(err, &ce) {
			t.Fatalf("bit %d: err = %v, want *CorruptError", bit, err)
		}
		_, start := completeBatches(ends, bit/8)
		if ce.Offset != int64(start) {
			t.Fatalf("bit %d: CorruptError.Offset = %d, want %d", bit, ce.Offset, start)
		}
		if !bytes.Equal(readRaw(t, m, "/data/wal"), b) {
			t.Fatalf("bit %d: Open modified a corrupt file", bit)
		}
	}
}

func TestCorruptOnRealFS(t *testing.T) {
	forEachFS(t, func(t *testing.T, fsys vfs.FS, path string) {
		b := encodeFrames(t, testBatches()...)
		b[frameHeaderSize] ^= 0xff // first frame's body
		writeRaw(t, fsys, path, b)
		if _, _, err := Open(fsys, path); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("err = %v, want ErrCorrupt", err)
		}
		if !bytes.Equal(readRaw(t, fsys, path), b) {
			t.Fatal("Open modified a corrupt file")
		}
	})
}

func TestIOFailureMakesWALUnusable(t *testing.T) {
	for _, op := range []vfs.Op{vfs.OpWrite, vfs.OpSync} {
		t.Run(op.String(), func(t *testing.T) {
			m := vfs.NewMem()
			w, _ := mustOpen(t, m, "/data/wal")
			first := Record{Type: 1, Data: []byte("acked")}
			mustAppend(t, w, first)

			errBoom := errors.New("boom")
			m.SetFault(func(o vfs.Op, _ string) error {
				if o == op {
					return errBoom
				}
				return nil
			})
			if err := w.Append(Record{Type: 1, Data: []byte("failed")}); !errors.Is(err, errBoom) {
				t.Fatalf("Append: err = %v, want injected error", err)
			}

			// Even once the fault is gone, the WAL must not touch the file:
			// after a failed fsync we no longer know what is on disk.
			var ops int
			m.SetFault(func(vfs.Op, string) error { ops++; return nil })
			if err := w.Append(Record{Type: 1, Data: []byte("later")}); !errors.Is(err, errBoom) {
				t.Fatalf("Append after failure: err = %v, want the original error", err)
			}
			if ops != 0 {
				t.Fatalf("Append after failure performed %d I/O operations", ops)
			}
			m.SetFault(nil)

			// The acknowledged record survives. The failed one may or may
			// not, since it was never acknowledged.
			m.Crash(rand.New(rand.NewPCG(1, 0)))
			w2, got := mustOpen(t, m, "/data/wal")
			defer w2.Close()
			if len(got) < 1 || !equalRecords(got[:1], []Record{first}) {
				t.Fatalf("after crash: got %s", fmtRecords(got))
			}
		})
	}
}

func TestRecoveryTruncationFailure(t *testing.T) {
	// If recovery cannot durably truncate a torn tail, Open must fail rather
	// than append after the garbage. A later Open must still recover.
	batches := testBatches()
	torn := append(encodeFrames(t, batches...), 0xde, 0xad)
	for _, op := range []vfs.Op{vfs.OpTruncate, vfs.OpSync} {
		t.Run(op.String(), func(t *testing.T) {
			for seed := range uint64(20) {
				m := vfs.NewMem()
				writeRaw(t, m, "/data/wal", torn)
				m.SetFault(func(o vfs.Op, _ string) error {
					if o == op {
						return errors.New("boom")
					}
					return nil
				})
				if _, _, err := Open(m, "/data/wal"); err == nil {
					t.Fatal("Open succeeded despite failed truncation")
				}
				m.SetFault(nil)
				m.Crash(rand.New(rand.NewPCG(seed, 0)))
				w, got := mustOpen(t, m, "/data/wal")
				checkRecords(t, got, flatten(batches))
				w.Close()
			}
		})
	}
}

func TestRecoveredRecordsAreDurable(t *testing.T) {
	// A process killed between writing a batch and fsyncing it leaves the
	// batch in the page cache. If the next Open returns it, it must also be
	// durable: Raft may acknowledge it to a leader, which would count it
	// toward a commit.
	a := []Record{{Type: 1, Data: []byte("synced")}}
	b := []Record{{Type: 1, Data: []byte("written but never synced")}}
	for seed := range uint64(50) {
		m := vfs.NewMem()
		w, _ := mustOpen(t, m, "/data/wal")
		mustAppend(t, w, a...)
		f, err := m.OpenFile("/data/wal", os.O_WRONLY|os.O_APPEND, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(encodeFrames(t, b)); err != nil {
			t.Fatal(err)
		}
		// The process dies here. A new one opens the log.
		_, got := mustOpen(t, m, "/data/wal")
		checkRecords(t, got, append(a, b...))

		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		_, got = mustOpen(t, m, "/data/wal")
		checkRecords(t, got, append(a, b...))
	}
}

func TestAppendIsAtomicAcrossCrashes(t *testing.T) {
	// A batch spanning many pages, crashed before it is synced, must come
	// back either complete or not at all, however much of it reached disk.
	first := []Record{{Type: 1, Data: []byte("acked")}}
	firstSize := frameEnds([][]Record{first})[0]
	batch := testRecords(30)
	torn := 0
	for seed := range uint64(300) {
		m := vfs.NewMem()
		m.SetPageSize(16)
		w, _ := mustOpen(t, m, "/data/wal")
		mustAppend(t, w, first...)
		m.SetFault(func(op vfs.Op, _ string) error {
			if op == vfs.OpSync {
				return errors.New("crash before fsync")
			}
			return nil
		})
		w.Append(batch...)
		m.SetFault(nil)
		m.Crash(rand.New(rand.NewPCG(seed, 0)))
		if len(readRaw(t, m, "/data/wal")) > firstSize {
			torn++ // part of the batch reached disk
		}

		_, got := mustOpen(t, m, "/data/wal")
		if !equalRecords(got, first) && !equalRecords(got, append(first, batch...)) {
			t.Fatalf("seed %d: partial batch recovered: %s", seed, fmtRecords(got))
		}
	}
	if torn < 100 {
		t.Fatalf("only %d of 300 crashes left part of the batch on disk; test is not exercising torn writes", torn)
	}
}

func TestCrashRecoveryProperty(t *testing.T) {
	// Random batches interrupted by crashes and I/O failures, with page
	// sizes small enough that one batch spans several pages written back in
	// any order. After every recovery:
	//   - every acknowledged batch is present,
	//   - the recovered records are exactly the first k batches attempted,
	//     for some k: nothing reordered, invented, or partially applied, and
	//   - recovery never reports corruption, since none was injected.
	const seeds = 300
	pageSizes := []int{16, 64, vfs.DefaultPageSize}
	for seed := range uint64(seeds) {
		rng := rand.New(rand.NewPCG(seed, 99))
		m := vfs.NewMem()
		m.SetPageSize(pageSizes[seed%uint64(len(pageSizes))])
		var attempted [][]Record
		acked := 0 // batches

		for round := 0; round < 10; round++ {
			w, got, err := Open(m, "/data/wal")
			if err != nil {
				t.Fatalf("seed %d round %d: Open: %v", seed, round, err)
			}
			k := -1
			for i := acked; i <= len(attempted); i++ {
				if equalRecords(got, flatten(attempted[:i])) {
					k = i
					break
				}
			}
			if k < 0 {
				t.Fatalf("seed %d round %d: %d of %d batches acked, recovered %s",
					seed, round, acked, len(attempted), fmtRecords(got))
			}
			// Whatever recovery returned is now durable and is the log.
			attempted, acked = attempted[:k], k

			// Optionally make one future write or sync fail.
			failAfter := -1
			if rng.IntN(3) == 0 {
				failAfter = rng.IntN(10)
			}
			calls := 0
			m.SetFault(func(op vfs.Op, _ string) error {
				if op != vfs.OpWrite && op != vfs.OpSync {
					return nil
				}
				calls++
				if calls-1 == failAfter {
					return errors.New("injected")
				}
				return nil
			})

			for range rng.IntN(8) {
				batch := randomBatch(rng)
				attempted = append(attempted, batch)
				if err := w.Append(batch...); err != nil {
					break
				}
				acked = len(attempted)
			}
			m.SetFault(nil)

			switch rng.IntN(3) {
			case 0: // power loss
				m.Crash(rng)
			case 1: // process killed: the page cache survives
			case 2: // clean shutdown
				w.Close()
			}
		}
	}
}

func randomBatch(rng *rand.Rand) []Record {
	batch := make([]Record, 1+rng.IntN(4))
	for i := range batch {
		data := make([]byte, rng.IntN(100))
		for j := range data {
			data[j] = byte(rng.Uint32())
		}
		batch[i] = Record{Type: uint8(1 + rng.IntN(255)), Data: data}
	}
	return batch
}
