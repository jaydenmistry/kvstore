package kv

import (
	"bytes"
	"errors"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// applier feeds commands to a state machine as consecutive log entries.
type applier struct {
	t  *testing.T
	sm *StateMachine
}

func newApplier(t *testing.T) *applier { return &applier{t, NewStateMachine()} }

func (a *applier) apply(c Command) Result {
	a.t.Helper()
	res, err := a.sm.Apply(raft.Entry{Index: a.sm.Applied() + 1, Term: 1, Data: c.Encode()})
	if err != nil {
		a.t.Fatal(err)
	}
	return res
}

func put(client, seq uint64, key, value string) Command {
	return Command{Op: OpPut, ClientID: client, Seq: seq, Key: key, Value: []byte(value)}
}

func del(client, seq uint64, key string) Command {
	return Command{Op: OpDelete, ClientID: client, Seq: seq, Key: key}
}

func (a *applier) want(key, value string, found bool) {
	a.t.Helper()
	v, ok := a.sm.Get(key)
	if ok != found || string(v) != value {
		a.t.Fatalf("Get(%q) = %q, %v; want %q, %v", key, v, ok, value, found)
	}
}

func TestPutGetDelete(t *testing.T) {
	a := newApplier(t)
	a.want("k", "", false)
	a.apply(put(1, 1, "k", "v1"))
	a.want("k", "v1", true)
	a.apply(put(1, 2, "k", "v2"))
	a.want("k", "v2", true)
	if res := a.apply(del(1, 3, "k")); !res.Found {
		t.Fatal("delete of existing key reported not found")
	}
	a.want("k", "", false)
	if res := a.apply(del(1, 4, "k")); res.Found {
		t.Fatal("delete of missing key reported found")
	}
}

func TestDuplicateIsNotReapplied(t *testing.T) {
	// The client's first Put was committed twice, the second copy after its
	// next Put. Applying the copy would roll the key back to v1.
	a := newApplier(t)
	a.apply(put(1, 1, "k", "v1"))
	a.apply(put(1, 2, "k", "v2"))
	if res := a.apply(put(1, 1, "k", "v1")); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("stale duplicate: %+v, want ErrStaleSequence", res)
	}
	a.want("k", "v2", true)
	a.apply(put(1, 2, "k", "v2"))
	a.want("k", "v2", true)
}

func TestDuplicateReturnsOriginalResult(t *testing.T) {
	// A retried Delete must report what the original did, not what
	// re-evaluating it now would say.
	a := newApplier(t)
	a.apply(put(1, 1, "k", "v"))
	if res := a.apply(del(2, 1, "k")); !res.Found {
		t.Fatal("delete reported not found")
	}
	if res := a.apply(del(2, 1, "k")); !res.Found || res.Err != nil {
		t.Fatalf("retried delete: %+v, want the original result (found)", res)
	}
}

func TestDuplicateOfOneClientDoesNotAffectAnother(t *testing.T) {
	a := newApplier(t)
	a.apply(put(1, 5, "k", "a"))
	a.apply(put(2, 5, "k", "b")) // same seq, different client: not a duplicate
	a.want("k", "b", true)
}

func TestSequenceNumbersMayHaveGaps(t *testing.T) {
	// A client may give up on a write and move on, so seq 2 is never seen.
	a := newApplier(t)
	a.apply(put(1, 1, "k", "a"))
	a.apply(put(1, 3, "k", "b"))
	a.want("k", "b", true)
}

func TestNoopOnlyAdvancesApplied(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(raft.Entry{Index: 1, Term: 1}); err != nil {
		t.Fatal(err)
	}
	if sm.Applied() != 1 || len(sm.data) != 0 {
		t.Fatalf("no-op changed state: applied %d, %d keys", sm.Applied(), len(sm.data))
	}
}

func TestApplyRejectsOutOfOrderEntries(t *testing.T) {
	sm := NewStateMachine()
	for _, i := range []uint64{0, 2} {
		if _, err := sm.Apply(raft.Entry{Index: i, Term: 1}); err == nil {
			t.Fatalf("applied entry %d first", i)
		}
	}
}

func TestApplyRejectsMalformedEntry(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(raft.Entry{Index: 1, Term: 1, Data: []byte("junk")}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("err = %v, want ErrInvalid", err)
	}
	if sm.Applied() != 0 {
		t.Fatal("malformed entry counted as applied")
	}
}

func restored(t *testing.T, sm *StateMachine) *StateMachine {
	t.Helper()
	out := NewStateMachine()
	if err := out.Restore(raft.Snapshot{Index: sm.Applied(), Term: 1, Data: sm.Snapshot()}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestSnapshotRoundTrip(t *testing.T) {
	a := newApplier(t)
	a.apply(put(1, 1, "k", "v"))
	a.apply(put(1, 2, "empty", ""))
	a.apply(put(2, 1, string([]byte{0, 0xff, '/'}), "binary"))
	a.apply(put(2, 2, "gone", "x"))
	a.apply(del(2, 3, "gone"))
	b := &applier{t, restored(t, a.sm)}
	if b.sm.Applied() != a.sm.Applied() {
		t.Fatalf("restored applied %d, want %d", b.sm.Applied(), a.sm.Applied())
	}
	b.want("k", "v", true)
	b.want("empty", "", true)
	b.want(string([]byte{0, 0xff, '/'}), "binary", true)
	b.want("gone", "", false)
	// Applying continues from the snapshot's index.
	b.apply(put(1, 3, "k", "w"))
	b.want("k", "w", true)
}

func TestRestoredStateMachineStillDetectsDuplicates(t *testing.T) {
	// The sessions are in the snapshot: a retried Delete restored from it
	// returns the original result, and an old write stays stale.
	a := newApplier(t)
	a.apply(put(1, 1, "k", "v"))
	a.apply(del(1, 2, "k"))
	b := &applier{t, restored(t, a.sm)}
	if res := b.apply(del(1, 2, "k")); !res.Found || res.Err != nil {
		t.Fatalf("retried delete after restore: %+v, want the original result (found)", res)
	}
	if res := b.apply(put(1, 1, "k", "v")); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("old write after restore: %+v, want ErrStaleSequence", res)
	}
	b.want("k", "", false)
}

func TestSnapshotIsDeterministic(t *testing.T) {
	// Map iteration order is random in Go; the encoding must not be.
	a := newApplier(t)
	for i := range uint64(50) {
		a.apply(put(i%7+1, i/7+1, string(rune('a'+i%26)), "v"))
	}
	first := a.sm.Snapshot()
	for range 10 {
		if !bytes.Equal(a.sm.Snapshot(), first) {
			t.Fatal("snapshot bytes differ between calls")
		}
	}
	if !bytes.Equal(restored(t, a.sm).Snapshot(), first) {
		t.Fatal("restored state machine snapshots differently")
	}
}

func TestRestoreRejectsMalformedSnapshot(t *testing.T) {
	a := newApplier(t)
	a.apply(put(1, 1, "key", "value"))
	good := a.sm.Snapshot()
	for name, data := range map[string][]byte{
		"empty":     nil,
		"version":   append([]byte{9}, good[1:]...),
		"truncated": good[:len(good)-1],
		"trailing":  append(bytes.Clone(good), 0),
	} {
		sm := NewStateMachine()
		if err := sm.Restore(raft.Snapshot{Index: 1, Term: 1, Data: data}); err == nil {
			t.Errorf("%s: restored", name)
		}
		if sm.Applied() != 0 {
			t.Errorf("%s: a failed restore changed the state", name)
		}
	}
}

func FuzzRestore(f *testing.F) {
	sm := NewStateMachine()
	c := put(1, 1, "k", "v")
	if _, err := sm.Apply(raft.Entry{Index: 1, Term: 1, Data: c.Encode()}); err != nil {
		f.Fatal(err)
	}
	f.Add(sm.Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		NewStateMachine().Restore(raft.Snapshot{Index: 1, Term: 1, Data: b}) // must not panic
	})
}
