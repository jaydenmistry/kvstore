package kv

import (
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
