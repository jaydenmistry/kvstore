package kv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

// register opens a session with room for max sessions and returns its
// client ID.
func (a *applier) register(max uint64) uint64 {
	a.t.Helper()
	res := a.apply(Command{Op: OpRegister, MaxSessions: max})
	if res.ClientID != a.sm.Applied() {
		a.t.Fatalf("registered client %d at index %d", res.ClientID, a.sm.Applied())
	}
	return res.ClientID
}

// client opens a session in a state machine with room for plenty.
func (a *applier) client() uint64 { return a.register(100) }

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
	c := a.client()
	a.want("k", "", false)
	a.apply(put(c, 1, "k", "v1"))
	a.want("k", "v1", true)
	a.apply(put(c, 2, "k", "v2"))
	a.want("k", "v2", true)
	if res := a.apply(del(c, 3, "k")); !res.Found {
		t.Fatal("delete of existing key reported not found")
	}
	a.want("k", "", false)
	if res := a.apply(del(c, 4, "k")); res.Found {
		t.Fatal("delete of missing key reported found")
	}
}

func TestDuplicateIsNotReapplied(t *testing.T) {
	// The client's first Put was committed twice, the second copy after its
	// next Put. Applying the copy would roll the key back to v1.
	a := newApplier(t)
	c := a.client()
	a.apply(put(c, 1, "k", "v1"))
	a.apply(put(c, 2, "k", "v2"))
	if res := a.apply(put(c, 1, "k", "v1")); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("stale duplicate: %+v, want ErrStaleSequence", res)
	}
	a.want("k", "v2", true)
	a.apply(put(c, 2, "k", "v2"))
	a.want("k", "v2", true)
}

func TestDuplicateReturnsOriginalResult(t *testing.T) {
	// A retried Delete must report what the original did, not what
	// re-evaluating it now would say.
	a := newApplier(t)
	c1, c2 := a.client(), a.client()
	a.apply(put(c1, 1, "k", "v"))
	if res := a.apply(del(c2, 1, "k")); !res.Found {
		t.Fatal("delete reported not found")
	}
	if res := a.apply(del(c2, 1, "k")); !res.Found || res.Err != nil {
		t.Fatalf("retried delete: %+v, want the original result (found)", res)
	}
}

func TestDuplicateOfOneClientDoesNotAffectAnother(t *testing.T) {
	a := newApplier(t)
	c1, c2 := a.client(), a.client()
	a.apply(put(c1, 5, "k", "a"))
	a.apply(put(c2, 5, "k", "b")) // same seq, different client: not a duplicate
	a.want("k", "b", true)
}

func TestSequenceNumbersMayHaveGaps(t *testing.T) {
	// A client may give up on a write and move on, so seq 2 is never seen.
	a := newApplier(t)
	c := a.client()
	a.apply(put(c, 1, "k", "a"))
	a.apply(put(c, 3, "k", "b"))
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
	c1, c2 := a.client(), a.client()
	a.apply(put(c1, 1, "k", "v"))
	a.apply(put(c1, 2, "empty", ""))
	a.apply(put(c2, 1, string([]byte{0, 0xff, '/'}), "binary"))
	a.apply(put(c2, 2, "gone", "x"))
	a.apply(del(c2, 3, "gone"))
	b := &applier{t, restored(t, a.sm)}
	if b.sm.Applied() != a.sm.Applied() {
		t.Fatalf("restored applied %d, want %d", b.sm.Applied(), a.sm.Applied())
	}
	b.want("k", "v", true)
	b.want("empty", "", true)
	b.want(string([]byte{0, 0xff, '/'}), "binary", true)
	b.want("gone", "", false)
	// Applying continues from the snapshot's index.
	b.apply(put(c1, 3, "k", "w"))
	b.want("k", "w", true)
}

func TestRestoredStateMachineStillDetectsDuplicates(t *testing.T) {
	// The sessions are in the snapshot: a retried Delete restored from it
	// returns the original result, and an old write stays stale.
	a := newApplier(t)
	c := a.client()
	a.apply(put(c, 1, "k", "v"))
	a.apply(del(c, 2, "k"))
	b := &applier{t, restored(t, a.sm)}
	if res := b.apply(del(c, 2, "k")); !res.Found || res.Err != nil {
		t.Fatalf("retried delete after restore: %+v, want the original result (found)", res)
	}
	if res := b.apply(put(c, 1, "k", "v")); !errors.Is(res.Err, ErrStaleSequence) {
		t.Fatalf("old write after restore: %+v, want ErrStaleSequence", res)
	}
	b.want("k", "", false)
}

func TestSnapshotIsDeterministic(t *testing.T) {
	// Map iteration order is random in Go; the encoding must not be.
	a := newApplier(t)
	var clients []uint64
	for range 7 {
		clients = append(clients, a.client())
	}
	for i := range uint64(50) {
		a.apply(put(clients[i%7], i/7+1, string(rune('a'+i%26)), "v"))
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
	c := a.client()
	a.apply(put(c, 1, "key", "value"))
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
	for i, c := range []Command{{Op: OpRegister, MaxSessions: 10}, put(1, 1, "k", "v")} {
		if _, err := sm.Apply(raft.Entry{Index: uint64(i + 1), Term: 1, Data: c.Encode()}); err != nil {
			f.Fatal(err)
		}
	}
	f.Add(sm.Snapshot())
	f.Fuzz(func(t *testing.T, b []byte) {
		NewStateMachine().Restore(raft.Snapshot{Index: 1, Term: 1, Data: b}) // must not panic
	})
}

func TestRegisterAssignsEntryIndexAsClientID(t *testing.T) {
	a := newApplier(t)
	a.apply(put(1, 1, "k", "v")) // refused: no session yet
	if c := a.client(); c != 2 {
		t.Fatalf("client ID %d, want the register entry's index 2", c)
	}
}

func TestWriteWithoutSessionIsRefused(t *testing.T) {
	// The state machine cannot tell whether such a write is a retry of one
	// it applied before the session was evicted, so it must not apply it.
	a := newApplier(t)
	if res := a.apply(put(42, 1, "k", "v")); !errors.Is(res.Err, ErrSessionExpired) {
		t.Fatalf("write from unknown client: %+v, want ErrSessionExpired", res)
	}
	a.want("k", "", false)
}

func TestLeastRecentlyUsedSessionIsEvicted(t *testing.T) {
	a := newApplier(t)
	c1, c2, c3 := a.register(3), a.register(3), a.register(3)
	a.apply(put(c1, 1, "k", "c1")) // c1 is now the most recently used
	c4 := a.register(3)            // evicts c2, the least recently used
	if res := a.apply(put(c2, 1, "k", "c2")); !errors.Is(res.Err, ErrSessionExpired) {
		t.Fatalf("write from evicted client: %+v, want ErrSessionExpired", res)
	}
	a.want("k", "c1", true)
	for _, c := range []uint64{c1, c3, c4} {
		if res := a.apply(put(c, 2, "k", "v")); res.Err != nil {
			t.Fatalf("client %d: %+v", c, res)
		}
	}
	if len(a.sm.sessions) != 3 {
		t.Fatalf("%d sessions, want 3", len(a.sm.sessions))
	}
}

func TestEvictedRetryIsNotReapplied(t *testing.T) {
	// The write is applied, the response is lost, and the session is
	// evicted before the client retries. Applying the retry would roll
	// the key back over another client's later write.
	a := newApplier(t)
	c1 := a.register(2)
	a.apply(put(c1, 1, "k", "old"))
	c2 := a.register(2)
	a.apply(put(c2, 1, "k", "new"))
	a.register(2) // evicts c1
	if res := a.apply(put(c1, 1, "k", "old")); !errors.Is(res.Err, ErrSessionExpired) {
		t.Fatalf("retry after eviction: %+v, want ErrSessionExpired", res)
	}
	a.want("k", "new", true)
}

func TestSessionLimitComesFromTheLog(t *testing.T) {
	// Each register entry carries its own limit, so a smaller one evicts
	// down to it at once.
	a := newApplier(t)
	for range 5 {
		a.register(10)
	}
	a.register(2)
	if len(a.sm.sessions) != 2 {
		t.Fatalf("%d sessions after registering with limit 2, want 2", len(a.sm.sessions))
	}
}

func TestRestoredStateMachineEvictsTheSame(t *testing.T) {
	// The last-used indexes are in the snapshot, so a replica restored
	// from it evicts the same sessions as one that applied the log.
	a := newApplier(t)
	var clients []uint64
	for range 4 {
		clients = append(clients, a.register(4))
	}
	for i, c := range []uint64{clients[2], clients[0], clients[3]} {
		a.apply(put(c, uint64(i+1), "k", "v"))
	}
	b := &applier{t, restored(t, a.sm)}
	a.register(4)
	b.register(4)
	if fmt.Sprint(a.sm.sessions) != fmt.Sprint(b.sm.sessions) {
		t.Fatalf("sessions differ after restore and register:\n%v\n%v", a.sm.sessions, b.sm.sessions)
	}
	if _, ok := a.sm.sessions[clients[1]]; ok {
		t.Fatal("evicted a session other than the least recently used")
	}
}

func TestRestoresVersion1Snapshot(t *testing.T) {
	// Version 1 has no last-used indexes. Its sessions restore as least
	// recently used, and ties between them break by client ID, so every
	// replica restored from it evicts the same ones.
	v1 := []byte{1}
	v1 = binary.BigEndian.AppendUint64(v1, 1) // one key
	v1 = binary.BigEndian.AppendUint32(v1, 1)
	v1 = append(v1, 'k')
	v1 = binary.BigEndian.AppendUint32(v1, 1)
	v1 = append(v1, 'v')
	v1 = binary.BigEndian.AppendUint64(v1, 3) // three sessions
	for _, c := range []uint64{7, 3, 9} {
		v1 = binary.BigEndian.AppendUint64(v1, c)
		v1 = binary.BigEndian.AppendUint64(v1, 1)
		v1 = append(v1, 0)
	}
	sm := NewStateMachine()
	if err := sm.Restore(raft.Snapshot{Index: 10, Term: 1, Data: v1}); err != nil {
		t.Fatal(err)
	}
	a := &applier{t, sm}
	a.want("k", "v", true)
	a.register(3)
	if _, ok := sm.sessions[3]; ok || len(sm.sessions) != 3 {
		t.Fatalf("sessions %v; want client 3 evicted first", sm.sessions)
	}
}
