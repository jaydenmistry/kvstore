package raft

import (
	"errors"
	"fmt"
	"testing"
)

// leaderWithLog returns node 1 of a three-node cluster as leader in term 2,
// with entries 1-5 from term 1 and its no-op at 6, all committed and
// applied: node 2 has acknowledged everything, node 3 nothing.
func leaderWithLog(t *testing.T) (*Raft, *testStorage) {
	t.Helper()
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1))
	makeLeader(t, r)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 6})
	if r.commit != 6 {
		t.Fatalf("commit %d, want 6", r.commit)
	}
	r.TakeCommitted()
	takeSent(st)
	return r, st
}

func TestCompactTrimsLogAndPersists(t *testing.T) {
	r, st := leaderWithLog(t)
	if err := r.Compact(4, []byte("state@4")); err != nil {
		t.Fatal(err)
	}
	if s := r.Status(); s.Snapshot != 4 || s.LastIndex != 6 || s.Commit != 6 {
		t.Fatalf("status %+v, want snapshot 4, last 6, commit 6", s)
	}
	checkLog(t, r, append(ents(5, 1), Entry{Index: 6, Term: 2}))
	if _, ok := r.Entry(4); ok {
		t.Fatal("compacted entry 4 still readable")
	}
	_, s, log, _ := st.MemoryStorage.Load()
	if s.Index != 4 || s.Term != 1 || string(s.Data) != "state@4" || len(log) != 2 {
		t.Fatalf("stored snapshot %d@%d %q with %d entries after it", s.Index, s.Term, s.Data, len(log))
	}
	// Compacting copies the data.
	if string(r.snapshot.Data) != "state@4" {
		t.Fatalf("snapshot data %q", r.snapshot.Data)
	}
}

func TestCompactRejectsUnappliedOrOldIndex(t *testing.T) {
	r, _ := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1))
	// Nothing is committed yet, so nothing can be compacted.
	if err := r.Compact(1, nil); err == nil {
		t.Fatal("compacted an entry that was not applied")
	}
	r2, _ := leaderWithLog(t)
	if err := r2.Compact(4, nil); err != nil {
		t.Fatal(err)
	}
	for _, i := range []uint64{3, 4, 7} {
		if err := r2.Compact(i, nil); err == nil {
			t.Fatalf("Compact(%d) succeeded with snapshot at 4 and applied 6", i)
		}
	}
	if r2.err != nil {
		t.Fatalf("a rejected Compact stopped the node: %v", r2.err)
	}
}

func TestLeaderSendsSnapshotWhenEntriesAreGone(t *testing.T) {
	r, st := leaderWithLog(t)
	if err := r.Compact(6, []byte("state@6")); err != nil {
		t.Fatal(err)
	}
	// Node 3 rejects the leader's first probe; it has nothing.
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, RejectIndex: 5, HintIndex: 1})
	m := onlyMessage(t, st)
	if m.Type != MsgSnap || m.Snapshot.Index != 6 || m.Snapshot.Term != 2 || string(m.Snapshot.Data) != "state@6" {
		t.Fatalf("sent %+v, want the snapshot at 6", m)
	}
	// The next heartbeat carries entries after the snapshot, not the
	// snapshot again.
	if r.next[3] != 7 {
		t.Fatalf("next %d, want 7", r.next[3])
	}
	// Once the snapshot is acknowledged, node 3 is caught up.
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 2, Success: true, MatchIndex: 6})
	if r.match[3] != 6 {
		t.Fatalf("match %d, want 6", r.match[3])
	}
}

func snapMsg(from, to, term uint64, s Snapshot) Message {
	return Message{Type: MsgSnap, From: from, To: to, Term: term, Snapshot: s}
}

func TestFollowerInstallsSnapshot(t *testing.T) {
	// Node 2 has a log from a deposed leader that conflicts with the
	// snapshot, which ends at index 6 in term 2.
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1))
	takeSent(st)
	mustStep(t, r, snapMsg(1, 2, 2, snap(6, 2)))
	want := []string{"save term 2 vote 0", "save snapshot 6@2", "send MsgAppResp to 1 term 2"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 6 {
		t.Fatalf("reply %+v, want success with MatchIndex 6", m)
	}
	checkLog(t, r, nil)
	if s := r.Status(); s.Commit != 6 || s.LastIndex != 6 || s.Snapshot != 6 || s.Lead != 1 {
		t.Fatalf("status %+v", s)
	}
	got := r.TakeSnapshot()
	if got == nil || got.Index != 6 || string(got.Data) != "state@6" {
		t.Fatalf("TakeSnapshot = %+v", got)
	}
	if r.TakeSnapshot() != nil || r.TakeCommitted() != nil {
		t.Fatal("snapshot or entries returned twice")
	}
	// Replication continues after the snapshot.
	mustStep(t, r, app(1, 2, 2, 6, 2, 7, ents(7, 2)))
	if es := r.TakeCommitted(); len(es) != 1 || es[0].Index != 7 {
		t.Fatalf("after the snapshot, applied %+v, want entry 7", es)
	}
}

func TestFollowerKeepsEntriesAfterMatchingSnapshot(t *testing.T) {
	// The follower holds the snapshot's last entry (5, term 1), so entries
	// 6-8 follow it in the leader's log too, and it may have acknowledged
	// them. They must be kept.
	log := ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1, 6, 1, 7, 1, 8, 1)
	r, _ := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, log)
	mustStep(t, r, snapMsg(1, 2, 1, snap(5, 1)))
	checkLog(t, r, ents(6, 1, 7, 1, 8, 1))
	if s := r.Status(); s.Commit != 5 || s.LastIndex != 8 {
		t.Fatalf("status %+v, want commit 5, last 8", s)
	}
}

func TestFollowerIgnoresStaleSnapshot(t *testing.T) {
	// A delayed snapshot covering less than we have committed must not move
	// the state machine backward.
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1))
	mustStep(t, r, app(1, 2, 1, 5, 1, 5, nil))
	r.TakeCommitted()
	takeSent(st)
	mustStep(t, r, snapMsg(1, 2, 1, snap(3, 1)))
	if hasEvent(st, "save snapshot") || r.TakeSnapshot() != nil {
		t.Fatalf("installed a stale snapshot: %q", st.ev.log)
	}
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 3 {
		t.Fatalf("reply %+v, want success with MatchIndex 3", m)
	}
	checkLog(t, r, ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1))
}

func TestAppendStartingInsideSnapshot(t *testing.T) {
	// A delayed AppendEntries whose PrevLogIndex falls inside our snapshot.
	// The covered entries are committed, so they match; the rest is
	// appended as usual.
	r, st := newNode(t, 2, 1, 2, 3)
	mustStep(t, r, snapMsg(1, 2, 1, snap(5, 1)))
	r.TakeSnapshot()
	takeSent(st)
	mustStep(t, r, app(1, 2, 1, 2, 1, 5, ents(3, 1, 4, 1, 5, 1, 6, 1, 7, 1)))
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 7 {
		t.Fatalf("reply %+v, want success with MatchIndex 7", m)
	}
	checkLog(t, r, ents(6, 1, 7, 1))
	// One that ends inside the snapshot still succeeds, claiming the
	// snapshot.
	mustStep(t, r, app(1, 2, 1, 1, 1, 5, ents(2, 1)))
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 5 {
		t.Fatalf("reply %+v, want success with MatchIndex 5", m)
	}
}

func TestRestartFromSnapshot(t *testing.T) {
	st := &testStorage{MemoryStorage: NewMemoryStorage(), ev: &events{}}
	st.MemoryStorage.Save(&HardState{Term: 2}, ents(1, 1, 2, 1, 3, 2))
	st.MemoryStorage.SaveSnapshot(snap(2, 1))
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, st)
	// Everything the snapshot covers is committed; what follows is not
	// known to be.
	if s := r.Status(); s.Commit != 2 || s.Snapshot != 2 || s.LastIndex != 3 {
		t.Fatalf("status %+v, want commit 2, snapshot 2, last 3", s)
	}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("TakeCommitted with a snapshot pending did not panic")
			}
		}()
		r.TakeCommitted()
	}()
	if s := r.TakeSnapshot(); s == nil || s.Index != 2 || string(s.Data) != "state@2" {
		t.Fatalf("TakeSnapshot = %+v", s)
	}
	// The candidate's last log entry is the one after the snapshot.
	tickUntilCampaign(t, r)
	if m := takeSent(st)[0]; m.LastLogIndex != 3 || m.LastLogTerm != 2 {
		t.Fatalf("vote request %+v, want last log 3@2", m)
	}
}

func TestCandidateWithOnlySnapshotReportsItsLastEntry(t *testing.T) {
	st := &testStorage{MemoryStorage: NewMemoryStorage(), ev: &events{}}
	st.MemoryStorage.Save(&HardState{Term: 3}, ents(1, 1, 2, 3))
	st.MemoryStorage.SaveSnapshot(snap(2, 3))
	r := newTestRaft(t, 1, []uint64{1, 2, 3}, st)
	r.TakeSnapshot()
	tickUntilCampaign(t, r)
	if m := takeSent(st)[0]; m.LastLogIndex != 2 || m.LastLogTerm != 3 {
		t.Fatalf("vote request %+v, want last log 2@3 from the snapshot", m)
	}
}

func TestSnapshotReplyEchoesRound(t *testing.T) {
	r, st := newNode(t, 2, 1, 2, 3)
	m := snapMsg(1, 2, 1, snap(5, 1))
	m.Round = 7
	mustStep(t, r, m)
	if resp := onlyMessage(t, st); resp.Round != 7 {
		t.Fatalf("reply %+v does not echo round 7", resp)
	}
}

func TestSnapshotFromOlderTermRefused(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 3, nil)
	mustStep(t, r, snapMsg(1, 2, 2, snap(5, 2)))
	if m := onlyMessage(t, st); m.Success || m.Term != 3 {
		t.Fatalf("reply %+v, want a refusal carrying term 3", m)
	}
	if r.TakeSnapshot() != nil {
		t.Fatal("installed a snapshot from an older term")
	}
}

func TestSnapshotSaveFailureStopsNode(t *testing.T) {
	r, st := newNode(t, 2, 1, 2, 3)
	mustStep(t, r, app(1, 2, 1, 0, 0, 0, nil))
	takeSent(st)
	st.fail = errors.New("disk full")
	if err := r.Step(snapMsg(1, 2, 1, snap(5, 1))); err == nil {
		t.Fatal("Step returned nil after the snapshot could not be saved")
	}
	if len(st.ev.sent) != 0 {
		t.Fatalf("replied %+v without having saved the snapshot", st.ev.sent)
	}
	if r.TakeSnapshot() != nil {
		t.Fatal("unsaved snapshot handed to the application")
	}
}
