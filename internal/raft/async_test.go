package raft

import (
	"fmt"
	"testing"
)

// asyncTestStorage makes a testStorage an AsyncStorage whose background
// writes finish only when the test calls finish, or when a Save or
// SaveSnapshot waits for them.
type asyncTestStorage struct {
	*testStorage
	pending [][]Entry
}

func newAsyncNode(t *testing.T, id uint64, peers ...uint64) (*Raft, *asyncTestStorage) {
	t.Helper()
	st := &asyncTestStorage{testStorage: &testStorage{MemoryStorage: NewMemoryStorage(), ev: &events{}}}
	r, err := New(Config{
		ID: id, Peers: peers, ElectionTick: testElection, HeartbeatTick: testHeartbeat,
		Storage: st, Transport: recordingTransport{st.ev},
	})
	if err != nil {
		t.Fatal(err)
	}
	return r, st
}

func (s *asyncTestStorage) SaveAsync(entries []Entry) {
	s.ev.log = append(s.ev.log, fmt.Sprintf("start entries %d-%d", entries[0].Index, entries[len(entries)-1].Index))
	s.pending = append(s.pending, entries)
}

// finish writes the pending entries and returns the position to report.
func (s *asyncTestStorage) finish(t *testing.T) (index, term uint64) {
	t.Helper()
	for _, ents := range s.pending {
		if err := s.testStorage.Save(nil, ents); err != nil {
			t.Fatal(err)
		}
		index, term = ents[len(ents)-1].Index, ents[len(ents)-1].Term
	}
	s.pending = nil
	return index, term
}

func (s *asyncTestStorage) Save(hs *HardState, entries []Entry) error {
	for _, ents := range s.pending {
		if err := s.testStorage.Save(nil, ents); err != nil {
			return err
		}
	}
	s.pending = nil
	return s.testStorage.Save(hs, entries)
}

func (s *asyncTestStorage) SaveSnapshot(snap Snapshot) error {
	if err := s.Save(nil, nil); err != nil {
		return err
	}
	return s.testStorage.SaveSnapshot(snap)
}

func appAck(from, to, term, match uint64) Message {
	return Message{Type: MsgAppResp, From: from, To: to, Term: term, Success: true, MatchIndex: match}
}

func TestLeaderSendsBeforeItsOwnWriteIsDurable(t *testing.T) {
	r, st := newAsyncNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	want := []string{"save term 1 vote 1", "send MsgVote to 2 term 1", "send MsgVote to 3 term 1",
		"start entries 1-1", "send MsgApp to 2 term 1", "send MsgApp to 3 term 1"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
	// One follower has the no-op, but the leader's own copy is not durable
	// yet: one durable copy of three is not a majority.
	mustStep(t, r, appAck(2, 1, 1, 1))
	if r.commit != 0 {
		t.Fatalf("committed %d with one durable copy", r.commit)
	}
	r.Persisted(st.finish(t))
	if r.commit != 1 {
		t.Fatalf("commit %d after the leader's write finished, want 1", r.commit)
	}
}

func TestFollowersCommitBeforeLeaderWriteFinishes(t *testing.T) {
	// Thesis §10.2.1: a majority of durable copies is enough, whether or
	// not one of them is the leader's.
	r, _ := newAsyncNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	mustStep(t, r, appAck(2, 1, 1, 1))
	mustStep(t, r, appAck(3, 1, 1, 1))
	if r.commit != 1 {
		t.Fatalf("commit %d with both followers durable, want 1", r.commit)
	}
}

func TestSingleNodeCommitsWhenWriteFinishes(t *testing.T) {
	r, st := newAsyncNode(t, 1, 1)
	tickUntilCampaign(t, r)
	r.Persisted(st.finish(t))
	// A report past the end of the log cannot be about this leader's
	// entries. Taking it would also hide the real reports that follow.
	r.Persisted(10, r.term)
	if _, _, err := r.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if r.commit != 1 {
		t.Fatalf("commit %d before the write finished, want 1 (the no-op)", r.commit)
	}
	r.Persisted(st.finish(t))
	if r.commit != 2 {
		t.Fatalf("commit %d after the write finished, want 2", r.commit)
	}
}

func TestStalePersistedReportIsIgnored(t *testing.T) {
	// Node 1 leads term 1 and writes entries 2 and 3 in the background.
	// The writes finish, but the report is still on its way when node 2's
	// term 2 replaces them. Node 1 then leads term 3 and starts writing
	// its no-op at index 3. The stale report names index 3, but the entry
	// there now is the no-op, which is not durable: counting it, one
	// follower's copy would wrongly make a majority.
	r, st := newAsyncNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	r.Persisted(st.finish(t))
	if _, _, err := r.Propose([]byte("a"), []byte("b")); err != nil {
		t.Fatal(err)
	}
	staleIndex, staleTerm := st.finish(t)

	mustStep(t, r, app(2, 1, 2, 1, 1, 0, ents(2, 2)))
	checkLog(t, r, []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2, Data: []byte("2@2")}})
	makeLeader(t, r)
	if r.term != 3 || r.lastIndex() != 3 {
		t.Fatalf("term %d last index %d, want 3 and 3", r.term, r.lastIndex())
	}

	r.Persisted(staleIndex, staleTerm)
	mustStep(t, r, appAck(2, 1, 3, 3))
	if r.commit == 3 {
		t.Fatal("committed index 3 counting the leader's copy, which is not durable")
	}
	r.Persisted(st.finish(t))
	if r.commit != 3 {
		t.Fatalf("commit %d after the leader's write finished, want 3", r.commit)
	}
}

func TestPersistedAfterSteppingDown(t *testing.T) {
	// A leader that steps down within its term, as CheckQuorum makes it
	// do, can still receive the report of a write it started.
	r, st := newAsyncNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	r.becomeFollower(r.term, None)
	r.Persisted(st.finish(t))
	if r.commit != 0 {
		t.Fatalf("follower committed %d from its own write's report", r.commit)
	}
}
