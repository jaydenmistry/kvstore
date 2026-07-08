package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

// events records saves and sends in the order they happen, so tests can
// check that state is persisted before messages depending on it go out.
type events struct {
	log  []string
	sent []Message
}

type recordingTransport struct{ ev *events }

func (t recordingTransport) Send(m Message) {
	t.ev.sent = append(t.ev.sent, m)
	t.ev.log = append(t.ev.log, fmt.Sprintf("send %v to %d term %d", m.Type, m.To, m.Term))
}

// testStorage wraps MemoryStorage, recording saves and optionally failing.
type testStorage struct {
	*MemoryStorage
	ev   *events
	fail error
}

func (s *testStorage) Save(hs *HardState, entries []Entry) error {
	if s.fail != nil {
		return s.fail
	}
	if hs != nil {
		s.ev.log = append(s.ev.log, fmt.Sprintf("save term %d vote %d", hs.Term, hs.Vote))
	}
	if len(entries) > 0 {
		s.ev.log = append(s.ev.log, fmt.Sprintf("save entries %d-%d", entries[0].Index, entries[len(entries)-1].Index))
	}
	return s.MemoryStorage.Save(hs, entries)
}

func (s *testStorage) SaveSnapshot(snap Snapshot) error {
	if s.fail != nil {
		return s.fail
	}
	s.ev.log = append(s.ev.log, fmt.Sprintf("save snapshot %d@%d", snap.Index, snap.Term))
	return s.MemoryStorage.SaveSnapshot(snap)
}

const (
	testElection  = 10
	testHeartbeat = 1
)

func newTestRaft(t *testing.T, id uint64, peers []uint64, st *testStorage) *Raft {
	t.Helper()
	r, err := New(Config{
		ID:            id,
		Peers:         peers,
		ElectionTick:  testElection,
		HeartbeatTick: testHeartbeat,
		Storage:       st,
		Transport:     recordingTransport{st.ev},
		Rand:          rand.New(rand.NewPCG(uint64(id), 0)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newNode(t *testing.T, id uint64, peers ...uint64) (*Raft, *testStorage) {
	t.Helper()
	st := &testStorage{MemoryStorage: NewMemoryStorage(), ev: &events{}}
	return newTestRaft(t, id, peers, st), st
}

func mustStep(t *testing.T, r *Raft, m Message) {
	t.Helper()
	if err := r.Step(m); err != nil {
		t.Fatalf("Step(%+v): %v", m, err)
	}
}

// tickUntilCampaign ticks r until it starts an election.
func tickUntilCampaign(t *testing.T, r *Raft) {
	t.Helper()
	for range 2 * testElection {
		if err := r.Tick(); err != nil {
			t.Fatal(err)
		}
		if r.state == Candidate || r.state == Leader {
			return
		}
	}
	t.Fatal("node never started an election")
}

func takeSent(st *testStorage) []Message {
	sent := st.ev.sent
	st.ev.sent = nil
	return sent
}

func onlyMessage(t *testing.T, st *testStorage) Message {
	t.Helper()
	sent := takeSent(st)
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want 1: %+v", len(sent), sent)
	}
	return sent[0]
}

func TestConfigValidation(t *testing.T) {
	st := NewMemoryStorage()
	tr := recordingTransport{&events{}}
	good := Config{ID: 1, Peers: []uint64{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 1, Storage: st, Transport: tr}
	if _, err := New(good); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range map[string]func(*Config){
		"zero id":            func(c *Config) { c.ID = 0 },
		"id not in peers":    func(c *Config) { c.ID = 4 },
		"zero peer":          func(c *Config) { c.Peers = []uint64{1, 0} },
		"duplicate peer":     func(c *Config) { c.Peers = []uint64{1, 2, 2} },
		"zero heartbeat":     func(c *Config) { c.HeartbeatTick = 0 },
		"heartbeat >= elect": func(c *Config) { c.HeartbeatTick = 10 },
		"missing storage":    func(c *Config) { c.Storage = nil },
		"missing transport":  func(c *Config) { c.Transport = nil },
	} {
		cfg := good
		cfg.Peers = append([]uint64(nil), good.Peers...)
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: config accepted", name)
		}
	}
}

func TestElectionTimeoutIsRandomizedInRange(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3)
	seen := map[int]bool{}
	for range 200 {
		r.resetElectionTimer()
		to := r.randomizedElectionTimeout
		if to < testElection || to >= 2*testElection {
			t.Fatalf("timeout %d outside [%d, %d)", to, testElection, 2*testElection)
		}
		seen[to] = true
	}
	if len(seen) < testElection/2 {
		t.Fatalf("only %d distinct timeouts in 200 resets", len(seen))
	}
}

func TestSingleNodeElectsItself(t *testing.T) {
	r, st := newNode(t, 1, 1)
	tickUntilCampaign(t, r)
	if s := r.Status(); s.State != Leader || s.Term != 1 || s.Lead != 1 {
		t.Fatalf("status %+v, want leader in term 1", s)
	}
	if len(st.ev.sent) != 0 {
		t.Fatalf("single node sent messages: %+v", st.ev.sent)
	}
}

func TestCampaignPersistsVoteBeforeRequestingVotes(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	want := []string{
		"save term 1 vote 1",
		"send MsgVote to 2 term 1",
		"send MsgVote to 3 term 1",
	}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
}

func TestCandidateWinsWithMajority(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3, 4, 5)
	tickUntilCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if r.state != Candidate {
		t.Fatalf("leader with 2 of 5 votes")
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 3, To: 1, Term: 1, VoteGranted: true})
	if r.state != Leader {
		t.Fatalf("state %v with 3 of 5 votes, want leader", r.state)
	}
	// Leadership is announced at once.
	if sent := takeSent(st); len(sent) != 4 || sent[0].Type != MsgApp {
		t.Fatalf("new leader sent %+v, want 4 heartbeats", sent)
	}
}

func TestDuplicateVoteCountsOnce(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3, 4, 5)
	tickUntilCampaign(t, r)
	grant := Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true}
	mustStep(t, r, grant)
	mustStep(t, r, grant)
	if r.state != Candidate {
		t.Fatal("duplicated vote from one node counted twice")
	}
}

func TestRejectionsDoNotCount(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1})
	mustStep(t, r, Message{Type: MsgVoteResp, From: 3, To: 1, Term: 1})
	if r.state != Candidate {
		t.Fatalf("state %v after only rejections", r.state)
	}
}

func TestVoteFromOlderTermIgnored(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r) // term 1
	r.electionElapsed = r.randomizedElectionTimeout
	r.Tick() // times out again: term 2
	if r.term != 2 {
		t.Fatalf("term %d, want 2", r.term)
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if r.state != Candidate {
		t.Fatal("vote granted in term 1 counted toward term 2")
	}
}

func TestVotesOncePerTerm(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 5})
	if m := onlyMessage(t, st); !m.VoteGranted || m.Term != 5 {
		t.Fatalf("first vote: %+v, want granted in term 5", m)
	}
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 5})
	if m := onlyMessage(t, st); m.VoteGranted {
		t.Fatal("granted a second vote in term 5")
	}
	// The same candidate asking again (its first response was lost) gets
	// the same answer.
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 5})
	if m := onlyMessage(t, st); !m.VoteGranted {
		t.Fatal("refused to repeat the vote already cast for the same candidate")
	}
	// A new term means a new vote.
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 6})
	if m := onlyMessage(t, st); !m.VoteGranted || m.Term != 6 {
		t.Fatalf("vote in term 6: %+v, want granted", m)
	}
}

func TestVotePersistedBeforeReply(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 3})
	want := []string{"save term 3 vote 0", "save term 3 vote 2", "send MsgVoteResp to 2 term 3"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
}

func TestRestartedNodeRemembersVote(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 4})
	takeSent(st)

	// Simulate a crash and restart on the same storage.
	r = newTestRaft(t, 1, []uint64{1, 2, 3}, st)
	if s := r.Status(); s.Term != 4 || s.Vote != 2 || s.State != Follower {
		t.Fatalf("after restart: %+v, want follower in term 4 with vote for 2", s)
	}
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 4})
	if m := onlyMessage(t, st); m.VoteGranted {
		t.Fatal("restarted node voted twice in term 4")
	}
}

func TestStorageFailureStopsNode(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	st.fail = errors.New("disk on fire")
	if err := r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1}); !errors.Is(err, st.fail) {
		t.Fatalf("Step: err = %v, want storage error", err)
	}
	if len(st.ev.sent) != 0 {
		t.Fatalf("replied despite failing to persist: %+v", st.ev.sent)
	}
	if s := r.Status(); s.Term != 0 || s.Vote != None {
		t.Fatalf("in-memory state changed without being persisted: %+v", s)
	}
	// The node stays stopped even if storage recovers.
	st.fail = nil
	if err := r.Tick(); err == nil {
		t.Fatal("Tick succeeded after the node stopped")
	}
	if err := r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 1}); err == nil {
		t.Fatal("Step succeeded after the node stopped")
	}
	if len(st.ev.sent) != 0 {
		t.Fatalf("stopped node sent %+v", st.ev.sent)
	}
}

func TestCampaignStorageFailureSendsNothing(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	st.fail = errors.New("disk on fire")
	for range 2 * testElection {
		if r.Tick() != nil {
			break
		}
	}
	if r.err == nil {
		t.Fatal("node did not stop")
	}
	if len(st.ev.sent) != 0 {
		t.Fatalf("requested votes without persisting own vote: %+v", st.ev.sent)
	}
}

func TestStaleTermRequestsAreRefused(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 5})
	takeSent(st)
	r.electionElapsed = 3

	mustStep(t, r, Message{Type: MsgApp, From: 3, To: 1, Term: 4})
	if m := onlyMessage(t, st); m.Type != MsgAppResp || m.Success || m.Term != 5 {
		t.Fatalf("reply to stale MsgApp: %+v, want refusal carrying term 5", m)
	}
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 4})
	if m := onlyMessage(t, st); m.Type != MsgVoteResp || m.VoteGranted || m.Term != 5 {
		t.Fatalf("reply to stale MsgVote: %+v, want refusal carrying term 5", m)
	}
	if r.lead != 2 || r.electionElapsed != 3 {
		t.Fatalf("stale messages changed leader (%d) or reset timer (%d)", r.lead, r.electionElapsed)
	}
}

func TestStaleResponsesAreIgnored(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 5})
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 4})
	mustStep(t, r, Message{Type: MsgVoteResp, From: 3, To: 1, Term: 4, VoteGranted: true})
	if len(st.ev.sent) != 0 {
		t.Fatalf("replied to stale responses: %+v", st.ev.sent)
	}
}

func TestLeaderStepsDownOnHigherTermResponse(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if r.state != Leader {
		t.Fatal("did not become leader")
	}
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 7})
	if s := r.Status(); s.State != Follower || s.Term != 7 || s.Vote != None || s.Lead != None {
		t.Fatalf("after higher-term response: %+v, want follower in term 7, no vote, no leader", s)
	}
}

func TestLeaderStepsDownOnHigherTermVoteRequest(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	// The candidate's log must be as up to date as ours, which now holds
	// the leader's no-op at index 1.
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 2, LastLogIndex: 1, LastLogTerm: 1})
	if s := r.Status(); s.State != Follower || s.Term != 2 || s.Vote != 3 {
		t.Fatalf("after higher-term MsgVote: %+v, want follower in term 2 voting for 3", s)
	}
}

func TestCandidateStepsDownForLeaderOfSameTerm(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 1})
	if s := r.Status(); s.State != Follower || s.Term != 1 || s.Lead != 2 || s.Vote != 1 {
		t.Fatalf("after MsgApp in own term: %+v, want follower of 2, still voted for self", s)
	}
	if m := onlyMessage(t, st); m.Type != MsgAppResp || !m.Success {
		t.Fatalf("reply %+v, want successful MsgAppResp", m)
	}
}

func TestElectionTimerResets(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)

	// A heartbeat from the current leader resets the timer.
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 1})
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 1})
	r.electionElapsed = 5
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 1})
	if r.electionElapsed != 0 {
		t.Fatal("heartbeat did not reset the election timer")
	}

	// Rejecting a vote does not (Figure 2). Otherwise a node that keeps
	// asking for votes it cannot get would hold off every election.
	r.electionElapsed = 5
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 1})
	if m := takeSent(st); m[len(m)-1].VoteGranted {
		t.Fatal("granted a second vote in term 1")
	}
	if r.electionElapsed != 5 {
		t.Fatal("rejecting a vote reset the election timer")
	}

	// Granting a vote does.
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 2})
	if r.electionElapsed != 0 {
		t.Fatal("granting a vote did not reset the election timer")
	}
}

func TestLeaderSendsHeartbeats(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	tickUntilCampaign(t, r)
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	takeSent(st)
	for range 5 * testElection {
		if err := r.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if r.state != Leader {
		t.Fatal("leader stepped down without any message")
	}
	if got := len(takeSent(st)); got != 2*5*testElection/testHeartbeat {
		t.Fatalf("sent %d heartbeats, want %d", got, 2*5*testElection/testHeartbeat)
	}
}

func TestIgnoresMessagesNotForThisNode(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 9, Term: 3}) // wrong recipient
	mustStep(t, r, Message{Type: MsgVote, From: 9, To: 1, Term: 3}) // not a member
	mustStep(t, r, Message{Type: MsgVote, From: 1, To: 1, Term: 3}) // from itself
	if r.term != 0 || len(st.ev.sent) != 0 {
		t.Fatalf("acted on a message it should ignore: term %d, sent %+v", r.term, st.ev.sent)
	}
}

func TestIsUpToDate(t *testing.T) {
	for _, tc := range []struct {
		term, index, myTerm, myIndex uint64
		want                         bool
	}{
		{0, 0, 0, 0, true},
		{2, 1, 1, 9, true},  // later last term wins regardless of length
		{1, 9, 2, 1, false}, // earlier last term loses regardless of length
		{2, 5, 2, 5, true},  // identical logs
		{2, 6, 2, 5, true},  // same last term, longer
		{2, 4, 2, 5, false}, // same last term, shorter
	} {
		if got := isUpToDate(tc.term, tc.index, tc.myTerm, tc.myIndex); got != tc.want {
			t.Errorf("isUpToDate(term %d, index %d vs term %d, index %d) = %v, want %v",
				tc.term, tc.index, tc.myTerm, tc.myIndex, got, tc.want)
		}
	}
}
