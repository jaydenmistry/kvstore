package raft

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// newNodeWithLog starts a node whose storage already holds term and log, as
// after a restart.
func newNodeWithLog(t *testing.T, id uint64, peers []uint64, term uint64, log []Entry) (*Raft, *testStorage) {
	t.Helper()
	st := &testStorage{MemoryStorage: NewMemoryStorage(), ev: &events{}}
	if err := st.MemoryStorage.Save(&HardState{Term: term}, log); err != nil {
		t.Fatal(err)
	}
	return newTestRaft(t, id, peers, st), st
}

// makeLeader elects r by delivering just enough granted votes.
func makeLeader(t *testing.T, r *Raft) {
	t.Helper()
	tickUntilCampaign(t, r)
	for _, p := range r.peers {
		if r.state == Leader {
			break
		}
		if p != r.id {
			mustStep(t, r, Message{Type: MsgVoteResp, From: p, To: r.id, Term: r.term, VoteGranted: true})
		}
	}
	if r.state != Leader {
		t.Fatal("did not become leader")
	}
}

func app(from, to, term, prevIndex, prevTerm, commit uint64, entries []Entry) Message {
	return Message{
		Type: MsgApp, From: from, To: to, Term: term,
		PrevLogIndex: prevIndex, PrevLogTerm: prevTerm, LeaderCommit: commit, Entries: entries,
	}
}

func checkLog(t *testing.T, r *Raft, want []Entry) {
	t.Helper()
	if !equalEntries(r.log.ents, want) {
		t.Fatalf("log %+v, want %+v", r.log.ents, want)
	}
}

func hasEvent(st *testStorage, prefix string) bool {
	for _, e := range st.ev.log {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

func TestFollowerPersistsEntriesBeforeReplying(t *testing.T) {
	r, st := newNode(t, 2, 1, 2, 3)
	mustStep(t, r, app(1, 2, 1, 0, 0, 0, ents(1, 1, 2, 1)))
	want := []string{"save term 1 vote 0", "save entries 1-2", "send MsgAppResp to 1 term 1"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 2 {
		t.Fatalf("reply %+v, want success with MatchIndex 2", m)
	}
	checkLoad(t, st.MemoryStorage, HardState{Term: 1}, ents(1, 1, 2, 1))
}

func TestFollowerRejectsWhenMissingPrevEntry(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1))
	mustStep(t, r, app(1, 2, 1, 3, 1, 0, ents(4, 1)))
	m := onlyMessage(t, st)
	if m.Success || m.RejectIndex != 3 || m.HintIndex != 2 {
		t.Fatalf("reply %+v, want rejection of index 3 with hint 2 (our last index + 1)", m)
	}
	checkLog(t, r, ents(1, 1))
}

func TestFollowerRejectionSkipsConflictingTerm(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 2, ents(1, 1, 2, 2, 3, 2, 4, 2))
	mustStep(t, r, app(1, 2, 3, 4, 3, 0, nil))
	m := onlyMessage(t, st)
	if m.Success || m.RejectIndex != 4 || m.HintIndex != 2 {
		t.Fatalf("reply %+v, want rejection of index 4 with hint 2 (first index of term 2)", m)
	}
}

func TestFollowerReplacesConflictingSuffix(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 2, ents(1, 1, 2, 2, 3, 2))
	mustStep(t, r, app(1, 2, 3, 1, 1, 0, ents(2, 3, 3, 3)))
	checkLog(t, r, ents(1, 1, 2, 3, 3, 3))
	checkLoad(t, st.MemoryStorage, HardState{Term: 3}, ents(1, 1, 2, 3, 3, 3))
}

func TestFollowerKeepsEntriesAfterStaleAppend(t *testing.T) {
	// A delayed copy of an earlier AppendEntries covers only part of what we
	// already have. Everything matches, so nothing may be truncated: the
	// follower may already have acknowledged entries 4 and 5, and the leader
	// may have counted them toward a commit.
	log := ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1)
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, log)
	mustStep(t, r, app(1, 2, 1, 1, 1, 0, ents(2, 1, 3, 1)))
	checkLog(t, r, log)
	if hasEvent(st, "save entries") {
		t.Fatalf("rewrote entries it already had: %q", st.ev.log)
	}
	// The reply covers only what this request verified, not our whole log.
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 3 {
		t.Fatalf("reply %+v, want success with MatchIndex 3", m)
	}
}

func TestFollowerCommitLimitedToVerifiedEntries(t *testing.T) {
	// Entries 2 and 3 are not covered by this request, so they might not
	// match the leader's log. LeaderCommit of 3 must not commit them.
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1))
	mustStep(t, r, app(1, 2, 1, 1, 1, 3, nil))
	if r.commit != 1 {
		t.Fatalf("commit %d, want 1 (min of LeaderCommit and last verified index)", r.commit)
	}
	mustStep(t, r, app(1, 2, 1, 3, 1, 3, nil))
	if r.commit != 3 {
		t.Fatalf("commit %d, want 3", r.commit)
	}
	takeSent(st)
	// Commit never moves backward, even if a stale message carries an older
	// LeaderCommit.
	mustStep(t, r, app(1, 2, 1, 1, 1, 1, nil))
	if r.commit != 3 {
		t.Fatalf("commit moved back to %d", r.commit)
	}
}

func TestFollowerMergesAppendsThatContinueEachOther(t *testing.T) {
	// Three requests from the leader, queued while the follower was busy,
	// each starting where the previous one ends: one write, one reply.
	r, st := newNode(t, 2, 1, 2, 3)
	first := app(1, 2, 1, 0, 0, 0, ents(1, 1, 2, 1))
	first.Round = 1
	second := app(1, 2, 1, 2, 1, 1, ents(3, 1))
	second.Round = 2
	heartbeat := app(1, 2, 1, 3, 1, 3, nil)
	heartbeat.Round = 3
	if err := r.Step(first, second, heartbeat); err != nil {
		t.Fatal(err)
	}
	want := []string{"save term 1 vote 0", "save entries 1-3", "send MsgAppResp to 1 term 1"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
	if m := onlyMessage(t, st); !m.Success || m.MatchIndex != 3 || m.Round != 3 {
		t.Fatalf("reply %+v, want success with MatchIndex 3 and Round 3", m)
	}
	if r.commit != 3 {
		t.Fatalf("commit %d, want 3", r.commit)
	}
	checkLoad(t, st.MemoryStorage, HardState{Term: 1}, ents(1, 1, 2, 1, 3, 1))
}

func TestFollowerMergesOnlyMatchingAppends(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second Message
		check  func(t *testing.T, r *Raft, replies []Message)
	}{{
		// The second request needs entries the first does not carry.
		name:   "gap",
		second: app(1, 2, 1, 4, 1, 0, ents(5, 1)),
		check: func(t *testing.T, r *Raft, replies []Message) {
			if replies[1].Success || replies[1].RejectIndex != 4 {
				t.Fatalf("second reply %+v, want rejection at 4", replies[1])
			}
		},
	}, {
		// The second request follows index 2 with a different term, so the
		// follower's entry 2 conflicts with the leader's (§5.3). Merging
		// would append entry 3 after an entry it does not follow.
		name:   "term at previous index differs",
		second: app(1, 2, 2, 2, 2, 0, ents(3, 2)),
		check: func(t *testing.T, r *Raft, replies []Message) {
			if replies[1].Success {
				t.Fatalf("second reply %+v, want a rejection", replies[1])
			}
			checkLog(t, r, ents(1, 1, 2, 1))
		},
	}, {
		// The same leader, reelected: the follower must adopt the new
		// term, which a request merged into the old term's would hide.
		name:   "newer term",
		second: app(1, 2, 2, 2, 1, 0, ents(3, 2)),
		check: func(t *testing.T, r *Raft, replies []Message) {
			if r.term != 2 {
				t.Fatalf("term %d, want 2", r.term)
			}
			if !replies[1].Success || replies[1].Term != 2 || replies[1].MatchIndex != 3 {
				t.Fatalf("second reply %+v, want success in term 2 with MatchIndex 3", replies[1])
			}
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			r, st := newNode(t, 2, 1, 2, 3)
			if err := r.Step(app(1, 2, 1, 0, 0, 0, ents(1, 1, 2, 1)), tc.second); err != nil {
				t.Fatal(err)
			}
			replies := takeSent(st)
			if len(replies) != 2 || !replies[0].Success || replies[0].MatchIndex != 2 {
				t.Fatalf("replies %+v, want two, the first a success with MatchIndex 2", replies)
			}
			tc.check(t, r, replies)
		})
	}
}

func TestFollowerMergeStaysWithinByteLimit(t *testing.T) {
	r, st := newNode(t, 2, 1, 2, 3)
	r.maxMsgBytes = len("1@1") + len("2@1") // two of these entries, not three
	if err := r.Step(
		app(1, 2, 1, 0, 0, 0, ents(1, 1)),
		app(1, 2, 1, 1, 1, 0, ents(2, 1)),
		app(1, 2, 1, 2, 1, 0, ents(3, 1)),
	); err != nil {
		t.Fatal(err)
	}
	want := []string{"save term 1 vote 0", "save entries 1-2", "send MsgAppResp to 1 term 1", "save entries 3-3", "send MsgAppResp to 1 term 1"}
	if fmt.Sprint(st.ev.log) != fmt.Sprint(want) {
		t.Fatalf("events:\n got %q\nwant %q", st.ev.log, want)
	}
}

func TestFollowerMergeLeavesMessagesUnchanged(t *testing.T) {
	// The first request's entries have spare capacity. Merging must not
	// write the second request's entries into it: the caller, or another
	// message sharing the array, still owns it.
	r, _ := newNode(t, 2, 1, 2, 3)
	backing := make([]Entry, 2, 4)
	copy(backing, ents(1, 1, 2, 1))
	if err := r.Step(app(1, 2, 1, 0, 0, 0, backing), app(1, 2, 1, 2, 1, 0, ents(3, 1, 4, 1))); err != nil {
		t.Fatal(err)
	}
	if spare := backing[:4][2:]; spare[0].Index != 0 || spare[1].Index != 0 {
		t.Fatalf("merging wrote %+v into the first request's array", spare)
	}
	checkLog(t, r, ents(1, 1, 2, 1, 3, 1, 4, 1))
}

func TestNewLeaderPersistsNoopBeforeSending(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	e, ok := r.Entry(1)
	if !ok || e.Term != 1 || len(e.Data) != 0 {
		t.Fatalf("entry 1 = %+v, want a no-op in term 1", e)
	}
	saved := strings.Join(st.ev.log, "\n")
	if i, j := strings.Index(saved, "save entries 1-1"), strings.LastIndex(saved, "send MsgApp to"); i < 0 || j < i {
		t.Fatalf("no-op not persisted before being sent:\n%s", saved)
	}
}

func TestLeaderCommitsOnMajority(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3, 4, 5)
	makeLeader(t, r)
	idx, term, err := r.Propose([]byte("x"))
	if err != nil || idx != 2 || term != 1 {
		t.Fatalf("Propose = %d, %d, %v; want 2, 1, nil", idx, term, err)
	}
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 2})
	if r.commit != 0 {
		t.Fatalf("committed with 2 of 5 nodes")
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 1, Success: true, MatchIndex: 2})
	if r.commit != 2 {
		t.Fatalf("commit %d with 3 of 5 nodes, want 2", r.commit)
	}
	got := r.TakeCommitted()
	if len(got) != 2 || len(got[0].Data) != 0 || string(got[1].Data) != "x" {
		t.Fatalf("TakeCommitted = %+v, want the no-op then x", got)
	}
	if again := r.TakeCommitted(); again != nil {
		t.Fatalf("entries returned twice: %+v", again)
	}
}

func TestLeaderDoesNotCommitEarlierTermByCounting(t *testing.T) {
	// Figure 8. Node 1 holds entry 2 from term 2 and becomes leader in term
	// 4, after a term 3 it took no part in. Node 3 may have been term 3's
	// leader and hold a different entry 2, from term 3.
	//
	// Once node 2 stores node 1's entry 2, it is on a majority (nodes 1 and
	// 2), but it is not safe to commit. If node 1 crashed, node 3 could win
	// an election with votes from itself and node 2, because its last entry
	// (term 3) is more up to date than node 2's (term 2), and would then
	// overwrite entry 2 everywhere. Only an entry from the leader's own
	// term may be committed by counting replicas (§5.4.2).
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 3, ents(1, 1, 2, 2))
	makeLeader(t, r) // term 4, no-op at index 3
	if r.term != 4 {
		t.Fatalf("leader in term %d, want 4", r.term)
	}
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Success: true, MatchIndex: 2})
	if r.commit != 0 {
		t.Fatalf("committed index %d from term 2 by counting replicas", r.commit)
	}
	// Once the term 4 no-op is on a majority, any node that could win a
	// later election must have it, and therefore entry 2 as well (Log
	// Matching). Both commit together.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 4, Success: true, MatchIndex: 3})
	if r.commit != 3 {
		t.Fatalf("commit %d, want 3", r.commit)
	}
}

func TestStaleSuccessDoesNotLowerMatch(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	r.Propose([]byte("a"))
	r.Propose([]byte("b"))
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 3})
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 1})
	if r.match[2] != 3 || r.next[2] != 4 {
		t.Fatalf("match %d next %d after a stale success, want 3 and 4", r.match[2], r.next[2])
	}
}

func TestLeaderBacksUpOnRejection(t *testing.T) {
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1))
	makeLeader(t, r) // term 2, no-op at index 4
	if r.next[2] != 4 {
		t.Fatalf("next %d, want 4", r.next[2])
	}
	takeSent(st)

	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 3, HintIndex: 2})
	if r.next[2] != 2 {
		t.Fatalf("next %d after hint 2, want 2", r.next[2])
	}
	if m := onlyMessage(t, st); m.Type != MsgApp || m.PrevLogIndex != 1 || len(m.Entries) != 3 {
		t.Fatalf("retry %+v, want MsgApp from index 2", m)
	}

	// A duplicate of the same rejection refers to a probe we have already
	// moved past. It must not move next again.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 3, HintIndex: 2})
	if r.next[2] != 2 || len(st.ev.sent) != 0 {
		t.Fatalf("stale rejection acted on: next %d, sent %+v", r.next[2], st.ev.sent)
	}

	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 1, HintIndex: 1})
	if r.next[2] != 1 {
		t.Fatalf("next %d, want 1", r.next[2])
	}
}

func TestLeaderNeverBacksUpBelowMatch(t *testing.T) {
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1))
	makeLeader(t, r)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 2})
	r.next[2] = 4
	takeSent(st)
	// A rejection with a hint below what is known to match must not throw
	// that knowledge away.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 3, HintIndex: 1})
	if r.next[2] != 3 {
		t.Fatalf("next %d, want 3 (match + 1)", r.next[2])
	}
}

func TestLeaderSendsBatchesUntilCaughtUp(t *testing.T) {
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1, 3, 1, 4, 1, 5, 1))
	r.maxMsgEntries = 2
	makeLeader(t, r) // no-op at 6
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 5, HintIndex: 1})
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 2})
	m := onlyMessage(t, st)
	if m.PrevLogIndex != 2 || len(m.Entries) != 2 || m.Entries[0].Index != 3 {
		t.Fatalf("next batch %+v, want entries 3-4 after index 2", m)
	}
	// Once caught up, a success triggers nothing more until the next tick.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 6})
	if len(st.ev.sent) != 0 {
		t.Fatalf("sent %+v to a caught-up follower", st.ev.sent)
	}
}

// entriesOf returns the data of m's entries, joined by commas.
func entriesOf(m Message) string {
	var data []string
	for _, e := range m.Entries {
		data = append(data, string(e.Data))
	}
	return strings.Join(data, ",")
}

func TestReplicatingSendsEachEntryOnce(t *testing.T) {
	// Once a follower's log has matched, each entry is sent to it once, as
	// it is proposed, without waiting for earlier ones to be acknowledged.
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r) // no-op at 1
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 1})
	takeSent(st)
	for i, d := range []string{"a", "b", "c"} {
		if _, _, err := r.Propose([]byte(d)); err != nil {
			t.Fatal(err)
		}
		// Node 3 has not answered, so it is still being probed.
		if m := onlyMessage(t, st); m.To != 2 || m.PrevLogIndex != uint64(i+1) || entriesOf(m) != d {
			t.Fatalf("proposing %s sent %+v, want %s alone to node 2", d, m, d)
		}
	}
	// A response is no reason to send again what is already on its way.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 2})
	if sent := takeSent(st); len(sent) != 0 {
		t.Fatalf("a response resent %+v", sent)
	}
	// A heartbeat carries nothing for node 2, which has everything on its
	// way, and probes node 3 from where it started.
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, m := range takeSent(st) {
		switch {
		case m.To == 2 && (m.PrevLogIndex != 4 || len(m.Entries) != 0):
			t.Fatalf("heartbeat to node 2: %+v, want no entries after 4", m)
		case m.To == 3 && (m.PrevLogIndex != 0 || len(m.Entries) != 4):
			t.Fatalf("heartbeat to node 3: %+v, want entries 1-4", m)
		}
	}
}

func TestProbingSendsOneMessageAtATime(t *testing.T) {
	// Until a follower answers, the leader does not know where its log
	// ends, so proposals do not each send it another message: only
	// heartbeats and its responses do.
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	takeSent(st)
	r.Propose([]byte("a"))
	r.Propose([]byte("b"))
	if sent := takeSent(st); len(sent) != 0 {
		t.Fatalf("proposals sent %+v to followers being probed", sent)
	}
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	for _, m := range takeSent(st) {
		if m.PrevLogIndex != 0 || len(m.Entries) != 3 {
			t.Fatalf("probe %+v, want entries 1-3", m)
		}
	}
	// Node 2 answers, so it is replicated to from then on.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 3})
	if sent := takeSent(st); len(sent) != 0 {
		t.Fatalf("sent %+v to a caught-up follower", sent)
	}
	r.Propose([]byte("c"))
	if m := onlyMessage(t, st); m.To != 2 || m.PrevLogIndex != 3 || entriesOf(m) != "c" {
		t.Fatalf("proposing c sent %+v, want c alone to node 2", m)
	}
}

func TestLostMessageFallsBackToProbing(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r) // no-op at 1
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 1})
	for _, d := range []string{"a", "b", "c"} {
		r.Propose([]byte(d))
	}
	takeSent(st)
	// The message carrying a (entry 2) is lost, so node 2 rejects the two
	// after it: it has nothing after entry 1.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, RejectIndex: 2, HintIndex: 2})
	if m := onlyMessage(t, st); m.PrevLogIndex != 1 || entriesOf(m) != "a,b,c" {
		t.Fatalf("after a rejection sent %+v, want a probe with a, b, c", m)
	}
	// The second rejection answers a message sent before the probe.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, RejectIndex: 3, HintIndex: 2})
	if sent := takeSent(st); len(sent) != 0 {
		t.Fatalf("an old rejection sent %+v", sent)
	}
	// While probing, a proposal waits for the probe's answer.
	r.Propose([]byte("d"))
	if sent := takeSent(st); len(sent) != 0 {
		t.Fatalf("proposal sent %+v while probing", sent)
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 4})
	if m := onlyMessage(t, st); m.PrevLogIndex != 4 || entriesOf(m) != "d" {
		t.Fatalf("after the probe succeeded sent %+v, want d alone", m)
	}
	// Replicating again, a rejection of something below match is old news.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, RejectIndex: 3, HintIndex: 2})
	if sent := takeSent(st); len(sent) != 0 || r.probing[2] {
		t.Fatalf("a rejection below match sent %+v, probing %v", sent, r.probing[2])
	}
}

func TestLeaderCapsBatchBytes(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	r.maxMsgBytes = 10
	makeLeader(t, r) // no-op at 1
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 1})
	for _, d := range []string{"aaaa", "bbbb", "cccc", "dddddddddddddddd", "e"} {
		if _, _, err := r.Propose([]byte(d)); err != nil {
			t.Fatal(err)
		}
	}
	// Node 2 is being replicated to, so each proposal went out at once.
	// Send them again from the start, as after a lost message, to see how
	// they are split: next moves past each message as it is sent.
	takeSent(st)
	r.next[2] = 2
	var got []string
	for range 4 {
		r.sendAppend(2)
		m := onlyMessage(t, st)
		var batch []string
		for _, e := range m.Entries {
			batch = append(batch, string(e.Data))
		}
		got = append(got, strings.Join(batch, ","))
	}
	// 4+4 bytes fit in 10, a third entry would not. The oversized entry
	// goes alone rather than never.
	want := []string{"aaaa,bbbb", "cccc", "dddddddddddddddd", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("batches %q, want %q", got, want)
	}
}

func TestProposeErrors(t *testing.T) {
	r, _ := newNode(t, 1, 1, 2, 3)
	if _, _, err := r.Propose([]byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Propose on follower: err = %v, want ErrNotLeader", err)
	}
	makeLeader(t, r)
	if _, _, err := r.Propose(nil); err == nil {
		t.Fatal("empty proposal accepted")
	}
}

func TestProposeBatchIsOneStorageWrite(t *testing.T) {
	// Group commit: a batch of commands costs one storage write and one
	// fsync, made before any of them is sent, and they commit together.
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	for _, p := range []uint64{2, 3} {
		mustStep(t, r, Message{Type: MsgAppResp, From: p, To: 1, Term: 1, Success: true, MatchIndex: 1})
	}
	takeSent(st)
	st.ev.log = nil
	idx, term, err := r.Propose([]byte("a"), []byte("b"), []byte("c"))
	if err != nil || idx != 2 || term != 1 {
		t.Fatalf("Propose = %d, %d, %v; want 2, 1, nil", idx, term, err)
	}
	saved := slices.DeleteFunc(slices.Clone(st.ev.log), func(e string) bool { return !strings.HasPrefix(e, "save") })
	if !slices.Equal(saved, []string{"save entries 2-4"}) {
		t.Fatalf("storage writes %q, want one write of entries 2-4", saved)
	}
	if !strings.HasPrefix(st.ev.log[0], "save") {
		t.Fatalf("sent before persisting:\n%s", strings.Join(st.ev.log, "\n"))
	}
	sent := takeSent(st)
	if len(sent) != 2 {
		t.Fatalf("sent %d messages, want one to each follower", len(sent))
	}
	for _, m := range sent {
		if len(m.Entries) != 3 || m.PrevLogIndex != 1 {
			t.Fatalf("MsgApp to %d after %d carries %+v, want entries 2-4", m.To, m.PrevLogIndex, m.Entries)
		}
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 4})
	if got := r.TakeCommitted(); len(got) != 4 || string(got[1].Data) != "a" || string(got[3].Data) != "c" {
		t.Fatalf("TakeCommitted = %+v, want the no-op then a, b, c", got)
	}
}

func TestProposeBatchFailsWhole(t *testing.T) {
	// A batch with an empty command is refused before anything is stored.
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	takeSent(st)
	if _, _, err := r.Propose([]byte("a"), nil); err == nil {
		t.Fatal("batch with an empty proposal accepted")
	}
	if _, _, err := r.Propose(); err == nil {
		t.Fatal("empty batch accepted")
	}
	if r.lastIndex() != 1 || len(takeSent(st)) != 0 {
		t.Fatalf("refused batch changed the log (last index %d) or sent messages", r.lastIndex())
	}

	// If the storage write fails, none of the batch is in the log, nothing
	// is sent, and the leader does not count itself as holding it.
	st.fail = errors.New("disk on fire")
	if _, _, err := r.Propose([]byte("a"), []byte("b")); !errors.Is(err, st.fail) {
		t.Fatalf("Propose: err = %v, want storage error", err)
	}
	if r.lastIndex() != 1 || r.match[1] != 1 || len(takeSent(st)) != 0 {
		t.Fatalf("after a failed write: last index %d, own match %d, want 1 and 1, and nothing sent", r.lastIndex(), r.match[1])
	}
}

func TestProposeCopiesData(t *testing.T) {
	r, _ := newNode(t, 1, 1)
	makeLeader(t, r)
	buf := []byte("original")
	idx, _, err := r.Propose(buf)
	if err != nil {
		t.Fatal(err)
	}
	copy(buf, "MUTATED!")
	if e, _ := r.Entry(idx); string(e.Data) != "original" {
		t.Fatalf("entry data changed with the caller's buffer: %q", e.Data)
	}
}

func TestSingleNodeCommitsImmediately(t *testing.T) {
	r, _ := newNode(t, 1, 1)
	makeLeader(t, r)
	idx, _, _ := r.Propose([]byte("x"))
	if r.commit != idx {
		t.Fatalf("commit %d, want %d", r.commit, idx)
	}
}

func TestRestartRelearnsCommitIndex(t *testing.T) {
	r, st := newNode(t, 2, 1, 2, 3)
	mustStep(t, r, app(1, 2, 1, 0, 0, 2, ents(1, 1, 2, 1)))
	if got := r.TakeCommitted(); len(got) != 2 {
		t.Fatalf("committed %d entries, want 2", len(got))
	}

	// Commit and applied indexes are volatile (Figure 2).
	r = newTestRaft(t, 2, []uint64{1, 2, 3}, st)
	if s := r.Status(); s.Commit != 0 || s.LastIndex != 2 {
		t.Fatalf("after restart: %+v, want commit 0 with 2 entries", s)
	}
	if got := r.TakeCommitted(); got != nil {
		t.Fatalf("returned entries before learning the commit index: %+v", got)
	}
	mustStep(t, r, app(1, 2, 1, 2, 1, 2, nil))
	if got := r.TakeCommitted(); len(got) != 2 || got[0].Index != 1 {
		t.Fatalf("after relearning commit: %+v, want entries 1-2 again", got)
	}
}

func TestVoteDeniedToCandidateWithStaleLog(t *testing.T) {
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 2, ents(1, 2))
	// Longer log but older last term: not up to date (§5.4.1).
	mustStep(t, r, Message{Type: MsgVote, From: 2, To: 1, Term: 3, LastLogIndex: 5, LastLogTerm: 1})
	if m := onlyMessage(t, st); m.VoteGranted {
		t.Fatal("voted for a candidate whose last entry is from an older term")
	}
	mustStep(t, r, Message{Type: MsgVote, From: 3, To: 1, Term: 3, LastLogIndex: 1, LastLogTerm: 2})
	if m := onlyMessage(t, st); !m.VoteGranted {
		t.Fatal("refused a candidate with an equally up-to-date log")
	}
}

func TestEntrySaveFailureSendsNothing(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, nil)
	st.fail = errors.New("disk on fire")
	if err := r.Step(app(1, 2, 1, 0, 0, 0, ents(1, 1))); !errors.Is(err, st.fail) {
		t.Fatalf("Step: err = %v, want storage error", err)
	}
	if len(st.ev.sent) != 0 || r.lastIndex() != 0 {
		t.Fatalf("acknowledged or kept entries it failed to persist: sent %+v, last %d", st.ev.sent, r.lastIndex())
	}
}

func TestOverwritingCommittedEntryPanics(t *testing.T) {
	// This cannot happen in a correct cluster. The check exists so that a
	// broken invariant fails loudly instead of silently changing history.
	r, _ := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1))
	mustStep(t, r, app(1, 2, 1, 2, 1, 2, nil))
	defer func() {
		if recover() == nil {
			t.Fatal("overwrote a committed entry without panicking")
		}
	}()
	r.Step(app(1, 2, 2, 1, 1, 2, ents(2, 2)))
}
