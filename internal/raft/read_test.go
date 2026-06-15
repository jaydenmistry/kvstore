package raft

import (
	"errors"
	"fmt"
	"testing"
)

// committedLeader returns node 1 of a three-node cluster as leader in term 1,
// with its no-op at index 1 committed and acknowledged by both followers.
func committedLeader(t *testing.T) (*Raft, *testStorage) {
	t.Helper()
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	for _, p := range []uint64{2, 3} {
		mustStep(t, r, Message{Type: MsgAppResp, From: p, To: 1, Term: 1, Success: true, MatchIndex: 1})
	}
	if r.commit != 1 {
		t.Fatalf("commit %d, want 1", r.commit)
	}
	takeSent(st)
	return r, st
}

func ack(from, round uint64) Message {
	return Message{Type: MsgAppResp, From: from, To: 1, Term: 1, Success: true, MatchIndex: 1, Round: round}
}

func readIndex(t *testing.T, r *Raft, id uint64) {
	t.Helper()
	if err := r.ReadIndex(id); err != nil {
		t.Fatalf("ReadIndex(%d): %v", id, err)
	}
}

func checkReadStates(t *testing.T, r *Raft, want ...ReadState) {
	t.Helper()
	if got := r.TakeReadStates(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("read states %+v, want %+v", got, want)
	}
}

func TestReadIndexNeedsMajorityAck(t *testing.T) {
	r, st := committedLeader(t)
	readIndex(t, r, 7)
	sent := takeSent(st)
	if len(sent) != 2 {
		t.Fatalf("sent %d messages, want a heartbeat to each follower", len(sent))
	}
	round := sent[0].Round
	if round == 0 || sent[1].Round != round {
		t.Fatalf("heartbeats carry rounds %d and %d, want the same new round", round, sent[1].Round)
	}
	checkReadStates(t, r) // only the leader itself so far
	mustStep(t, r, ack(2, round))
	checkReadStates(t, r, ReadState{ID: 7, Index: 1})
}

func TestReadIndexIgnoresAcksOfEarlierRounds(t *testing.T) {
	// Answers to heartbeats sent before the read began may predate a newer
	// leader's election, so they must not confirm it.
	r, st := committedLeader(t)
	readIndex(t, r, 1)
	round := takeSent(st)[0].Round
	mustStep(t, r, ack(2, round-1))
	mustStep(t, r, ack(3, 0))
	checkReadStates(t, r)
	mustStep(t, r, ack(3, round))
	checkReadStates(t, r, ReadState{ID: 1, Index: 1})
}

func TestReadIndexRejectionConfirmsLeadership(t *testing.T) {
	// A rejection still shows the follower accepted our term.
	r, st := committedLeader(t)
	readIndex(t, r, 1)
	round := takeSent(st)[0].Round
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, RejectIndex: 1, HintIndex: 1, Round: round})
	checkReadStates(t, r, ReadState{ID: 1, Index: 1})
}

func TestReadIndexWaitsForCurrentTermCommit(t *testing.T) {
	// A restarted node becomes leader in term 2 with entries 1-2 from term
	// 1. Its commit index is 0, though 1-2 may have committed in term 1. A
	// read served at index 0 could miss writes that completed long ago.
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 1, ents(1, 1, 2, 1))
	makeLeader(t, r) // term 2, no-op at 3
	takeSent(st)
	readIndex(t, r, 1)
	round := takeSent(st)[0].Round
	// Leadership is confirmed, but nothing from term 2 has committed yet.
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, RejectIndex: 2, HintIndex: 1, Round: round})
	checkReadStates(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 2, Success: true, MatchIndex: 3, Round: round})
	if r.commit != 3 {
		t.Fatalf("commit %d, want 3", r.commit)
	}
	checkReadStates(t, r, ReadState{ID: 1, Index: 3})
}

func TestReadIndexUsesCommitWhenRequested(t *testing.T) {
	// Entries committed after the read began are concurrent with it, so the
	// read index stays at the commit index when the read was requested. A
	// later index would also be safe, but the read would wait longer for it
	// to be applied.
	r, st := committedLeader(t)
	readIndex(t, r, 1)
	round := takeSent(st)[0].Round
	if _, _, err := r.Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 1, Success: true, MatchIndex: 2})
	if r.commit != 2 {
		t.Fatalf("commit %d, want 2", r.commit)
	}
	mustStep(t, r, ack(3, round))
	checkReadStates(t, r, ReadState{ID: 1, Index: 1})
}

func TestReadIndexEvenClusterNeedsMajority(t *testing.T) {
	// With four nodes, the leader and one follower are half, not a majority.
	r, st := newNode(t, 1, 1, 2, 3, 4)
	makeLeader(t, r)
	for _, p := range []uint64{2, 3, 4} {
		mustStep(t, r, Message{Type: MsgAppResp, From: p, To: 1, Term: 1, Success: true, MatchIndex: 1})
	}
	takeSent(st)
	readIndex(t, r, 1)
	round := takeSent(st)[0].Round
	mustStep(t, r, ack(2, round))
	checkReadStates(t, r)
	mustStep(t, r, ack(4, round))
	checkReadStates(t, r, ReadState{ID: 1, Index: 1})
}

func TestReadIndexResolvesInOrder(t *testing.T) {
	r, st := committedLeader(t)
	readIndex(t, r, 1)
	first := takeSent(st)[0].Round
	readIndex(t, r, 2)
	second := takeSent(st)[0].Round
	mustStep(t, r, ack(2, first))
	checkReadStates(t, r, ReadState{ID: 1, Index: 1})
	// An ack of a later round confirms every earlier one too.
	readIndex(t, r, 3)
	third := takeSent(st)[0].Round
	if !(first < second && second < third) {
		t.Fatalf("rounds %d, %d, %d not increasing", first, second, third)
	}
	mustStep(t, r, ack(3, third))
	checkReadStates(t, r, ReadState{ID: 2, Index: 1}, ReadState{ID: 3, Index: 1})
}

func TestReadIndexFailsOnStepDown(t *testing.T) {
	r, _ := committedLeader(t)
	readIndex(t, r, 1)
	readIndex(t, r, 2)
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2})
	checkReadStates(t, r, ReadState{ID: 1, Err: ErrNotLeader}, ReadState{ID: 2, Err: ErrNotLeader})
	if err := r.ReadIndex(3); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("ReadIndex on follower: err = %v, want ErrNotLeader", err)
	}
}

func TestReadIndexAcksDoNotCarryAcrossTerms(t *testing.T) {
	// Node 1 leads term 1, steps down, and wins term 3. Rounds restart, so
	// acknowledgments recorded in term 1 must not confirm reads in term 3.
	r, st := committedLeader(t)
	readIndex(t, r, 1)
	for range 5 {
		readIndex(t, r, 1)
	}
	mustStep(t, r, ack(2, 6))
	mustStep(t, r, ack(3, 6))
	r.TakeReadStates()
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1, LeaderCommit: 1})
	makeLeader(t, r) // term 3, no-op at 2
	mustStep(t, r, Message{Type: MsgAppResp, From: 2, To: 1, Term: 3, Success: true, MatchIndex: 2})
	takeSent(st)
	readIndex(t, r, 9)
	checkReadStates(t, r)
}

func TestSingleNodeReadIndex(t *testing.T) {
	r, _ := newNode(t, 1, 1)
	makeLeader(t, r)
	readIndex(t, r, 4)
	checkReadStates(t, r, ReadState{ID: 4, Index: 1})
}

func TestFollowerEchoesRound(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 1, ents(1, 1))
	for _, m := range []Message{
		app(1, 2, 1, 1, 1, 0, nil), // success
		app(1, 2, 1, 5, 1, 0, nil), // missing entry
		app(1, 2, 1, 1, 9, 0, nil), // conflicting term
	} {
		m.Round = 42
		mustStep(t, r, m)
		if resp := onlyMessage(t, st); resp.Round != 42 {
			t.Fatalf("response %+v to %+v does not echo round 42", resp, m)
		}
	}
}
