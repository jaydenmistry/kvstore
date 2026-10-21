package raft

import "testing"

// quorumLeader returns node 1 of a cluster of the given size as leader in
// term 1, with CheckQuorum on and its window just started.
func quorumLeader(t *testing.T, size int) (*Raft, *testStorage) {
	t.Helper()
	var peers []uint64
	for i := range size {
		peers = append(peers, uint64(i+1))
	}
	r, st := newNode(t, 1, peers...)
	r.checkQuorum = true
	makeLeader(t, r)
	takeSent(st)
	return r, st
}

// tickLeader ticks r n times and fails if it stops being leader.
func tickLeader(t *testing.T, r *Raft, n int) {
	t.Helper()
	for i := range n {
		if err := r.Tick(); err != nil {
			t.Fatal(err)
		}
		if r.state != Leader {
			t.Fatalf("stepped down after %d of %d ticks", i+1, n)
		}
	}
}

// tickStepsDown ticks r once and fails unless it steps down to follower in
// the same term.
func tickStepsDown(t *testing.T, r *Raft) {
	t.Helper()
	term := r.term
	if err := r.Tick(); err != nil {
		t.Fatal(err)
	}
	if r.state != Follower || r.term != term || r.lead != None {
		t.Fatalf("state %v term %d lead %d, want follower in term %d with no leader", r.state, r.term, r.lead, term)
	}
}

func appResp(from, term uint64) Message {
	return Message{Type: MsgAppResp, From: from, To: 1, Term: term, Success: true, MatchIndex: 1}
}

func TestCheckQuorumStepsDownWithoutMajority(t *testing.T) {
	r, _ := quorumLeader(t, 3)
	readIndex(t, r, 5)
	tickLeader(t, r, testElection-1)
	tickStepsDown(t, r)
	// Pending reads fail, as on any step-down.
	checkReadStates(t, r, ReadState{ID: 5, Err: ErrNotLeader})
}

func TestCheckQuorumOffKeepsLeader(t *testing.T) {
	r, st := newNode(t, 1, 1, 2, 3)
	makeLeader(t, r)
	takeSent(st)
	tickLeader(t, r, 10*testElection)
}

func TestCheckQuorumMajorityKeepsLeader(t *testing.T) {
	// One answering follower in each window is a majority of three.
	r, _ := quorumLeader(t, 3)
	for range 10 {
		tickLeader(t, r, testElection-1)
		mustStep(t, r, appResp(2, 1))
		tickLeader(t, r, 1)
	}
}

func TestCheckQuorumMinorityIsNotEnough(t *testing.T) {
	// In a cluster of five, the leader and one follower are two of five.
	r, _ := quorumLeader(t, 5)
	tickLeader(t, r, testElection-1)
	mustStep(t, r, appResp(2, 1))
	mustStep(t, r, appResp(2, 1)) // a duplicate counts once
	tickStepsDown(t, r)
}

func TestCheckQuorumCountsRejections(t *testing.T) {
	// A rejection still shows the follower accepts us as leader.
	r, _ := quorumLeader(t, 3)
	tickLeader(t, r, testElection-1)
	mustStep(t, r, Message{Type: MsgAppResp, From: 3, To: 1, Term: 1, RejectIndex: 1, HintIndex: 1})
	tickLeader(t, r, 1)
}

func TestCheckQuorumWindowsDoNotCarryOver(t *testing.T) {
	// Answers from one window say nothing about the next.
	r, _ := quorumLeader(t, 3)
	mustStep(t, r, appResp(2, 1))
	mustStep(t, r, appResp(3, 1))
	tickLeader(t, r, testElection)
	tickLeader(t, r, testElection-1)
	tickStepsDown(t, r)
}

func TestCheckQuorumActivityDoesNotCarryAcrossTerms(t *testing.T) {
	// Node 1 hears from everyone in term 1, steps down, and wins term 3.
	// Answers from term 1, recorded then or arriving late, must not count
	// toward its first window in term 3.
	r, st := quorumLeader(t, 3)
	mustStep(t, r, appResp(2, 1))
	mustStep(t, r, appResp(3, 1))
	mustStep(t, r, Message{Type: MsgApp, From: 2, To: 1, Term: 2, PrevLogIndex: 1, PrevLogTerm: 1})
	makeLeader(t, r)
	takeSent(st)
	tickLeader(t, r, testElection-1)
	mustStep(t, r, appResp(2, 1))
	tickStepsDown(t, r)
}

func TestCheckQuorumNewLeaderGetsFullWindow(t *testing.T) {
	// A node elected late in its election timer still gets ElectionTick
	// ticks before its first check.
	r, st := newNode(t, 1, 1, 2, 3)
	r.checkQuorum = true
	tickUntilCampaign(t, r)
	for range testElection - 1 {
		if err := r.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	mustStep(t, r, Message{Type: MsgVoteResp, From: 2, To: 1, Term: r.term, VoteGranted: true})
	takeSent(st)
	tickLeader(t, r, testElection-1)
	tickStepsDown(t, r)
}

func TestCheckQuorumSingleNode(t *testing.T) {
	r, _ := quorumLeader(t, 1)
	tickLeader(t, r, 10*testElection)
}
