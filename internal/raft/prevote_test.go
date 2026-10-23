package raft

import (
	"slices"
	"strings"
	"testing"
)

// saves returns the saves recorded in st's event log.
func saves(st *testStorage) []string {
	var out []string
	for _, e := range st.ev.log {
		if strings.HasPrefix(e, "save") {
			out = append(out, e)
		}
	}
	return out
}

func newPreVoteNode(t *testing.T, id uint64, peers ...uint64) (*Raft, *testStorage) {
	t.Helper()
	r, st := newNode(t, id, peers...)
	r.preVote = true
	return r, st
}

// tickUntilPreCampaign ticks r until its election timeout elapses.
func tickUntilPreCampaign(t *testing.T, r *Raft) {
	t.Helper()
	for range 2 * testElection {
		if err := r.Tick(); err != nil {
			t.Fatal(err)
		}
		if r.state != Follower {
			return
		}
	}
	t.Fatal("node never started a pre-campaign")
}

func preVoteReq(from, to, term, lastIndex, lastTerm uint64) Message {
	return Message{Type: MsgPreVote, From: from, To: to, Term: term, LastLogIndex: lastIndex, LastLogTerm: lastTerm}
}

func TestPreCampaignKeepsTerm(t *testing.T) {
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 4, ents(1, 2, 2, 4))
	r.preVote = true
	st.ev.log = nil
	tickUntilPreCampaign(t, r)
	if r.state != PreCandidate || r.term != 4 || r.vote != None {
		t.Fatalf("state %v term %d vote %d, want pre-candidate in term 4 with no vote", r.state, r.term, r.vote)
	}
	if s := saves(st); len(s) != 0 {
		t.Fatalf("pre-campaign saved %v", s)
	}
	sent := takeSent(st)
	if len(sent) != 2 {
		t.Fatalf("sent %+v, want a pre-vote request to each peer", sent)
	}
	for _, m := range sent {
		if m.Type != MsgPreVote || m.Term != 5 || m.LastLogIndex != 2 || m.LastLogTerm != 4 {
			t.Fatalf("sent %+v, want MsgPreVote for term 5 with last entry 2@4", m)
		}
	}
}

func TestPreVoteMajorityStartsElection(t *testing.T) {
	r, st := newPreVoteNode(t, 1, 1, 2, 3, 4, 5)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true}) // counts once
	if r.state != PreCandidate {
		t.Fatalf("state %v after 2 of 5 grants, want pre-candidate", r.state)
	}
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 3, To: 1, Term: 1, VoteGranted: true})
	if r.state != Candidate || r.term != 1 || r.vote != 1 {
		t.Fatalf("state %v term %d vote %d, want candidate in term 1 voting for itself", r.state, r.term, r.vote)
	}
	sent := takeSent(st)
	if len(sent) != 4 || sent[0].Type != MsgVote || sent[0].Term != 1 {
		t.Fatalf("sent %+v, want MsgVote for term 1 to each peer", sent)
	}
}

func TestPreVoteGrantDoesNotRaiseTerm(t *testing.T) {
	// A grant carries the term the pre-candidate asked about. It is not a
	// sign of a newer term, so the pre-candidate must not adopt it and
	// become a follower.
	r, st := newPreVoteNode(t, 1, 1, 2, 3, 4, 5)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	if r.state != PreCandidate || r.term != 0 {
		t.Fatalf("state %v term %d after a grant, want pre-candidate in term 0", r.state, r.term)
	}
}

func TestPreVoteIgnoresGrantsForOtherTerms(t *testing.T) {
	// Grants from a pre-campaign made in an earlier term ask about a term
	// we have since entered.
	r, st := newNodeWithLog(t, 1, []uint64{1, 2, 3}, 3, nil)
	r.preVote = true
	tickUntilPreCampaign(t, r)
	takeSent(st)
	for _, term := range []uint64{3, 2, 9} {
		mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: term, VoteGranted: true})
	}
	if r.state != PreCandidate || r.term != 3 {
		t.Fatalf("state %v term %d, want pre-candidate in term 3", r.state, r.term)
	}
}

func TestPreVoteRefusalWithHigherTermCatchesUp(t *testing.T) {
	// A pre-candidate that fell behind learns the current term from a
	// refusal, rather than asking about a term that has passed forever.
	r, st := newPreVoteNode(t, 1, 1, 2, 3)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 7})
	if r.state != Follower || r.term != 7 {
		t.Fatalf("state %v term %d, want follower in term 7", r.state, r.term)
	}
}

func TestPreVoteRefusalsDoNotCount(t *testing.T) {
	r, st := newPreVoteNode(t, 1, 1, 2, 3)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 0})
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 3, To: 1, Term: 0})
	if r.state != PreCandidate {
		t.Fatalf("state %v after two refusals, want pre-candidate", r.state)
	}
}

func TestPreCandidateFollowsLeaderOfItsTerm(t *testing.T) {
	r, st := newPreVoteNode(t, 1, 1, 2, 3)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	mustStep(t, r, app(2, 1, 0, 0, 0, 0, nil))
	if r.state != Follower || r.lead != 2 {
		t.Fatalf("state %v lead %d, want follower of 2", r.state, r.lead)
	}
}

func TestPreVoteGrantChangesNothing(t *testing.T) {
	r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 3, ents(1, 3))
	st.ev.log = nil
	mustStep(t, r, preVoteReq(1, 2, 4, 1, 3))
	resp := onlyMessage(t, st)
	if resp.Type != MsgPreVoteResp || !resp.VoteGranted || resp.Term != 4 {
		t.Fatalf("response %+v, want a grant for term 4", resp)
	}
	if r.term != 3 || r.vote != None || len(saves(st)) != 0 {
		t.Fatalf("term %d vote %d saves %v after granting a pre-vote; want term 3, no vote, nothing saved", r.term, r.vote, saves(st))
	}
	// The election timer is not reset, unlike for a real vote.
	if r.electionElapsed != 0 {
		t.Fatal("test setup: timer not at zero")
	}
	r.Tick()
	mustStep(t, r, preVoteReq(3, 2, 4, 1, 3))
	if r.electionElapsed != 1 {
		t.Fatalf("electionElapsed %d after a pre-vote, want 1", r.electionElapsed)
	}
	// A pre-vote may be granted to several nodes: it promises nothing.
	if resp := onlyMessage(t, st); !resp.VoteGranted {
		t.Fatalf("second pre-vote refused: %+v", resp)
	}
}

func TestPreVoteRefusals(t *testing.T) {
	cases := []struct {
		name string
		m    Message
	}{
		{"log behind by term", preVoteReq(1, 2, 4, 5, 2)},
		{"log behind by index", preVoteReq(1, 2, 4, 1, 3)},
		{"term already reached", preVoteReq(1, 2, 3, 9, 3)},
		{"older term", preVoteReq(1, 2, 2, 9, 3)},
	}
	for _, tc := range cases {
		r, st := newNodeWithLog(t, 2, []uint64{1, 2, 3}, 3, ents(1, 3, 2, 3))
		mustStep(t, r, tc.m)
		resp := onlyMessage(t, st)
		if resp.Type != MsgPreVoteResp || resp.VoteGranted || resp.Term != 3 {
			t.Errorf("%s: response %+v, want a refusal carrying term 3", tc.name, resp)
		}
		if r.term != 3 || r.vote != None {
			t.Errorf("%s: term %d vote %d, want 3 and none", tc.name, r.term, r.vote)
		}
	}
}

// followerOf returns node 2 of three, following leader 1 in term 1, with
// CheckQuorum set as given.
func followerOf(t *testing.T, checkQuorum bool) (*Raft, *testStorage) {
	t.Helper()
	r, st := newNode(t, 2, 1, 2, 3)
	r.checkQuorum = checkQuorum
	mustStep(t, r, app(1, 2, 1, 0, 0, 0, nil))
	takeSent(st)
	return r, st
}

func TestPreVoteRefusedWhileLeaderIsHeard(t *testing.T) {
	r, st := followerOf(t, true)
	for range testElection - 1 {
		r.Tick()
	}
	mustStep(t, r, preVoteReq(3, 2, 2, 0, 0))
	if resp := onlyMessage(t, st); resp.VoteGranted {
		t.Fatalf("granted a pre-vote %d ticks after hearing from the leader", testElection-1)
	}
	r.Tick()
	mustStep(t, r, preVoteReq(3, 2, 2, 0, 0))
	if resp := onlyMessage(t, st); !resp.VoteGranted {
		t.Fatalf("refused a pre-vote %d ticks after hearing from the leader", testElection)
	}
}

func TestPreVoteNotStickyWithoutCheckQuorum(t *testing.T) {
	// See inLeaderLease for why stickiness needs CheckQuorum.
	r, st := followerOf(t, false)
	mustStep(t, r, preVoteReq(3, 2, 2, 0, 0))
	if resp := onlyMessage(t, st); !resp.VoteGranted {
		t.Fatal("refused a pre-vote without CheckQuorum")
	}
}

func TestLeaderRefusesPreVote(t *testing.T) {
	r, st := quorumLeader(t, 3)
	for range 3 * testElection {
		mustStep(t, r, appResp(2, 1))
		r.Tick()
	}
	takeSent(st)
	mustStep(t, r, preVoteReq(3, 1, 2, 9, 1))
	if resp := onlyMessage(t, st); resp.VoteGranted || r.state != Leader || r.term != 1 {
		t.Fatalf("leader answered %+v and is %v in term %d; want a refusal and no change", resp, r.state, r.term)
	}
}

func TestSingleNodePreVoteElectsItself(t *testing.T) {
	r, _ := newPreVoteNode(t, 1, 1)
	tickUntilPreCampaign(t, r)
	if r.state != Leader || r.term != 1 {
		t.Fatalf("state %v term %d, want leader in term 1", r.state, r.term)
	}
}

func TestPreCampaignAgainAfterTimeout(t *testing.T) {
	// A pre-candidate that gets no answers tries again, still in its term,
	// and a candidate whose election timed out pre-campaigns first too.
	r, st := newPreVoteNode(t, 1, 1, 2, 3)
	tickUntilPreCampaign(t, r)
	takeSent(st)
	tickUntilTimeout(t, r)
	if r.state != PreCandidate || r.term != 0 {
		t.Fatalf("state %v term %d, want pre-candidate in term 0", r.state, r.term)
	}
	if sent := takeSent(st); !slices.ContainsFunc(sent, func(m Message) bool { return m.Type == MsgPreVote && m.Term == 1 }) {
		t.Fatalf("sent %+v, want new pre-vote requests for term 1", sent)
	}
	mustStep(t, r, Message{Type: MsgPreVoteResp, From: 2, To: 1, Term: 1, VoteGranted: true})
	takeSent(st)
	tickUntilTimeout(t, r)
	if r.state != PreCandidate || r.term != 1 {
		t.Fatalf("timed-out candidate is %v in term %d, want pre-candidate in term 1", r.state, r.term)
	}
}

// tickUntilTimeout ticks r until its election timer restarts.
func tickUntilTimeout(t *testing.T, r *Raft) {
	t.Helper()
	for range 2 * testElection {
		r.Tick()
		if r.electionElapsed == 0 {
			return
		}
	}
	t.Fatal("election timer never restarted")
}
