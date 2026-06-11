package raft_test

import (
	"math/rand/v2"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// manualNet queues messages until the test decides which to deliver, for
// scripting exact interleavings that random testing would rarely produce.
type manualNet struct {
	queue []raft.Message
}

type manualEndpoint struct {
	n  *manualNet
	id uint64
}

func (e manualEndpoint) Send(m raft.Message) {
	m.From = e.id
	e.n.queue = append(e.n.queue, m)
}

// deliver repeatedly delivers queued messages for which allow returns true,
// including messages sent in response, until none are left. Messages that
// are not allowed, or addressed to a crashed node, are lost.
func (n *manualNet) deliver(t *testing.T, nodes map[uint64]*raft.Raft, allow func(raft.Message) bool) {
	t.Helper()
	for len(n.queue) > 0 {
		batch := n.queue
		n.queue = nil
		for _, m := range batch {
			if r := nodes[m.To]; r != nil && nodes[m.From] != nil && allow(m) {
				if err := r.Step(m); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

func between(a, b uint64) func(raft.Message) bool {
	return func(m raft.Message) bool {
		return (m.From == a && m.To == b) || (m.From == b && m.To == a)
	}
}

func anyOf(fs ...func(raft.Message) bool) func(raft.Message) bool {
	return func(m raft.Message) bool {
		for _, f := range fs {
			if f(m) {
				return true
			}
		}
		return false
	}
}

func carries(index uint64) func(raft.Message) bool {
	return func(m raft.Message) bool {
		for _, e := range m.Entries {
			if e.Index == index {
				return true
			}
		}
		return false
	}
}

func all(fs ...func(raft.Message) bool) func(raft.Message) bool {
	return func(m raft.Message) bool {
		for _, f := range fs {
			if !f(m) {
				return false
			}
		}
		return true
	}
}

func isVote(m raft.Message) bool {
	return m.Type == raft.MsgVote || m.Type == raft.MsgVoteResp
}

// TestFigure8 replays Figure 8 of the Raft paper with five nodes. It shows
// why a leader may not commit an entry from an earlier term just because it
// is stored on a majority: a later leader can still overwrite it. Term
// numbers differ from the paper's, because a restarted node may need more
// than one campaign to win, but each step is the same.
func TestFigure8(t *testing.T) {
	ids := []uint64{1, 2, 3, 4, 5}
	net := &manualNet{}
	storage := map[uint64]*raft.MemoryStorage{}
	nodes := map[uint64]*raft.Raft{}
	start := func(id uint64) {
		r, err := raft.New(raft.Config{
			ID: id, Peers: ids, ElectionTick: 10, HeartbeatTick: 1,
			// One entry per message, so the script controls exactly which
			// entries reach which node.
			MaxEntriesPerMessage: 1,
			Storage:              storage[id],
			Transport:            manualEndpoint{net, id},
			Rand:                 rand.New(rand.NewPCG(id, 8)),
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = r
	}
	for _, id := range ids {
		storage[id] = raft.NewMemoryStorage()
		start(id)
	}
	crash := func(id uint64) { nodes[id] = nil }
	everyone := func(raft.Message) bool { return true }

	// elect makes id campaign, delivering only the vote messages allow
	// permits, until it wins. Everything else sent meanwhile is lost; the
	// new leader then ticks once, queueing AppendEntries for the caller's
	// next deliver to route.
	elect := func(id uint64, allow func(raft.Message) bool) uint64 {
		t.Helper()
		for range 10 {
			before := nodes[id].Status().Term
			for nodes[id].Status().Term == before {
				if err := nodes[id].Tick(); err != nil {
					t.Fatal(err)
				}
			}
			net.deliver(t, nodes, all(isVote, allow))
			if s := nodes[id].Status(); s.State == raft.Leader {
				if err := nodes[id].Tick(); err != nil {
					t.Fatal(err)
				}
				return s.Term
			}
		}
		t.Fatalf("node %d never won an election", id)
		return 0
	}

	// Setup: node 1 leads, and its no-op at index 1 commits everywhere.
	t1 := elect(1, everyone)
	net.deliver(t, nodes, everyone)
	if s := nodes[1].Status(); s.Commit != 1 {
		t.Fatalf("setup: node 1 is %+v", s)
	}

	// (a) Node 1 appends "x" at index 2. It reaches node 2 only.
	if _, _, err := nodes[1].Propose([]byte("x")); err != nil {
		t.Fatal(err)
	}
	net.deliver(t, nodes, between(1, 2))

	// (b) Node 1 crashes. Node 5 wins a later term with votes from 3 and 4
	// (node 2 would refuse: its log is longer) and appends its no-op at
	// index 2, which reaches no one before node 5 crashes too.
	crash(1)
	t2 := elect(5, anyOf(between(5, 3), between(5, 4)))
	net.queue = nil
	crash(5)

	// (c) Node 1 restarts and wins a later term still, with votes from 2
	// and 3. It replicates its old entry "x" to nodes 3 and 4. Its new no-op
	// at index 3 is accepted by no one.
	start(1)
	t3 := elect(1, anyOf(between(1, 2), between(1, 3)))
	if !(t1 < t2 && t2 < t3) {
		t.Fatalf("terms %d, %d, %d not increasing", t1, t2, t3)
	}
	rejectsIndex3 := func(m raft.Message) bool {
		// Let a message carrying index 3 through only if the follower lacks
		// its PrevLogIndex and must reject it.
		if !carries(3)(m) {
			return true
		}
		_, ok := nodes[m.To].Entry(m.PrevLogIndex)
		return !ok
	}
	net.deliver(t, nodes, all(anyOf(between(1, 3), between(1, 4)), rejectsIndex3))
	for _, id := range []uint64{1, 2, 3, 4} {
		if e, _ := nodes[id].Entry(2); string(e.Data) != "x" {
			t.Fatalf("(c): node %d has %+v at index 2, want x", id, e)
		}
	}
	// "x" is now on nodes 1, 2, 3, and 4, and node 1 knows of three of
	// them: a majority. But "x" is from term t1, not the current term t3,
	// so counting replicas must not commit it (§5.4.2).
	if c := nodes[1].Status().Commit; c >= 2 {
		t.Fatalf("(c): node 1 committed index %d, an entry from an earlier term, by counting replicas", c)
	}

	// (d) Node 1 crashes again. Node 5 restarts and wins with votes from 2,
	// 3, and 4: its last entry is from term t2, newer than their t1. It then
	// overwrites index 2 everywhere. Had node 1 committed "x" in (c), a
	// committed entry would now be gone.
	crash(1)
	start(5)
	elect(5, everyone)
	net.deliver(t, nodes, everyone)
	if s := nodes[5].Status(); s.Commit != s.LastIndex {
		t.Fatalf("(d): node 5 is %+v, want everything committed", s)
	}
	for _, id := range []uint64{2, 3, 4, 5} {
		if e, _ := nodes[id].Entry(2); e.Term != t2 || len(e.Data) != 0 {
			t.Fatalf("(d): node %d has %+v at index 2, want node 5's no-op from term %d", id, e, t2)
		}
	}
}
