package kv

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

func others(ids []uint64, id uint64) []uint64 {
	var out []uint64
	for _, x := range ids {
		if x != id {
			out = append(out, x)
		}
	}
	return out
}

func TestPutGetDeleteThroughLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		c := newCluster(t, n, 1, false)
		l := c.leader()
		if res := c.do(l, getReq("k")); res.Found {
			t.Fatalf("n=%d: empty store has k = %q", n, res.Value)
		}
		id := c.register(l)
		c.do(l, putReq(id, 1, "k", "v"))
		if res := c.do(l, getReq("k")); !res.Found || string(res.Value) != "v" {
			t.Fatalf("n=%d: Get after Put = %+v", n, res)
		}
		if res := c.do(l, deleteReq(id, 2, "k")); !res.Found {
			t.Fatalf("n=%d: Delete of existing key reported not found", n)
		}
		if res := c.do(l, getReq("k")); res.Found {
			t.Fatalf("n=%d: Get after Delete = %+v", n, res)
		}
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		c.checkReplicasAgree()
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := newCluster(t, 3, 1, false)
	l := c.leader()
	for _, f := range others(c.ids, l) {
		for _, req := range []Request{getReq("k"), putReq(1, 1, "k", "v"), deleteReq(1, 1, "k")} {
			cl := c.submit(f, req)
			if !cl.done || !errors.Is(cl.res.Err, ErrNotLeader) || cl.res.Leader != l {
				t.Fatalf("%v on follower %d: done %v, %+v; want ErrNotLeader naming %d at once", req.Op, f, cl.done, cl.res, l)
			}
		}
	}
	// Nothing was proposed: only the leader's no-op is in the log.
	if last := c.nodes[l].raft.Status().LastIndex; last != 1 {
		t.Fatalf("leader's last index %d, want 1", last)
	}
}

func TestNoLeaderKnown(t *testing.T) {
	c := newCluster(t, 3, 1, false)
	cl := c.submit(1, getReq("k"))
	if !errors.Is(cl.res.Err, ErrNotLeader) || cl.res.Leader != raft.None {
		t.Fatalf("before any election: %+v, want ErrNotLeader with no leader", cl.res)
	}
}

func TestInvalidRequests(t *testing.T) {
	c := newCluster(t, 1, 1, false)
	l := c.leader()
	for _, req := range []Request{
		{Op: OpGet},
		{Op: OpPut, Key: "k", Seq: 1},
		{Op: OpPut, Key: "k", ClientID: 1},
		{Op: OpPut, Key: "k", ClientID: 1, Seq: 1, Value: make([]byte, MaxValueSize+1)},
		{Op: OpRegister, Key: "k"},
		{Op: OpRegister, ClientID: 1},
		{Op: 0, Key: "k"},
	} {
		if cl := c.submit(l, req); !errors.Is(cl.res.Err, ErrInvalid) {
			t.Fatalf("%+v: %+v, want ErrInvalid", req, cl.res)
		}
	}
}

func TestDeposedLeaderDoesNotServeStaleRead(t *testing.T) {
	// The leader is cut off from the rest of the cluster, which elects a new
	// leader and overwrites k. The old leader does not know it was deposed;
	// if it answered reads from its own state machine, it would return the
	// old value after the new one was acknowledged. ReadIndex cannot get a
	// majority to confirm its leadership, so the read never succeeds.
	for seed := range uint64(10) {
		c := newCluster(t, 5, seed, false)
		old := c.leader()
		id := c.register(old)
		c.do(old, putReq(id, 1, "k", "old"))
		c.net.Partition([]uint64{old}, others(c.ids, old))
		lead := c.leaderAmong(others(c.ids, old), 30*electionTick)
		c.do(lead, putReq(id, 2, "k", "new"))

		if c.nodes[old].raft.Status().State != raft.Leader {
			c.fatalf("old leader stepped down; the scenario needs it to believe it still leads")
		}
		res := c.wait(c.submit(old, getReq("k")), 2*requestTimeout)
		if res.Err == nil {
			c.fatalf("deposed leader served a read: %q", res.Value)
		}
		if !errors.Is(res.Err, ErrTimeout) {
			c.fatalf("read on deposed leader: %v, want ErrTimeout", res.Err)
		}
	}
}

func TestDeposedLeaderWriteFailsAfterHeal(t *testing.T) {
	// A write accepted by a deposed leader never commits: the new leader
	// replaces it. When the old leader learns of the new term, it must
	// report the write as failed, never as done.
	for seed := range uint64(10) {
		c := newCluster(t, 5, seed, false)
		old := c.leader()
		c.net.Partition([]uint64{old}, others(c.ids, old))
		cl := c.submit(old, putReq(1, 1, "k", "lost"))
		lead := c.leaderAmong(others(c.ids, old), 30*electionTick)
		c.do(lead, putReq(c.register(lead), 1, "k", "kept"))
		c.net.Heal()
		res := c.wait(cl, 2*requestTimeout)
		if !errors.Is(res.Err, ErrLeadershipLost) && !errors.Is(res.Err, ErrTimeout) {
			c.fatalf("write on deposed leader: %+v, want ErrLeadershipLost or ErrTimeout", res)
		}
		lead = c.leader()
		if res := c.do(lead, getReq("k")); string(res.Value) != "kept" {
			c.fatalf("k = %q, want kept", res.Value)
		}
	}
}

func TestRetryAfterLeaderCrashAppliesOnce(t *testing.T) {
	// The leader commits a Delete and crashes before the client hears back.
	// The client retries the same (client, seq) on the next leader. The
	// retry is committed a second time, but must report the original
	// result and must not be applied again.
	for seed := range uint64(10) {
		c := newCluster(t, 3, seed, seed%2 == 1)
		l := c.leader()
		c7 := c.register(l)
		c.do(l, putReq(c7, 1, "k", "v"))
		del := c.submit(l, deleteReq(c7, 2, "k"))
		c.wait(del, 2*requestTimeout)
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		c.crash(l) // pretend the reply was lost

		next := c.leaderAmong(others(c.ids, l), 30*electionTick)
		res := c.do(next, deleteReq(c7, 2, "k"))
		if !res.Found {
			c.fatalf("retried delete reported not found; the original deleted the key")
		}
		// A different client's write in between, then a late duplicate of
		// client c7's Put: it must not resurrect the key.
		c.do(next, putReq(c.register(next), 1, "other", "x"))
		if res := c.wait(c.submit(next, putReq(c7, 1, "k", "v")), 2*requestTimeout); !errors.Is(res.Err, ErrStaleSequence) {
			c.fatalf("late duplicate of seq 1: %+v, want ErrStaleSequence", res)
		}
		if res := c.do(next, getReq("k")); res.Found {
			c.fatalf("late duplicate resurrected k = %q", res.Value)
		}
	}
}

func TestDuplicateDetectionSurvivesSnapshotRestore(t *testing.T) {
	// Every node compacts past client c7's Delete, then the whole cluster
	// restarts and rebuilds its state from those snapshots. A retry of the
	// Delete must still be recognized as a duplicate.
	for _, disk := range []bool{false, true} {
		c := newCluster(t, 3, 1, disk)
		c.setSnapshotEvery(3)
		l := c.leader()
		c7 := c.register(l) // index 2
		c.do(l, putReq(c7, 1, "k", "v"))
		c.do(l, deleteReq(c7, 2, "k")) // index 4
		c8 := c.register(l)
		for i := range 6 {
			c.do(l, putReq(c8, uint64(i+1), "other", fmt.Sprint(i)))
		}
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		for _, id := range c.ids {
			c.crash(id)
		}
		for _, id := range c.ids {
			c.start(id)
			if s := c.nodes[id].raft.Status().Snapshot; s < 4 {
				t.Fatalf("disk=%v: node %d restarted with its snapshot at %d, before the Delete", disk, id, s)
			}
		}
		l = c.leader()
		if res := c.do(l, deleteReq(c7, 2, "k")); !res.Found {
			t.Fatalf("disk=%v: retried delete after restore reported not found", disk)
		}
		if res := c.wait(c.submit(l, putReq(c7, 1, "k", "v")), 2*requestTimeout); !errors.Is(res.Err, ErrStaleSequence) {
			t.Fatalf("disk=%v: old write after restore: %+v, want ErrStaleSequence", disk, res)
		}
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		c.checkReplicasAgree()
	}
}

func TestFollowerCatchesUpBySnapshot(t *testing.T) {
	for seed := range uint64(10) {
		c := newCluster(t, 3, seed, seed%2 == 1)
		c.setSnapshotEvery(5)
		l := c.leader()
		f := others(c.ids, l)[0]
		c.net.Partition([]uint64{f}, others(c.ids, f))
		id := c.register(l)
		for i := range 30 {
			c.do(l, putReq(id, uint64(i+1), fmt.Sprint("k", i%4), fmt.Sprint(i)))
		}
		if s, last := c.nodes[l].raft.Status().Snapshot, c.nodes[f].raft.Status().LastIndex; s <= last {
			c.fatalf("leader's snapshot at %d does not pass the follower's last index %d", s, last)
		}
		c.net.Heal()
		c.waitApplied(c.nodes[l].sm.Applied(), 20*electionTick)
		c.checkReplicasAgree()
	}
}

func TestReadsAreBatched(t *testing.T) {
	c := newCluster(t, 3, 1, false)
	l := c.leader()
	c.do(l, putReq(c.register(l), 1, "k", "v"))
	first := c.nodes[l].lastID
	var calls []*call
	for range 10 {
		calls = append(calls, c.submit(l, getReq("k")))
	}
	for _, cl := range calls {
		if res := c.wait(cl, requestTimeout); res.Err != nil || string(res.Value) != "v" {
			t.Fatalf("read: %+v", res)
		}
	}
	// The first read starts a round at once; the other nine arrive while it
	// is in flight and share the next one.
	if rounds := c.nodes[l].lastID - first; rounds != 2 {
		t.Fatalf("%d ReadIndex rounds for 10 reads, want 2", rounds)
	}
}

func TestReadSeesPrecedingWrite(t *testing.T) {
	// A read submitted right after a write completes must see it, on every
	// leader, including just after an election.
	for seed := range uint64(20) {
		c := newCluster(t, 3, seed, false)
		id := c.register(c.leader())
		for i := range 5 {
			l := c.leader()
			v := fmt.Sprint(i)
			c.do(l, putReq(id, uint64(i+1), "k", v))
			if res := c.do(l, getReq("k")); string(res.Value) != v {
				c.fatalf("read %q after writing %q", res.Value, v)
			}
			c.crash(l)
			c.start(l)
		}
	}
}

func TestRequestsTimeOut(t *testing.T) {
	c := newCluster(t, 3, 1, false)
	l := c.leader()
	c.net.Partition([]uint64{l}, others(c.ids, l))
	get, put := c.submit(l, getReq("k")), c.submit(l, putReq(1, 1, "k", "v"))
	c.run(requestTimeout - 1)
	if get.done || put.done {
		t.Fatalf("completed before the timeout: get %+v, put %+v", get.res, put.res)
	}
	c.tick()
	if !errors.Is(get.res.Err, ErrTimeout) || !errors.Is(put.res.Err, ErrTimeout) {
		t.Fatalf("get %+v, put %+v; want ErrTimeout", get.res, put.res)
	}
}

func TestCheckQuorumFailsRequestsBeforeTimeout(t *testing.T) {
	// With CheckQuorum, the isolated leader in TestRequestsTimeOut steps
	// down within two election windows, well before the request timeout, and
	// its pending requests fail so clients can look for the new leader.
	c := newCluster(t, 3, 1, false, withCheckQuorum(true))
	l := c.leader()
	c.net.Partition([]uint64{l}, others(c.ids, l))
	get, put := c.submit(l, getReq("k")), c.submit(l, putReq(1, 1, "k", "v"))
	c.run(2 * electionTick)
	if !errors.Is(get.res.Err, ErrLeadershipLost) || !errors.Is(put.res.Err, ErrLeadershipLost) {
		t.Fatalf("get %+v, put %+v; want ErrLeadershipLost", get.res, put.res)
	}
	// It no longer accepts requests, and knows no leader to redirect to.
	if res := c.wait(c.submit(l, getReq("k")), 1); !errors.Is(res.Err, ErrNotLeader) || res.Leader != raft.None {
		t.Fatalf("get on stepped-down leader: %+v, want ErrNotLeader with no leader hint", res)
	}
}

func TestLeadershipLossFailsPendingRequests(t *testing.T) {
	c := newCluster(t, 3, 1, false)
	l := c.leader()
	c.net.Partition([]uint64{l}, others(c.ids, l))
	get, put := c.submit(l, getReq("k")), c.submit(l, putReq(1, 1, "k", "v"))
	// A message from a newer term deposes it.
	if err := c.nodes[l].Step(raft.Message{Type: raft.MsgApp, From: others(c.ids, l)[0], To: l, Term: 99}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(get.res.Err, ErrLeadershipLost) || !errors.Is(put.res.Err, ErrLeadershipLost) {
		t.Fatalf("get %+v, put %+v; want ErrLeadershipLost", get.res, put.res)
	}
}

func TestRestartedNodeRebuildsState(t *testing.T) {
	for _, disk := range []bool{false, true} {
		c := newCluster(t, 3, 1, disk)
		l := c.leader()
		id := c.register(l)
		for i := range 20 {
			c.do(l, putReq(id, uint64(i+1), fmt.Sprint("k", i%5), fmt.Sprint(i)))
		}
		for _, id := range c.ids {
			c.crash(id)
		}
		for _, id := range c.ids {
			c.start(id)
		}
		l = c.leader()
		for i := 15; i < 20; i++ {
			if res := c.do(l, getReq(fmt.Sprint("k", i%5))); string(res.Value) != fmt.Sprint(i) {
				t.Fatalf("disk=%v: k%d = %q after restart, want %d", disk, i%5, res.Value, i)
			}
		}
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		c.checkReplicasAgree()
	}
}

func TestEvictedClientRetryIsRefused(t *testing.T) {
	// The leader applies client a's Put and crashes before replying. While
	// a waits, two other clients register on the next leader, which keeps
	// two sessions, so a's is evicted, and one of them overwrites k. a's
	// retry must be refused, not applied as a new write that rolls k back,
	// and every replica must have evicted the same session.
	for seed := range uint64(10) {
		c := newCluster(t, 3, seed, seed%2 == 1, withMaxSessions(2))
		l := c.leader()
		a := c.register(l)
		c.do(l, putReq(a, 1, "k", "old"))
		c.waitApplied(c.nodes[l].sm.Applied(), 10*electionTick)
		c.crash(l)

		next := c.leaderAmong(others(c.ids, l), 30*electionTick)
		b := c.register(next)
		c.do(next, putReq(b, 1, "k", "new"))
		c.register(next)
		res := c.wait(c.submit(next, putReq(a, 1, "k", "old")), 2*requestTimeout)
		if !errors.Is(res.Err, ErrSessionExpired) {
			c.fatalf("retry from evicted client: %+v, want ErrSessionExpired", res)
		}
		if res := c.do(next, getReq("k")); string(res.Value) != "new" {
			c.fatalf("k = %q after the refused retry, want new", res.Value)
		}
		c.start(l)
		c.waitApplied(c.nodes[next].sm.Applied(), 20*electionTick)
		c.checkReplicasAgree()
	}
}
