package raft_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// checkAllApplied verifies that every live node has applied exactly the same
// entries.
func (c *cluster) checkAllApplied() {
	c.t.Helper()
	var ref uint64
	for _, id := range c.ids {
		if c.nodes[id] == nil {
			continue
		}
		if ref == 0 {
			ref = id
			continue
		}
		a, b := c.applied[ref], c.applied[id]
		if !slices.EqualFunc(a, b, sameEntry) {
			c.fatalf("nodes %d and %d applied different entries (%d vs %d)", ref, id, len(a), len(b))
		}
	}
}

func TestReplicatesToEveryNode(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		for seed := range uint64(20) {
			c := newCluster(t, n, seed)
			c.waitForLeader(20 * electionTick)
			var want []string
			for i := range 100 {
				cmd := fmt.Sprintf("cmd-%d", i)
				if !c.propose(cmd) {
					c.fatalf("no leader for proposal %d", i)
				}
				want = append(want, cmd)
				c.tick()
			}
			c.waitConverged(20 * electionTick)
			for _, id := range c.ids {
				if got := c.appliedData(id); !slices.Equal(got, want) {
					c.fatalf("node %d applied %d commands, want the %d proposed in order", id, len(got), len(want))
				}
			}
			c.checkAllApplied()
		}
	}
}

func TestLaggingFollowerCatchesUp(t *testing.T) {
	// A follower cut off while 300 entries commit must catch up, through
	// several batches, once it can be reached again.
	for seed := range uint64(20) {
		c := newCluster(t, 5, seed)
		lead := c.waitForLeader(20 * electionTick)
		lag := others(c.ids, lead)[0]
		c.net.Partition([]uint64{lag}, others(c.ids, lag))

		for i := range 300 {
			c.propose(fmt.Sprintf("cmd-%d", i))
			if i%10 == 0 {
				c.tick()
			}
		}
		c.run(5)
		if len(c.applied[lag]) >= len(c.applied[lead]) {
			c.fatalf("partitioned follower kept up")
		}

		c.net.Heal()
		c.waitConverged(50 * electionTick)
		c.checkAllApplied()
	}
}

func TestLaggingFollowerCatchesUpBySnapshot(t *testing.T) {
	// While a follower is cut off, the others compact their logs past what
	// it has. It can only catch up by installing a snapshot.
	for seed := range uint64(20) {
		c := newCluster(t, 5, seed)
		c.compactEvery = 20
		lead := c.waitForLeader(20 * electionTick)
		lag := others(c.ids, lead)[0]
		c.net.Partition([]uint64{lag}, others(c.ids, lag))
		for i := range 200 {
			c.propose(fmt.Sprintf("cmd-%d", i))
			if i%10 == 0 {
				c.tick()
			}
		}
		c.run(5)
		if s := c.nodes[lead].Status(); s.Snapshot <= c.nodes[lag].Status().LastIndex {
			c.fatalf("leader's snapshot at %d does not pass the follower's last index %d", s.Snapshot, c.nodes[lag].Status().LastIndex)
		}
		c.net.Heal()
		c.waitConverged(50 * electionTick)
		c.checkAllApplied()
		if c.installed == 0 {
			c.fatalf("the follower caught up without installing a snapshot")
		}
	}
}

func TestDeposedLeaderEntriesAreReplaced(t *testing.T) {
	// Figure 7 in miniature. The leader is cut off and keeps accepting
	// proposals it can never commit. The majority elects a new leader and
	// commits different entries at the same indexes. After healing, the old
	// leader's uncommitted entries must be replaced everywhere, and none of
	// them may ever be applied.
	for seed := range uint64(30) {
		c := newCluster(t, 5, seed)
		old := c.waitForLeader(20 * electionTick)
		rest := others(c.ids, old)
		c.net.Partition([]uint64{old}, rest)

		for i := range 10 {
			c.propose(fmt.Sprintf("lost-%d", i)) // reaches only the old leader
		}
		c.leaderAmong(rest, 20*electionTick)
		for i := range 10 {
			c.propose(fmt.Sprintf("kept-%d", i))
			c.tick()
		}
		c.net.Heal()
		c.waitConverged(20 * electionTick)
		c.checkAllApplied()

		for _, id := range c.ids {
			got := c.appliedData(id)
			for _, cmd := range got {
				if strings.HasPrefix(cmd, "lost-") {
					c.fatalf("node %d applied %q, which was never committed", id, cmd)
				}
			}
			if !slices.Equal(got, []string{"kept-0", "kept-1", "kept-2", "kept-3", "kept-4",
				"kept-5", "kept-6", "kept-7", "kept-8", "kept-9"}) {
				c.fatalf("node %d applied %v", id, got)
			}
		}
	}
}

func TestLeaderCrashMidReplication(t *testing.T) {
	// The leader crashes after its entries reached only some followers.
	// Whatever had committed must survive into the next term; the rest may
	// or may not, and the invariant checks make sure nothing is applied
	// inconsistently either way.
	for seed := range uint64(50) {
		c := newCluster(t, 5, seed)
		c.net.SetDelay(1, 3)
		old := c.waitForLeader(30 * electionTick)
		for i := range 20 {
			c.propose(fmt.Sprintf("a-%d", i))
		}
		c.run(1 + int(seed%3)) // let replication get partway
		committedBefore := len(c.committed)
		c.crash(old)

		c.waitForLeader(30 * electionTick)
		for i := range 5 {
			c.propose(fmt.Sprintf("b-%d", i))
			c.tick()
		}
		c.restart(old)
		c.waitConverged(30 * electionTick)
		c.checkAllApplied()
		if len(c.applied[old]) < committedBefore {
			c.fatalf("restarted old leader applied %d entries, fewer than the %d committed before it crashed",
				len(c.applied[old]), committedBefore)
		}
	}
}

func TestCommittedEntriesSurviveChaos(t *testing.T) {
	// Proposals under message loss, duplication, delay, and reordering, with
	// random partitions and crashes. Half the runs persist through the WAL
	// on a crash-simulating filesystem, where a crash is a power loss.
	// Safety is checked after every step. Afterwards, with everything
	// repaired, every node must converge on the same applied log, which
	// must contain every entry that ever committed.
	const seeds = 200
	var proposed, committed, installed int
	for seed := range uint64(seeds) {
		var c *cluster
		quorum := withCheckQuorum(seed/4%2 == 1)
		if seed%2 == 0 {
			c = newCluster(t, 5, seed, quorum)
		} else {
			c = newDiskCluster(t, 5, seed, quorum)
		}
		// A third of the runs never compact, a third compact often.
		c.compactEvery = []int{0, 7, 30}[seed%3]
		rng := rand.New(rand.NewPCG(seed, 3))
		c.net.SetDropRate(0.1)
		c.net.SetDuplicateRate(0.05)
		c.net.SetDelay(1, []int{3, 2 * electionTick}[seed/2%2])

		for i := range 1000 {
			switch p := rng.IntN(100); {
			case p < 2:
				ids := slices.Clone(c.ids)
				rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				k := 1 + rng.IntN(len(ids)-1)
				c.net.Partition(ids[:k], ids[k:])
			case p < 4:
				c.net.Heal()
			case p < 5:
				if id := c.ids[rng.IntN(len(c.ids))]; c.nodes[id] != nil {
					c.crash(id)
				}
			case p < 8:
				if id := c.ids[rng.IntN(len(c.ids))]; c.nodes[id] == nil {
					c.restart(id)
				}
			case p < 40:
				if c.propose(fmt.Sprintf("s%d-%d", seed, i)) {
					proposed++
				}
			}
			c.tick()
		}

		// Repair everything and let the cluster converge.
		c.net.Heal()
		c.net.SetDropRate(0)
		c.net.SetDuplicateRate(0)
		c.net.SetDelay(1, 1)
		for _, id := range c.ids {
			if c.nodes[id] == nil {
				c.restart(id)
			}
		}
		c.waitForLeader(50 * electionTick)
		if !c.propose("final") {
			c.fatalf("no leader to accept the final proposal")
		}
		c.waitConverged(50 * electionTick)
		c.checkAllApplied()
		c.checkLogMatching()

		// Every entry that ever committed is in every node's applied log.
		for _, id := range c.ids {
			if got := len(c.applied[id]); got != len(c.committed) {
				c.fatalf("node %d applied %d entries, but %d committed", id, got, len(c.committed))
			}
		}
		if last := c.applied[c.ids[0]]; string(last[len(last)-1].Data) != "final" {
			c.fatalf("final proposal not applied last")
		}
		committed += len(c.committed)
		installed += c.installed
	}
	t.Logf("%d proposals accepted by a leader, %d entries committed, %d snapshots installed, across %d runs", proposed, committed, installed, seeds)
	if committed < seeds*50 {
		t.Fatalf("only %d entries committed across %d runs; chaos too severe to test much", committed, seeds)
	}
}

func TestDiskClusterSurvivesFullPowerLoss(t *testing.T) {
	// Every node loses power at once, repeatedly. Committed entries must
	// survive because each was fsynced on a majority before it committed.
	for seed := range uint64(30) {
		c := newDiskCluster(t, 3, seed)
		if seed%2 == 1 {
			c.compactEvery = 5
		}
		for round := range 5 {
			c.waitForLeader(30 * electionTick)
			for i := range 10 {
				c.propose(fmt.Sprintf("r%d-%d", round, i))
				c.tick()
			}
			c.run(int(seed % 4))
			for _, id := range c.ids {
				c.crash(id)
			}
			for _, id := range c.ids {
				c.restart(id)
			}
		}
		c.waitForLeader(30 * electionTick)
		c.propose("final")
		c.waitConverged(30 * electionTick)
		c.checkAllApplied()
	}
}
