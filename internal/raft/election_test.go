package raft_test

import (
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

func TestElectsOneStableLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		for seed := range uint64(50) {
			// CheckQuorum must not depose a leader that can reach everyone.
			c := newCluster(t, n, seed, withCheckQuorum(seed%2 == 1), withPreVote(seed/2%2 == 1))
			lead := c.waitForLeader(20 * electionTick)
			term := c.nodes[lead].Status().Term

			// On a reliable network, leadership must not change.
			c.run(10 * electionTick)
			if l := c.leader(); l != lead || c.nodes[lead].Status().Term != term {
				c.fatalf("leadership changed from %d (term %d) to %d on a reliable network: %s",
					lead, term, l, c.describe())
			}
			for _, id := range c.ids {
				if s := c.nodes[id].Status(); id != lead && s.State != raft.Follower {
					c.fatalf("node %d is %v under a stable leader", id, s.State)
				}
			}
		}
	}
}

func TestNewLeaderAfterLeaderPartitioned(t *testing.T) {
	for _, n := range []int{3, 5} {
		for seed := range uint64(30) {
			c := newCluster(t, n, seed)
			old := c.waitForLeader(20 * electionTick)
			oldTerm := c.nodes[old].Status().Term
			rest := others(c.ids, old)

			c.net.Partition([]uint64{old}, rest)
			lead := c.leaderAmong(rest, 20*electionTick)
			if term := c.nodes[lead].Status().Term; term <= oldTerm {
				c.fatalf("new leader %d in term %d, want a term after %d", lead, term, oldTerm)
			}
			// The isolated leader cannot learn it was deposed. That is
			// allowed, because its term is older, but it is why reads must
			// confirm leadership with a majority before answering.
			if s := c.nodes[old].Status(); s.State != raft.Leader || s.Term != oldTerm {
				c.fatalf("isolated old leader is %+v, want still leader in term %d", s, oldTerm)
			}

			// After healing, the old leader sees the higher term and steps
			// down, and the whole cluster converges on one leader.
			c.net.Heal()
			c.waitForLeader(20 * electionTick)
			if s := c.nodes[old].Status(); s.Term <= oldTerm {
				c.fatalf("old leader still in term %d after healing", s.Term)
			}
		}
	}
}

func TestCheckQuorumDeposesIsolatedLeader(t *testing.T) {
	// Without CheckQuorum, an isolated leader stays leader for as long as
	// it is cut off (TestNewLeaderAfterLeaderPartitioned). With it, the
	// leader steps down within two ElectionTick windows, whether it is
	// alone or with one follower in a cluster of five. Its term does not
	// change until it campaigns.
	for _, n := range []int{3, 5} {
		for seed := range uint64(30) {
			c := newCluster(t, n, seed, withCheckQuorum(true))
			old := c.waitForLeader(20 * electionTick)
			oldTerm := c.nodes[old].Status().Term
			minority := []uint64{old}
			if n == 5 {
				minority = append(minority, others(c.ids, old)[0])
			}
			majority := others(c.ids, minority...)
			c.net.Partition(minority, majority)

			stepped := false
			for range 2 * electionTick {
				c.tick()
				if s := c.nodes[old].Status(); s.State != raft.Leader {
					if s.Term != oldTerm {
						c.fatalf("old leader moved from term %d to %d on stepping down", oldTerm, s.Term)
					}
					stepped = true
					break
				}
			}
			if !stepped {
				c.fatalf("isolated leader %d still leads after %d ticks: %s", old, 2*electionTick, c.describe())
			}
			lead := c.leaderAmong(majority, 20*electionTick)
			for range 10 * electionTick {
				c.tick()
				for _, id := range minority {
					if c.nodes[id].Status().State == raft.Leader {
						c.fatalf("node %d in the minority became leader", id)
					}
				}
			}
			if l := c.leaderAmong(majority, 1); l != lead {
				c.fatalf("majority leader changed from %d to %d during the partition", lead, l)
			}
			c.net.Heal()
			c.waitForLeader(20 * electionTick)
		}
	}
}

func TestMinorityCannotElectLeader(t *testing.T) {
	for seed := range uint64(30) {
		c := newCluster(t, 5, seed)
		lead := c.waitForLeader(20 * electionTick)
		leadTerm := c.nodes[lead].Status().Term
		minority := others(c.ids, lead)[:2]
		majority := others(c.ids, minority...)

		c.net.Partition(minority, majority)
		for range 30 * electionTick {
			c.tick()
			for _, id := range minority {
				if c.nodes[id].Status().State == raft.Leader {
					c.fatalf("node %d in a 2-node minority became leader", id)
				}
			}
		}
		// The minority keeps timing out and raising its term...
		for _, id := range minority {
			if s := c.nodes[id].Status(); s.Term <= leadTerm {
				c.fatalf("minority node %d still in term %d; expected repeated elections", id, s.Term)
			}
		}
		// ...while the majority keeps its leader undisturbed.
		if s := c.nodes[lead].Status(); s.State != raft.Leader || s.Term != leadTerm {
			c.fatalf("majority leader changed to %+v during the partition", s)
		}

		// When the partition heals, the minority's higher term forces the
		// leader to step down and a new election happens. This disruption is
		// what Pre-Vote (Ongaro's thesis, section 9.6) would prevent.
		c.net.Heal()
		newLead := c.waitForLeader(20 * electionTick)
		if c.nodes[newLead].Status().Term <= leadTerm {
			c.fatalf("no new term after healing")
		}
	}
}

func TestReelectionAfterLeaderCrash(t *testing.T) {
	for seed := range uint64(30) {
		c := newCluster(t, 5, seed)
		old := c.waitForLeader(20 * electionTick)
		c.crash(old)
		lead := c.waitForLeader(20 * electionTick)
		if lead == old {
			c.fatalf("crashed node %d still leader", old)
		}
		// The old leader restarts as a follower and rejoins.
		c.restart(old)
		c.waitForLeader(20 * electionTick)
		if s := c.nodes[old].Status(); s.State != raft.Follower {
			c.fatalf("restarted node is %v, want follower", s.State)
		}
	}
}

func TestNoTwoLeadersInSameTermUnderChaos(t *testing.T) {
	// Random message loss, delay, duplication, and reordering, plus random
	// partitions, crashes, and restarts. The cluster checks the safety
	// invariants after every step. Afterwards, with the network repaired,
	// a leader must still emerge.
	const seeds = 300
	delays := []int{4, 2 * electionTick}
	elections := make([]int, len(delays))
	for seed := range uint64(seeds) {
		c := newCluster(t, 5, seed, withCheckQuorum(seed/2%2 == 1), withPreVote(seed/4%2 == 1))
		rng := rand.New(rand.NewPCG(seed, 2))
		c.net.SetDropRate(0.2)
		c.net.SetDuplicateRate(0.1)
		// Some runs delay messages for longer than an election timeout, so
		// responses can arrive after the sender has moved on to a new term.
		c.net.SetDelay(1, delays[seed%2])

		for range 1000 {
			switch p := rng.IntN(100); {
			case p < 2:
				// Partition into two random groups.
				ids := slices.Clone(c.ids)
				rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
				k := 1 + rng.IntN(len(ids)-1)
				c.net.Partition(ids[:k], ids[k:])
			case p < 4:
				c.net.Heal()
			case p < 6:
				if id := c.ids[rng.IntN(len(c.ids))]; c.nodes[id] != nil {
					c.crash(id)
				}
			case p < 9:
				if id := c.ids[rng.IntN(len(c.ids))]; c.nodes[id] == nil {
					c.restart(id)
				}
			}
			c.tick()
		}
		elections[seed%2] += len(c.leaders)

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
	}
	// Guard against chaos so severe that few elections happen and the
	// invariants are barely exercised.
	t.Logf("leaders elected per delay profile %v: %v", delays, elections)
	for i, n := range elections {
		if runs := seeds / len(delays); n < runs/2 {
			t.Fatalf("only %d leaders elected with max delay %d; chaos too severe to test much", n, delays[i])
		}
	}
}
