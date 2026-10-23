package raft_test

import (
	"fmt"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

func TestPreVoteRejoiningNodeDoesNotDisrupt(t *testing.T) {
	// Without PreVote, a node cut off from the cluster raises its term with
	// every election timeout, and when it rejoins, its higher term deposes
	// the leader (TestMinorityCannotElectLeader). With PreVote no one
	// answers it, so it keeps its term, and the leader's next heartbeat
	// makes it a follower again.
	for _, n := range []int{3, 5} {
		for seed := range uint64(30) {
			c := newCluster(t, n, seed, withPreVote(true), withCheckQuorum(seed%2 == 1))
			lead := c.waitForLeader(20 * electionTick)
			term := c.nodes[lead].Status().Term
			f := others(c.ids, lead)[0]
			c.net.Partition([]uint64{f}, others(c.ids, f))
			c.run(10 * electionTick)
			if s := c.nodes[f].Status(); s.Term != term || s.State != raft.PreCandidate {
				c.fatalf("isolated node is %v in term %d, want pre-candidate in term %d", s.State, s.Term, term)
			}
			c.net.Heal()
			c.run(10 * electionTick)
			if l := c.leader(); l != lead || c.nodes[lead].Status().Term != term {
				c.fatalf("leader %d after the rejoin, want %d still in term %d: %s", l, lead, term, c.describe())
			}
		}
	}
}

func TestPreVoteRefusesNodeThatCannotHearLeader(t *testing.T) {
	// The leader's messages to f are lost, but f reaches everyone. f times
	// out again and again. If the others granted its pre-votes, it would
	// start elections, and each would depose the leader. They refuse, for
	// one of two reasons:
	//
	//   - With CheckQuorum, they have heard from the leader recently
	//     (leader stickiness). Here no entries are proposed, so f's log is
	//     as up to date as theirs and only stickiness stops it.
	//   - Without CheckQuorum, f's log is behind: the leader keeps
	//     committing entries f never receives.
	for _, checkQuorum := range []bool{true, false} {
		for _, n := range []int{3, 5} {
			for seed := range uint64(30) {
				c := newCluster(t, n, seed, withPreVote(true), withCheckQuorum(checkQuorum))
				lead := c.waitForLeader(20 * electionTick)
				term := c.nodes[lead].Status().Term
				f := others(c.ids, lead)[0]
				c.net.Cut(lead, f)
				for i := range 20 * electionTick {
					if !checkQuorum && i%electionTick == 0 {
						c.propose(fmt.Sprintf("e%d", i))
					}
					c.tick()
					if s := c.nodes[lead].Status(); s.State != raft.Leader || s.Term != term {
						c.fatalf("checkQuorum %v: leader %d deposed by node %d, which cannot hear it: %s",
							checkQuorum, lead, f, c.describe())
					}
				}
			}
		}
	}
}

func TestPreVoteOneWayPartitionMakesProgress(t *testing.T) {
	// The leader's messages reach follower a, but nothing from a reaches
	// the leader, and the leader and b cannot reach each other at all. The
	// leader cannot commit. b must still be elected, with a's vote, so the
	// cluster can make progress.
	//
	// If a refused pre-votes because it keeps hearing from the leader, b
	// could never win. That is why such refusals need CheckQuorum, which
	// makes the leader step down (see inLeaderLease).
	for _, checkQuorum := range []bool{false, true} {
		for seed := range uint64(30) {
			c := newCluster(t, 3, seed, withPreVote(true), withCheckQuorum(checkQuorum))
			old := c.waitForLeader(20 * electionTick)
			a, b := others(c.ids, old)[0], others(c.ids, old)[1]
			c.net.Cut(a, old)
			c.net.Cut(b, old)
			c.net.Cut(old, b)

			lead := c.leaderAmong([]uint64{a, b}, 30*electionTick)
			index, _, err := c.nodes[lead].Propose([]byte("progress"))
			if err != nil {
				c.fatalf("Propose on new leader %d: %v", lead, err)
			}
			for i := 0; c.nodes[a].Status().Commit < index || c.nodes[b].Status().Commit < index; i++ {
				if i == 10*electionTick {
					c.fatalf("checkQuorum %v: entry %d not committed on %d and %d: %s", checkQuorum, index, a, b, c.describe())
				}
				c.tick()
			}
		}
	}
}
