package raft_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
	"github.com/jaydenmistry/kvstore/internal/transport"
)

const (
	electionTick  = 10
	heartbeatTick = 1
)

// cluster runs several Raft nodes on a simulated network, all driven from
// one goroutine so every run is reproducible from its seed. After every Step
// and Tick it checks the election safety invariants.
type cluster struct {
	t       *testing.T
	seed    uint64
	rng     *rand.Rand
	net     *transport.Network
	ids     []uint64
	nodes   map[uint64]*raft.Raft // nil while crashed
	storage map[uint64]*raft.MemoryStorage
	ticks   int

	// For invariant checks, accumulated over the whole run.
	leaders  map[uint64]uint64            // term -> the node that led it
	votes    map[uint64]map[uint64]uint64 // node -> term -> vote cast
	maxTerm  map[uint64]uint64            // node -> highest term seen
	restarts int
}

func newCluster(t *testing.T, n int, seed uint64) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		seed:    seed,
		rng:     rand.New(rand.NewPCG(seed, 1)),
		net:     transport.NewNetwork(seed),
		nodes:   make(map[uint64]*raft.Raft),
		storage: make(map[uint64]*raft.MemoryStorage),
		leaders: make(map[uint64]uint64),
		votes:   make(map[uint64]map[uint64]uint64),
		maxTerm: make(map[uint64]uint64),
	}
	for i := range n {
		id := uint64(i + 1)
		c.ids = append(c.ids, id)
		c.storage[id] = raft.NewMemoryStorage()
		c.votes[id] = make(map[uint64]uint64)
	}
	for _, id := range c.ids {
		c.start(id)
	}
	return c
}

func (c *cluster) fatalf(format string, args ...any) {
	c.t.Helper()
	c.t.Fatalf("seed %d tick %d: "+format, append([]any{c.seed, c.ticks}, args...)...)
}

// start creates a node from its storage, as on first boot or after a crash.
func (c *cluster) start(id uint64) {
	r, err := raft.New(raft.Config{
		ID:            id,
		Peers:         c.ids,
		ElectionTick:  electionTick,
		HeartbeatTick: heartbeatTick,
		Storage:       c.storage[id],
		Transport:     c.net.Endpoint(id),
		Rand:          rand.New(rand.NewPCG(c.rng.Uint64(), id)),
	})
	if err != nil {
		c.fatalf("starting node %d: %v", id, err)
	}
	c.nodes[id] = r
	c.net.SetDown(id, false)
	c.check(id)
}

// crash stops a node. Its storage survives; its in-memory state does not.
func (c *cluster) crash(id uint64) {
	c.nodes[id] = nil
	c.net.SetDown(id, true)
}

func (c *cluster) restart(id uint64) {
	c.restarts++
	c.start(id)
}

// tick delivers due messages, then ticks every live node once.
func (c *cluster) tick() {
	c.t.Helper()
	c.ticks++
	c.net.Tick(func(m raft.Message) {
		r := c.nodes[m.To]
		if r == nil {
			return
		}
		if err := r.Step(m); err != nil {
			c.fatalf("node %d Step(%+v): %v", m.To, m, err)
		}
		c.check(m.To)
	})
	for _, id := range c.ids {
		if r := c.nodes[id]; r != nil {
			if err := r.Tick(); err != nil {
				c.fatalf("node %d Tick: %v", id, err)
			}
			c.check(id)
		}
	}
}

func (c *cluster) run(ticks int) {
	c.t.Helper()
	for range ticks {
		c.tick()
	}
}

// check verifies the invariants for one node after it changed state.
func (c *cluster) check(id uint64) {
	c.t.Helper()
	s := c.nodes[id].Status()

	// Election Safety (§5.2): at most one leader per term.
	if s.State == raft.Leader {
		if prev, ok := c.leaders[s.Term]; ok && prev != id {
			c.fatalf("two leaders in term %d: %d and %d", s.Term, prev, id)
		}
		c.leaders[s.Term] = id
	}

	// currentTerm never decreases, even across restarts.
	if s.Term < c.maxTerm[id] {
		c.fatalf("node %d term went from %d to %d", id, c.maxTerm[id], s.Term)
	}
	c.maxTerm[id] = s.Term

	// votedFor never changes within a term, even across restarts. This is
	// what persisting the vote before replying guarantees.
	if s.Vote != raft.None {
		if prev, ok := c.votes[id][s.Term]; ok && prev != s.Vote {
			c.fatalf("node %d voted for %d and then %d in term %d", id, prev, s.Vote, s.Term)
		}
		c.votes[id][s.Term] = s.Vote
	}
}

// leader returns the leader that every live node in the cluster agrees on:
// it is the only leader among the nodes in the highest term, and every other
// live node is its follower. Otherwise it returns None.
func (c *cluster) leader() uint64 {
	var top uint64
	for _, r := range c.nodes {
		if r != nil {
			top = max(top, r.Status().Term)
		}
	}
	lead := raft.None
	for _, id := range c.ids {
		r := c.nodes[id]
		if r == nil {
			continue
		}
		s := r.Status()
		if s.Term != top || s.Lead == raft.None || (lead != raft.None && s.Lead != lead) {
			return raft.None
		}
		lead = s.Lead
	}
	if lead == raft.None || c.nodes[lead] == nil || c.nodes[lead].Status().State != raft.Leader {
		return raft.None
	}
	return lead
}

// waitForLeader ticks until the live nodes agree on a leader.
func (c *cluster) waitForLeader(maxTicks int) uint64 {
	c.t.Helper()
	for range maxTicks {
		if l := c.leader(); l != raft.None {
			return l
		}
		c.tick()
	}
	c.fatalf("no agreed leader within %d ticks: %s", maxTicks, c.describe())
	return raft.None
}

// leaderAmong ticks until the given nodes agree on a leader that is one of
// them, ignoring every other node.
func (c *cluster) leaderAmong(ids []uint64, maxTicks int) uint64 {
	c.t.Helper()
	for range maxTicks {
		var top uint64
		for _, id := range ids {
			top = max(top, c.nodes[id].Status().Term)
		}
		lead, ok := raft.None, true
		for _, id := range ids {
			s := c.nodes[id].Status()
			if s.Term != top || s.Lead == raft.None || (lead != raft.None && s.Lead != lead) {
				ok = false
				break
			}
			lead = s.Lead
		}
		if ok && slices.Contains(ids, lead) && c.nodes[lead].Status().State == raft.Leader {
			return lead
		}
		c.tick()
	}
	c.fatalf("nodes %v did not agree on a leader within %d ticks: %s", ids, maxTicks, c.describe())
	return raft.None
}

func (c *cluster) describe() string {
	var b strings.Builder
	for _, id := range c.ids {
		if r := c.nodes[id]; r == nil {
			fmt.Fprintf(&b, "\n  node %d down", id)
		} else {
			s := r.Status()
			fmt.Fprintf(&b, "\n  node %d %v term %d vote %d lead %d", s.ID, s.State, s.Term, s.Vote, s.Lead)
		}
	}
	return b.String()
}

func others(ids []uint64, exclude ...uint64) []uint64 {
	var out []uint64
	for _, id := range ids {
		if !slices.Contains(exclude, id) {
			out = append(out, id)
		}
	}
	return out
}

func TestElectsOneStableLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		for seed := range uint64(50) {
			c := newCluster(t, n, seed)
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
		c := newCluster(t, 5, seed)
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
