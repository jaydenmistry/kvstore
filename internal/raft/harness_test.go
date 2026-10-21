package raft_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
	"github.com/jaydenmistry/kvstore/internal/transport"
	"github.com/jaydenmistry/kvstore/internal/vfs"
)

const (
	electionTick  = 10
	heartbeatTick = 1
)

// cluster runs several Raft nodes on a simulated network, all driven from
// one goroutine so every run is reproducible from its seed. After every Step
// and Tick it checks Raft's safety properties (Figure 3) for the node that
// just changed:
//
//   - Election Safety: at most one leader per term.
//   - State Machine Safety: every committed index holds the same entry on
//     every node, forever, and each node applies entries in order, once.
//   - Leader Completeness: a new leader's log holds every committed entry.
//
// plus that terms never decrease and votes never change within a term, even
// across restarts. Log Matching is checked across all pairs of nodes every
// few ticks, because it is quadratic.
//
// Each node's state machine is the list of entries it has applied. With
// compactEvery set, nodes snapshot that list and compact their logs as they
// go, and a snapshot a node restores must hold exactly the committed
// entries.
type cluster struct {
	t     *testing.T
	seed  uint64
	rng   *rand.Rand
	net   *transport.Network
	ids   []uint64
	nodes map[uint64]*raft.Raft // nil while crashed
	ticks int

	storage map[uint64]raft.Storage
	// disks is set for disk-backed clusters: each node has DiskStorage on
	// its own crash-simulating filesystem, and a crash is a power loss.
	disks map[uint64]*vfs.Mem

	// Invariant state, accumulated over the whole run.
	leaders   map[uint64]uint64            // term -> the node that led it
	votes     map[uint64]map[uint64]uint64 // node -> term -> vote cast
	maxTerm   map[uint64]uint64            // node -> highest term seen
	committed map[uint64]raft.Entry        // index -> the entry committed there

	// Per node, reset when the node restarts.
	seenCommit    map[uint64]uint64       // commit index already recorded
	applied       map[uint64][]raft.Entry // the node's state machine
	checkedLeader map[uint64]uint64       // term whose Leader Completeness was checked

	// compactEvery, if positive, makes each node compact its log whenever
	// it has applied that many entries past its snapshot.
	compactEvery int
	restoring    bool // set while start runs check on a new node
	// installed counts snapshots restored by running nodes, which come
	// from InstallSnapshot (not from a restart).
	installed int
	// checkQuorum is passed to every node's Config.
	checkQuorum bool
}

// clusterOption configures a cluster before its nodes start.
type clusterOption func(*cluster)

func withCheckQuorum(on bool) clusterOption {
	return func(c *cluster) { c.checkQuorum = on }
}

func newCluster(t *testing.T, n int, seed uint64, opts ...clusterOption) *cluster {
	return buildCluster(t, n, seed, false, opts)
}

// newDiskCluster is like newCluster, but nodes persist through DiskStorage
// and the WAL, and crash by losing power.
func newDiskCluster(t *testing.T, n int, seed uint64, opts ...clusterOption) *cluster {
	return buildCluster(t, n, seed, true, opts)
}

func buildCluster(t *testing.T, n int, seed uint64, disk bool, opts []clusterOption) *cluster {
	t.Helper()
	c := &cluster{
		t:             t,
		seed:          seed,
		rng:           rand.New(rand.NewPCG(seed, 1)),
		net:           transport.NewNetwork(seed),
		nodes:         make(map[uint64]*raft.Raft),
		storage:       make(map[uint64]raft.Storage),
		leaders:       make(map[uint64]uint64),
		votes:         make(map[uint64]map[uint64]uint64),
		maxTerm:       make(map[uint64]uint64),
		committed:     make(map[uint64]raft.Entry),
		seenCommit:    make(map[uint64]uint64),
		applied:       make(map[uint64][]raft.Entry),
		checkedLeader: make(map[uint64]uint64),
	}
	if disk {
		c.disks = make(map[uint64]*vfs.Mem)
	}
	for _, o := range opts {
		o(c)
	}
	for i := range n {
		id := uint64(i + 1)
		c.ids = append(c.ids, id)
		c.votes[id] = make(map[uint64]uint64)
		if disk {
			c.disks[id] = vfs.NewMem()
			c.disks[id].SetPageSize(64)
		} else {
			c.storage[id] = raft.NewMemoryStorage()
		}
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
	c.t.Helper()
	if c.disks != nil {
		s, err := raft.OpenDiskStorage(c.disks[id], "/data")
		if err != nil {
			c.fatalf("node %d: opening storage: %v", id, err)
		}
		c.storage[id] = s
	}
	r, err := raft.New(raft.Config{
		ID:            id,
		Peers:         c.ids,
		ElectionTick:  electionTick,
		HeartbeatTick: heartbeatTick,
		CheckQuorum:   c.checkQuorum,
		Storage:       c.storage[id],
		Transport:     c.net.Endpoint(id),
		Rand:          rand.New(rand.NewPCG(c.rng.Uint64(), id)),
	})
	if err != nil {
		c.fatalf("starting node %d: %v", id, err)
	}
	c.nodes[id] = r
	c.seenCommit[id] = r.Status().Snapshot
	c.applied[id] = nil
	delete(c.checkedLeader, id)
	c.net.SetDown(id, false)
	c.restoring = true
	c.check(id)
	c.restoring = false
}

// crash stops a node, losing its in-memory state. With MemoryStorage,
// everything saved survives, as after a process crash. With DiskStorage, the
// machine loses power and the crash-simulating filesystem decides what
// reached disk.
func (c *cluster) crash(id uint64) {
	c.nodes[id] = nil
	c.net.SetDown(id, true)
	if c.disks != nil {
		c.disks[id].Crash(c.rng)
	}
}

func (c *cluster) restart(id uint64) {
	c.t.Helper()
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
	if c.ticks%10 == 0 {
		c.checkLogMatching()
	}
}

func (c *cluster) run(ticks int) {
	c.t.Helper()
	for range ticks {
		c.tick()
	}
}

func sameEntry(a, b raft.Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && bytes.Equal(a.Data, b.Data)
}

// check verifies the invariants for one node after it changed state, and
// applies its newly committed entries.
func (c *cluster) check(id uint64) {
	c.t.Helper()
	r := c.nodes[id]
	s := r.Status()

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

	// Committed entries are permanent: whichever node commits an index,
	// whenever, it must be the same entry. The commit index also never moves
	// backward while a node is running.
	if s.Commit < c.seenCommit[id] {
		c.fatalf("node %d commit went from %d to %d", id, c.seenCommit[id], s.Commit)
	}
	for i := c.seenCommit[id] + 1; i <= s.Commit; i++ {
		e, ok := r.Entry(i)
		if !ok && i <= s.Snapshot {
			continue // installed from a snapshot; checked when it is restored
		}
		if !ok {
			c.fatalf("node %d commit %d is past its last index %d", id, s.Commit, s.LastIndex)
		}
		if prev, ok := c.committed[i]; ok && !sameEntry(prev, e) {
			c.fatalf("index %d committed as %+v and as %+v (node %d)", i, prev, e, id)
		}
		c.committed[i] = e
	}
	c.seenCommit[id] = s.Commit

	// Leader Completeness (§5.4): checked once each time a node becomes
	// leader.
	if s.State == raft.Leader && c.checkedLeader[id] != s.Term {
		c.checkedLeader[id] = s.Term
		for i, want := range c.committed {
			if i <= s.Snapshot {
				continue // in its snapshot, which was checked when taken
			}
			if got, ok := r.Entry(i); !ok || !sameEntry(got, want) {
				c.fatalf("node %d became leader of term %d without committed entry %+v (has %+v)", id, s.Term, want, got)
			}
		}
	}

	// A snapshot to restore replaces the state machine. It must hold
	// exactly the committed entries up to its index.
	if snap := r.TakeSnapshot(); snap != nil {
		ents := decodeApplied(snap.Data)
		if uint64(len(ents)) != snap.Index {
			c.fatalf("node %d restored a snapshot at %d holding %d entries", id, snap.Index, len(ents))
		}
		for _, e := range ents {
			if want, ok := c.committed[e.Index]; ok && !sameEntry(e, want) {
				c.fatalf("node %d restored %+v from a snapshot, but %+v is committed there", id, e, want)
			}
			c.committed[e.Index] = e
		}
		if last := ents[len(ents)-1]; last.Term != snap.Term {
			c.fatalf("node %d snapshot at %d claims term %d, but its last entry has term %d", id, snap.Index, snap.Term, last.Term)
		}
		c.applied[id] = ents
		if !c.restoring {
			c.installed++
		}
	}

	// Apply: entries come out in order, each exactly once, and each is the
	// entry committed at its index.
	for _, e := range r.TakeCommitted() {
		if want := uint64(len(c.applied[id])) + 1; e.Index != want {
			c.fatalf("node %d applied index %d, want %d", id, e.Index, want)
		}
		if !sameEntry(e, c.committed[e.Index]) {
			c.fatalf("node %d applied %+v but %+v is committed there", id, e, c.committed[e.Index])
		}
		c.applied[id] = append(c.applied[id], e)
	}

	if c.compactEvery > 0 {
		if n := uint64(len(c.applied[id])); n >= s.Snapshot+uint64(c.compactEvery) {
			if err := r.Compact(n, encodeApplied(c.applied[id])); err != nil {
				c.fatalf("node %d Compact(%d): %v", id, n, err)
			}
		}
	}
}

// encodeApplied serializes a state machine for a snapshot.
func encodeApplied(ents []raft.Entry) []byte {
	var b []byte
	for _, e := range ents {
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	return b
}

func decodeApplied(b []byte) []raft.Entry {
	var ents []raft.Entry
	for len(b) > 0 {
		e := raft.Entry{Index: binary.LittleEndian.Uint64(b), Term: binary.LittleEndian.Uint64(b[8:])}
		n := binary.LittleEndian.Uint32(b[16:])
		if n > 0 {
			e.Data = b[20 : 20+n]
		}
		ents = append(ents, e)
		b = b[20+n:]
	}
	return ents
}

// checkLogMatching verifies Log Matching (§5.3) for every pair of live
// nodes: if two logs hold an entry with the same index and term, the logs
// are identical up to and including it.
func (c *cluster) checkLogMatching() {
	c.t.Helper()
	for i, a := range c.ids {
		for _, b := range c.ids[i+1:] {
			ra, rb := c.nodes[a], c.nodes[b]
			if ra == nil || rb == nil {
				continue
			}
			n := min(ra.Status().LastIndex, rb.Status().LastIndex)
			diverged := false
			for idx := max(ra.Status().Snapshot, rb.Status().Snapshot) + 1; idx <= n; idx++ {
				ea, _ := ra.Entry(idx)
				eb, _ := rb.Entry(idx)
				if ea.Term != eb.Term {
					diverged = true
					continue
				}
				if diverged || !bytes.Equal(ea.Data, eb.Data) {
					c.fatalf("Log Matching violated between nodes %d and %d at index %d (term %d)", a, b, idx, ea.Term)
				}
			}
		}
	}
}

// propose submits data through the node that leads the highest term among
// live nodes, if any. It reports whether a leader accepted it.
func (c *cluster) propose(data string) bool {
	c.t.Helper()
	var lead *raft.Raft
	var top uint64
	for _, id := range c.ids {
		if r := c.nodes[id]; r != nil {
			if s := r.Status(); s.State == raft.Leader && s.Term >= top {
				lead, top = r, s.Term
			}
		}
	}
	if lead == nil {
		return false
	}
	if _, _, err := lead.Propose([]byte(data)); err != nil {
		c.fatalf("Propose(%q) on node %d: %v", data, lead.Status().ID, err)
	}
	c.check(lead.Status().ID)
	return true
}

// appliedData returns the commands a node has applied, skipping no-ops.
func (c *cluster) appliedData(id uint64) []string {
	var out []string
	for _, e := range c.applied[id] {
		if len(e.Data) > 0 {
			out = append(out, string(e.Data))
		}
	}
	return out
}

// waitConverged ticks until the live nodes agree on a leader, the leader has
// committed every entry in its log, and every live node has applied all of
// them. It returns the number of entries applied.
func (c *cluster) waitConverged(maxTicks int) int {
	c.t.Helper()
	for range maxTicks {
		if l := c.leader(); l != 0 {
			s := c.nodes[l].Status()
			done := s.Commit == s.LastIndex
			for _, id := range c.ids {
				if c.nodes[id] != nil && uint64(len(c.applied[id])) != s.LastIndex {
					done = false
				}
			}
			if done {
				return int(s.LastIndex)
			}
		}
		c.tick()
	}
	c.fatalf("cluster did not converge within %d ticks: %s", maxTicks, c.describe())
	return 0
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
			fmt.Fprintf(&b, "\n  node %d %v term %d vote %d lead %d commit %d last %d applied %d",
				s.ID, s.State, s.Term, s.Vote, s.Lead, s.Commit, s.LastIndex, len(c.applied[id]))
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
