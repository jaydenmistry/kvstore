package kv

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
	"github.com/jaydenmistry/kvstore/internal/transport"
	"github.com/jaydenmistry/kvstore/internal/vfs"
)

const (
	electionTick   = 10
	heartbeatTick  = 1
	requestTimeout = 3 * electionTick
)

// cluster runs a Replica per node on a simulated network, all from one
// goroutine, so a run is reproducible from its seed.
type cluster struct {
	t     *testing.T
	seed  uint64
	rng   *rand.Rand
	net   *transport.Network
	ids   []uint64
	nodes map[uint64]*Replica // nil while crashed
	ticks int

	storage map[uint64]raft.Storage
	// disks is set for disk-backed clusters, where a crash is a power loss
	// (see the raft package's harness).
	disks map[uint64]*vfs.Mem
	// snapshotEvery is passed to every Replica; see setSnapshotEvery.
	snapshotEvery uint64
	// checkQuorum and preVote are passed to every node's raft.Config.
	checkQuorum bool
	preVote     bool
}

// clusterOption configures a cluster before its nodes start.
type clusterOption func(*cluster)

func withCheckQuorum(on bool) clusterOption {
	return func(c *cluster) { c.checkQuorum = on }
}

func withPreVote(on bool) clusterOption {
	return func(c *cluster) { c.preVote = on }
}

// setSnapshotEvery sets how often every node, running or restarted later,
// compacts its log.
func (c *cluster) setSnapshotEvery(n uint64) {
	c.snapshotEvery = n
	for _, p := range c.nodes {
		if p != nil {
			p.snapshotEvery = n
		}
	}
}

func newCluster(t *testing.T, n int, seed uint64, disk bool, opts ...clusterOption) *cluster {
	t.Helper()
	c := &cluster{
		t:       t,
		seed:    seed,
		rng:     rand.New(rand.NewPCG(seed, 1)),
		net:     transport.NewNetwork(seed),
		nodes:   map[uint64]*Replica{},
		storage: map[uint64]raft.Storage{},
	}
	if disk {
		c.disks = map[uint64]*vfs.Mem{}
	}
	for _, o := range opts {
		o(c)
	}
	for i := range n {
		id := uint64(i + 1)
		c.ids = append(c.ids, id)
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

// start creates a node from its storage. Its state machine starts empty and
// is rebuilt as the node relearns which entries are committed.
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
		PreVote:       c.preVote,
		Storage:       c.storage[id],
		Transport:     c.net.Endpoint(id),
		Rand:          rand.New(rand.NewPCG(c.rng.Uint64(), id)),
	})
	if err != nil {
		c.fatalf("starting node %d: %v", id, err)
	}
	c.nodes[id] = NewReplica(r, Config{RequestTimeout: requestTimeout, SnapshotEvery: c.snapshotEvery})
	c.net.SetDown(id, false)
}

func (c *cluster) crash(id uint64) {
	c.nodes[id] = nil
	c.net.SetDown(id, true)
	if c.disks != nil {
		c.disks[id].Crash(c.rng)
	}
}

// tick delivers due messages, then ticks every live node once.
func (c *cluster) tick() {
	c.t.Helper()
	c.ticks++
	c.net.Tick(func(m raft.Message) {
		if p := c.nodes[m.To]; p != nil {
			if err := p.Step(m); err != nil {
				c.fatalf("node %d Step: %v", m.To, err)
			}
		}
	})
	for _, id := range c.ids {
		if p := c.nodes[id]; p != nil {
			if err := p.Tick(); err != nil {
				c.fatalf("node %d Tick: %v", id, err)
			}
		}
	}
}

func (c *cluster) run(ticks int) {
	c.t.Helper()
	for range ticks {
		c.tick()
	}
}

// leaderAmong ticks until one of ids is leader and every live node in ids
// agrees on it, and returns it.
func (c *cluster) leaderAmong(ids []uint64, maxTicks int) uint64 {
	c.t.Helper()
	for range maxTicks {
		var lead uint64
		agreed := true
		for _, id := range ids {
			if p := c.nodes[id]; p != nil {
				l := p.raft.Status().Lead
				if l == raft.None || (lead != raft.None && l != lead) {
					agreed = false
				}
				lead = l
			}
		}
		if agreed && lead != raft.None && slices.Contains(ids, lead) && c.nodes[lead] != nil && c.nodes[lead].term != 0 {
			return lead
		}
		c.tick()
	}
	c.fatalf("no leader among %v within %d ticks", ids, maxTicks)
	return 0
}

func (c *cluster) leader() uint64 {
	c.t.Helper()
	return c.leaderAmong(c.ids, 30*electionTick)
}

// call is one request in flight.
type call struct {
	done bool
	res  Result
}

// submit sends req to node id and returns a handle that is filled in when
// the request completes.
func (c *cluster) submit(id uint64, req Request) *call {
	c.t.Helper()
	cl := &call{}
	err := c.nodes[id].Submit(req, func(res Result) {
		if cl.done {
			c.fatalf("request %+v completed twice", req)
		}
		cl.done, cl.res = true, res
	})
	if err != nil {
		c.fatalf("node %d Submit: %v", id, err)
	}
	return cl
}

// wait ticks until cl completes and returns its result.
func (c *cluster) wait(cl *call, maxTicks int) Result {
	c.t.Helper()
	for range maxTicks {
		if cl.done {
			return cl.res
		}
		c.tick()
	}
	if !cl.done {
		c.fatalf("request did not complete within %d ticks", maxTicks)
	}
	return cl.res
}

// do submits req to node id, waits for it, and fails the test if it fails.
func (c *cluster) do(id uint64, req Request) Result {
	c.t.Helper()
	res := c.wait(c.submit(id, req), 2*requestTimeout)
	if res.Err != nil {
		c.fatalf("%v %q on node %d: %v", req.Op, req.Key, id, res.Err)
	}
	return res
}

func getReq(key string) Request { return Request{Op: OpGet, Key: key} }

func putReq(client, seq uint64, key, value string) Request {
	return Request{Op: OpPut, ClientID: client, Seq: seq, Key: key, Value: []byte(value)}
}

func deleteReq(client, seq uint64, key string) Request {
	return Request{Op: OpDelete, ClientID: client, Seq: seq, Key: key}
}

// waitApplied ticks until every live node has applied index.
func (c *cluster) waitApplied(index uint64, maxTicks int) {
	c.t.Helper()
	for range maxTicks {
		all := true
		for _, id := range c.ids {
			if p := c.nodes[id]; p != nil && p.sm.Applied() < index {
				all = false
			}
		}
		if all {
			return
		}
		c.tick()
	}
	c.fatalf("not every node applied index %d within %d ticks", index, maxTicks)
}

// checkReplicasAgree verifies that every live node that has applied the
// same number of entries holds the same data and client sessions.
func (c *cluster) checkReplicasAgree() {
	c.t.Helper()
	var ref *Replica
	var refID uint64
	for _, id := range c.ids {
		p := c.nodes[id]
		if p == nil {
			continue
		}
		if ref == nil {
			ref, refID = p, id
			continue
		}
		if p.sm.Applied() != ref.sm.Applied() {
			c.fatalf("nodes %d and %d applied %d and %d entries", refID, id, ref.sm.Applied(), p.sm.Applied())
		}
		if !maps.EqualFunc(p.sm.data, ref.sm.data, func(a, b []byte) bool { return string(a) == string(b) }) ||
			!maps.EqualFunc(p.sm.sessions, ref.sm.sessions, func(a, b session) bool { return fmt.Sprint(a) == fmt.Sprint(b) }) {
			c.fatalf("nodes %d and %d diverged at applied index %d:\n%s\n%s",
				refID, id, p.sm.Applied(), dump(ref.sm), dump(p.sm))
		}
	}
}

func dump(sm *StateMachine) string {
	var b strings.Builder
	for _, k := range slices.Sorted(maps.Keys(sm.data)) {
		fmt.Fprintf(&b, "%s=%s ", k, sm.data[k])
	}
	return b.String()
}
