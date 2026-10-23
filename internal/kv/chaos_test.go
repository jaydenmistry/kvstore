package kv

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/linearize"
)

// simClient is a client that behaves as a real one should: one request at a
// time, retrying until it learns the outcome. It follows leader hints, and
// after a timeout or lost leadership it tries another node. A retried write
// keeps its sequence number, so the cluster can detect the duplicate.
type simClient struct {
	c   *cluster
	h   *history
	rng *rand.Rand
	id  uint64
	seq uint64

	cur      *linearize.Op // the operation in progress, nil when idle
	req      Request
	target   uint64 // node to send the next attempt to; 0 for any
	attempt  int    // incremented per attempt; older callbacks are ignored
	waiting  bool
	deadline int // tick at which to give up on the current attempt
}

// history numbers events so operations can be ordered in real time.
type history struct {
	events int64
	ops    []linearize.Op
}

func (h *history) event() int64 {
	h.events++
	return h.events
}

// clientTimeout is longer than requestTimeout, so a node normally answers
// first; the client's own timeout covers nodes that crashed.
const clientTimeout = requestTimeout + electionTick

var keys = []string{"a", "b", "c"}

// step is called once per tick. If idle and start is true, the client may
// begin a new operation.
func (cl *simClient) step(start bool) {
	c := cl.c
	if cl.cur == nil {
		if !start || cl.rng.IntN(3) != 0 {
			return
		}
		key := keys[cl.rng.IntN(len(keys))]
		cl.cur = &linearize.Op{Client: cl.id, Key: key, Call: cl.h.event()}
		switch p := cl.rng.IntN(100); {
		case p < 50:
			cl.cur.Kind = linearize.Get
			cl.req = getReq(key)
		case p < 85:
			cl.seq++
			v := fmt.Sprintf("c%d-%d", cl.id, cl.seq)
			cl.cur.Kind, cl.cur.Value = linearize.Put, v
			cl.req = putReq(cl.id, cl.seq, key, v)
		default:
			cl.seq++
			cl.cur.Kind = linearize.Delete
			cl.req = deleteReq(cl.id, cl.seq, key)
		}
		cl.waiting = false
	}
	if cl.waiting {
		if c.ticks < cl.deadline {
			return
		}
		cl.waiting, cl.target = false, 0 // the node probably crashed
	}
	node := cl.target
	if node == 0 || c.nodes[node] == nil {
		var live []uint64
		for _, id := range c.ids {
			if c.nodes[id] != nil {
				live = append(live, id)
			}
		}
		if len(live) == 0 {
			return
		}
		node = live[cl.rng.IntN(len(live))]
	}
	cl.attempt++
	attempt := cl.attempt
	cl.waiting, cl.deadline = true, c.ticks+clientTimeout
	if err := c.nodes[node].Submit(cl.req, func(res Result) {
		if attempt == cl.attempt && cl.waiting {
			cl.handle(res)
		}
	}); err != nil {
		c.fatalf("node %d Submit: %v", node, err)
	}
}

func (cl *simClient) handle(res Result) {
	switch {
	case res.Err == nil:
		o := cl.cur
		o.Return = cl.h.event()
		o.Found = res.Found
		if o.Kind == linearize.Get {
			o.Value = string(res.Value)
		}
		cl.h.ops = append(cl.h.ops, *o)
		cl.cur, cl.waiting = nil, false
	case errors.Is(res.Err, ErrNotLeader):
		cl.waiting, cl.target = false, res.Leader
	case errors.Is(res.Err, ErrLeadershipLost), errors.Is(res.Err, ErrTimeout):
		cl.waiting, cl.target = false, 0
	default:
		// ErrStaleSequence cannot reach the current attempt: a client with
		// one request at a time never has a newer write applied before it.
		cl.c.fatalf("client %d: %v %q failed: %v", cl.id, cl.req.Op, cl.req.Key, res.Err)
	}
}

func TestLinearizableUnderChaos(t *testing.T) {
	// Clients read and write a few keys while the network drops, delays,
	// duplicates, and reorders messages, partitions come and go, and nodes
	// crash and restart (half the runs losing power with the WAL on a
	// crash-simulating filesystem). Afterwards, everything is repaired and
	// every operation completes. The full history, with real-time order,
	// must be linearizable, and the replicas' state machines identical.
	const seeds = 100
	var completed, writes, reads int
	for seed := range uint64(seeds) {
		c := newCluster(t, 5, seed, seed%2 == 1, withCheckQuorum(seed/4%2 == 1))
		// A third of the runs never compact, a third compact often.
		c.setSnapshotEvery([]uint64{0, 5, 50}[seed%3])
		rng := rand.New(rand.NewPCG(seed, 4))
		c.net.SetDropRate(0.1)
		c.net.SetDuplicateRate(0.05)
		c.net.SetDelay(1, []int{3, 2 * electionTick}[seed/2%2])
		h := &history{}
		var clients []*simClient
		for i := range 5 {
			clients = append(clients, &simClient{c: c, h: h, rng: rand.New(rand.NewPCG(seed, uint64(10+i))), id: uint64(i + 1)})
		}

		for range 1500 {
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
					c.start(id)
				}
			}
			for _, cl := range clients {
				cl.step(true)
			}
			c.tick()
		}

		// Repair everything and let every operation finish.
		c.net.Heal()
		c.net.SetDropRate(0)
		c.net.SetDuplicateRate(0)
		c.net.SetDelay(1, 1)
		for _, id := range c.ids {
			if c.nodes[id] == nil {
				c.start(id)
			}
		}
		for i := 0; ; i++ {
			idle := true
			for _, cl := range clients {
				cl.step(false)
				idle = idle && cl.cur == nil
			}
			if idle {
				break
			}
			if i == 100*electionTick {
				c.fatalf("operations still unfinished after repair")
			}
			c.tick()
		}

		if ok, bad := linearize.Check(h.ops); !ok {
			var b strings.Builder
			for _, o := range bad {
				fmt.Fprintln(&b, o)
			}
			c.fatalf("history for key %q is not linearizable:\n%s", bad[0].Key, b.String())
		}
		lead := c.leader()
		c.waitApplied(c.nodes[lead].sm.Applied(), 20*electionTick)
		c.checkReplicasAgree()

		completed += len(h.ops)
		for _, o := range h.ops {
			if o.Kind == linearize.Get {
				reads++
			} else {
				writes++
			}
		}
	}
	t.Logf("%d operations (%d reads, %d writes) across %d runs", completed, reads, writes, seeds)
	if completed < seeds*100 {
		t.Fatalf("only %d operations completed across %d runs; chaos too severe to test much", completed, seeds)
	}
}
