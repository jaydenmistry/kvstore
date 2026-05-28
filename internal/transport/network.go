// Package transport carries Raft messages between nodes.
package transport

import (
	"math/rand/v2"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// Network is a simulated network for deterministic tests.
//
// Time is measured in ticks and only advances when the test calls Tick, so
// the same seed and the same sequence of calls always produce the same
// deliveries. The network can drop, delay, duplicate, and reorder messages,
// cut links to form partitions, and take nodes down.
//
// Network is not safe for concurrent use: a test drives it, and every node,
// from a single goroutine.
type Network struct {
	rng      *rand.Rand
	now      int64
	inflight []envelope

	dropRate float64
	dupRate  float64
	minDelay int
	maxDelay int

	cut  map[link]bool
	down map[uint64]bool
}

type link struct{ from, to uint64 }

type envelope struct {
	m  raft.Message
	at int64 // tick at which the message becomes deliverable
}

// NewNetwork returns a reliable network where every message arrives exactly
// one tick after it is sent.
func NewNetwork(seed uint64) *Network {
	return &Network{
		rng:      rand.New(rand.NewPCG(seed, 0x6e6574)),
		minDelay: 1,
		maxDelay: 1,
		cut:      make(map[link]bool),
		down:     make(map[uint64]bool),
	}
}

// Endpoint returns the Transport that node id uses to send.
func (n *Network) Endpoint(id uint64) raft.Transport {
	return endpoint{n: n, id: id}
}

type endpoint struct {
	n  *Network
	id uint64
}

func (e endpoint) Send(m raft.Message) {
	m.From = e.id
	e.n.send(m)
}

// SetDropRate sets the probability that each message is lost.
func (n *Network) SetDropRate(p float64) { n.dropRate = p }

// SetDuplicateRate sets the probability that a message is delivered twice.
// Each copy is delayed and possibly dropped independently.
func (n *Network) SetDuplicateRate(p float64) { n.dupRate = p }

// SetDelay makes each message take between min and max ticks, inclusive,
// chosen at random. Different delays reorder messages. min must be at least
// 1, so a reply is never delivered in the same tick as its request.
func (n *Network) SetDelay(min, max int) {
	if min < 1 || max < min {
		panic("transport: delay must satisfy 1 <= min <= max")
	}
	n.minDelay, n.maxDelay = min, max
}

// Partition splits nodes into groups that cannot reach each other,
// replacing any existing partition. Links within a group, and to nodes not
// listed, are unaffected. Messages already in flight across the new
// partition are lost.
func (n *Network) Partition(groups ...[]uint64) {
	n.Heal()
	for i, a := range groups {
		for j, b := range groups {
			if i == j {
				continue
			}
			for _, from := range a {
				for _, to := range b {
					n.cut[link{from, to}] = true
				}
			}
		}
	}
}

// Cut drops all messages from one node to another, in that direction only.
func (n *Network) Cut(from, to uint64) { n.cut[link{from, to}] = true }

// Heal removes every cut link.
func (n *Network) Heal() { clear(n.cut) }

// SetDown marks a node as crashed or restarted. A down node sends nothing,
// and messages arriving for it are lost.
func (n *Network) SetDown(id uint64, down bool) {
	if down {
		n.down[id] = true
	} else {
		delete(n.down, id)
	}
}

// InFlight returns the number of messages sent but not yet delivered or lost.
func (n *Network) InFlight() int { return len(n.inflight) }

func (n *Network) send(m raft.Message) {
	if n.down[m.From] {
		return
	}
	// Draw from rng the same way whatever the rates are, so changing a rate
	// does not shift every later random choice.
	copies := 1
	if n.rng.Float64() < n.dupRate {
		copies = 2
	}
	for range copies {
		lost := n.rng.Float64() < n.dropRate
		delay := n.minDelay + n.rng.IntN(n.maxDelay-n.minDelay+1)
		if !lost {
			n.inflight = append(n.inflight, envelope{m: m, at: n.now + int64(delay)})
		}
	}
}

// Tick advances time by one tick and passes every message now due to
// deliver, in random order. Partitions and down nodes are checked at
// delivery time. Messages sent from inside deliver are due on a later tick.
func (n *Network) Tick(deliver func(raft.Message)) {
	n.now++
	var due, later []envelope
	for _, e := range n.inflight {
		if e.at <= n.now {
			due = append(due, e)
		} else {
			later = append(later, e)
		}
	}
	n.inflight = later
	n.rng.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, e := range due {
		if n.down[e.m.To] || n.cut[link{e.m.From, e.m.To}] {
			continue
		}
		deliver(e.m)
	}
}
