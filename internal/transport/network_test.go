package transport

import (
	"reflect"
	"slices"
	"testing"

	"github.com/jaydenmistry/kvstore/internal/raft"
)

// record runs ticks and returns, per tick, the terms of delivered messages.
// Tests use Term as a message label.
func record(n *Network, ticks int) [][]uint64 {
	out := make([][]uint64, ticks)
	for i := range out {
		n.Tick(func(m raft.Message) { out[i] = append(out[i], m.Term) })
	}
	return out
}

func send(n *Network, from, to uint64, labels ...uint64) {
	for _, l := range labels {
		n.Endpoint(from).Send(raft.Message{To: to, Term: l})
	}
}

func TestDeliversAfterOneTick(t *testing.T) {
	n := NewNetwork(1)
	var got []raft.Message
	n.Endpoint(1).Send(raft.Message{Type: raft.MsgVote, To: 2, Term: 7})
	n.Tick(func(m raft.Message) { got = append(got, m) })
	want := raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 7}
	if len(got) != 1 || !reflect.DeepEqual(got[0], want) {
		t.Fatalf("delivered %+v, want [%+v]", got, want)
	}
	if n.InFlight() != 0 {
		t.Fatalf("%d messages still in flight", n.InFlight())
	}
}

func TestReplyIsNotDeliveredInSameTick(t *testing.T) {
	n := NewNetwork(1)
	send(n, 1, 2, 1)
	var delivered int
	n.Tick(func(m raft.Message) {
		delivered++
		n.Endpoint(m.To).Send(raft.Message{To: m.From, Term: 2})
	})
	if delivered != 1 || n.InFlight() != 1 {
		t.Fatalf("delivered %d, in flight %d; want 1, 1", delivered, n.InFlight())
	}
}

func TestDelayRange(t *testing.T) {
	n := NewNetwork(1)
	n.SetDelay(3, 5)
	send(n, 1, 2, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	ticks := record(n, 6)
	for i := 0; i < 2; i++ {
		if len(ticks[i]) != 0 {
			t.Fatalf("tick %d delivered %v before the minimum delay", i+1, ticks[i])
		}
	}
	if total := len(ticks[2]) + len(ticks[3]) + len(ticks[4]); total != 10 {
		t.Fatalf("delivered %d messages in ticks 3-5, want 10", total)
	}
}

func TestReorders(t *testing.T) {
	n := NewNetwork(1)
	n.SetDelay(1, 5)
	labels := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	send(n, 1, 2, labels...)
	got := slices.Concat(record(n, 5)...)
	if len(got) != len(labels) {
		t.Fatalf("delivered %d, want %d", len(got), len(labels))
	}
	if slices.Equal(got, labels) {
		t.Fatal("messages arrived in send order; expected reordering")
	}
}

func TestDropAll(t *testing.T) {
	n := NewNetwork(1)
	n.SetDropRate(1)
	send(n, 1, 2, 1, 2, 3)
	if got := slices.Concat(record(n, 3)...); len(got) != 0 {
		t.Fatalf("delivered %v with drop rate 1", got)
	}
}

func TestDropSome(t *testing.T) {
	n := NewNetwork(1)
	n.SetDropRate(0.5)
	for i := range uint64(1000) {
		send(n, 1, 2, i)
	}
	got := len(slices.Concat(record(n, 1)...))
	if got < 400 || got > 600 {
		t.Fatalf("delivered %d of 1000 with drop rate 0.5", got)
	}
}

func TestDuplicates(t *testing.T) {
	n := NewNetwork(1)
	n.SetDuplicateRate(1)
	send(n, 1, 2, 42)
	if got := slices.Concat(record(n, 1)...); !slices.Equal(got, []uint64{42, 42}) {
		t.Fatalf("delivered %v, want [42 42]", got)
	}
}

func TestPartition(t *testing.T) {
	n := NewNetwork(1)
	n.Partition([]uint64{1}, []uint64{2, 3})
	send(n, 1, 2, 1)
	send(n, 2, 1, 2)
	send(n, 2, 3, 3)
	send(n, 4, 1, 4) // node 4 is not in any group
	if got := slices.Concat(record(n, 1)...); !slices.Equal(sorted(got), []uint64{3, 4}) {
		t.Fatalf("delivered %v, want [3 4]", got)
	}
	n.Heal()
	send(n, 1, 2, 5)
	if got := slices.Concat(record(n, 1)...); !slices.Equal(got, []uint64{5}) {
		t.Fatalf("after heal delivered %v, want [5]", got)
	}
}

func TestPartitionDropsMessagesInFlight(t *testing.T) {
	n := NewNetwork(1)
	n.SetDelay(2, 2)
	send(n, 1, 2, 1)
	n.Partition([]uint64{1}, []uint64{2})
	if got := slices.Concat(record(n, 2)...); len(got) != 0 {
		t.Fatalf("message crossed a partition created while it was in flight: %v", got)
	}
}

func TestCutIsOneWay(t *testing.T) {
	n := NewNetwork(1)
	n.Cut(1, 2)
	send(n, 1, 2, 1)
	send(n, 2, 1, 2)
	if got := slices.Concat(record(n, 1)...); !slices.Equal(got, []uint64{2}) {
		t.Fatalf("delivered %v, want [2]", got)
	}
}

func TestDownNode(t *testing.T) {
	n := NewNetwork(1)
	send(n, 1, 2, 1) // in flight when 2 goes down
	n.SetDown(2, true)
	send(n, 2, 1, 2) // a down node sends nothing
	if got := slices.Concat(record(n, 1)...); len(got) != 0 {
		t.Fatalf("delivered %v involving a down node", got)
	}
	n.SetDown(2, false)
	send(n, 1, 2, 3)
	if got := slices.Concat(record(n, 1)...); !slices.Equal(got, []uint64{3}) {
		t.Fatalf("after restart delivered %v, want [3]", got)
	}
}

func TestDeterministic(t *testing.T) {
	run := func() [][]uint64 {
		n := NewNetwork(42)
		n.SetDelay(1, 4)
		n.SetDropRate(0.2)
		n.SetDuplicateRate(0.2)
		for i := range uint64(50) {
			send(n, 1, 2, i)
		}
		return record(n, 5)
	}
	a, b := run(), run()
	if !slices.EqualFunc(a, b, slices.Equal) {
		t.Fatalf("same seed produced different deliveries:\n%v\n%v", a, b)
	}
}

func sorted(s []uint64) []uint64 {
	s = slices.Clone(s)
	slices.Sort(s)
	return s
}
