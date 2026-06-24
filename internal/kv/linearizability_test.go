package kv

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"testing"
)

// histOp is one logical client operation, from the moment the client first
// sent it to the moment it learned the outcome, across all its retries.
type histOp struct {
	client uint64
	op     Op
	key    string
	value  string // OpPut: the value written; OpGet: the value read
	found  bool   // OpGet, OpDelete: the result
	call   int    // event number of the invocation
	ret    int    // event number of the response, or pending
}

// pending is the return time of an operation whose outcome was never
// learned. A pending write may have taken effect at any time after its
// call, or never.
const pending = math.MaxInt

func (o histOp) String() string {
	ret := "?"
	if o.ret != pending {
		ret = fmt.Sprint(o.ret)
	}
	switch o.op {
	case OpPut:
		return fmt.Sprintf("[%d,%s] c%d put %s=%s", o.call, ret, o.client, o.key, o.value)
	case OpGet:
		if !o.found {
			return fmt.Sprintf("[%d,%s] c%d get %s -> none", o.call, ret, o.client, o.key)
		}
		return fmt.Sprintf("[%d,%s] c%d get %s -> %s", o.call, ret, o.client, o.key, o.value)
	default:
		return fmt.Sprintf("[%d,%s] c%d delete %s -> found=%v", o.call, ret, o.client, o.key, o.found)
	}
}

// register is the model of one key.
type register struct {
	value   string
	present bool
}

// apply returns the state after o, and whether o's recorded result is what
// the model produces from s.
func (s register) apply(o histOp) (register, bool) {
	switch o.op {
	case OpPut:
		return register{o.value, true}, true
	case OpDelete:
		return register{}, o.found == s.present
	default:
		return s, o.found == s.present && (!o.found || o.value == s.value)
	}
}

// checkLinearizable reports whether a history of operations on independent
// keys is linearizable, and if not, the operations on the first key that is
// not. Linearizability is compositional (Herlihy and Wing), so each key is
// checked on its own.
func checkLinearizable(history []histOp) (ok bool, bad []histOp) {
	byKey := map[string][]histOp{}
	for _, o := range history {
		if o.op == OpGet && o.ret == pending {
			continue // a read whose result is unknown constrains nothing
		}
		byKey[o.key] = append(byKey[o.key], o)
	}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		if !linearizable(byKey[k]) {
			return false, byKey[k]
		}
	}
	return true, nil
}

// linearizable searches for an order of ops, all on one key, that is
// consistent with the register model and with real time: if a returned
// before b was called, a comes first. Pending operations may be placed
// anywhere after their call, or left out.
//
// This is the Wing and Gong search with Lowe's memoization: the next
// operation in the order can be any not-yet-placed one that was called
// before the earliest return among the rest, and a (placed set, state) pair
// already explored is never explored again.
func linearizable(ops []histOp) bool {
	ops = slices.Clone(ops)
	slices.SortFunc(ops, func(a, b histOp) int { return cmp.Compare(a.call, b.call) })
	placed := make([]uint64, (len(ops)+63)/64)
	remaining := 0 // completed ops not yet placed
	for _, o := range ops {
		if o.ret != pending {
			remaining++
		}
	}
	seen := map[string]bool{}
	var search func(s register, remaining int) bool
	search = func(s register, remaining int) bool {
		if remaining == 0 {
			return true
		}
		var key strings.Builder
		for _, w := range placed {
			fmt.Fprintf(&key, "%x.", w)
		}
		fmt.Fprintf(&key, "%v.%s", s.present, s.value)
		if seen[key.String()] {
			return false
		}
		seen[key.String()] = true

		minRet := pending
		for i, o := range ops {
			if placed[i/64]&(1<<(i%64)) == 0 {
				minRet = min(minRet, o.ret)
			}
		}
		for i, o := range ops {
			if o.call > minRet {
				break // ops is sorted by call
			}
			if placed[i/64]&(1<<(i%64)) != 0 {
				continue
			}
			next, ok := s.apply(o)
			if !ok {
				continue
			}
			placed[i/64] |= 1 << (i % 64)
			r := remaining
			if o.ret != pending {
				r--
			}
			if search(next, r) {
				return true
			}
			placed[i/64] &^= 1 << (i % 64)
		}
		return false
	}
	return search(register{}, remaining)
}

func TestCheckerAcceptsAndRejects(t *testing.T) {
	put := func(call, ret int, v string) histOp {
		return histOp{op: OpPut, key: "k", value: v, call: call, ret: ret}
	}
	get := func(call, ret int, v string) histOp {
		return histOp{op: OpGet, key: "k", value: v, found: v != "", call: call, ret: ret}
	}
	del := func(call, ret int, found bool) histOp {
		return histOp{op: OpDelete, key: "k", found: found, call: call, ret: ret}
	}
	cases := []struct {
		name string
		ops  []histOp
		want bool
	}{
		{"empty", nil, true},
		{"read of missing key", []histOp{get(1, 2, "")}, true},
		{"sequential", []histOp{put(1, 2, "a"), get(3, 4, "a"), put(5, 6, "b"), get(7, 8, "b")}, true},
		{"stale read", []histOp{put(1, 2, "a"), put(3, 4, "b"), get(5, 6, "a")}, false},
		{"read of value never written", []histOp{put(1, 2, "a"), get(3, 4, "z")}, false},
		{"read overlapping write sees either", []histOp{put(1, 2, "a"), put(3, 6, "b"), get(4, 5, "a")}, true},
		{"read overlapping write sees either", []histOp{put(1, 2, "a"), put(3, 6, "b"), get(4, 5, "b")}, true},
		{"reads cannot go back", []histOp{put(1, 2, "a"), put(3, 10, "b"), get(4, 5, "b"), get(6, 7, "a")}, false},
		{"pending write may apply", []histOp{put(1, 2, "a"), put(3, pending, "b"), get(5, 6, "b")}, true},
		{"pending write may not apply", []histOp{put(1, 2, "a"), put(3, pending, "b"), get(5, 6, "a")}, true},
		{"pending write applies at most once", []histOp{put(1, 2, "a"), put(3, pending, "b"), get(5, 6, "b"), get(7, 8, "a")}, false},
		{"pending write not before its call", []histOp{put(1, 2, "a"), get(3, 4, "b"), put(5, pending, "b")}, false},
		{"delete reports presence", []histOp{put(1, 2, "a"), del(3, 4, true), del(5, 6, false), get(7, 8, "")}, true},
		{"delete twice reports found twice", []histOp{put(1, 2, "a"), del(3, 4, true), del(5, 6, true)}, false},
		{"lost delete result", []histOp{put(1, 2, "a"), del(3, 4, false)}, false},
	}
	for _, tc := range cases {
		if got := linearizable(tc.ops); got != tc.want {
			t.Errorf("%s: linearizable = %v, want %v\n%v", tc.name, got, tc.want, tc.ops)
		}
	}
	// Keys are independent: a stale read on one key is found among others.
	h := []histOp{
		{op: OpPut, key: "x", value: "1", call: 1, ret: 2},
		{op: OpPut, key: "y", value: "1", call: 3, ret: 4},
		{op: OpPut, key: "y", value: "2", call: 5, ret: 6},
		{op: OpGet, key: "x", value: "1", found: true, call: 7, ret: 8},
		{op: OpGet, key: "y", value: "1", found: true, call: 9, ret: 10},
	}
	if ok, bad := checkLinearizable(h); ok || bad[0].key != "y" {
		t.Fatalf("checkLinearizable = %v, %v; want key y reported", ok, bad)
	}
}
