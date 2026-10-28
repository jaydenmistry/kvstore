// Package linearize checks whether a history of operations on a key-value
// store is linearizable (Herlihy and Wing): whether every operation can be
// given a single point in time, between its call and its return, at which
// it took effect, such that the results match a sequential store.
package linearize

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
)

// Kind is the type of an operation.
type Kind uint8

const (
	Get Kind = iota + 1
	Put
	Delete
)

func (k Kind) String() string {
	switch k {
	case Get:
		return "get"
	case Put:
		return "put"
	case Delete:
		return "delete"
	}
	return "unknown"
}

// Op is one logical client operation, from the moment the client first sent
// it to the moment it learned the outcome, across all its retries.
//
// Call and Return are points on one clock shared by every client, such as
// event numbers or monotonic nanoseconds. Only their order matters.
type Op struct {
	Client uint64
	Kind   Kind
	Key    string
	Value  string // Put: the value written; Get: the value read
	Found  bool   // Get, Delete: the result
	Call   int64
	Return int64 // or Pending
}

// Pending is the Return of an operation whose outcome was never learned. A
// pending write may have taken effect at any time after its call, or never,
// and its result (Found, for a delete) is ignored.
const Pending = math.MaxInt64

func (o Op) String() string {
	ret := "?"
	if o.Return != Pending {
		ret = fmt.Sprint(o.Return)
	}
	switch o.Kind {
	case Put:
		return fmt.Sprintf("[%d,%s] c%d put %s=%s", o.Call, ret, o.Client, o.Key, o.Value)
	case Get:
		if !o.Found {
			return fmt.Sprintf("[%d,%s] c%d get %s -> none", o.Call, ret, o.Client, o.Key)
		}
		return fmt.Sprintf("[%d,%s] c%d get %s -> %s", o.Call, ret, o.Client, o.Key, o.Value)
	default:
		return fmt.Sprintf("[%d,%s] c%d delete %s -> found=%v", o.Call, ret, o.Client, o.Key, o.Found)
	}
}

// register is the model of one key.
type register struct {
	value   string
	present bool
}

// apply returns the state after o, and whether o's recorded result is what
// the model produces from s.
func (s register) apply(o Op) (register, bool) {
	switch o.Kind {
	case Put:
		return register{o.Value, true}, true
	case Delete:
		return register{}, o.Return == Pending || o.Found == s.present
	default:
		return s, o.Found == s.present && (!o.Found || o.Value == s.value)
	}
}

// Check reports whether a history of operations on independent keys, each
// starting absent, is linearizable, and if not, the operations on the first
// key that is not. Linearizability is compositional, so each key is checked
// on its own.
func Check(history []Op) (ok bool, bad []Op) {
	byKey := map[string][]Op{}
	for _, o := range history {
		if o.Kind == Get && o.Return == Pending {
			continue // a read whose result is unknown constrains nothing
		}
		byKey[o.Key] = append(byKey[o.Key], o)
	}
	for _, k := range slices.Sorted(maps.Keys(byKey)) {
		if !linearizable(byKey[k]) {
			return false, byKey[k]
		}
	}
	return true, nil
}

// FirstViolation narrows down a history that is not linearizable. It
// returns the earliest time t such that the history as it stood at t is not
// linearizable: the operations called by t, with those that had not
// returned by t treated as pending. The operation that returned at t is the
// first whose result cannot be explained. found is false if the whole
// history is linearizable.
//
// This works because linearizability is prefix-closed: if the history at t
// has no valid order, neither does any later one, so t can be found by
// binary search.
func FirstViolation(history []Op) (t int64, found bool) {
	if ok, _ := Check(history); ok {
		return 0, false
	}
	var times []int64
	for _, o := range history {
		if o.Return != Pending {
			times = append(times, o.Return)
		}
	}
	slices.Sort(times)
	times = slices.Compact(times)
	// The whole history fails, and it equals the history at the last
	// return time plus pending operations, which constrain nothing more.
	i, _ := slices.BinarySearchFunc(times, true, func(t int64, _ bool) int {
		if ok, _ := Check(At(history, t)); ok {
			return -1
		}
		return 1
	})
	return times[i], true
}

// At returns history as it stood at time t: the operations called by t,
// with those that had not returned by t marked pending.
func At(history []Op, t int64) []Op {
	var out []Op
	for _, o := range history {
		if o.Call > t {
			continue
		}
		if o.Return > t {
			o.Return = Pending
		}
		out = append(out, o)
	}
	return out
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
func linearizable(ops []Op) bool {
	ops = slices.Clone(ops)
	slices.SortFunc(ops, func(a, b Op) int { return cmp.Compare(a.Call, b.Call) })
	placed := make([]uint64, (len(ops)+63)/64)
	remaining := 0 // completed ops not yet placed
	for _, o := range ops {
		if o.Return != Pending {
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

		var minRet int64 = Pending
		for i, o := range ops {
			if placed[i/64]&(1<<(i%64)) == 0 {
				minRet = min(minRet, o.Return)
			}
		}
		for i, o := range ops {
			if o.Call > minRet {
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
			if o.Return != Pending {
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
