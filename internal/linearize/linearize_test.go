package linearize

import "testing"

func TestCheckerAcceptsAndRejects(t *testing.T) {
	put := func(call, ret int64, v string) Op {
		return Op{Kind: Put, Key: "k", Value: v, Call: call, Return: ret}
	}
	get := func(call, ret int64, v string) Op {
		return Op{Kind: Get, Key: "k", Value: v, Found: v != "", Call: call, Return: ret}
	}
	del := func(call, ret int64, found bool) Op {
		return Op{Kind: Delete, Key: "k", Found: found, Call: call, Return: ret}
	}
	cases := []struct {
		name string
		ops  []Op
		want bool
	}{
		{"empty", nil, true},
		{"read of missing key", []Op{get(1, 2, "")}, true},
		{"sequential", []Op{put(1, 2, "a"), get(3, 4, "a"), put(5, 6, "b"), get(7, 8, "b")}, true},
		{"stale read", []Op{put(1, 2, "a"), put(3, 4, "b"), get(5, 6, "a")}, false},
		{"read of value never written", []Op{put(1, 2, "a"), get(3, 4, "z")}, false},
		{"read overlapping write sees either", []Op{put(1, 2, "a"), put(3, 6, "b"), get(4, 5, "a")}, true},
		{"read overlapping write sees either", []Op{put(1, 2, "a"), put(3, 6, "b"), get(4, 5, "b")}, true},
		{"reads cannot go back", []Op{put(1, 2, "a"), put(3, 10, "b"), get(4, 5, "b"), get(6, 7, "a")}, false},
		{"Pending write may apply", []Op{put(1, 2, "a"), put(3, Pending, "b"), get(5, 6, "b")}, true},
		{"Pending write may not apply", []Op{put(1, 2, "a"), put(3, Pending, "b"), get(5, 6, "a")}, true},
		{"Pending write applies at most once", []Op{put(1, 2, "a"), put(3, Pending, "b"), get(5, 6, "b"), get(7, 8, "a")}, false},
		{"Pending write not before its call", []Op{put(1, 2, "a"), get(3, 4, "b"), put(5, Pending, "b")}, false},
		{"delete reports presence", []Op{put(1, 2, "a"), del(3, 4, true), del(5, 6, false), get(7, 8, "")}, true},
		{"delete twice reports found twice", []Op{put(1, 2, "a"), del(3, 4, true), del(5, 6, true)}, false},
		{"lost delete result", []Op{put(1, 2, "a"), del(3, 4, false)}, false},
	}
	for _, tc := range cases {
		if got := linearizable(tc.ops); got != tc.want {
			t.Errorf("%s: linearizable = %v, want %v\n%v", tc.name, got, tc.want, tc.ops)
		}
	}
	// Keys are independent: a stale read on one key is found among others.
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: 1, Return: 2},
		{Kind: Put, Key: "y", Value: "1", Call: 3, Return: 4},
		{Kind: Put, Key: "y", Value: "2", Call: 5, Return: 6},
		{Kind: Get, Key: "x", Value: "1", Found: true, Call: 7, Return: 8},
		{Kind: Get, Key: "y", Value: "1", Found: true, Call: 9, Return: 10},
	}
	if ok, bad := Check(h); ok || bad[0].Key != "y" {
		t.Fatalf("Check = %v, %v; want key y reported", ok, bad)
	}
}
