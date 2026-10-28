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
		{"pending delete may report anything", []Op{put(1, 2, "a"), del(3, Pending, false), get(5, 6, "")}, true},
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

func TestFirstViolation(t *testing.T) {
	put := func(call, ret int64, v string) Op {
		return Op{Kind: Put, Key: "k", Value: v, Call: call, Return: ret}
	}
	get := func(call, ret int64, v string) Op {
		return Op{Kind: Get, Key: "k", Value: v, Found: v != "", Call: call, Return: ret}
	}
	ok := []Op{put(1, 2, "a"), get(3, 4, "a"), put(5, 6, "b")}
	if _, found := FirstViolation(ok); found {
		t.Fatal("found a violation in a linearizable history")
	}
	// The stale read returning at 12 is the first violation. The one at 20
	// is another, and the read overlapping the write at 7-9 is fine.
	h := []Op{
		put(1, 2, "a"),
		put(3, 9, "b"),
		get(7, 8, "a"),
		get(10, 12, "a"),
		get(11, 13, "b"),
		get(19, 20, "a"),
	}
	if got, found := FirstViolation(h); !found || got != 12 {
		t.Fatalf("FirstViolation = %d, %v; want 12", got, found)
	}
	// The history at 12 does fail, and the one just before it does not.
	if ok, _ := Check(At(h, 12)); ok {
		t.Fatal("history at 12 is linearizable")
	}
	if ok, _ := Check(At(h, 11)); !ok {
		t.Fatal("history at 11 is not linearizable")
	}
	// Operations still running at t are pending: the write called at 3 and
	// returning at 9 may or may not have happened by 8.
	if got := At(h, 8); len(got) != 3 || got[1].Return != Pending || got[2].Return != 8 {
		t.Fatalf("At(8) = %v", got)
	}
}
