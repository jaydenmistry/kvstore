package main

import (
	"slices"
	"testing"
	"time"
)

func TestParseCounts(t *testing.T) {
	got, err := parseCounts("1,8,32")
	if err != nil || !slices.Equal(got, []int{1, 8, 32}) {
		t.Fatalf("parseCounts(1,8,32) = %v, %v", got, err)
	}
	for _, s := range []string{"", "0", "-1", "1,,2", "x"} {
		if _, err := parseCounts(s); err == nil {
			t.Errorf("parseCounts(%q) accepted", s)
		}
	}
}

func TestLatencyPercentiles(t *testing.T) {
	var r result
	for i := 1; i <= 1000; i++ {
		r.latencies = append(r.latencies, time.Duration(i))
	}
	// Nearest rank: the p-th percentile is the smallest value with at
	// least p percent of the values at or below it.
	for p, want := range map[float64]time.Duration{50: 500, 90: 900, 99: 990, 99.9: 999, 100: 1000, 0.01: 1} {
		if got := r.latency(p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
	if got := (result{latencies: []time.Duration{7}}).latency(50); got != 7 {
		t.Errorf("p50 of one value = %v, want 7", got)
	}
	if got := (result{}).latency(99); got != 0 {
		t.Errorf("p99 of nothing = %v, want 0", got)
	}
}
