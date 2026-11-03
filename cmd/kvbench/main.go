// Command kvbench measures a running kvstore cluster's throughput and
// latency. For each operation and each number of concurrent clients, it
// runs clients that send requests back to back, one at a time each, and
// reports operations per second and latency percentiles.
//
// Usage:
//
//	kvbench [-config FILE | -servers URLS] [-ops put,get] [-clients 1,8,32] [-duration 10s]
//
// scripts/bench.sh starts a fresh local cluster and runs kvbench against
// it. Each client is a client.Client, which talks to the leader, so the
// numbers include HTTP and, for writes, a fsync on the leader and on a
// majority before the reply.
//
// Exit status is 0 on success, 1 if a request failed, and 2 for bad usage.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jaydenmistry/kvstore/internal/client"
	"github.com/jaydenmistry/kvstore/internal/config"
)

const defaultServers = "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003"

func main() { os.Exit(run()) }

type options struct {
	servers   []string
	ops       []string
	clients   []int
	duration  time.Duration
	warmup    time.Duration
	keys      int
	valueSize int
}

func run() int {
	var o options
	servers := flag.String("servers", defaultServers, "comma-separated server `URLs`")
	configPath := flag.String("config", "", "cluster configuration `file` to take the servers from")
	ops := flag.String("ops", "put,get", "comma-separated `operations` to measure: put, get")
	clients := flag.String("clients", "1,8,32", "comma-separated `counts` of concurrent clients")
	flag.DurationVar(&o.duration, "duration", 10*time.Second, "how `long` to measure each case")
	flag.DurationVar(&o.warmup, "warmup", 2*time.Second, "how `long` to run each case before measuring")
	flag.IntVar(&o.keys, "keys", 1000, "number of distinct `keys`")
	flag.IntVar(&o.valueSize, "value-size", 100, "size of each value in `bytes`")
	flag.Parse()

	var err error
	o.clients, err = parseCounts(*clients)
	o.ops = strings.Split(*ops, ",")
	validOps := !slices.ContainsFunc(o.ops, func(op string) bool { return op != "put" && op != "get" })
	if err != nil || !validOps || flag.NArg() > 0 || o.keys < 1 || o.valueSize < 0 || o.duration <= 0 || o.warmup < 0 {
		flag.Usage()
		return 2
	}
	o.servers = strings.Split(*servers, ",")
	if *configPath != "" {
		c, err := config.Load(*configPath)
		if err != nil {
			log.Print(err)
			return 2
		}
		o.servers = c.APIURLs()
	}

	if err := bench(o); err != nil {
		log.Print(err)
		return 1
	}
	return 0
}

// parseCounts parses a comma-separated list of positive integers.
func parseCounts(s string) ([]int, error) {
	var counts []int
	for f := range strings.SplitSeq(s, ",") {
		n, err := strconv.Atoi(f)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("bad client count %q", f)
		}
		counts = append(counts, n)
	}
	return counts, nil
}

func bench(o options) error {
	// Every key holds a value before reads are measured, so a read returns
	// a value rather than taking the shorter not-found path.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	c, err := client.New(o.servers)
	if err != nil {
		return err
	}
	value := make([]byte, o.valueSize)
	for i := range o.keys {
		if err := c.Put(ctx, key(i), value); err != nil {
			return fmt.Errorf("loading keys: %w", err)
		}
	}

	// Rows are printed as each case finishes, so the columns have fixed
	// widths.
	const row = "%-4s %7s %8s %9s %9s %9s %9s %9s\n"
	fmt.Printf(row, "op", "clients", "ops/s", "p50", "p90", "p99", "p99.9", "max")
	for _, op := range o.ops {
		for _, n := range o.clients {
			r, err := runCase(o, op, n)
			if err != nil {
				return fmt.Errorf("%s with %d clients: %w", op, n, err)
			}
			fmt.Printf(row, op, fmt.Sprint(n), fmt.Sprintf("%.0f", r.throughput),
				ms(r.latency(50)), ms(r.latency(90)), ms(r.latency(99)), ms(r.latency(99.9)), ms(r.latency(100)))
		}
	}
	return nil
}

func key(i int) string { return fmt.Sprintf("bench-%d", i) }

// ms formats d in milliseconds, to two decimal places.
func ms(d time.Duration) string { return fmt.Sprintf("%.2fms", float64(d)/float64(time.Millisecond)) }

type result struct {
	throughput float64         // operations per second
	latencies  []time.Duration // sorted
}

// latency returns the p-th percentile latency, by nearest rank.
func (r result) latency(p float64) time.Duration {
	if len(r.latencies) == 0 {
		return 0
	}
	i := int(float64(len(r.latencies))*p/100+0.999999) - 1
	return r.latencies[min(max(i, 0), len(r.latencies)-1)]
}

// runCase runs n clients doing op for the warmup and then the measured
// duration. Only operations that start and finish inside the measured
// window count.
func runCase(o options, op string, n int) (result, error) {
	start := time.Now().Add(o.warmup)
	end := start.Add(o.duration)
	ctx, cancel := context.WithDeadline(context.Background(), end.Add(10*time.Second))
	defer cancel()

	var mu sync.Mutex
	var all []time.Duration
	var errs []error
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lat, err := runClient(ctx, o, op, uint64(i), start, end)
			mu.Lock()
			defer mu.Unlock()
			all = append(all, lat...)
			errs = append(errs, err)
		}()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return result{}, err
	}
	slices.Sort(all)
	return result{throughput: float64(len(all)) / o.duration.Seconds(), latencies: all}, nil
}

// runClient sends requests back to back until end, and returns the
// latencies of those that ran inside [start, end].
func runClient(ctx context.Context, o options, op string, seed uint64, start, end time.Time) ([]time.Duration, error) {
	c, err := client.New(o.servers)
	if err != nil {
		return nil, err
	}
	rng := rand.New(rand.NewPCG(seed, uint64(time.Now().UnixNano())))
	value := make([]byte, o.valueSize)
	var lat []time.Duration
	for {
		k := key(rng.IntN(o.keys))
		t := time.Now()
		if !t.Before(end) {
			return lat, nil
		}
		switch op {
		case "put":
			err = c.Put(ctx, k, value)
		case "get":
			_, _, err = c.Get(ctx, k)
		}
		if err != nil {
			return lat, fmt.Errorf("%s %q: %w", op, k, err)
		}
		if done := time.Now(); !t.Before(start) && !done.After(end) {
			lat = append(lat, done.Sub(t))
		}
	}
}
