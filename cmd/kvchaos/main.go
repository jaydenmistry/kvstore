// Command kvchaos tests a real kvstore cluster under faults. It starts
// kvserver processes on this machine, runs clients against them, and
// meanwhile kills, pauses, and restarts nodes and partitions the network
// between them. Then it repairs everything, lets every operation finish,
// and checks that:
//
//   - the history of every client operation is linearizable;
//   - the nodes converge on one leader and the same log;
//   - no node exited on its own (a crash, or exit status 3 after a storage
//     error), and every node stops cleanly with exit status 0.
//
// Usage:
//
//	kvchaos [-nodes 5] [-clients 10] [-duration 60s] [-seed N] [-kvserver bin/kvserver]
//
// Raft traffic between nodes goes through proxies inside kvchaos, one per
// direction per pair of nodes, which is how it cuts links without root
// privileges or firewall rules. Client traffic goes straight to the nodes.
//
// Unlike the simulated tests, a run depends on real timing and cannot be
// replayed exactly. The seed fixes the sequence of faults and operations
// that are chosen; when they happen still varies from run to run.
//
// Exit status is 0 if every check passed, 1 if one failed, and 2 for bad
// usage. On failure, the history, node logs, and data directories are kept.
package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"maps"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jaydenmistry/kvstore/internal/linearize"
	"github.com/jaydenmistry/kvstore/internal/server"
)

func main() { os.Exit(run()) }

type options struct {
	bin           string
	nodes         int
	clients       int
	keys          int
	duration      time.Duration
	seed          uint64
	dir           string
	keep          bool
	tick          time.Duration
	snapshotEvery int
	// checkQuorum and preVote are "on", "off", or "random" (chosen from
	// the seed).
	checkQuorum, preVote string
}

func run() int {
	var o options
	flag.StringVar(&o.bin, "kvserver", "bin/kvserver", "kvserver `binary`")
	flag.IntVar(&o.nodes, "nodes", 5, "cluster `size`")
	flag.IntVar(&o.clients, "clients", 10, "concurrent `clients`")
	flag.IntVar(&o.keys, "keys", 5, "number of distinct `keys`")
	flag.DurationVar(&o.duration, "duration", 60*time.Second, "how `long` to inject faults")
	flag.Uint64Var(&o.seed, "seed", 0, "random `seed` (default: from the clock)")
	flag.StringVar(&o.dir, "dir", "", "`directory` for node data and logs (default: a new temporary one)")
	flag.BoolVar(&o.keep, "keep", false, "keep the directory even if every check passes")
	flag.DurationVar(&o.tick, "tick", 20*time.Millisecond, "Raft tick `interval` for the nodes")
	flag.IntVar(&o.snapshotEvery, "snapshot-every", 50, "nodes compact their logs after this many applied `entries`")
	flag.StringVar(&o.checkQuorum, "check-quorum", "random", "run nodes with CheckQuorum `on`, off, or random")
	flag.StringVar(&o.preVote, "pre-vote", "random", "run nodes with PreVote `on`, off, or random")
	flag.Parse()
	valid := func(s string) bool { return s == "on" || s == "off" || s == "random" }
	if flag.NArg() > 0 || o.nodes < 1 || o.clients < 1 || o.keys < 1 || !valid(o.checkQuorum) || !valid(o.preVote) {
		flag.Usage()
		return 2
	}
	if o.seed == 0 {
		o.seed = uint64(time.Now().UnixNano())
	}
	// CheckQuorum and PreVote narrow the windows in which some bugs show:
	// a leader cut off in a minority steps down before clients notice.
	// Safety must not depend on them, so runs turn them off too.
	rng := rand.New(rand.NewPCG(o.seed, 1))
	for _, s := range []*string{&o.checkQuorum, &o.preVote} {
		if *s == "random" {
			*s = []string{"on", "off"}[rng.IntN(2)]
		}
	}
	if _, err := os.Stat(o.bin); err != nil {
		log.Printf("kvserver binary: %v (build it with make build)", err)
		return 2
	}
	if o.dir == "" {
		d, err := os.MkdirTemp("", "kvchaos-")
		if err != nil {
			log.Print(err)
			return 1
		}
		o.dir = d
	}
	log.SetFlags(log.Ltime | log.Lmicroseconds)
	log.Printf("seed %d, %d nodes, %d clients, %v of faults, CheckQuorum %s, PreVote %s, directory %s",
		o.seed, o.nodes, o.clients, o.duration, o.checkQuorum, o.preVote, o.dir)

	err := chaos(o)
	if err != nil {
		log.Printf("FAIL: %v", err)
		log.Printf("seed %d; history and node logs are in %s", o.seed, o.dir)
		return 1
	}
	log.Print("PASS")
	if !o.keep {
		os.RemoveAll(o.dir)
	}
	return 0
}

// cluster is the set of nodes and the links between them.
type cluster struct {
	nodes []*node
	links map[[2]uint64]*link // [from, to]

	failMu sync.Mutex
	failed error // the first unexpected node exit
}

func (c *cluster) fail(err error) {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	if c.failed == nil {
		c.failed = err
	}
}

func (c *cluster) failure() error {
	c.failMu.Lock()
	defer c.failMu.Unlock()
	return c.failed
}

// freeAddrs returns n distinct free addresses for kvserver to listen on.
//
// They are below 32768, where Linux starts the ephemeral ports it gives
// outgoing connections and listeners on port 0 (the proxies'). A port from
// that range could be taken by one of those between being chosen here and
// kvserver binding it, as could a port this function had released while
// still choosing, so every port stays held until all are chosen.
func freeAddrs(n int) ([]string, error) {
	var lns []net.Listener
	defer func() {
		for _, ln := range lns {
			ln.Close()
		}
	}()
	for tries := 0; len(lns) < n; tries++ {
		if tries == 100*n {
			return nil, errors.New("no free ports below 32768")
		}
		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", 20000+rand.IntN(12768)))
		if err == nil {
			lns = append(lns, ln)
		}
	}
	addrs := make([]string, n)
	for i, ln := range lns {
		addrs[i] = ln.Addr().String()
	}
	return addrs, nil
}

func newCluster(o options) (*cluster, error) {
	c := &cluster{links: map[[2]uint64]*link{}}
	addrs, err := freeAddrs(2 * o.nodes)
	if err != nil {
		return nil, err
	}
	raftAddr := map[uint64]string{}
	apiAddr := map[uint64]string{}
	for i := 1; i <= o.nodes; i++ {
		raftAddr[uint64(i)], apiAddr[uint64(i)] = addrs[2*i-2], addrs[2*i-1]
	}
	for from := range raftAddr {
		for to := range raftAddr {
			if from != to {
				l, err := newLink(from, to, raftAddr[to])
				if err != nil {
					return nil, err
				}
				c.links[[2]uint64{from, to}] = l
			}
		}
	}
	for i := 1; i <= o.nodes; i++ {
		id := uint64(i)
		args := []string{
			"-id", fmt.Sprint(id),
			"-data", filepath.Join(o.dir, fmt.Sprintf("node%d", id)),
			"-tick", o.tick.String(),
			"-snapshot-every", fmt.Sprint(o.snapshotEvery),
			"-check-quorum=" + fmt.Sprint(o.checkQuorum == "on"),
			"-pre-vote=" + fmt.Sprint(o.preVote == "on"),
		}
		// Each node reaches every peer through the link from it to that
		// peer, so its peer list differs from the others' in the Raft
		// addresses only.
		for j := 1; j <= o.nodes; j++ {
			peer := uint64(j)
			addr := raftAddr[peer]
			if peer != id {
				addr = c.links[[2]uint64{id, peer}].addr()
			}
			args = append(args, "-peer", fmt.Sprintf("%d=%s,%s", peer, addr, apiAddr[peer]))
		}
		f, err := os.Create(filepath.Join(o.dir, fmt.Sprintf("node%d.log", id)))
		if err != nil {
			return nil, err
		}
		c.nodes = append(c.nodes, &node{id: id, bin: o.bin, args: args, api: "http://" + apiAddr[id], log: f})
	}
	return c, nil
}

func (c *cluster) servers() []string {
	var s []string
	for _, n := range c.nodes {
		s = append(s, n.api)
	}
	return s
}

func (c *cluster) heal() {
	for _, l := range c.links {
		l.setCut(false)
	}
}

// partition cuts every link between the two groups, both ways.
func (c *cluster) partition(a, b []uint64) {
	c.heal()
	for _, x := range a {
		for _, y := range b {
			c.links[[2]uint64{x, y}].setCut(true)
			c.links[[2]uint64{y, x}].setCut(true)
		}
	}
}

// faults counts the faults injected, by kind.
type faults map[string]int

func (f faults) String() string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(f)) {
		parts = append(parts, fmt.Sprintf("%s %d", k, f[k]))
	}
	return strings.Join(parts, ", ")
}

// leader returns the node that a running node reports as leader, if any.
func (c *cluster) leader() *node {
	for _, n := range c.nodes {
		if n.getState() != running {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		st, err := n.fetchStatus(ctx)
		cancel()
		if err == nil && st.State == "leader" {
			return n
		}
	}
	return nil
}

// inject applies one random fault, or repairs some earlier ones.
//
// At most a minority of nodes is killed or paused at a time, so the
// processes that remain could form a majority, though partitions may still
// prevent it. Without the cap, faults pile up and the cluster spends most
// of a run unable to commit, which tests little.
func (c *cluster) inject(rng *rand.Rand, f faults) error {
	pick := func(state nodeState) *node {
		var ns []*node
		for _, n := range c.nodes {
			if n.getState() == state {
				ns = append(ns, n)
			}
		}
		if len(ns) == 0 {
			return nil
		}
		return ns[rng.IntN(len(ns))]
	}
	// victim picks a running node to kill or pause: half the time the
	// leader, whose failure is the most interesting.
	victim := func() *node {
		faulty := 0
		for _, n := range c.nodes {
			if n.getState() != running {
				faulty++
			}
		}
		if faulty >= (len(c.nodes)-1)/2 {
			return nil
		}
		if rng.IntN(2) == 0 {
			if l := c.leader(); l != nil {
				return l
			}
		}
		return pick(running)
	}
	ids := make([]uint64, len(c.nodes))
	for i, n := range c.nodes {
		ids[i] = n.id
	}
	switch p := rng.IntN(100); {
	case p < 15:
		if n := victim(); n != nil {
			log.Printf("kill node %d", n.id)
			n.kill()
			f["kill"]++
		}
	case p < 30:
		if n := pick(down); n != nil {
			log.Printf("restart node %d", n.id)
			f["restart"]++
			return n.start(c.fail)
		}
	case p < 38:
		if n := victim(); n != nil {
			log.Printf("pause node %d", n.id)
			n.pause()
			f["pause"]++
		}
	case p < 48:
		if n := pick(paused); n != nil {
			log.Printf("resume node %d", n.id)
			n.resume()
			f["resume"]++
		}
	case p < 53:
		// A clean restart exercises shutdown and recovery from a log that
		// was closed properly.
		if n := victim(); n != nil {
			log.Printf("clean restart of node %d", n.id)
			if err := n.stop(); err != nil {
				return err
			}
			f["clean restart"]++
			return n.start(c.fail)
		}
	case p < 58:
		rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
		k := 1 + rng.IntN(len(ids)-1)
		log.Printf("partition %v | %v", ids[:k], ids[k:])
		c.partition(ids[:k], ids[k:])
		f["partition"]++
	case p < 68:
		// Put the leader in a minority, alone or with some followers. The
		// majority elects a new leader while the old one may still believe
		// it leads: the case ReadIndex exists for.
		l := c.leader()
		if l == nil {
			break
		}
		rest := slices.DeleteFunc(ids, func(id uint64) bool { return id == l.id })
		rng.Shuffle(len(rest), func(i, j int) { rest[i], rest[j] = rest[j], rest[i] })
		k := rng.IntN((len(ids) + 1) / 2) // followers that join the leader's side
		minority := append([]uint64{l.id}, rest[:k]...)
		log.Printf("partition leader: %v | %v", minority, rest[k:])
		c.partition(minority, rest[k:])
		f["partition leader"]++
	case p < 78:
		var cut []string
		for range 1 + rng.IntN(len(c.nodes)) {
			from, to := ids[rng.IntN(len(ids))], ids[rng.IntN(len(ids))]
			if from != to {
				c.links[[2]uint64{from, to}].setCut(true)
				cut = append(cut, fmt.Sprintf("%d->%d", from, to))
			}
		}
		if len(cut) > 0 {
			log.Printf("cut %s", strings.Join(cut, " "))
			f["one-way cut"]++
		}
	case p < 90:
		log.Print("heal network")
		c.heal()
		f["heal"]++
	}
	return nil
}

// repair undoes every fault.
func (c *cluster) repair() error {
	c.heal()
	for _, n := range c.nodes {
		switch n.getState() {
		case paused:
			n.resume()
		case down:
			if err := n.start(c.fail); err != nil {
				return err
			}
		}
	}
	return nil
}

// waitConverged waits until every node is reachable, they agree on one
// leader in one term, and every node has applied everything the leader has
// committed, which must be its whole log.
func (c *cluster) waitConverged(timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		if err := c.failure(); err != nil {
			return "", err
		}
		ok, desc := c.converged()
		if ok {
			return desc, nil
		}
		last = desc
		time.Sleep(100 * time.Millisecond)
	}
	return "", fmt.Errorf("nodes did not converge within %v: %s", timeout, last)
}

func (c *cluster) converged() (bool, string) {
	var desc []string
	var sts []server.Status
	for _, n := range c.nodes {
		st, err := n.fetchStatus(context.Background())
		if err != nil {
			desc = append(desc, fmt.Sprintf("node %d: %v", n.id, err))
			continue
		}
		desc = append(desc, fmt.Sprintf("node %d: %s term %d leader %d commit %d applied %d last %d snapshot %d",
			st.ID, st.State, st.Term, st.Leader, st.Commit, st.Applied, st.LastIndex, st.Snapshot))
		sts = append(sts, st)
	}
	ok := len(sts) == len(c.nodes) && sts[0].Leader != 0
	for _, st := range sts {
		lead := sts[0]
		ok = ok && st.Leader == lead.Leader && st.Term == lead.Term
	}
	if ok {
		lead := sts[slices.IndexFunc(sts, func(st server.Status) bool { return st.ID == sts[0].Leader })]
		for _, st := range sts {
			ok = ok && st.Applied == lead.Commit && st.LastIndex == lead.Commit
		}
	}
	return ok, strings.Join(desc, "; ")
}

func chaos(o options) error {
	c, err := newCluster(o)
	if err != nil {
		return err
	}
	defer func() {
		for _, n := range c.nodes {
			n.kill()
			n.log.Close()
		}
		for _, l := range c.links {
			l.close()
		}
	}()
	for _, n := range c.nodes {
		if err := n.start(c.fail); err != nil {
			return err
		}
	}
	if _, err := c.waitConverged(20 * time.Second); err != nil {
		return fmt.Errorf("cluster did not start: %w", err)
	}
	log.Print("cluster is up")

	// Operations get the whole run, plus time for repair, to finish.
	const repairTime = 60 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), o.duration+repairTime)
	defer cancel()
	rec := &recorder{start: time.Now()}
	stop := make(chan struct{})
	errs := make(chan error, o.clients)
	keys := make([]string, o.keys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%d", i)
	}
	for i := range o.clients {
		go func() { errs <- runClient(ctx, stop, rec, c.servers(), keys, o.seed, i) }()
	}

	rng := rand.New(rand.NewPCG(o.seed, 0))
	f := faults{}
	end := time.Now().Add(o.duration)
	for time.Now().Before(end) {
		time.Sleep(time.Duration(200+rng.IntN(1800)) * time.Millisecond)
		if err := c.failure(); err != nil {
			cancel()
			return err
		}
		if err := c.inject(rng, f); err != nil {
			cancel()
			return err
		}
	}
	log.Printf("faults injected: %v", f)

	log.Print("repairing every fault and letting operations finish")
	if err := c.repair(); err != nil {
		cancel()
		return err
	}
	close(stop)
	var clientErr error
	for range o.clients {
		clientErr = errors.Join(clientErr, <-errs)
	}
	ops := rec.ops
	if err := writeHistory(filepath.Join(o.dir, "history.txt"), ops); err != nil {
		return err
	}
	if clientErr != nil {
		return clientErr
	}
	if err := c.failure(); err != nil {
		return err
	}

	desc, err := c.waitConverged(30 * time.Second)
	if err != nil {
		return err
	}
	log.Printf("converged: %s", desc)

	var stopErr error
	for _, n := range c.nodes {
		stopErr = errors.Join(stopErr, n.stop())
	}
	if stopErr != nil {
		return stopErr
	}

	var reads, writes int
	for _, op := range ops {
		if op.Kind == linearize.Get {
			reads++
		} else {
			writes++
		}
	}
	log.Printf("checking %d operations (%d reads, %d writes) for linearizability", len(ops), reads, writes)
	start := time.Now()
	if ok, bad := linearize.Check(ops); !ok {
		return explain(o.dir, bad)
	}
	log.Printf("linearizable (checked in %v)", time.Since(start).Round(time.Millisecond))
	return nil
}

// explain reports a key whose history is not linearizable, narrowed down to
// the first operation whose result cannot be explained, and writes the
// operations leading up to it to violation.txt.
func explain(dir string, bad []linearize.Op) error {
	t, _ := linearize.FirstViolation(bad)
	at := linearize.At(bad, t)
	slices.SortFunc(at, func(a, b linearize.Op) int { return cmp.Compare(a.Call, b.Call) })
	var culprit linearize.Op
	var b strings.Builder
	fmt.Fprintf(&b, "The history of key %q is not linearizable. The operations called\n", bad[0].Key)
	fmt.Fprintf(&b, "by %d ns have no valid order; those called last are below, and the\n", t)
	fmt.Fprintf(&b, "operation marked <- returned at that time. Times are ns since the start.\n\n")
	for _, o := range at[max(0, len(at)-40):] {
		mark := ""
		if o.Return == t {
			culprit, mark = o, "  <-"
		}
		fmt.Fprintf(&b, "%v%s\n", o, mark)
	}
	path := filepath.Join(dir, "violation.txt")
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		return err
	}
	return fmt.Errorf("history of key %q is not linearizable: first unexplainable result is %v; details in %s", bad[0].Key, culprit, path)
}

func writeHistory(path string, ops []linearize.Op) error {
	ops = slices.Clone(ops)
	slices.SortFunc(ops, func(a, b linearize.Op) int { return cmp.Compare(a.Call, b.Call) })
	var b strings.Builder
	for _, o := range ops {
		fmt.Fprintln(&b, o)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}
