package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/jaydenmistry/kvstore/internal/client"
	"github.com/jaydenmistry/kvstore/internal/vfs"
)

// These tests run real nodes on the loopback interface with a fast tick, so
// unlike the simulated tests they depend on real time. They poll with
// generous deadlines rather than sleeping for fixed times.

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

type node struct {
	cfg    Config
	cancel context.CancelFunc
	done   chan error
}

type testCluster struct {
	t     *testing.T
	nodes map[uint64]*node
	urls  []string
}

func listen(t *testing.T, addr string) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	return ln
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	c := &testCluster{t: t, nodes: map[uint64]*node{}}
	peers := map[uint64]Peer{}
	lns := map[uint64][2]net.Listener{}
	for i := range n {
		id := uint64(i + 1)
		r, a := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
		lns[id] = [2]net.Listener{r, a}
		peers[id] = Peer{Raft: r.Addr().String(), API: a.Addr().String()}
		c.urls = append(c.urls, "http://"+a.Addr().String())
	}
	for id := range peers {
		cfg := Config{
			ID: id, Peers: peers, DataDir: t.TempDir(),
			TickInterval: 10 * time.Millisecond, ElectionTick: 10, HeartbeatTick: 2,
			// Compact often, so a restarted node catches up through
			// InstallSnapshot over TCP.
			SnapshotEvery:  5,
			RequestTimeout: time.Second, Logger: quiet,
		}
		c.nodes[id] = &node{cfg: cfg}
		c.start(id, lns[id][0], lns[id][1])
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *testCluster) start(id uint64, raftLn, apiLn net.Listener) {
	c.t.Helper()
	n := c.nodes[id]
	s, err := New(n.cfg, raftLn, apiLn)
	if err != nil {
		c.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n.cancel, n.done = cancel, make(chan error, 1)
	go func() { n.done <- s.Run(ctx) }()
}

// restart starts a stopped node again on the same addresses and data.
func (c *testCluster) restart(id uint64) {
	c.t.Helper()
	p := c.nodes[id].cfg.Peers[id]
	c.start(id, listen(c.t, p.Raft), listen(c.t, p.API))
}

func (c *testCluster) stop(id uint64) {
	c.t.Helper()
	n := c.nodes[id]
	if n.cancel == nil {
		return
	}
	n.cancel()
	if err := <-n.done; err != nil {
		c.t.Errorf("node %d: Run: %v", id, err)
	}
	n.cancel = nil
}

func (c *testCluster) status(id uint64) (Status, error) {
	resp, err := http.Get("http://" + c.nodes[id].cfg.Peers[id].API + "/status")
	if err != nil {
		return Status{}, err
	}
	defer resp.Body.Close()
	var st Status
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		<-tick.C
	}
}

func (c *testCluster) leader() uint64 {
	c.t.Helper()
	var lead uint64
	eventually(c.t, "a leader is elected", func() bool {
		for id, n := range c.nodes {
			if n.cancel == nil {
				continue
			}
			if st, err := c.status(id); err == nil && st.State == "leader" {
				lead = id
				return true
			}
		}
		return false
	})
	return lead
}

func (c *testCluster) client() *client.Client {
	c.t.Helper()
	cl, err := client.New(c.urls)
	if err != nil {
		c.t.Fatal(err)
	}
	return cl
}

func testCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestClusterServesRequests(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	ctx := testCtx(t)
	if err := cl.Put(ctx, "greeting", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if v, ok, err := cl.Get(ctx, "greeting"); err != nil || !ok || string(v) != "hello" {
		t.Fatalf("Get = %q, %v, %v", v, ok, err)
	}
	if found, err := cl.Delete(ctx, "greeting"); err != nil || !found {
		t.Fatalf("Delete = %v, %v", found, err)
	}
	if _, ok, err := cl.Get(ctx, "greeting"); err != nil || ok {
		t.Fatalf("Get after Delete = %v, %v", ok, err)
	}
	if found, err := cl.Delete(ctx, "greeting"); err != nil || found {
		t.Fatalf("second Delete = %v, %v; want not found", found, err)
	}
}

func TestFollowerRedirectsToLeader(t *testing.T) {
	c := newTestCluster(t, 3)
	lead := c.leader()
	var follower uint64
	for id := range c.nodes {
		if id != lead {
			follower = id
		}
	}
	// Wait until the follower has heard from the leader.
	eventually(t, "the follower knows the leader", func() bool {
		st, err := c.status(follower)
		return err == nil && st.Leader == lead
	})
	noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := noFollow.Get("http://" + c.nodes[follower].cfg.Peers[follower].API + "/kv/a%2Fb")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	want := "http://" + c.nodes[lead].cfg.Peers[lead].API + "/kv/a%2Fb"
	if resp.StatusCode != http.StatusTemporaryRedirect || resp.Header.Get("Location") != want {
		t.Fatalf("follower answered %d Location %q; want 307 to %q", resp.StatusCode, resp.Header.Get("Location"), want)
	}
	if got := resp.Header.Get(HeaderLeader); got != fmt.Sprint(lead) {
		t.Fatalf("%s header %q, want %d", HeaderLeader, got, lead)
	}
}

func TestWritesNeedClientHeaders(t *testing.T) {
	c := newTestCluster(t, 1)
	c.leader()
	req, _ := http.NewRequest(http.MethodPut, c.urls[0]+"/kv/k", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("PUT without client headers: %d, want 400", resp.StatusCode)
	}
}

func TestSurvivesLeaderLossAndRestartCatchesUp(t *testing.T) {
	c := newTestCluster(t, 3)
	cl := c.client()
	ctx := testCtx(t)
	for i := range 20 {
		if err := cl.Put(ctx, fmt.Sprint("k", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	old := c.leader()
	c.stop(old)
	// The remaining two nodes elect a leader and keep serving.
	for i := 20; i < 40; i++ {
		if err := cl.Put(ctx, fmt.Sprint("k", i), []byte(fmt.Sprint(i))); err != nil {
			t.Fatal(err)
		}
	}
	c.restart(old)
	lead := c.leader()
	want, err := c.status(lead)
	if err != nil {
		t.Fatal(err)
	}
	if want.Snapshot <= 20 {
		t.Fatalf("leader's snapshot is at %d; the restarted node would not need it", want.Snapshot)
	}
	eventually(t, "the restarted node catches up", func() bool {
		st, err := c.status(old)
		return err == nil && st.Applied >= want.Commit
	})
	for i := range 40 {
		if v, ok, err := cl.Get(ctx, fmt.Sprint("k", i)); err != nil || !ok || string(v) != fmt.Sprint(i) {
			t.Fatalf("k%d = %q, %v, %v", i, v, ok, err)
		}
	}
}

func TestStorageFailureIsFatal(t *testing.T) {
	// A single node on an in-memory filesystem whose fsync starts failing.
	// Run must return ErrFatal, not keep serving.
	fs := vfs.NewMem()
	raftLn, apiLn := listen(t, "127.0.0.1:0"), listen(t, "127.0.0.1:0")
	cfg := Config{
		ID: 1, Peers: map[uint64]Peer{1: {Raft: raftLn.Addr().String(), API: apiLn.Addr().String()}},
		DataDir: "/data", FS: fs, TickInterval: 10 * time.Millisecond, Logger: quiet,
	}
	s, err := New(cfg, raftLn, apiLn)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	cl, _ := client.New([]string{"http://" + apiLn.Addr().String()})
	if err := cl.Put(testCtx(t), "k", []byte("v")); err != nil {
		t.Fatal(err)
	}
	fs.SetFault(func(op vfs.Op, name string) error {
		if op == vfs.OpSync {
			return errors.New("injected fsync failure")
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	cl.Put(ctx, "k", []byte("w")) // fails one way or another
	select {
	case err := <-done:
		if !errors.Is(err, ErrFatal) {
			t.Fatalf("Run returned %v, want ErrFatal", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("node kept running after fsync failed")
	}
}
