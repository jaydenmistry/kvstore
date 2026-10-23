// Package server runs one node of the cluster: a kv.Replica driven by a
// wall-clock ticker, Raft traffic over TCP, and the client API over HTTP.
//
// The Replica, like Raft, is single-threaded. One goroutine, the loop,
// owns it; HTTP handlers hand it work through a channel and wait for the
// result.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/jaydenmistry/kvstore/internal/kv"
	"github.com/jaydenmistry/kvstore/internal/raft"
	"github.com/jaydenmistry/kvstore/internal/transport"
	"github.com/jaydenmistry/kvstore/internal/vfs"
)

// Peer is one cluster member's addresses, as host:port.
type Peer struct {
	Raft string // Raft traffic between nodes
	API  string // the HTTP client API
}

// Config configures a node.
type Config struct {
	ID uint64
	// Peers lists every member of the cluster, including this node.
	Peers map[uint64]Peer
	// DataDir holds this node's Raft log.
	DataDir string
	// FS is the filesystem for DataDir. If nil, the real one is used;
	// tests substitute a vfs.Mem.
	FS vfs.FS

	// TickInterval is the length of one Raft tick. ElectionTick and
	// HeartbeatTick are in ticks; see raft.Config.
	TickInterval  time.Duration
	ElectionTick  int
	HeartbeatTick int
	// RequestTimeout bounds how long a request waits inside the node.
	RequestTimeout time.Duration
	// SnapshotEvery is how many applied entries trigger a snapshot and log
	// compaction. Zero means DefaultSnapshotEvery; negative disables it.
	SnapshotEvery int

	Logger *slog.Logger
}

// Defaults for the timing fields of Config, chosen for a LAN: elections time
// out after 0.5 to 1 seconds, and leaders send heartbeats every 100 ms.
const (
	DefaultTickInterval   = 50 * time.Millisecond
	DefaultElectionTick   = 10
	DefaultHeartbeatTick  = 2
	DefaultRequestTimeout = 3 * time.Second
	DefaultSnapshotEvery  = 10000
)

// ErrFatal wraps errors that stop the node: storage failed, or a committed
// entry could not be applied. Restarting cannot fix either without someone
// looking (see CLAUDE.md, "fsync failure is fail-stop").
var ErrFatal = errors.New("server: fatal")

// errStopped is returned to HTTP handlers once the loop has exited.
var errStopped = errors.New("server: node stopped")

// Server is a running node.
type Server struct {
	cfg     Config
	log     *slog.Logger
	storage *raft.DiskStorage
	tr      *transport.TCP
	replica *kv.Replica
	http    *http.Server
	apiLn   net.Listener

	ops     chan func() error // work for the loop
	stopped chan struct{}     // closed when the loop exits
}

// New opens the node's storage and starts its transport. raftLn and apiLn
// are the listeners for Raft traffic and the client API. Call Run to start
// serving.
func New(cfg Config, raftLn, apiLn net.Listener) (*Server, error) {
	if cfg.TickInterval == 0 {
		cfg.TickInterval = DefaultTickInterval
	}
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = DefaultElectionTick
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = DefaultHeartbeatTick
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.SnapshotEvery == 0 {
		cfg.SnapshotEvery = DefaultSnapshotEvery
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.FS == nil {
		cfg.FS = vfs.OS{}
	}
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("server: node %d is not in the peer list", cfg.ID)
	}
	log := cfg.Logger.With("node", cfg.ID)

	storage, err := raft.OpenDiskStorage(cfg.FS, cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("%w: opening storage: %w", ErrFatal, err)
	}
	ids := make([]uint64, 0, len(cfg.Peers))
	raftAddrs := map[uint64]string{}
	for id, p := range cfg.Peers {
		ids = append(ids, id)
		raftAddrs[id] = p.Raft
	}
	tr := transport.NewTCP(cfg.ID, raftLn, raftAddrs, log)
	r, err := raft.New(raft.Config{
		ID:            cfg.ID,
		Peers:         ids,
		ElectionTick:  cfg.ElectionTick,
		HeartbeatTick: cfg.HeartbeatTick,
		// Without CheckQuorum, a leader cut off from the majority keeps
		// accepting requests until they time out. Without PreVote, a node
		// that loses touch with the leader deposes it when it reconnects.
		// Tests turn both off to show that safety never depends on them.
		CheckQuorum: true,
		PreVote:     true,
		Storage:     storage,
		Transport:   tr,
	})
	if err != nil {
		tr.Close()
		storage.Close()
		return nil, fmt.Errorf("%w: %w", ErrFatal, err)
	}
	timeoutTicks := max(1, int(cfg.RequestTimeout/cfg.TickInterval))
	s := &Server{
		cfg:     cfg,
		log:     log,
		storage: storage,
		tr:      tr,
		replica: kv.NewReplica(r, kv.Config{RequestTimeout: timeoutTicks, SnapshotEvery: uint64(max(0, cfg.SnapshotEvery))}),
		apiLn:   apiLn,
		ops:     make(chan func() error),
		stopped: make(chan struct{}),
	}
	s.http = &http.Server{Handler: s.handler(), ReadHeaderTimeout: 10 * time.Second}
	return s, nil
}

// Run serves until ctx is canceled, returning nil, or until the node fails,
// returning an error wrapping ErrFatal. Either way it shuts everything down
// and closes storage before returning.
func (s *Server) Run(ctx context.Context) error {
	httpErr := make(chan error, 1)
	go func() { httpErr <- s.http.Serve(s.apiLn) }()
	s.log.Info("node started", "api", s.apiLn.Addr(), "peers", len(s.cfg.Peers))

	err := s.loop(ctx)
	close(s.stopped)

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.http.Shutdown(shutdown)
	if e := <-httpErr; e != nil && !errors.Is(e, http.ErrServerClosed) && err == nil {
		err = fmt.Errorf("server: HTTP: %w", e)
	}
	s.tr.Close()
	if e := s.storage.Close(); e != nil && err == nil {
		err = fmt.Errorf("%w: closing storage: %w", ErrFatal, e)
	}
	return err
}

// loop is the only goroutine that touches the Replica.
func (s *Server) loop(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.TickInterval)
	defer ticker.Stop()
	var last raft.Status
	for {
		var err error
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			err = s.replica.Tick()
		case m := <-s.tr.Recv():
			err = s.replica.Step(m)
		case op := <-s.ops:
			err = op()
		}
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFatal, err)
		}
		if st := s.replica.Raft().Status(); st.State != last.State || st.Lead != last.Lead {
			s.log.Info("raft state changed", "state", st.State, "term", st.Term, "leader", st.Lead)
			last = st
		}
	}
}

// exec runs f on the loop goroutine and waits for it to finish.
func (s *Server) exec(ctx context.Context, f func()) error {
	done := make(chan struct{})
	op := func() error { f(); close(done); return nil }
	select {
	case s.ops <- op:
	case <-ctx.Done():
		return ctx.Err()
	case <-s.stopped:
		return errStopped
	}
	<-done
	return nil
}

// submit hands req to the Replica and waits for its result. The Replica
// always completes a request within its timeout, unless the node stops.
func (s *Server) submit(ctx context.Context, req kv.Request) (kv.Result, error) {
	done := make(chan kv.Result, 1)
	op := func() error {
		return s.replica.Submit(req, func(res kv.Result) { done <- res })
	}
	select {
	case s.ops <- op:
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-s.stopped:
		return kv.Result{}, errStopped
	}
	select {
	case res := <-done:
		return res, nil
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	case <-s.stopped:
		return kv.Result{}, errStopped
	}
}

// Status is a node's state, as reported by GET /status.
type Status struct {
	ID        uint64 `json:"id"`
	State     string `json:"state"`
	Term      uint64 `json:"term"`
	Leader    uint64 `json:"leader"`
	Commit    uint64 `json:"commit"`
	Applied   uint64 `json:"applied"`
	LastIndex uint64 `json:"last_index"`
	Snapshot  uint64 `json:"snapshot"`
}

func (s *Server) status(ctx context.Context) (Status, error) {
	var st Status
	err := s.exec(ctx, func() {
		rs := s.replica.Raft().Status()
		st = Status{
			ID: rs.ID, State: rs.State.String(), Term: rs.Term, Leader: rs.Lead,
			Commit: rs.Commit, Applied: s.replica.Applied(), LastIndex: rs.LastIndex, Snapshot: rs.Snapshot,
		}
	})
	return st, err
}
