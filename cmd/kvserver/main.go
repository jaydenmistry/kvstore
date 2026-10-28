// Command kvserver runs one node of a kvstore cluster.
//
//	kvserver -config /etc/kvstore/cluster.json -id 1 -data /var/lib/kvstore
//
// The config file lists every member and the cluster's timing (see package
// config); every node needs the same file. Instead of a file, members can be
// given with -peer, one per member, including this node:
//
//	kvserver -id 1 -data /var/lib/kvstore \
//	    -peer 1=10.0.0.1:7001,10.0.0.1:8001 \
//	    -peer 2=10.0.0.2:7001,10.0.0.2:8001 \
//	    -peer 3=10.0.0.3:7001,10.0.0.3:8001
//
// -tick and -snapshot-every, if given, override the file.
//
// Exit status is 0 after SIGINT or SIGTERM, 2 for bad usage, 1 if the node
// could not start, and 3 (exitFatal) if storage or the state machine failed.
// Do not restart automatically after exit status 3: a failed fsync may have
// left the page cache claiming data that never reached disk, so drop the
// host's page cache or reboot it first.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jaydenmistry/kvstore/internal/config"
	"github.com/jaydenmistry/kvstore/internal/server"
	"github.com/jaydenmistry/kvstore/internal/vfs"
)

const (
	exitError = 1
	exitUsage = 2
	exitFatal = 3
)

func main() { os.Exit(run()) }

func run() int {
	peers := peerFlag{}
	configPath := flag.String("config", "", "cluster configuration `file`")
	id := flag.Uint64("id", 0, "this node's `ID`")
	flag.Var(peers, "peer", "cluster member as `ID=RAFT_ADDR,API_ADDR`; repeat for every member, including this node")
	dataDir := flag.String("data", "", "data `directory`")
	tick := flag.Duration("tick", server.DefaultTickInterval, "Raft tick `interval`")
	snapEvery := flag.Int("snapshot-every", server.DefaultSnapshotEvery, "compact the log after this many applied `entries`; negative disables")
	flag.Parse()
	if flag.NArg() > 0 || *id == 0 || *dataDir == "" || (len(peers) == 0) == (*configPath == "") {
		fmt.Fprintln(os.Stderr, "kvserver: -id and -data are required, and exactly one of -config or -peer")
		flag.Usage()
		return exitUsage
	}
	cfg := server.Config{ID: *id, Peers: peers, DataDir: *dataDir, TickInterval: *tick, SnapshotEvery: *snapEvery}
	if *configPath != "" {
		c, err := config.Load(*configPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "kvserver: %v\n", err)
			return exitUsage
		}
		cfg.Peers = c.Peers()
		cfg.TickInterval = time.Duration(c.Tick)
		cfg.ElectionTick = c.ElectionTick
		cfg.HeartbeatTick = c.HeartbeatTick
		cfg.RequestTimeout = time.Duration(c.RequestTimeout)
		cfg.SnapshotEvery = c.SnapshotEvery
		// Flags given explicitly override the file.
		flag.Visit(func(f *flag.Flag) {
			switch f.Name {
			case "tick":
				cfg.TickInterval = *tick
			case "snapshot-every":
				cfg.SnapshotEvery = *snapEvery
			}
		})
	}
	self, ok := cfg.Peers[*id]
	if !ok {
		fmt.Fprintf(os.Stderr, "kvserver: node %d is not a cluster member\n", *id)
		return exitUsage
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	cfg.Logger = log

	if err := mkdirDurable(*dataDir); err != nil {
		log.Error("creating data directory", "err", err)
		return exitError
	}
	raftLn, err := net.Listen("tcp", self.Raft)
	if err != nil {
		log.Error("listening for Raft traffic", "err", err)
		return exitError
	}
	apiLn, err := net.Listen("tcp", self.API)
	if err != nil {
		log.Error("listening for clients", "err", err)
		return exitError
	}
	s, err := server.New(cfg, raftLn, apiLn)
	if err != nil {
		return failed(log, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := s.Run(ctx); err != nil {
		return failed(log, err)
	}
	log.Info("node stopped")
	return 0
}

func failed(log *slog.Logger, err error) int {
	if errors.Is(err, server.ErrFatal) {
		log.Error("node stopped on a fatal error; do not restart it before dropping the host page cache or rebooting",
			"err", err, "exit", exitFatal)
		return exitFatal
	}
	log.Error("node failed", "err", err)
	return exitError
}

// mkdirDurable creates dir and any missing parents, and fsyncs the parent of
// each, so the directory entries survive a power loss. The WAL syncs the
// directory holding its file, but not that directory's own entry in its
// parent: without this, a power cut soon after a node's first start could
// remove the whole data directory, and the node would restart having
// forgotten votes and entries it had acknowledged.
func mkdirDurable(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	var missing []string
	for d := dir; ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		missing = append(missing, d)
		if d == filepath.Dir(d) {
			break
		}
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// Sync the parent of every directory created, and always dir's own
	// parent: an operator may have created dir without syncing it. All of
	// them finish before the WAL is opened, so their order does not matter.
	toSync := []string{filepath.Dir(dir)}
	for i := len(missing) - 1; i >= 0; i-- {
		if p := filepath.Dir(missing[i]); p != toSync[0] {
			toSync = append(toSync, p)
		}
	}
	for _, d := range toSync {
		if err := (vfs.OS{}).SyncDir(d); err != nil {
			return fmt.Errorf("syncing %s: %w", d, err)
		}
	}
	return nil
}

// peerFlag collects -peer flags.
type peerFlag map[uint64]server.Peer

func (p peerFlag) String() string { return fmt.Sprint(map[uint64]server.Peer(p)) }

func (p peerFlag) Set(s string) error {
	idStr, addrs, ok := strings.Cut(s, "=")
	raftAddr, apiAddr, ok2 := strings.Cut(addrs, ",")
	if !ok || !ok2 || raftAddr == "" || apiAddr == "" {
		return errors.New("want ID=RAFT_ADDR,API_ADDR")
	}
	id, err := strconv.ParseUint(idStr, 10, 64)
	if err != nil || id == 0 {
		return fmt.Errorf("bad node ID %q", idStr)
	}
	if _, dup := p[id]; dup {
		return fmt.Errorf("node %d listed twice", id)
	}
	p[id] = server.Peer{Raft: raftAddr, API: apiAddr}
	return nil
}
