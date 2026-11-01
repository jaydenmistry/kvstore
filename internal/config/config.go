// Package config reads the cluster configuration file shared by kvserver
// and kvctl. It is JSON:
//
//	{
//	  "nodes": [
//	    {"id": 1, "raft": "10.0.0.1:7001", "api": "10.0.0.1:8001"},
//	    {"id": 2, "raft": "10.0.0.2:7001", "api": "10.0.0.2:8001"},
//	    {"id": 3, "raft": "10.0.0.3:7001", "api": "10.0.0.3:8001"}
//	  ],
//	  "tick": "50ms",
//	  "election_tick": 10,
//	  "heartbeat_tick": 2,
//	  "request_timeout": "3s",
//	  "snapshot_every": 10000,
//	  "max_sessions": 10000
//	}
//
// Every node must be given the same file. Fields other than nodes are
// optional and default to the server package's defaults. Unknown fields are
// errors, so a misspelled option is not silently ignored.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"time"

	"github.com/jaydenmistry/kvstore/internal/server"
)

// Cluster is the parsed configuration.
type Cluster struct {
	Nodes []Node `json:"nodes"`

	// Timing, in the units of server.Config. Zero means the default.
	// ElectionTick and HeartbeatTick must be the same on every node: the
	// election timeout has to stay longer than the heartbeat interval of
	// whichever node leads.
	Tick           Duration `json:"tick"`
	ElectionTick   int      `json:"election_tick"`
	HeartbeatTick  int      `json:"heartbeat_tick"`
	RequestTimeout Duration `json:"request_timeout"`
	// SnapshotEvery is server.Config.SnapshotEvery: zero means the
	// default, negative disables snapshots.
	SnapshotEvery int `json:"snapshot_every"`
	// MaxSessions is server.Config.MaxSessions: how many client sessions
	// the cluster keeps. Zero means the default.
	MaxSessions uint64 `json:"max_sessions"`
}

// Node is one cluster member.
type Node struct {
	ID   uint64 `json:"id"`
	Raft string `json:"raft"` // host:port for Raft traffic between nodes
	API  string `json:"api"`  // host:port of the HTTP client API
}

// Duration is a time.Duration written as a string, such as "50ms".
type Duration time.Duration

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`want a duration string such as "50ms"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Load reads and validates the file at path.
func Load(path string) (*Cluster, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return c, nil
}

// Parse parses and validates a configuration.
func Parse(b []byte) (*Cluster, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var c Cluster
	if err := dec.Decode(&c); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, errors.New("unexpected data after the configuration")
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Cluster) validate() error {
	if len(c.Nodes) == 0 {
		return errors.New("no nodes")
	}
	var ids []uint64
	var addrs []string
	for _, n := range c.Nodes {
		if n.ID == 0 {
			return errors.New("node IDs must be nonzero")
		}
		if slices.Contains(ids, n.ID) {
			return fmt.Errorf("node %d listed twice", n.ID)
		}
		ids = append(ids, n.ID)
		for _, a := range []string{n.Raft, n.API} {
			if _, _, err := net.SplitHostPort(a); err != nil {
				return fmt.Errorf("node %d: address %q: %v", n.ID, a, err)
			}
			if slices.Contains(addrs, a) {
				return fmt.Errorf("node %d: address %s used twice", n.ID, a)
			}
			addrs = append(addrs, a)
		}
	}
	switch {
	case c.Tick < 0 || c.RequestTimeout < 0:
		return errors.New("durations must not be negative")
	case c.ElectionTick < 0 || c.HeartbeatTick < 0:
		return errors.New("election_tick and heartbeat_tick must not be negative")
	}
	election, heartbeat := c.ElectionTick, c.HeartbeatTick
	if election == 0 {
		election = server.DefaultElectionTick
	}
	if heartbeat == 0 {
		heartbeat = server.DefaultHeartbeatTick
	}
	if heartbeat >= election {
		return fmt.Errorf("heartbeat_tick %d must be less than election_tick %d", heartbeat, election)
	}
	return nil
}

// Peers returns the members in the form server.Config takes.
func (c *Cluster) Peers() map[uint64]server.Peer {
	peers := make(map[uint64]server.Peer, len(c.Nodes))
	for _, n := range c.Nodes {
		peers[n.ID] = server.Peer{Raft: n.Raft, API: n.API}
	}
	return peers
}

// APIURLs returns the base URL of every member's client API, in file order.
func (c *Cluster) APIURLs() []string {
	urls := make([]string, len(c.Nodes))
	for i, n := range c.Nodes {
		urls[i] = "http://" + n.API
	}
	return urls
}
