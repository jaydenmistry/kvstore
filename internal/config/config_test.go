package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jaydenmistry/kvstore/internal/server"
)

const three = `{
  "nodes": [
    {"id": 1, "raft": "127.0.0.1:7001", "api": "127.0.0.1:8001"},
    {"id": 2, "raft": "127.0.0.1:7002", "api": "127.0.0.1:8002"},
    {"id": 3, "raft": "127.0.0.1:7003", "api": "127.0.0.1:8003"}
  ],
  "tick": "20ms",
  "election_tick": 20,
  "heartbeat_tick": 3,
  "request_timeout": "2s",
  "snapshot_every": -1
}`

func TestParse(t *testing.T) {
	c, err := Parse([]byte(three))
	if err != nil {
		t.Fatal(err)
	}
	if time.Duration(c.Tick) != 20*time.Millisecond || c.ElectionTick != 20 || c.HeartbeatTick != 3 ||
		time.Duration(c.RequestTimeout) != 2*time.Second || c.SnapshotEvery != -1 {
		t.Fatalf("parsed %+v", c)
	}
	if p := c.Peers(); len(p) != 3 || p[2] != (server.Peer{Raft: "127.0.0.1:7002", API: "127.0.0.1:8002"}) {
		t.Fatalf("Peers() = %v", p)
	}
	if u := c.APIURLs(); strings.Join(u, ",") != "http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003" {
		t.Fatalf("APIURLs() = %v", u)
	}
}

func TestParseDefaults(t *testing.T) {
	c, err := Parse([]byte(`{"nodes": [{"id": 1, "raft": "h:1", "api": "h:2"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Tick != 0 || c.ElectionTick != 0 || c.SnapshotEvery != 0 {
		t.Fatalf("parsed %+v, want zero timing fields", c)
	}
}

func TestParseRejects(t *testing.T) {
	node := func(id, raft, api string) string {
		return `{"id": ` + id + `, "raft": "` + raft + `", "api": "` + api + `"}`
	}
	one := node("1", "h:1", "h:2")
	cases := map[string]string{
		"not JSON":           `nodes: 1`,
		"no nodes":           `{"nodes": []}`,
		"unknown field":      `{"nodes": [` + one + `], "snapshot_evry": 5}`,
		"unknown node field": `{"nodes": [{"id": 1, "raft": "h:1", "api": "h:2", "port": 3}]}`,
		"trailing data":      `{"nodes": [` + one + `]} {}`,
		"zero ID":            `{"nodes": [` + node("0", "h:1", "h:2") + `]}`,
		"duplicate ID":       `{"nodes": [` + one + `, ` + node("1", "h:3", "h:4") + `]}`,
		"missing port":       `{"nodes": [` + node("1", "h", "h:2") + `]}`,
		"missing address":    `{"nodes": [{"id": 1, "raft": "h:1"}]}`,
		"shared address":     `{"nodes": [` + one + `, ` + node("2", "h:3", "h:1") + `]}`,
		"numeric duration":   `{"nodes": [` + one + `], "tick": 50}`,
		"bad duration":       `{"nodes": [` + one + `], "tick": "fast"}`,
		"negative duration":  `{"nodes": [` + one + `], "request_timeout": "-1s"}`,
		"negative tick":      `{"nodes": [` + one + `], "election_tick": -1}`,
		"heartbeat too long": `{"nodes": [` + one + `], "election_tick": 4, "heartbeat_tick": 4}`,
		"default election":   `{"nodes": [` + one + `], "heartbeat_tick": 10}`,
	}
	for name, in := range cases {
		if c, err := Parse([]byte(in)); err == nil {
			t.Errorf("%s: parsed %+v, want an error", name, c)
		}
	}
}

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cluster.json")
	if err := os.WriteFile(path, []byte(three), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(path, []byte(`{}`), 0o600)
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("Load of an invalid file: err = %v, want one naming the file", err)
	}
}
