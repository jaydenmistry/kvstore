# kvstore

A distributed key-value store in Go with Raft implemented from scratch. This is a
learning and portfolio project: every design decision must be explainable in a
technical interview. Correctness and clarity matter more than speed.

Module: `github.com/jaydenmistry/kvstore`. Go 1.27, Linux.

## Goal

- Clients can Put, Get, and Delete keys.
- Writes are replicated through Raft and survive the loss of a minority of nodes.
- Reads are linearizable: served by the leader and confirmed through the log or a
  ReadIndex check, never from a possibly stale follower.
- A crashed node restarts, recovers from disk, and catches up.
- Cluster membership changes are out of scope.

## Hard constraints

- Implement Raft ourselves. Never use hashicorp/raft, etcd/raft, or any consensus library.
- Prefer the standard library. Ask before adding any dependency (module or tool) and
  explain why it is worth it.
- The network transport sits behind an interface. Tests use an in-memory transport
  that can drop, delay, reorder, and partition messages deterministically.
- Storage sits behind an interface (`internal/vfs`). Tests simulate crashes and
  torn writes with the in-memory filesystem.

## Layout

```
cmd/kvserver/        node binary
cmd/kvctl/           CLI client
internal/vfs/        filesystem interface: OS implementation + crash-simulating in-memory FS
internal/wal/        append-only record log (length + CRC32C framing, torn-tail recovery); Raft-agnostic
internal/raft/       consensus core; defines the Transport and Storage interfaces it consumes
internal/transport/  in-memory deterministic transport for tests; real network transport
internal/kv/         state machine and client dedup table (client ID + sequence number)
scripts/             local cluster and chaos scripts
```

Create packages only when the phase that needs them begins.

## Design conventions

- The consumer defines the interface (e.g. `raft.Transport`), so implementations
  import `raft`, not the other way round.
- The Raft core is tick-driven (`Tick()`), not wall-clock driven, so tests are deterministic.
- Persist `currentTerm`, `votedFor`, and log entries before replying to any RPC
  (Raft paper, Figure 2).
- The WAL is append-only. `Append` returns only after fsync. `Open` fsyncs the
  parent directory and the file itself, because bytes read back after a process
  kill may exist only in the page cache.
- A failed fsync is fatal: the WAL refuses further writes and the node must restart
  and recover. Never retry fsync (on Linux a failed fsync can drop dirty pages).
- Recovery may truncate only a torn final record, which was never acknowledged.
  Corruption followed by valid data returns `ErrCorrupt` and the file is left untouched.

## Testing

- Tests are deterministic: seeded RNGs (log the seed on failure), no `time.Sleep`,
  no reliance on real timing.
- WAL tests run against both `vfs.Mem` and real files in `t.TempDir()`.
- Prefer property-style tests over many seeds for crash and partition scenarios.

## Workflow

- Work in phases, one at a time: 0 scaffold, 1 storage, 2 leader election,
  3 log replication, 4 KV + client API, 5 snapshots/compaction, 6 real cluster + chaos.
- At the start of a phase, explain its key invariants and failure cases, citing
  the Raft paper (Ongaro and Ousterhout) sections.
- At the end of a phase, stop and summarize what was built, the design decisions
  and why, what the tests cover, and open questions. Wait for approval before
  starting the next phase.
- Call out subtle points in the spec explicitly (e.g. why a leader may not commit
  entries from earlier terms by counting replicas, §5.4.2) instead of quietly
  handling them.

## Commits

- Before every commit: `gofmt -l .` is empty, `go vet ./...` and `go test -race ./...`
  pass. Never commit failing tests.
- Small, focused commits. Subject line only, no body.
- No `Co-Authored-By` trailer and no "Generated with Claude Code" line.

## Commands

`make build | test | race | vet | fmtcheck | staticcheck | lint | fuzz | clean`.
`staticcheck` runs a pinned version through `go run`, which keeps it out of go.mod.
