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
cmd/kvchaos/         real-process chaos test: link proxies, faults, linearizability check
cmd/kvbench/         throughput and latency of a running cluster
internal/vfs/        filesystem interface: OS implementation + crash-simulating in-memory FS
internal/wal/        append-only log; one CRC32C frame per Append (crash-atomic batch); Raft-agnostic
internal/raft/       consensus core; defines the Transport and Storage interfaces it consumes
internal/transport/  in-memory deterministic transport for tests; TCP transport (gob)
internal/kv/         state machine, client dedup table, Replica (requests <-> Raft); no I/O
internal/server/     one node: wall-clock loop owning the Replica, TCP peers, HTTP client API
internal/client/     Go client: leader redirects, retries with the same sequence number
internal/config/     cluster configuration file (JSON), shared by kvserver and kvctl
internal/linearize/  linearizability checker (Wing-Gong with Lowe's memoization), per key
deploy/              systemd unit, example cluster file
scripts/             local cluster and chaos scripts
```

Create packages only when the phase that needs them begins.

## Design conventions

- The consumer defines the interface (e.g. `raft.Transport`), so implementations
  import `raft`, not the other way round.
- The Raft core is tick-driven (`Tick()`), not wall-clock driven, so tests are deterministic.
- Persist `currentTerm`, `votedFor`, and log entries before replying to any RPC
  (Raft paper, Figure 2).
- The WAL is append-only. Each `Append` is one checksummed frame: all of its
  records survive a crash or none do. `Append` returns only after fsync. `Open` fsyncs the
  parent directory and the file itself, because bytes read back after a process
  kill may exist only in the page cache.
- A failed fsync is fatal: the WAL refuses further writes and the node process must
  exit. Never retry fsync (on Linux a failed fsync can drop dirty pages and mark
  them clean, so even a restarted process may read data that is not on disk).
- Reads are linearizable through ReadIndex (§8): the leader needs a committed
  entry from its current term, then a majority must answer a heartbeat round
  started after the read. Never serve reads from local state, and never from a
  follower.
- CheckQuorum (thesis §6.2): a leader that has not heard from a majority
  (any AppendEntries response in its term) within an ElectionTick window
  steps down, staying in its term. It is a liveness aid, not a lease: reads
  still need the ReadIndex round. kvserver always enables it; tests run with
  it on and off, so safety is never shown only with it on.
- PreVote (thesis §9.6): before raising its term, a node asks whether a
  majority would vote for it. MsgPreVote and a granted MsgPreVoteResp carry
  the proposed term and must not be adopted (the one exception to §5.1); a
  refusal carries the voter's term. Refusing because a leader was heard
  recently (stickiness) applies only with CheckQuorum: without it, a one-way
  partition can deadlock the cluster (see `inLeaderLease`). Same on/off rule
  as CheckQuorum.
- Group commit: the server loop takes the client requests waiting behind the
  one it received, up to `kv.MaxBatch`, and submits them together; their
  writes become one `Propose`, so one storage write and one fsync. There is
  no timer: batches form only from requests that queued while the loop was
  busy, so a lone request is never delayed.
- Pipelining: the leader probes a follower (one message per heartbeat or
  response, `next` held still) until a success shows where its log ends,
  then replicates (sends entries as they are appended and moves `next` past
  them at once). A rejection goes back to probing. A rejection counts only
  if it is above `match` and below `next`, and while probing only if it
  answers the latest probe. Safety rests on the follower's consistency
  check, never on `next`.
- Writes carry (client ID, seq). The state machine keeps each client's latest
  seq and its result, so a retried write is applied once and returns the
  original result. A client has one request outstanding at a time.
- Client sessions (thesis §6.3): a client registers through the log (OpRegister)
  and its ID is that entry's index, so an unknown ID always means an evicted
  session. A write without a session is refused (ErrSessionExpired, HTTP 410,
  outcome unknown), never applied. At most N sessions are kept, evicting the
  least recently used by log index; N travels in each register entry, and
  last-used indexes are in snapshots, so every replica evicts the same ones.
- Snapshots (§7) hold only applied state, and must include the dedup sessions.
  `DiskStorage.SaveSnapshot` writes the snapshot file, then rewrites the WAL
  (each step: temp file, fsync, rename, fsync dir). A crash between the steps
  is finished by `OpenDiskStorage` using the same keep-or-drop rule as
  InstallSnapshot (Figure 13). A follower ignores a snapshot at or below its
  commit index. Snapshots travel whole in one message (no chunking).
- `kv.Replica` and `raft.Raft` are single-threaded; in `kvserver` only the loop
  goroutine touches them.
- Recovery may discard only a damaged final frame, which was never acknowledged
  (pages of one write may reach disk in any order, so the damage can be a hole).
  Damage followed by a valid frame returns `ErrCorrupt` and the file is left untouched.

## Decisions

- **fsync failure is fail-stop.** On any WAL write or fsync error, kvserver logs
  what happened and exits with a dedicated exit code, and must not restart itself
  (ship a systemd unit with `RestartPreventExitStatus=`). Before restarting the
  node, drop the host page cache (`echo 3 > /proc/sys/vm/drop_caches`) or reboot
  the host; restarting an LXC container does not clear the host's cache.
  kvserver's exit status for this is 3.
  `O_DIRECT` was considered and deferred: it needs 4 KiB-aligned writes (padding
  every append, or rewriting blocks that hold acknowledged data), and behaves
  differently on tmpfs and ZFS.

## Testing

- Tests are deterministic: seeded RNGs (log the seed on failure), no `time.Sleep`,
  no reliance on real timing.
- WAL tests run against both `vfs.Mem` and real files in `t.TempDir()`.
- `vfs.Mem` loses or keeps unsynced name changes (create, rename, remove) per
  directory on a crash, so recovery code is tested against both outcomes.
- Storage crash tests fail every filesystem operation in turn (fault hook),
  then `Crash`, and require the old state or the new one, nothing in between.
- Prefer property-style tests over many seeds for crash and partition scenarios.
- Raft cluster tests (harness in `internal/raft/harness_test.go`) run every node on the
  simulated `transport.Network` from one goroutine and check safety invariants
  after every `Step` and `Tick`, not just at the end.
- KV tests (`internal/kv`) run simulated clients that retry like real ones and
  check the whole history with a linearizability checker, per key.
- Tests over real sockets (TCP transport, server end to end) cannot avoid real
  time. They poll with generous deadlines and never sleep for a fixed time.
- `make chaos` (cmd/kvchaos) runs real kvserver processes with Raft traffic
  through per-direction proxies, kills, pauses, and partitions them (including
  partitioning the leader), then checks the history for linearizability and
  that nodes converge. It is not deterministic and not part of `make check`.
  Half its clients use `client.Spread`, or stale leaders are never queried.
  Plant bugs by building a mutated kvserver and passing it with `-kvserver`.
  CI runs it too: 30 s on every push, five 2-minute runs nightly.
- When adding a safety test, plant the bug it targets in a scratch copy and confirm
  the test fails. A test that passes on broken code is not testing anything.

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

- Before every commit, `make check` (gofmt, vet, staticcheck, race tests) must
  pass. Never commit failing tests. Do not pipe its output into another command
  when gating a commit: the pipe hides its exit status.
- Small, focused commits. Subject line only, no body.
- No `Co-Authored-By` trailer and no "Generated with Claude Code" line.

## Commands

`make build | test | race | vet | fmtcheck | staticcheck | lint | check | fuzz | chaos | bench | clean`.
`scripts/cluster.sh start|stop|status|kill N|pause N|...` runs a local three-node cluster.
`scripts/bench.sh` runs kvbench against a fresh one on the same ports.
`staticcheck` runs a pinned version through `go run`, which keeps it out of go.mod.
