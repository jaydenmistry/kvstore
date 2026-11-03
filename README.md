# kvstore

[![ci](https://github.com/jaydenmistry/kvstore/actions/workflows/ci.yml/badge.svg)](https://github.com/jaydenmistry/kvstore/actions/workflows/ci.yml)

A distributed key-value store in Go, with the Raft consensus algorithm implemented from scratch. It replicates every write to a majority of nodes, keeps working when a minority of them crash or get cut off, and every read is linearizable, so a client never sees stale data even right after a leader change.

There are no consensus libraries and no third-party dependencies. Everything is built on the Go standard library, and the parts that touch the network or the disk sit behind interfaces so the whole cluster can be tested deterministically.

## What it does

- **Put, Get, and Delete** over a small HTTP API, with a Go client and a CLI (`kvctl`).
- **Leader election and log replication** following the Raft paper, plus PreVote and CheckQuorum from Diego Ongaro's thesis.
- **Linearizable reads** through ReadIndex, without going through the log and without relying on clocks.
- **Exactly-once writes.** Clients register a session, and the cluster detects retried writes, so a write that times out and gets retried is applied once.
- **Crash recovery.** Each node keeps a checksummed write-ahead log and fsyncs before it acknowledges anything, and a node that restarts catches up from its peers.
- **Snapshots and log compaction,** including sending a snapshot to a follower that's fallen too far behind.

## Architecture

```
   kvctl / Go client
          │  HTTP: PUT/GET/DELETE /kv/{key}, POST /session
          ▼
 ┌─────────────────────── one node (kvserver) ───────────────────────┐
 │  HTTP API ──► loop goroutine (the only one touching Raft state)    │
 │                 │                                                  │
 │                 ▼                                                  │
 │   kv.Replica: requests ⇄ Raft, ReadIndex batching, sessions        │
 │       │                         │                                  │
 │       ▼                         ▼                                  │
 │   StateMachine              raft.Raft (tick-driven, no I/O)        │
 │   (map + dedup table)        │                 │                   │
 │                              ▼                 ▼                   │
 │                     Storage interface   Transport interface        │
 │                     WAL + snapshots     TCP (gob)                  │
 └────────────────────────────────────────────────────────────────────┘
```

| Package | What it is |
|---|---|
| `internal/raft` | The consensus core. It's a single-threaded state machine driven by `Tick()` and `Step()`, and it never reads a clock or does I/O itself. |
| `internal/wal`, `internal/vfs` | The append-only log, plus a filesystem interface with a crash-simulating in-memory version for tests. |
| `internal/kv` | The replicated state machine, client sessions, and the layer that connects requests to Raft. |
| `internal/transport` | The TCP transport, plus a simulated network for tests that can drop, delay, duplicate, reorder, and partition messages. |
| `internal/server`, `internal/client` | One node's HTTP API and wall-clock loop, and the Go client. |
| `internal/linearize` | A linearizability checker used by the tests. |
| `cmd/kvserver`, `cmd/kvctl`, `cmd/kvchaos`, `cmd/kvbench` | The node binary, the CLI, a chaos tester that runs real processes, and a benchmark. |

## Design decisions

These are the decisions I'd want to talk through in an interview. Each one has the reasoning next to it, and the code comments go into more detail with references to the Raft paper.

**The Raft core is deterministic.** It advances on `Tick()` calls instead of timers, gets its randomness from a seeded source, and sends messages and writes to disk through interfaces. So a test can run a five-node cluster in one goroutine on a simulated network, and any failure can be replayed exactly from its seed.

**Nothing is acknowledged before it's on disk.** The current term, vote, and log entries are fsynced before any reply that depends on them (Figure 2 of the paper). Each WAL append is one CRC32C-checksummed frame, so a batch survives a crash completely or not at all. Recovery only discards a damaged final frame, since that's the only write that can't have been acknowledged.

**A failed fsync stops the node.** On Linux, a failed fsync can drop the dirty pages and mark them clean, so retrying it can "succeed" without the data being on disk. The node exits with a dedicated status (3) instead, and the systemd unit in `deploy/` is set up so it won't restart automatically.

**Leaders only commit entries from their own term by counting replicas.** This is the subtle case in Figure 8 of the paper. An entry from an earlier term can be on a majority and still get overwritten, so a new leader appends a no-op entry and commits through that.

**Reads use ReadIndex, not leases.** A leader serves a read once it has committed an entry in its current term and a majority has answered a heartbeat sent after the read arrived. A lease would be faster, but it depends on bounded clock drift, and I didn't want correctness to depend on clocks. Reads that arrive together share one heartbeat round, so the cost is paid once per batch.

**Sessions are registered through the log.** Duplicate detection needs to remember each client's last write, but that table can't grow forever. Following §6.3 of the thesis, a client registers a session, and its ID is the index of the log entry that registered it. So when the cluster sees an ID it doesn't know, it knows the session was evicted, and it refuses the write instead of risking applying it twice. The cluster keeps a fixed number of sessions and evicts the least recently used, measured by log position rather than time. That way every replica evicts the same ones, and the limit is written into each registration entry so replicas with different configs still agree.

**Snapshots are crash-safe across two files.** Compaction writes the snapshot file and then rewrites the WAL, each with a temp file, fsync, rename, and directory fsync. If the node crashes between the two steps, startup finishes the job using the same rule a follower uses when it installs a snapshot from the leader (Figure 13).

**PreVote and CheckQuorum are on, but correctness never depends on them.** CheckQuorum makes a leader that's cut off from the majority step down, and PreVote stops a node that's been partitioned from bumping its term and disrupting the cluster when it comes back. They only help with availability, so the tests run with them both on and off. One subtle part is that the "a leader exists, so refuse pre-votes" rule is only safe with CheckQuorum on. Without it, a one-way network partition can leave the cluster stuck with no usable leader, and there's a test for exactly that case.

## How it's tested

Most of the effort in this project went into testing, since a distributed system that looks right in a demo can still lose data under the wrong timing.

- **Deterministic cluster simulation.** The Raft and KV tests run whole clusters on a simulated network with message loss, duplication, reordering, and random partitions, crashes, and restarts across hundreds of seeds. Raft's safety properties (one leader per term, matching logs, committed entries never lost) are checked after every single message and tick, not just at the end.
- **Simulated power loss.** The in-memory filesystem can crash and decide which unsynced writes and renames survive, including torn writes. Storage tests fail every filesystem operation in turn and then crash, and the recovered state has to be either the old state or the new one, nothing in between.
- **Linearizability checking.** Simulated clients retry like real ones, and the full history of every operation is checked with a Wing and Gong search (with Lowe's memoization) to confirm there's a valid order for every result a client saw. The KV chaos test checks about 30,000 operations across 100 seeds.
- **Real processes under chaos.** `kvchaos` starts real `kvserver` processes and routes the Raft traffic between them through proxies it controls. That lets it kill, pause, and restart nodes and partition the network, including one-way cuts and cutting off the current leader, without needing root. At the end it checks the whole history for linearizability and confirms the nodes converge. A 60 second run usually checks tens of thousands of operations.
- **Planted bugs.** Every safety test is checked by putting the bug it's meant to catch into a scratch copy of the code and confirming the test fails, since a test that passes on broken code isn't testing anything. For example, `kvchaos` catches a build that skips the leadership check on reads, and the simulation catches a state machine that forgets its sessions after a snapshot.
- **Fuzzing** for the WAL, command, and snapshot decoders.

Every commit has to pass `make check`, which runs gofmt, go vet, staticcheck, and the full test suite with the race detector. CI runs the same checks, plus a 30 second `kvchaos` run on every push and five 2 minute runs every night. If a chaos run fails, CI keeps its history and node logs so the failure can be looked at.

## Performance

I measured a three-node cluster with `scripts/bench.sh`, which starts a fresh local cluster and runs `kvbench` against it. Each client sends one request at a time, back to back, with 100 byte values spread over 1,000 keys. Everything ran on one machine (a 4-core Ryzen 3 PRO 5350GE with an NVMe SSD, on ext4), so the three nodes share one disk and there's no real network delay.

| Operation | Clients | Throughput | p50 latency | p99 latency |
|---|---:|---:|---:|---:|
| Put | 1 | 920/s | 1.04 ms | 2.24 ms |
| Put | 8 | 1,156/s | 6.68 ms | 12.21 ms |
| Put | 64 | 1,178/s | 53.41 ms | 88.25 ms |
| Get | 1 | 5,706/s | 0.17 ms | 0.24 ms |
| Get | 8 | 23,718/s | 0.32 ms | 0.69 ms |
| Get | 64 | 53,688/s | 1.07 ms | 3.54 ms |

Writes top out at about 1,200 per second. A single write takes about 1 ms, which is two fsyncs in a row (the leader's, then a follower's, each about 0.43 ms on this disk) plus the HTTP round trip. More clients don't raise the throughput, they just wait longer, because the leader writes and fsyncs every entry on its own. The obvious next step is group commit, specifically writing all the entries that arrive during one fsync as a single WAL append. The WAL already supports that, since one `Append` can hold many records.

Reads scale much better, up to about 54,000 per second with 64 clients, since they don't touch the disk. Each read still needs a heartbeat round to a majority (ReadIndex), but reads that arrive together share one round, so the more clients there are, the more reads each round serves.

Running the benchmark also turned up a bug in the Go client. Every `Client` shared Go's default HTTP connection pool, which only keeps two idle connections per host, so concurrent clients were opening a new TCP connection for almost every request. Giving each `Client` its own pool raised read throughput with 32 clients from about 29,000 to 45,000 per second.

## Running it

You need Go 1.27 on Linux.

```sh
make build                       # builds bin/kvserver, bin/kvctl, bin/kvchaos, bin/kvbench
scripts/cluster.sh start         # starts a local three-node cluster
bin/kvctl put greeting hello
bin/kvctl get greeting
bin/kvctl status                 # each node's role, term, and commit index

scripts/cluster.sh kill 2        # crash a node, then keep using the cluster
scripts/cluster.sh start 2       # it recovers from disk and catches up
scripts/cluster.sh clean         # stop everything and delete the data
```

```sh
make test     # unit and simulation tests
make check    # the full pre-commit gate
make chaos    # a 60 second real-process chaos run
make bench    # throughput and latency of a fresh local cluster
```

For a real deployment, `deploy/` has a systemd unit and an example cluster config. Every node gets the same JSON file listing the members, and is started with its own ID.

## Scope

Cluster membership changes are out of scope, so the set of nodes is fixed at startup. A few other things are known limitations. Snapshots are sent in one message instead of in chunks, taking a snapshot pauses the node while it serializes the state, and there's no TLS or authentication. Power loss is tested with the simulated filesystem, not on real hardware.

## How I built this

I built this with Claude Code, an AI coding agent. I set up the project so that I set the direction and signed off on each step, and so the AI's output gets checked by things that don't depend on trusting it.

I wrote the spec and the rules up front, specifically that Raft had to be implemented from scratch with no consensus libraries, that it should stick to the standard library, and that storage and networking had to sit behind interfaces so everything could be tested deterministically. Those rules live in `CLAUDE.md`, and the agent works within them. I broke the project into phases (storage, leader election, replication, the KV layer, snapshots, and running a real cluster). Before each phase, I had the agent explain the invariants and failure cases with references to the Raft paper. At the end of each one, it had to stop and summarize what it built, the design decisions and why, what the tests cover, and the open questions. I reviewed that and decided what came next before moving on. A lot of the design discussion happened there, like how fsync failures should be handled and whether to add CheckQuorum and PreVote.

The agent wrote most of the code and tests. Since that's the case, I didn't want correctness to rest on the code looking right, so the process has checks that catch mistakes on their own. Every safety test has to fail when its bug is planted, every commit has to pass gofmt, vet, staticcheck, and the race-enabled test suite, and the whole system is checked end to end for linearizability under real crashes and partitions. Those checks caught real problems along the way, including a test that only passed by luck and a chaos tool that at first couldn't detect stale reads at all.

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf) (the Raft paper).
- Diego Ongaro, [Consensus: Bridging Theory and Practice](https://github.com/ongardie/dissertation) (the thesis, for PreVote, CheckQuorum, ReadIndex, and client sessions).
- Gavin Lowe, Testing for Linearizability (the memoized checker).
- Herlihy and Wing, Linearizability: A Correctness Condition for Concurrent Objects.
