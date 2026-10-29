#!/usr/bin/env bash
# Builds kvserver and kvchaos, then runs kvchaos: a cluster of real kvserver
# processes on this machine, under crashes, pauses, and network partitions,
# with a linearizability check of every client operation at the end.
#
#   scripts/chaos.sh                          one 60 s run
#   scripts/chaos.sh -duration 5m -nodes 3    arguments go to kvchaos (see bin/kvchaos -h)
#   RUNS=10 scripts/chaos.sh                  ten runs, stopping at the first failure
#
# A failed run keeps its history, node logs, and data, and prints where.
set -euo pipefail

cd "$(dirname "$0")/.."
go build -o bin/ ./cmd/kvserver ./cmd/kvchaos
runs=${RUNS:-1}
for i in $(seq "$runs"); do
	if ((runs > 1)); then echo "=== run $i of $runs"; fi
	bin/kvchaos -kvserver bin/kvserver "$@"
done
