#!/usr/bin/env bash
# Starts a fresh local three-node cluster (scripts/cluster.sh, in its own
# directory), runs kvbench against it, then stops it and deletes its data.
#
#   scripts/bench.sh                              puts and gets with 1, 8, and 32 clients
#   scripts/bench.sh -ops put -clients 1,64       arguments go to kvbench (see bin/kvbench -h)
#
# The cluster uses the same ports as scripts/cluster.sh, so stop that one
# first. All three nodes share this machine's disk, which every write fsyncs
# on a majority of nodes before it is acknowledged.
set -euo pipefail

cd "$(dirname "$0")/.."
export KVSTORE_DIR=${KVSTORE_DIR:-.bench}
if [[ -e $KVSTORE_DIR ]]; then
	echo "$KVSTORE_DIR already exists; remove it or set KVSTORE_DIR" >&2
	exit 1
fi
go build -o bin/ ./cmd/kvbench
trap 'scripts/cluster.sh clean >/dev/null' EXIT
scripts/cluster.sh start >/dev/null
bin/kvbench -config "$KVSTORE_DIR/cluster.json" "$@"
