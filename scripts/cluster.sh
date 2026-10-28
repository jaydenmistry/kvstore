#!/usr/bin/env bash
# Runs a three-node kvstore cluster on this machine, for trying it out.
#
#   scripts/cluster.sh start          build, then start every stopped node
#   scripts/cluster.sh stop           stop every node (SIGTERM)
#   scripts/cluster.sh status         show each node's Raft state
#   scripts/cluster.sh start N        start node N
#   scripts/cluster.sh stop N         stop node N cleanly
#   scripts/cluster.sh kill N         kill node N (SIGKILL), as in a crash
#   scripts/cluster.sh pause N        freeze node N (SIGSTOP), as in a long GC or VM pause
#   scripts/cluster.sh resume N       unfreeze node N (SIGCONT)
#   scripts/cluster.sh clean          stop every node and delete all data
#
# Node N listens for Raft traffic on 127.0.0.1:700N and for clients on
# 127.0.0.1:800N, which is where kvctl looks by default:
#
#   bin/kvctl put greeting hello && bin/kvctl get greeting
#
# Data, logs, and the cluster file go in $KVSTORE_DIR (default .cluster).
set -euo pipefail

cd "$(dirname "$0")/.."
dir=${KVSTORE_DIR:-.cluster}
ids=(1 2 3)

usage() {
	sed -n '2,/^set /{/^set /d;s/^# \{0,1\}//;p}' "$0" >&2
	exit 2
}

pidfile() { echo "$dir/node$1.pid"; }

# pid prints node $1's PID if it is running. A stale PID file, left by a
# crash, may name a PID since reused by another process, so the command line
# is checked too.
pid() {
	local f p
	f=$(pidfile "$1")
	[[ -f $f ]] || return 1
	p=$(<"$f")
	[[ -r /proc/$p/cmdline ]] && tr '\0' ' ' <"/proc/$p/cmdline" | grep -q "kvserver .*-id $1 " || return 1
	echo "$p"
}

build() {
	mkdir -p "$dir"
	go build -o bin/ ./cmd/kvserver ./cmd/kvctl
	cat >"$dir/cluster.json" <<JSON
{
  "nodes": [
    {"id": 1, "raft": "127.0.0.1:7001", "api": "127.0.0.1:8001"},
    {"id": 2, "raft": "127.0.0.1:7002", "api": "127.0.0.1:8002"},
    {"id": 3, "raft": "127.0.0.1:7003", "api": "127.0.0.1:8003"}
  ],
  "snapshot_every": 1000
}
JSON
}

start_node() {
	if p=$(pid "$1"); then
		echo "node $1 already running (pid $p)"
		return
	fi
	bin/kvserver -config "$dir/cluster.json" -id "$1" -data "$dir/node$1" >>"$dir/node$1.log" 2>&1 &
	echo $! >"$(pidfile "$1")"
	echo "node $1 started (pid $!, log $dir/node$1.log)"
}

stop_node() {
	local p
	if ! p=$(pid "$1"); then
		echo "node $1 not running"
		return
	fi
	kill -CONT "$p" # a paused node cannot handle SIGTERM
	kill -TERM "$p"
	while kill -0 "$p" 2>/dev/null; do sleep 0.1; done
	rm -f "$(pidfile "$1")"
	echo "node $1 stopped"
}

signal_node() {
	local p
	p=$(pid "$2") || { echo "node $2 not running" >&2; exit 1; }
	kill "-$1" "$p"
	echo "node $2: sent SIG$1"
}

wait_for_leader() {
	for _ in $(seq 100); do
		if bin/kvctl -config "$dir/cluster.json" -timeout 1s status 2>/dev/null | grep -q ' leader '; then
			bin/kvctl -config "$dir/cluster.json" status
			return
		fi
		sleep 0.1
	done
	echo "no leader after 10 seconds; see $dir/node*.log" >&2
	exit 1
}

node_arg() {
	[[ $1 =~ ^[123]$ ]] || { echo "unknown node $1" >&2; exit 2; }
}

cmd=${1:-}
case $cmd,$# in
start,1)
	build
	for id in "${ids[@]}"; do start_node "$id"; done
	wait_for_leader
	;;
start,2) node_arg "$2"; build; start_node "$2" ;;
stop,1) for id in "${ids[@]}"; do stop_node "$id"; done ;;
stop,2) node_arg "$2"; stop_node "$2" ;;
kill,2) node_arg "$2"; signal_node KILL "$2"; rm -f "$(pidfile "$2")" ;;
pause,2) node_arg "$2"; signal_node STOP "$2" ;;
resume,2) node_arg "$2"; signal_node CONT "$2" ;;
status,1) bin/kvctl -config "$dir/cluster.json" status ;;
clean,1)
	for id in "${ids[@]}"; do stop_node "$id"; done
	rm -rf "$dir"
	;;
*) usage ;;
esac
