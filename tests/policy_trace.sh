#!/bin/bash
# policy_trace.sh -- produces the scheduling trace for the report.
#
#   ./tests/policy_trace.sh [port]
#
# Starts its own server (priority policy, aging 1 s per level so the effect
# shows within seconds) in a temporary directory, and ONE worker with K=1.
# Fills the queue with jobs of different priorities while the worker is busy
# and prints the server's dispatch lines: the order in which jobs ran.
#
# What it shows:
#   - higher priority goes first (9 before 5 before 0),
#   - equal priorities go in submission order,
#   - the priority-0 job "starver" is not starved: while it waits, aging
#     raises it one level per second, so after ~5 s it beats newer
#     priority-5 jobs that keep arriving.

cd "$(dirname "$0")/.." || exit 1
ROOT=$(pwd)
PORT=${1:-5100}
C="$ROOT/client 127.0.0.1 $PORT"
DIR=$(mktemp -d)               # its own jobs.log and out/
LOG="$DIR/server.log"

( cd "$DIR" && POLICY=priority AGING=1 exec "$ROOT/server" "$PORT" ) >"$LOG" 2>&1 &
SPID=$!
sleep 0.3
"$ROOT/worker" 127.0.0.1 "$PORT" 1 >/dev/null 2>&1 &
WPID=$!
sleep 0.3

$C submit "sleep 4" 10 0 >/dev/null          # keeps the worker busy
sleep 0.2
$C submit "echo starver"  10 0 >/dev/null    # lowest priority, submitted first
$C submit "echo low"      10 0 >/dev/null
$C submit "echo mid-a"    10 5 >/dev/null
$C submit "echo high"     10 9 >/dev/null
$C submit "echo mid-b"    10 5 >/dev/null

# a steady stream of priority-5 work arriving later
for i in 1 2 3 4; do
    sleep 2.5
    $C submit "sleep 2; echo late-$i" 10 5 >/dev/null
done

sleep 12
kill -TERM "$WPID" "$SPID" 2>/dev/null
wait 2>/dev/null

echo "--- jobs ---"
grep 'accepted' "$LOG" | sed 's/^server: //'
echo "--- dispatch order ---"
grep 'dispatch' "$LOG" | sed 's/^server: //'
rm -rf "$DIR"
