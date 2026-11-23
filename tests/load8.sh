#!/bin/bash
# load8.sh -- eight clients submit in the same second, then wait for all.
#
#   ./tests/load8.sh [host] [port] [command] [timeout]
#
# Needs a running server and worker(s).

cd "$(dirname "$0")/.." || exit 1
HOST=${1:-127.0.0.1}
PORT=${2:-5000}
CMD=${3:-sleep 2}
TIMEOUT=${4:-10}
C="./client $HOST $PORT"
TMP=$(mktemp -d)

for i in $(seq 1 8); do
    ( $C submit "$CMD" "$TIMEOUT" | head -n 1 >"$TMP/$i" ) &
done
wait
echo "ids: $(cat "$TMP"/* | tr '\n' ' ')"

for _ in $(seq 1 60); do
    DONE=0
    for f in "$TMP"/*; do
        st=$($C status "$(cat "$f")" | head -n 1)
        case "$st" in *QUEUED|*RUNNING) ;; *) DONE=$((DONE + 1)) ;; esac
    done
    echo "finished: $DONE / 8"
    [ "$DONE" -eq 8 ] && break
    sleep 1
done

for f in "$TMP"/*; do $C status "$(cat "$f")" | head -n 1; done
rm -rf "$TMP"
