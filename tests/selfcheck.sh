#!/bin/bash
# selfcheck.sh -- runs rows 1-12 of Details 1 section 5.4 and row 13 of
# Details 2 section 4 unattended, on 127.0.0.1.
#
#   ./tests/selfcheck.sh [port]        (default 5000)
#
# Run it from the repository root. It deletes jobs.log and out/ first.
# Logs go to tests/logs/.

cd "$(dirname "$0")/.." || exit 1
PORT=${1:-5000}
C="./client 127.0.0.1 $PORT"
LOGS=tests/logs
mkdir -p "$LOGS"

PASS=0
FAIL=0
ok()   { echo "  PASS  row $1: $2"; PASS=$((PASS + 1)); }
bad()  { echo "  FAIL  row $1: $2"; FAIL=$((FAIL + 1)); }
check() { # check <row> <text> <condition...>
    local row=$1 text=$2; shift 2
    if "$@"; then ok "$row" "$text"; else bad "$row" "$text"; fi
}
first() { head -n 1; }
fdcount() { ls "/proc/$1/fd" 2>/dev/null | wc -l; }

cleanup() {
    for p in $WPID $SPID $S2PID; do kill -TERM "$p" 2>/dev/null; done
    [ -n "$NCPID" ] && kill "$NCPID" 2>/dev/null
}
trap cleanup EXIT

pkill -xf "sleep 100" 2>/dev/null
pkill -xf "sleep 30" 2>/dev/null
rm -rf jobs.log out

# ---- row 1 -------------------------------------------------------------
make >"$LOGS/make.log" 2>&1
./server "$PORT" >"$LOGS/server.log" 2>&1 &
SPID=$!
sleep 0.5
./worker 127.0.0.1 "$PORT" 1 >"$LOGS/worker.log" 2>&1 &
WPID=$!
sleep 0.5
check 1 "three executables, two start lines" \
    test -x server -a -x worker -a -x client \
    -a "$(head -n 1 "$LOGS/server.log")" = "server: listening on port $PORT" \
    -a "$(head -n 1 "$LOGS/worker.log")" = "worker: connected to 127.0.0.1:$PORT, K=1"

# ---- row 2 -------------------------------------------------------------
ID=$($C submit "echo hello" 10 | first)
sleep 2
ST=$($C status "$ID" | first)
OUT=$($C fetch "$ID")
check 2 "echo hello -> '$ST' / '$OUT'" test "$ST" = "$ID DONE 0" -a "$OUT" = "hello"

# ---- row 3 -------------------------------------------------------------
A=$($C submit "exit 3" 10 | first)
B=$($C submit "ls /nonexistent" 10 | first)
sleep 2
SA=$($C status "$A" | first)
SB=$($C status "$B" | first)
$C fetch "$A" >/dev/null
FB=$($C fetch "$B")
check 3 "exit 3 / ls -> '$SA' / '$SB'" \
    test "$SA" = "$A DONE 3" -a "$SB" = "$B DONE 2" -a -n "$(echo "$FB" | grep 'No such file')"

# ---- row 4 -------------------------------------------------------------
ID=$($C submit "sleep 100" 2 | first)
sleep 4
ST=$($C status "$ID" | first)
LEFT=$(pgrep -xf "sleep 100")
check 4 "timeout -> '$ST', pgrep '${LEFT}'" test "$ST" = "$ID KILLED TIMEOUT" -a -z "$LEFT"

# ---- row 5 -------------------------------------------------------------
ID=$($C submit "sleep 30" 60 | first)
sleep 1
S1=$($C status "$ID" | first)
CA=$($C cancel "$ID" | first)
sleep 0.5
S2=$($C status "$ID" | first)
LEFT=$(pgrep -xf "sleep 30")
check 5 "cancel -> '$S1' / '$CA' / '$S2', pgrep '${LEFT}'" \
    test "$S1" = "$ID RUNNING" -a "$CA" = "$ID CANCELLED" -a "$S2" = "$ID CANCELLED" -a -z "$LEFT"

# ---- row 6 -------------------------------------------------------------
I1=$($C submit "sleep 3" 10 | first)
I2=$($C submit "sleep 3" 10 | first)
I3=$($C submit "sleep 3" 10 | first)
MAX=0
ALL=no
for _ in $(seq 1 24); do
    N=$(pgrep -xfc "sleep 3")
    [ "$N" -gt "$MAX" ] && MAX=$N
    if [ "$($C status "$I1" | first)" = "$I1 DONE 0" ] &&
       [ "$($C status "$I2" | first)" = "$I2 DONE 0" ] &&
       [ "$($C status "$I3" | first)" = "$I3 DONE 0" ]; then
        ALL=yes
        break
    fi
    sleep 0.5
done
check 6 "K=1: max $MAX running at once, all done: $ALL" test "$MAX" -le 1 -a "$ALL" = yes

# ---- row 7 -------------------------------------------------------------
rm -f "$LOGS"/c7.*
PIDS=
for i in $(seq 1 8); do
    ( $C submit "sleep 2" 10 | first >"$LOGS/c7.$i" ) &
    PIDS="$PIDS $!"
done
for p in $PIDS; do wait "$p"; done
IDS=$(cat "$LOGS"/c7.* 2>/dev/null | sort -u | grep -c .)
DONE=0
for _ in $(seq 1 23); do
    DONE=0
    for f in "$LOGS"/c7.*; do
        id=$(cat "$f")
        [ -n "$id" ] && [ "$($C status "$id" | first)" = "$id DONE 0" ] && DONE=$((DONE + 1))
    done
    [ "$DONE" -eq 8 ] && break
    sleep 1
done
check 7 "8 clients: $IDS different ids, $DONE DONE 0" test "$IDS" -eq 8 -a "$DONE" -eq 8

# ---- row 8 -------------------------------------------------------------
sleep 100000 | nc 127.0.0.1 "$PORT" >/dev/null 2>&1 &
NCPID=$!
sleep 0.3
ID=$(timeout 2 $C submit "echo x" 10 | first)
check 8 "silent nc does not block a submit -> '$ID'" test -n "$ID"
kill "$NCPID" 2>/dev/null
pkill -xf "sleep 100000" 2>/dev/null
NCPID=

# ---- row 9 -------------------------------------------------------------
OUT=$($C status T12-99999 | first)
CODE=$($C status T12-99999 >/dev/null; echo $?)
check 9 "unknown id -> '$OUT', exit $CODE" test "$OUT" = "T12-99999 UNKNOWN" -a "$CODE" -eq 2

# ---- row 10 ------------------------------------------------------------
sleep 1
Z=$(ps -o pid,ppid,stat,comm --ppid "$WPID" | awk 'NR > 1 && $3 ~ /Z/')
check 10 "no zombie children of the worker" test -z "$Z"

# ---- row 11 ------------------------------------------------------------
SF1=$(fdcount "$SPID"); WF1=$(fdcount "$WPID")
LAST=
for _ in $(seq 1 100); do LAST=$($C submit "echo hi" 10 | first); done
for _ in $(seq 1 60); do
    [ "$($C status "$LAST" | first)" = "$LAST DONE 0" ] && break
    sleep 0.5
done
sleep 0.5
SF2=$(fdcount "$SPID"); WF2=$(fdcount "$WPID")
check 11 "descriptors server $SF1 -> $SF2, worker $WF1 -> $WF2" \
    test "$SF1" -eq "$SF2" -a "$WF1" -eq "$WF2"

# ---- row 12 ------------------------------------------------------------
ID=$($C submit "sleep 30" 60 | first)
sleep 1
kill -TERM "$WPID"
sleep 2
ST=$($C status "$ID" | first)
LEFT=$(pgrep -xf "sleep 30")
WGONE=yes; kill -0 "$WPID" 2>/dev/null && WGONE=no
kill -TERM "$SPID"
./server "$PORT" >"$LOGS/server2.log" 2>&1 &
S2PID=$!
sleep 2
SGONE=yes; kill -0 "$SPID" 2>/dev/null && SGONE=no
INUSE=$(grep -c "Address already in use" "$LOGS/server2.log")
S2UP=no
kill -0 "$S2PID" 2>/dev/null &&
    [ "$(head -n 1 "$LOGS/server2.log")" = "server: listening on port $PORT" ] && S2UP=yes
check 12 "SIGTERM -> '$ST', pgrep '${LEFT}', worker gone $WGONE, server gone $SGONE, new server up $S2UP" \
    test "$ST" = "$ID KILLED SIGNAL 9" -a -z "$LEFT" -a "$WGONE" = yes -a "$SGONE" = yes \
    -a "$INUSE" -eq 0 -a "$S2UP" = yes
WPID=; SPID=
kill -TERM "$S2PID" 2>/dev/null
wait "$S2PID" 2>/dev/null
S2PID=

# ---- row 13 (Details 2): kill -9 the server, nothing accepted is lost ----
rm -f jobs.log
./server "$PORT" >"$LOGS/server13a.log" 2>&1 &
SPID=$!
sleep 0.5
./worker 127.0.0.1 "$PORT" 1 >"$LOGS/worker13a.log" 2>&1 &
WPID=$!
sleep 0.5
JA=$($C submit "echo a" 10 | first)
sleep 2
JB=$($C submit "sleep 4" 20 | first)
JC=$($C submit "echo c" 10 | first)
sleep 1
kill -9 "$SPID"
kill -TERM "$WPID"
wait "$SPID" "$WPID" 2>/dev/null
cp jobs.log "$LOGS/jobs13-before-restart.log"
./server "$PORT" >"$LOGS/server13b.log" 2>&1 &
SPID=$!
sleep 0.5
./worker 127.0.0.1 "$PORT" 1 >"$LOGS/worker13b.log" 2>&1 &
WPID=$!
sleep 12
SA=$($C status "$JA" | first)
SB=$($C status "$JB" | first)
SC=$($C status "$JC" | first)
FA=$($C fetch "$JA")
FC=$($C fetch "$JC")
JD=$($C submit "echo d" 10 | first)
check 13 "after kill -9: '$SA' / '$SB' / '$SC', fetch '$FA' '$FC', new id $JD" \
    test "$SA" = "$JA DONE 0" -a "$SB" = "$JB DONE 0" -a "$SC" = "$JC DONE 0" \
    -a "$FA" = "a" -a "$FC" = "c" -a -n "$JD" \
    -a "$JD" != "$JA" -a "$JD" != "$JB" -a "$JD" != "$JC"
cp jobs.log "$LOGS/jobs13-after.log"

echo
echo "$PASS passed, $FAIL failed (logs in $LOGS/)"
[ "$FAIL" -eq 0 ]
