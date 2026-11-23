#!/bin/bash
# repro_row12.sh -- the failure of report section 5: the OLD row-12 check
# passes although the new server never gets the port. Run from the repo root.
cd "$(dirname "$0")/.." || exit 1
PORT=5000; T=$(mktemp -d)
nc -l 127.0.0.1 $PORT >/dev/null 2>&1 &      # an old process still holds the port
NC=$!; sleep 0.3
( cd "$T" && exec "$OLDPWD/server" $PORT ) >"$T/server2.log" 2>&1 &
S2=$!; sleep 2
INUSE=$(grep -c "Address already in use" "$T/server2.log")
S2UP=no; kill -0 $S2 2>/dev/null && S2UP=yes
echo "old check after 2 s:  alive=$S2UP  'Address already in use' lines=$INUSE  -> $([ $S2UP = yes ] && [ $INUSE -eq 0 ] && echo PASS || echo FAIL)"
echo "server2.log at 2 s: [$(cat "$T/server2.log")]"
NEW=FAIL; [ $S2UP = yes ] && [ "$(head -n1 "$T/server2.log")" = "server: listening on port $PORT" ] && NEW=PASS
echo "new check after 2 s: first line must be 'server: listening on port $PORT' -> $NEW"
sleep 2
echo "server2.log at 4 s: [$(cat "$T/server2.log")]"
kill $NC $S2 2>/dev/null; rm -rf "$T"
