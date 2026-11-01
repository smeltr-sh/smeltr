#!/usr/bin/env bash
# A demo fleet from nothing, and every major GUI page captured in light and dark. README.md has the setup.
#
#   ./run.sh              # services, traffic, a fresh fleet and its history, a settle, the captures: ~25 min
#   ./run.sh capture      # the captures again, against the fleet already up
#   ./run.sh demo         # the website demo's recorded answers (demo.mjs), staging a fleet first if none is up
#   ./run.sh stop         # stop the fleet and the traffic; `just down` takes Kafka and Elasticsearch
#
#   PACE=0 SETTLE_MIN=2 ./run.sh   # quick: the history lands at once and the charts are short
set -euo pipefail
cd "$(dirname "$0")"
REPO=$(cd ../.. && pwd)
SETTLE_MIN=${SETTLE_MIN:-8}
export OUT=${OUT:-$PWD/shots}

case "${1:-}" in
stop)
    python3 stage.py stop
    exit 0
    ;;
capture | demo | "") ;;
*)
    echo "usage: $0 [capture|demo|stop]" >&2
    exit 2
    ;;
esac

python3 -c 'import PIL' 2>/dev/null || {
    cat >&2 <<SETUP
Python packages missing. Run the one-time setup from $PWD/README.md:
  pip install -r $PWD/requirements.txt
  npm --prefix $PWD ci
  npx --prefix $PWD playwright install chromium
SETUP
    exit 1
}
[ -d node_modules/playwright ] || npm ci
# The traffic, the samples, the lookup tables (bundle.py) and the recording's fingerprint.
(cd "$REPO" && cargo build --release --example demo-traffic)

if [ "${1:-}" = "" ] || { [ "${1:-}" = demo ] && ! curl -sk -o /dev/null https://127.0.0.1:8443/; }; then
    [ -x "${CARGO_TARGET_DIR:-$REPO/target}/release/smeltr" ] || { echo "no release binary: just build" >&2; exit 1; }
    docker compose -f "$REPO/tests/live/compose.live.yaml" up -d --wait kafka elasticsearch elasticsearch-2
    python3 stage.py traffic
    python3 -u stage.py
    echo "settling for $SETTLE_MIN min, so the charts have history"
    sleep $((SETTLE_MIN * 60))
fi

[ -f state/sessions.json ] || { echo "no fleet staged here: run $0 without \`capture\` first" >&2; exit 1; }
if [ "${1:-}" = demo ]; then
    node demo.mjs "$REPO/gui-frontend/src/demo/recorded.json"
    # What the generators made, so a test says when the recording no longer shows it.
    "${CARGO_TARGET_DIR:-$REPO/target}/release/examples/demo-traffic" fingerprint \
        >"$REPO/gui-frontend/src/demo/recorded.fingerprint"
    exit 0
fi
mkdir -p "$OUT"
python3 contact_sheet.py --clean "$OUT"
node shoot.mjs shots.json
# Issues, and a Status that needs attention, need something wrong: one worker frozen until the leader says so.
python3 incident.py
# Each step example on the website, as the GUI draws its step.
python3 examples.py
python3 contact_sheet.py "$OUT" "$OUT/contact-sheet.png"
echo "screenshots in $OUT; the fleet is still up, and \`$0 stop\` ends it"
