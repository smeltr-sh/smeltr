#!/usr/bin/env bash
# Stop what `up.sh` started and discard its state, so a rerun is clean.
set -uo pipefail
# By pidfile: a pattern would also match a broker this harness did not
# start -- `just live-up`'s, or the Kerberized one.
[ -f /tmp/smeltr-tls/broker.pid ] && kill "$(cat /tmp/smeltr-tls/broker.pid)" >/dev/null 2>&1
sleep 1
rm -rf /tmp/smeltr-tls /tmp/kafka-tls /tmp/kafka-tls-logs /tmp/kafka-tls-broker.log
echo "stopped; /tmp/kafka.tgz kept for the next run"
