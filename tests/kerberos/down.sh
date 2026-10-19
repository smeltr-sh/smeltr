#!/usr/bin/env bash
# Stop what `up.sh` started and discard its state, so a rerun is clean.
set -uo pipefail
# By pidfile, both of them: a pattern would also match a broker or a KDC
# this harness did not start -- `just live-up`'s, or another realm's.
[ -f /tmp/krb/broker.pid ] && kill "$(cat /tmp/krb/broker.pid)" >/dev/null 2>&1
[ -f /tmp/krb/kdc.pid ] && kill "$(cat /tmp/krb/kdc.pid)" >/dev/null 2>&1
sleep 1
rm -rf /tmp/krb /tmp/kafka /tmp/kafka-logs /tmp/kafka-broker.log
echo "stopped; /tmp/kafka.tgz kept for the next run"
