#!/bin/sh
# Shapes one container's link from both ends: its eth0, and the veth that is
# its peer on this host. Runs inside the image this directory builds, with the
# host's network and processes. `shape.sh PID` puts network.env's link on it;
# `shape.sh PID off` takes it away. `shape.sh PID inside` shapes its eth0
# alone: a relay's traffic crosses its link twice, in and out, so shaping one
# way of each crossing gives it the link once each way.
set -eu
pid=$1
inside() { nsenter -t "$pid" -n "$@"; }
# `eth0@if12`: 12 is the peer's index on this host.
peer=$(inside ip -o link show eth0 | sed -n 's/^[0-9]*: eth0@if\([0-9]*\):.*/\1/p')
outside=$(ip -o link | sed -n "s/^$peer: \([^:@]*\)[:@].*/\1/p")
[ -n "$outside" ] || { echo "no host end for pid $pid's eth0" >&2; exit 1; }
if [ "${2:-}" = off ]; then
    inside tc qdisc del dev eth0 root 2>/dev/null || true
    tc qdisc del dev "$outside" root 2>/dev/null || true
    exit 0
fi
# netem loses and delays whole packets, and an offloaded segment is up to
# 64 KiB of them: without this, one loss drops forty packets at once.
inside ethtool -K eth0 tso off gso off gro off >/dev/null 2>&1 || true
ethtool -K "$outside" tso off gso off gro off >/dev/null 2>&1 || true
# The queue holds a full round trip at the rate and more: netem's default of
# 1000 packets would itself drop at 1 Gbit/s and 40 ms.
link="delay $NETEM_DELAY $NETEM_JITTER distribution normal loss $NETEM_LOSS rate $NETEM_RATE limit 100000"
# shellcheck disable=SC2086
inside tc qdisc replace dev eth0 root netem $link
[ "${2:-}" = inside ] && exit 0
# shellcheck disable=SC2086
tc qdisc replace dev "$outside" root netem $link
