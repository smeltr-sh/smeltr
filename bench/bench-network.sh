#!/bin/sh
# Puts the link bench/network/network.env describes between this host and
# containers, or takes it away. Each target is a compose file, for every
# container it runs, or one container:
#
#   bench/bench-network.sh on tests/live/compose.live.yaml
#   bench/bench-network.sh off tests/live/compose.live.yaml
#   bench/bench-network.sh on 3f2a9c...
#   bench/bench-network.sh on inside:3f2a9c...
#   bench/bench-network.sh image
#
# `image` prints the image that shapes and relays, building it first when it
# must.
# Shaped at both ends of each container's veth, so each direction crosses it
# once. `inside:` shapes a relay's own end alone (bench/network/shape.sh). Refuses rather than leaving a link unshaped: a benchmark that ran on a
# perfect network would read as one that ran on this one.
set -eu
state=$1
shift
root=$(cd "$(dirname "$0")/.." && pwd)
# Tagged by its Dockerfile, so it builds once per change and not on every run.
image=smeltr-bench-network:$(sha256sum "$root/bench/network/Dockerfile" | cut -c1-12)
docker image inspect "$image" >/dev/null 2>&1 || docker build -q -t "$image" "$root/bench/network" >/dev/null
if [ "$state" = image ]; then
    echo "$image"
    exit 0
fi
containers=
for target in "$@"; do
    case $target in
    *.yaml) containers="$containers $(docker compose -f "$target" ps -q)" ;;
    *) containers="$containers $target" ;;
    esac
done
[ -n "$(echo $containers)" ] || { echo "no container to shape" >&2; exit 1; }
for target in $containers; do
    c=${target#inside:}
    how=
    [ "$c" != "$target" ] && how=inside
    [ "$state" = off ] && how=off
    pid=$(docker inspect -f '{{.State.Pid}}' "$c")
    if ! docker run --rm --privileged --net host --pid host \
        --env-file "$root/bench/network/network.env" \
        -v "$root/bench/network/shape.sh:/shape.sh:ro" \
        "$image" /shape.sh "$pid" "$how"; then
        echo "could not shape $(docker inspect -f '{{.Name}}' "$c"): this kernel needs sch_netem" >&2
        exit 1
    fi
done
