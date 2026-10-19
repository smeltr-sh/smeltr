# tc, ip and ethtool, run with the host's network and processes to shape a
# container's link from both ends; socat, to relay a sender on this host through
# a shaped container to a receiver on this host.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends iproute2 ethtool socat && rm -rf /var/lib/apt/lists/*
