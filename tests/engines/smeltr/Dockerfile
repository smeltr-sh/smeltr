# The distribution the `optimized` binary is built on. The binary is mounted, not copied, so a rebuild needs no new image.
FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
