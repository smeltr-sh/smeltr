# syntax=docker/dockerfile:1
# The stages split so that nothing which compiled the binary ships with it.

# ---- frontends: what the binary embeds (tools/frontends.json), built as `just` builds them --------------------
FROM node:26-bookworm-slim AS frontends
WORKDIR /src
COPY gui-frontend/package.json gui-frontend/package-lock.json gui-frontend/
COPY website/package.json website/package-lock.json website/
RUN npm --prefix gui-frontend ci && npm --prefix website ci
COPY gui-frontend/ gui-frontend/
COPY website/ website/
COPY tools/frontends.json tools/build-if-stale.mjs tools/
# The site's license page shows it.
COPY COMMERCIAL-LICENSE.md ./
RUN node tools/build-if-stale.mjs

# ---- wolfi: the builder's and the image's filesystems, from Wolfi's packages -------------------------------------
# Wolfi is a glibc distribution built for containers, whose packages follow upstream within days, so an image
# carries what its sources have fixed. Alpine's apk installs Wolfi's packages into roots of their own
# (tools/release/wolfi-root.sh), and nothing of Alpine is copied out. The binary's glibc floor is the builder's
# glibc: the image's root resolves after it, and Wolfi's glibc only moves forward and keeps its old symbol
# versions, so the image never carries an older one.
FROM alpine:3.23@sha256:469c9ea410b9215eaee7976ba6c0639039b444be8e5d638f7abbada189971eb2 AS wolfi
ADD --checksum=sha256:f0031424cf46f7db780ce63a45f0fd6aa6f85f601e6bb3b7a91fe3d4d5b7d2cc \
    https://packages.wolfi.dev/os/wolfi-signing.rsa.pub /etc/apk/keys/wolfi-signing.rsa.pub
COPY tools/release/wolfi-root.sh /wolfi-root.sh
RUN sh /wolfi-root.sh builder /builder && sh /wolfi-root.sh image /rootfs

# ---- builder: the `smeltr` binary ------------------------------------------------------------------------------
# Rust comes from rustup, as the checks get it, so the image builds with the toolchain they test.
FROM scratch AS builder
COPY --from=wolfi /builder /
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
# `cargo auditable` writes the crate graph into the binary, so Trivy and the SBOM list its crates, not only the image's
# packages. The record is a loadable section, which `strip` keeps. It names crates, so a vendored C library shows as
# its sys crate's version, and advisories reach it only through RUSTSEC; the weekly check lists those libraries.
RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo install cargo-auditable --version 0.7.7 --locked
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ src/
COPY tools/citations/ tools/citations/
COPY tools/check-scope/ tools/check-scope/
COPY tools/frontends.json tools/
COPY gui-frontend/not-built/ gui-frontend/not-built/
COPY .git/ .git/
COPY --from=frontends /src/gui-frontend/dist gui-frontend/dist
COPY --from=frontends /src/website/leader-docs website/leader-docs
# The registry and `target` are cache mounts, so a source change recompiles smeltr alone: the vendored C --
# AWS-LC, librdkafka, krb5, SASL, SQLite, curl -- turns over only when Cargo.lock does. A cache mount is
# not part of the image, so the binary is copied out of it here, and the stages below take it from /smeltr.
# The vendored C is written for C17, and Wolfi's GCC defaults to C23. krb5 1.19, which krb5-src vendors, also
# promotes warnings to errors, and Wolfi's glibc raises one of them; WARN_CFLAGS stops the promotion.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/target \
    CFLAGS=-std=gnu17 WARN_CFLAGS=-Wall cargo auditable build --locked --profile dist --bin smeltr \
    && strip target/dist/smeltr \
    && cp target/dist/smeltr /smeltr

# ---- export: the handoff folder -------------------------------------------------------------------------------
# Writes a folder rather than an image, for a site that builds the last step itself:
#
#     docker build --target export --output type=local,dest=handoff .
#
# It sits ahead of the runtime stage so a plain build still ends at the image.
FROM scratch AS export
COPY --from=builder /smeltr /smeltr
COPY tools/release/Dockerfile.handoff /Dockerfile
COPY tools/release/wolfi-root.sh /wolfi-root.sh
COPY compose.yaml /compose.yaml
COPY THIRD-PARTY-NOTICES.md /THIRD-PARTY-NOTICES.md
COPY LICENSE /LICENSE

# ---- runtime ------------------------------------------------------------------------------------------------
# The image is the Wolfi root alone, with no shell and no package manager. `Dockerfile.handoff` builds the same.
FROM scratch
COPY --from=wolfi /rootfs /
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt
COPY --from=builder /smeltr /usr/local/bin/smeltr
COPY LICENSE THIRD-PARTY-NOTICES.md /usr/share/doc/smeltr/

ARG VERSION=""
LABEL org.opencontainers.image.title="smeltr" \
      org.opencontainers.image.description="Smeltr leader and worker" \
      org.opencontainers.image.vendor="John Pavlik" \
      org.opencontainers.image.licenses="BUSL-1.1" \
      org.opencontainers.image.version="${VERSION}" \
      name="smeltr" \
      summary="Smeltr leader and worker" \
      description="Smeltr leader and worker" \
      io.k8s.display-name="smeltr" \
      io.k8s.description="Smeltr leader and worker" \
      vendor="John Pavlik" \
      maintainer="John Pavlik" \
      version="${VERSION}"

USER 1000
# The state paths are relative, so they land here: the leader's data and its sealing key, and the worker's
# state. Mount this to keep them across a replacement.
WORKDIR /var/lib/smeltr

# No port here. The default is 443, which a non-root process cannot bind, so whoever runs this picks one.
# `docker run` wants `-e SMELTR_GUI_PORT=8443 -p 443:8443`, and a deployment sets
# `leader.http.bind = "0.0.0.0"` besides, since the bind defaults to loopback.
ENTRYPOINT ["/usr/local/bin/smeltr"]
