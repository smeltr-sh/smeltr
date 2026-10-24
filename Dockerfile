# syntax=docker/dockerfile:1
# The stages split so that nothing which compiled the binary ships with it.

# ---- frontend: gui-frontend/dist, embedded into the binary at compile time (build.rs) -------------------------
FROM node:22-bookworm-slim AS frontend
WORKDIR /src/gui-frontend
COPY gui-frontend/package.json gui-frontend/package-lock.json ./
RUN npm ci
COPY gui-frontend/ ./
# Expression fields color code with the website's tokenizer and suggest from its language reference; the palette
# and controls are the website's too.
COPY website/src/lib/highlight/ /src/website/src/lib/highlight/
COPY website/src/lib/foundry/ /src/website/src/lib/foundry/
COPY website/src/lib/reference.json website/src/lib/code-language.json website/src/lib/signatures.json /src/website/src/lib/
RUN npm run build

# ---- builder: the `smeltr` binary ------------------------------------------------------------------------------
# The same base as the runtime stage, so the binary's glibc floor cannot land above the glibc it runs on. Build
# against anything newer than RHEL 9's 2.34 and the image builds clean, then dies at startup.
#
# Every native dependency is vendored here. AWS-LC's FIPS module refuses to configure without cmake, Go and
# perl. clang-devel is zstd-sys's bindgen. krb5 and Cyrus SASL want diffutils. build.rs reads the version with
# git. Rust ships no RPM, so rustup fetches it -- the one step that needs the internet.
FROM registry.access.redhat.com/ubi9/ubi:9.8 AS builder
RUN dnf install -y \
    gcc gcc-c++ make cmake pkgconf-pkg-config clang-devel golang \
    perl perl-FindBin perl-IPC-Cmd perl-Data-Dumper \
    libcurl-devel git diffutils \
    && dnf clean all && rm -rf /var/cache/dnf
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
# `cargo auditable` writes the crate graph into the binary, so Trivy and the SBOM list its crates, not only ubi-micro's
# packages. The record is a loadable section, which `strip` keeps. It names crates, so a vendored C library shows as
# its sys crate's version, and advisories reach it only through RUSTSEC; the weekly check lists those libraries.
RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo install cargo-auditable --version 0.7.7 --locked
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ src/
COPY tools/citations/ tools/citations/
COPY tools/check-scope/ tools/check-scope/
COPY .git/ .git/
COPY --from=frontend /src/gui-frontend/dist gui-frontend/dist
# The registry and `target` are cache mounts, so a source change recompiles smeltr alone: the vendored C --
# AWS-LC, librdkafka, krb5, SASL, OpenSSL, SQLite, curl -- turns over only when Cargo.lock does. A cache mount is
# not part of the image, so the binary is copied out of it here, and the stages below take it from /smeltr.
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/target \
    cargo auditable build --locked --profile dist --bin smeltr \
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
COPY compose.yaml /compose.yaml
COPY THIRD-PARTY-NOTICES.md /THIRD-PARTY-NOTICES.md
COPY LICENSE /LICENSE
COPY UBI-EULA.txt /UBI-EULA.txt

# ---- runtime ------------------------------------------------------------------------------------------------
# UBI9 micro carries no package manager, so a full UBI9 installs into a copy of it, and the image starts from
# that copy alone. The binary links nothing but glibc, so git is the whole surface: it serves the leader's config
# repository through `git http-backend`, and libcurl carries its push to a host, which is HTTPS only
# (src/leader/remote.rs). git-core goes in without its package's requirements, which bring ssh, a pager, PAM and
# util-linux that nothing here runs; libcurl and expat are what its binaries link. ca-certificates verifies the TLS
# at the far end. A worker fetches in-process through gix and spawns no git.
FROM registry.access.redhat.com/ubi9/ubi-micro:9.8 AS micro
FROM registry.access.redhat.com/ubi9/ubi:9.8 AS rootfs
COPY --from=micro / /rootfs
# The builder's own useradd writes the account, so the image needs no shadow-utils, whose gpasswd, newgrp and chage
# are the only setuid files it would carry. Without them nothing in the image can gain privilege.
# A fixed number, not a name: a kubelet enforcing runAsNonRoot cannot read this image's passwd file. A pod
# mounting a volume here needs `fsGroup: 1000`.
RUN dnf install -y --installroot=/rootfs --releasever=9 --setopt=install_weak_deps=False --nodocs \
      ca-certificates libcurl expat \
    && dnf download --destdir=/tmp --releasever=9 git-core \
    && rpm --root /rootfs -i --nodeps --excludedocs /tmp/git-core-*.rpm \
    && dnf upgrade -y --installroot=/rootfs --releasever=9 --nodocs \
    && dnf --installroot=/rootfs clean all \
    && groupadd --root /rootfs --gid 1000 smeltr \
    && useradd --root /rootfs --uid 1000 --gid 1000 --create-home --home-dir /var/lib/smeltr \
         --shell /sbin/nologin smeltr \
    && rpm --root /rootfs -e --nodeps shadow-utils \
    && test -z "$(find /rootfs -xdev -perm /6000 -type f)"

# DISA's RHEL 9 STIG, as far as it reaches into an image (`just stig` reports the rest). Each line is the fix the
# STIG profile in ComplianceAsCode gives, with its values. The crypto policy is generated here, by the same
# crypto-policies the image carries, since the image has no python to run update-crypto-policies. What is left
# fails because it belongs to the host or the runtime: partitions, /dev/shm's mount options, resolv.conf.
RUN dnf upgrade -y crypto-policies \
    && test "$(rpm -q crypto-policies)" = "$(rpm --root /rootfs -q crypto-policies)" \
    && printf 'cipher@SSH=AES-256-GCM AES-256-CTR AES-128-GCM AES-128-CTR\nmac@SSH=HMAC-SHA2-256 HMAC-SHA2-512\n' \
      > /etc/crypto-policies/policies/modules/STIG.pmod \
    && update-crypto-policies --set FIPS:STIG \
    && cp -a /etc/crypto-policies/. /rootfs/etc/crypto-policies/ \
    && sed -i -E 's/^([^#]*\bumask)[[:space:]]+[[:digit:]]+/\1 077/g' /rootfs/etc/bashrc \
    && echo 'umask 077' >> /rootfs/etc/profile \
    && find /rootfs/root -maxdepth 1 -name '.*' -type f -exec chmod u-s,g-wxs,o= {} +

FROM scratch
COPY --from=rootfs /rootfs /
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
COPY --from=builder /smeltr /usr/local/bin/smeltr
# The UBI EULA rides in every image built on UBI: its section 2 asks for that.
COPY LICENSE THIRD-PARTY-NOTICES.md UBI-EULA.txt /usr/share/doc/smeltr/

# The image starts from scratch, so it carries only these labels. They name smeltr, not Red Hat, so the published
# image does not read as theirs or as endorsed by them (UBI-EULA.txt, section 2).
ARG VERSION=""
LABEL org.opencontainers.image.title="smeltr" \
      org.opencontainers.image.description="Smeltr leader and worker" \
      org.opencontainers.image.vendor="John Pavlik" \
      org.opencontainers.image.licenses="BUSL-1.1 AND LicenseRef-scancode-rh-ubi-eula-2019" \
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
