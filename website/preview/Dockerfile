# syntax=docker/dockerfile:1
# The demo's preview service, `smeltr-demo`, with the demo's lookup tables. Built from the repository root
# (`image_build_context` in wrangler.jsonc), so the root's .dockerignore decides what it sees.

# The root Dockerfile's builder, for the same reasons: its base sets the glibc floor, and smeltr's vendored native
# dependencies need these tools.
FROM registry.access.redhat.com/ubi9/ubi:9.8 AS builder
RUN dnf install -y \
    gcc gcc-c++ make cmake pkgconf-pkg-config clang-devel golang \
    perl perl-FindBin perl-IPC-Cmd perl-Data-Dumper \
    libcurl-devel git diffutils \
    && dnf clean all && rm -rf /var/cache/dnf
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
COPY Cargo.toml Cargo.lock build.rs ./
COPY src/ src/
COPY tools/citations/ tools/citations/
COPY tools/check-scope/ tools/check-scope/
COPY .git/ .git/
# The service serves no GUI; the page that asks it is the website's.
ENV SMELTR_NO_GUI=1
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release --bin smeltr-demo \
    && strip target/release/smeltr-demo \
    && cp target/release/smeltr-demo /smeltr-demo

# It links nothing past glibc and libgcc, which ubi-micro carries.
FROM registry.access.redhat.com/ubi9/ubi-micro:9.8
COPY --from=builder /smeltr-demo /usr/local/bin/smeltr-demo
# The tables the recorded fleet's pipelines look up; the service reads them from `$SMELTR_DEMO_BUNDLE/lookups`.
COPY gui-frontend/src/demo/lookups/ /demo/lookups/
USER 1000
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/smeltr-demo"]
