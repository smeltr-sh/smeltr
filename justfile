# Common commands. `just` alone lists them; every recipe is one or two
# plain commands, so this file also reads as the list to copy.
#
# `cargo build` does not run the bundler (build.rs says why), so building
# only Rust after a `.svelte` edit embeds the previous frontend.

# On Windows `just` needs a shell and does not fall back to a native one.
# `cmd.exe /c` propagates exit codes unambiguously; `powershell -Command`
# is version-dependent about it
[windows]
set shell := ["cmd.exe", "/c"]
#
# So a recipe sets an environment variable with `[env]`, not `VAR=1 cmd`,
# which works only in a POSIX shell. `[env]` reaches a recipe's commands
# from just 1.56.0.

[private]
default:
    @just --list

# Each tool a recipe calls is checked before it runs, so a missing one stops the recipe with how to install it rather
# than with the error of whatever called it. cmd.exe's `echo` takes none of `& < > | ( )`, so a hint holds none.
[private]
[unix]
need tool how:
    @command -v {{tool}} >/dev/null || { echo "{{tool}} is not installed. {{how}}" >&2; exit 1; }

[private]
[windows]
need tool how:
    @where {{tool}} >nul 2>nul || (echo {{tool}} is not installed. {{how}} 1>&2 & exit /b 1)

# What every Rust build needs: aws-lc-fips-sys builds its FIPS module with CMake and Go, and a C compiler; on Windows
# with Ninja too. CMake finds MSVC by itself.
[private]
[unix]
need-rust: (need "cargo" "Install Rust from https://rustup.rs") (need "cmake" "Install CMake with your package manager.") (need "go" "Install Go from https://go.dev/dl") (need "cc" "Install gcc or clang with your package manager.") (need "c++" "Install g++ or clang with your package manager.")

[private]
[windows]
need-rust: (need "cargo" "Install Rust from https://rustup.rs") (need "cmake" "Run: winget install Kitware.CMake") (need "go" "Run: winget install GoLang.Go") (need "ninja" "Run: winget install Ninja-build.Ninja")

[private]
need-node: (need "node" node_hint) (need "npm" node_hint)
    @node -e "if (+process.versions.node.split('.')[0] < 22) { console.error('Node ' + process.versions.node + ' is too old. ' + process.argv[1]); process.exit(1) }" "{{node_hint}}"

node_hint := if os_family() == "windows" { "Install Node 22 or later: winget install OpenJS.NodeJS.LTS" } else { "Install Node 22 or later from https://nodejs.org" }

[private]
need-nextest: (need "cargo-nextest" "Run: cargo install cargo-nextest --locked")

[private]
need-docker: (need "docker" "Install Docker from https://docs.docker.com/get-docker")

# Frontend, then an optimized binary embedding it. Quick to rebuild; `dist` builds what you ship.
[group("dev")]
build: need-rust gui
    cargo build --release --bin smeltr

# What the GUI tests and screenshots run: the frontend, the demo site and unoptimized binaries embedding them. They
# share `target/debug` with the Rust tests, so the dependencies build once, and a GUI edit rebuilds in less than half
# of the time `build` takes.
[doc("Frontend and demo, then unoptimized binaries embedding them: what the GUI tests run.")]
[group("dev")]
build-dev: need-rust gui fips-dll
    npm --prefix gui-frontend run build:demo
    cargo build --bin smeltr --bin smeltr-demo

# Windows loads the AWS-LC FIPS module from a DLL beside each binary, the test binaries in `deps` included
# (`tools/fips-dll.ps1`), so what runs a debug binary copies it there first. The one crate builds it, and reuses the
# build the tests make.
[private]
[windows]
fips-dll: need-rust
    cargo build -p aws-lc-fips-sys
    powershell -NoProfile -ExecutionPolicy Bypass -File tools/fips-dll.ps1 "{{ env('CARGO_TARGET_DIR', 'target') }}/debug"

[private]
[unix]
fips-dll: need-rust

# The website demo: the GUI over the leader its page plays, in
# gui-frontend/dist-demo, and the preview service it asks.
[group("dev")]
demo: need-rust gui
    npm --prefix gui-frontend run build:demo
    cargo build --release --bin smeltr-demo

# Frontend only -- what to run when build.rs says dist is stale.
[group("dev")]
gui: deps-gui
    node gui-frontend/build-if-stale.mjs

[private]
deps-gui: need-node
    npm --prefix gui-frontend install

[private]
deps-website: need-node
    npm --prefix website install

# The version a binary carries, as `build.rs` stamps it: read out of the binary, so an archive's name
# cannot disagree with what is inside it.
version := '[0-9]{4}\.[0-9]+\.[0-9]+\+[A-Za-z0-9-]+\.[0-9a-f]{12}(-dirty-[0-9a-f]{12})?'

# BuildKit only: the build caches cargo's registry and `target` across runs, which is what keeps a rebuild
# off the vendored C.
#
# ./handoff and the tarball to send. The version is read out of the binary rather than from git, so the
# name cannot disagree with what the binary reports -- and a binary carrying no branch and sha fails here
# rather than reaching a site that cannot tell what it is running. POSIX shell.
#
# THIRD-PARTY-NOTICES.md ships as committed. `just notices` after a dependency change, not here: it costs
# most of a minute and wants cargo-about installed.
#
# The export writes over ./handoff without clearing it, and the tarball takes all of it, so the folder goes first.
[private]
handoff: need-docker
    rm -rf handoff
    docker build --target export --output type=local,dest=handoff .
    v=$(grep -aoE '{{version}}' handoff/smeltr | head -1) \
      && : "${v:?no branch and sha in handoff/smeltr, so build.rs read no repository}" \
      && tar -czf "smeltr-$v.tar.gz" -C handoff . \
      && sha256sum "smeltr-$v.tar.gz" | tee "smeltr-$v.tar.gz.sha256"

# A release, from a commit with nothing uncommitted, so its version names a commit anyone can check out:
# the handoff tarball (binary, license, notices) as `smeltr-linux.tar.gz`, the image built from it as
# `smeltr-image.tar.gz`, and SHA256SUMS over both. Any branch builds one; the release workflow holds a published
# release to `main`. On Windows, `smeltr-windows.zip` of the binary, the license and the notices, and SHA256SUMS
# over it. The file names carry no version, so a link to the newest release's file never changes; the release
# they are attached to, the binary and the image's tags carry it. On a commit tagged `v1.2.0` the image is
# tagged `1.2.0`, the release that binary is; on any other, by its build version. A tag pushed runs both in
# `.github/workflows/release.yml`.
[doc("Release archives and the image, from a committed branch, and SHA256SUMS.")]
[unix]
dist: need-docker committed handoff
    v=$(grep -aoE '{{version}}' handoff/smeltr | head -1) \
      && r=$(git tag --points-at HEAD --list 'v[0-9]*') \
      && { test "$(echo "$r" | wc -w)" -le 1 || { echo "more than one v* tag on this commit: $r" >&2; exit 1; }; } \
      && n=${r#v} && n=${n:-$v} \
      && mv "smeltr-$v.tar.gz" smeltr-linux.tar.gz \
      && docker build --build-arg VERSION="$n" -t smeltr:latest -t "smeltr:$(echo "$n" | tr + -)" handoff \
      && docker save -o smeltr-image.tar smeltr:latest "smeltr:$(echo "$n" | tr + -)" \
      && gzip -f smeltr-image.tar \
      && sha256sum smeltr-linux.tar.gz smeltr-image.tar.gz | tee SHA256SUMS

# The RHEL 9 STIG, scanned against an image: `stig/report.html` and the rule-by-rule `stig/results.txt`. A
# report, not a gate. `dist` tags the image it builds `smeltr:latest`.
[unix]
stig image="smeltr:latest": need-docker
    docker build --build-arg IMAGE={{image}} --output type=local,dest=stig - < tools/stig.Dockerfile

# Known vulnerabilities in an image's OS packages and in the binary's crates, which `cargo auditable` records in it:
# `cves/report.txt`, and `cves/report.json` for tools. A report, not a gate.
[unix]
cves image="smeltr:latest": need-docker
    mkdir -p cves && docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD/cves:/out" \
      aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa \
      image --scanners vuln --format json --output /out/report.json {{image}}
    docker run --rm -v "$PWD/cves:/out" \
      aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa \
      convert --format table --output /out/report.txt /out/report.json

# An image's packages and the binary's crates as a CycloneDX SBOM: `sbom/sbom.cdx.json`. The release attests it on
# the pushed image.
[unix]
sbom image="smeltr:latest": need-docker
    mkdir -p sbom && docker run --rm -v /var/run/docker.sock:/var/run/docker.sock -v "$PWD/sbom:/out" \
      aquasec/trivy:0.75.0@sha256:af6acf9a6b85dfe389a1941505c0ce9efef52a4719635e1a962f022a3d855daa \
      image --format cyclonedx --output /out/sbom.cdx.json {{image}}

[doc("Release archives from a committed branch, and SHA256SUMS.")]
[windows]
dist: need-rust gui
    powershell -NoProfile -ExecutionPolicy Bypass -File tools/dist.ps1 "{{version}}"

[private]
[unix]
committed:
    changes=$(git status --porcelain) && test -z "$changes" \
      || { echo "dist builds a commit: commit or stash what is uncommitted" >&2; exit 1; }

# A checkout of a release tag, as CI makes one, is detached: it counts as `main` when the tag is on it.
[private]
[unix]
clean-main:
    changes=$(git status --porcelain) && test -z "$changes" \
      && { test "$(git branch --show-current)" = main \
        || { test -n "$(git tag --points-at HEAD --list 'v[0-9]*')" && git merge-base --is-ancestor HEAD origin/main; }; } \
      || { echo "a release builds from main, or a release tag on it, with nothing uncommitted" >&2; exit 1; }

[private]
[windows]
clean-main:
    powershell -NoProfile -Command "$changes = @(git status --porcelain); if ($LASTEXITCODE -ne 0) { exit 1 }; $branch = git branch --show-current; if ($LASTEXITCODE -ne 0 -or $branch -ne 'main' -or $changes.Count -gt 0) { [Console]::Error.WriteLine('a release builds from main with nothing uncommitted'); exit 1 }"

# The check before every push; nothing else runs one. `tools/check-scope` picks the checks the files changed since
# origin/main can break, down to the Rust tests of the modules they touch, and runs them side by side. It can miss a
# test: `check-all` runs everything, before a merge to main.
[doc("The check before every push, scoped to what changed since origin/main.")]
check: need-rust need-nextest
    cargo run -q -p check-scope

# Kerberos is not in it, since it needs a KDC: after a change to `Cargo.toml`, `Cargo.lock` or the Kerberos tests,
# run `just check-kerberos`. `just check-live` adds the live services. Windows is checked nowhere.
#
# The five share nothing but `stable`, so `[parallel]` runs them at once: 25s
# of work in 12s. Two costs. Their output interleaves -- run one by name
# (`just check-rust`) to read a failure on its own. And a failing one no
# longer stops the others, so a fmt error that used to abort in a second
# now waits out the slowest.
#
# The attribute needs just 1.42.0; an older one refuses it rather than
# quietly running in sequence.
#
# `--list` shows a recipe's last comment line, so `[doc]` supplies the
# summary rather than this paragraph's tail.
[doc("Every check but the fuzzers, whatever changed. The five run at once.")]
[parallel]
check-all: check-rust clippy check-frontend website deny

# What a change to documents alone can break.
[private]
check-docs: need-rust need-nextest
    cargo nextest run -p citations

[private]
fmt-check: need-rust
    cargo fmt --all --check

# The Rust tests but the fuzzers that `filter`, a nextest filter, picks: `check` names them. A change to a fuzzer
# alone picks none, and passes.
[private]
[env("SMELTR_NO_GUI", "1")]
check-tests filter: need-nextest stable fips-dll
    cargo nextest run --workspace -j {{test_threads}} --no-tests=pass -E "not test(/fuzz/) & ({{filter}})"

# One crate under `tools/`.
[private]
check-package package: need-nextest stable
    cargo nextest run -p {{package}}

# A release: its fixtures recorded and committed, and that commit tagged `v<release>`. Nothing leaves
# this machine: `git push --atomic origin main v<release>` starts the release workflow. Semver: a major release
# raises the upgrade floor or ships a breaking store step, a minor one adds features and additive steps
# only, a patch fixes and adds no step. A pre-release tag (`v1.2.0-rc1`), made by hand with `git tag`, runs
# the workflow as a trial: a draft pre-release and its own image tag, with nothing recorded.
[doc("Record a release, commit it and tag it.")]
cut-release release: (has-notes release) (record-release if release =~ '^[0-9]+\.[0-9]+\.[0-9]+$' { release } else { error('a release is named major.minor.patch, like 1.0.0') })
    git add tests/fixtures/releases
    git commit -m "Release {{release}}"
    git tag -a "v{{release}}" -m "Release {{release}}"

# A release is cut only once its notes are written and dated in `website/src/lib/releases.json`.
[private]
has-notes release: need-node
    node website/scripts/release-notes.js {{release}} > /dev/null

# Run from the commit being released, once. Every later build must start on
# what this one leaves and still read what it sends (`src/releases.rs`).
[private]
record-release $SMELTR_RECORD_RELEASE: need-rust need-nextest clean-main
    cargo nextest run --lib --run-ignored only -E "test(=releases::record_release)"

# The checks build with the newest stable, so they see its new lints.
# `check-rust`, `clippy` and `deny` wait on this rather than run beside it: rustup may
# be replacing the toolchain they call. Offline, the update fails and the
# installed stable builds.

# Bring stable Rust up to date.
# `check` runs it once before the checks it starts, which set `SMELTR_STABLE_UPDATED` so none runs it again.
[private]
stable: need-rust
    -{{ if env("SMELTR_STABLE_UPDATED", "") == "" { "rustup update stable --no-self-update" } else { "rustup --version" } }}

# nextest runs each test in a process of its own. It runs no doctests; the
# crate has none. Install it with `cargo install cargo-nextest --locked`.
#
# Twice as many at once as there are CPUs: many tests wait on a socket or a timer,
# so at one per CPU a quarter of the machine sits idle. just has no arithmetic, so each shell doubles it.
test_threads := if os_family() == "windows" { `set /a %NUMBER_OF_PROCESSORS% * 2` } else { `echo $(( $(getconf _NPROCESSORS_ONLN) * 2 ))` }

# The profiling suites `Cargo.toml` marks `test = false`: `measure` runs
# them, so the check builds none of them, but clippy still reads them.
profiling := "--test flush-cost --test record-cost --test logs-cost --test memory-budget-cost --test guard-cost --test tick-profile"

# Clippy builds into a `target/` of its own, inside the one every checkout shares, so it runs
# beside the test build rather than queue on that build's lock: a minute off `check-all`, for
# 1.1G of disk and one cold build per machine.
clippy_target := env("CARGO_TARGET_DIR", justfile_directory() / "target") / "clippy"

[private]
[env("SMELTR_NO_GUI", "1")]
[env("CARGO_TARGET_DIR", clippy_target)]
clippy: stable
    cargo clippy --workspace --all-targets -- -D warnings
    cargo clippy {{profiling}} -- -D warnings

# fmt and the Rust tests but the fuzzers, fastest-failing first. Then the language
# cases not built yet, reported but never failing the check. No test needs
# the GUI, and `check-frontend` builds it, so the binary embeds none.
[private]
[env("SMELTR_NO_GUI", "1")]
check-rust: need-nextest stable fips-dll
    cargo fmt --all --check
    cargo nextest run --workspace -j {{test_threads}} -E "not test(/fuzz/)"
    -cargo test --test integration language_tests:: -- --ignored

# Every offline fuzzer, by the `fuzz` its tests are named for. They run many
# seeds each and take a minute or more, so `check` leaves them here.
[doc("Every offline fuzzer.")]
[env("SMELTR_NO_GUI", "1")]
check-fuzz: need-nextest fips-dll && fuzz-gui fuzz-journey fuzz-pipeline
    cargo nextest run --workspace -j {{test_threads}} -E "test(/fuzz/)"

# The GUI fuzzer, `tests/gui/specs/gui-fuzz.spec.ts`, over more seeds than
# `check-gui` gives it: 64. `SMELTR_FUZZ_SEED=n` replays one.
[doc("The GUI fuzzer, over many seeds.")]
[env("SMELTR_FUZZ_SEEDS", "64")]
fuzz-gui: build-dev
    npm --prefix tests/gui install
    cd tests/gui && npx playwright test --project fuzz-coverage

# The journey fuzzer, `tests/gui/specs/journey-fuzz.spec.ts`, over more and
# longer journeys than `check-gui` gives it: 16 of 52 moves, enough to make
# every move. `SMELTR_FUZZ_SEED=n` replays one.
[doc("The admin journey fuzzer, over many seeds.")]
[env("SMELTR_FUZZ_SEEDS", "16")]
[env("SMELTR_JOURNEY_MOVES", "52")]
fuzz-journey: build-dev
    npm --prefix tests/gui install
    cd tests/gui && npx playwright test --project journey-coverage

# The pipeline journey fuzzer, `tests/gui/specs/pipeline-fuzz.spec.ts`, over
# more and longer journeys than `check-gui` gives it: 16 of 100 moves.
# `SMELTR_FUZZ_SEED=n` replays one.
[doc("The pipeline journey fuzzer, over many seeds.")]
[env("SMELTR_FUZZ_SEEDS", "16")]
[env("SMELTR_PIPELINE_MOVES", "100")]
fuzz-pipeline: build-dev
    npm --prefix tests/gui install
    cd tests/gui && npx playwright test --project pipeline-coverage

# The build goes to `target/gui-check`, not `dist`: `check-rust` runs alongside
# and embeds `dist`, which vite empties before it writes.
#
# `npm ci`, not `npm install`: it fails on a lockfile out of step with
# `package.json`, and it never writes one. It empties `node_modules` first, so
# a dev server running from it breaks.

# prettier, svelte-check/tsc, the production build and the frontend tests.
[private]
check-frontend: need-node
    npm --prefix gui-frontend ci
    npm --prefix gui-frontend run format:check
    npm --prefix gui-frontend run check
    npm --prefix gui-frontend run build -- --outDir ../target/gui-check --emptyOutDir
    npm --prefix gui-frontend test

# Needs cargo-deny, and the network for the advisory database. A new advisory
# fails this with no change here.

# Advisories, licenses, bans and sources, against `deny.toml`.
[private]
deny: (need "cargo-deny" "Run: cargo install cargo-deny --locked") stable
    cargo deny check

# uap-core's current user-agent rules, which `parseUserAgent` compiles in. POSIX shell.
[private]
uap-refresh:
    curl -fsSL https://raw.githubusercontent.com/ua-parser/uap-core/master/regexes.yaml -o src/ops/useragent/regexes.yaml

# The ECS and OCSF releases the Add schema dialog offers built in (`schema::standard`), as
# `just schemas-refresh 9.5.0 1.9.0`: ECS's fields.csv at that tag, and OCSF compiled and cut.
[private]
schemas-refresh ecs ocsf:
    tools/schema-standards/refresh {{ecs}} {{ocsf}}

# Fails where ECS or OCSF has a release newer than the one built in. Run weekly.
[private]
schemas-latest:
    tools/schema-standards/latest

# The reference page's type lines, written from the engine's function table.
[private]
[env("SMELTR_WRITE_SIGNATURES", "1")]
signatures: need-rust
    cargo test --test integration website_reference_tests::the_signatures_file_matches_the_engine

# The build prerenders every route, so a broken internal link fails here.
# `npm ci`, as `check-frontend` installs.

# The public site: prettier, svelte-check and the prerendering build.
[private]
website: need-node
    npm --prefix website ci
    npm --prefix website run format:check
    npm --prefix website run check
    npm --prefix website run build
    node website/scripts/docs-markdown.js --check

# The docs as Markdown the leader's AI agent reads, written from the website's build. Run it after a change to the docs.
docs-markdown: need-node
    npm --prefix website run build
    node website/scripts/docs-markdown.js --leader

# `--wait` blocks on tests/live/compose.live.yaml's healthchecks. The fixture topics
# need four partitions each; an auto-created topic gets one. These recipes
# assume a POSIX shell.
#
# Every one names its file. `compose.yaml` is a deployment, and `down -v`
# aimed at that would take an operator's volumes with it.

# Start Kafka, Elasticsearch, Splunk, the S3, GCS, Azure and Event Hubs emulators, an Iceberg catalog, SFTP and PostgreSQL for the live tests.
[private]
live-up: need-docker
    # What the universal forwarder reads, made before compose mounts it.
    mkdir -p target/s2s-forwarder
    docker compose -f tests/live/compose.live.yaml up -d --wait
    # Elasticsearch's two nodes would copy every index to both, which `just connector-speed` would then measure.
    curl -sf -XPUT http://localhost:9200/_index_template/smeltr-live-no-replicas -H 'content-type: application/json' -d '{"index_patterns":["*"],"priority":1,"template":{"settings":{"number_of_replicas":0}}}' -o /dev/null
    # The forwarder copies its app into etc only on a fresh etc volume, so a kept one would go on testing an older
    # fixture.
    docker compose -f tests/live/compose.live.yaml up -d --wait --force-recreate --renew-anon-volumes splunk-uf
    # A HEC token with indexer acknowledgement on, beside the container's own with it off. Refused as a duplicate
    # once it exists.
    -curl -sf -u admin:smeltr-live-1 http://localhost:8089/servicesNS/nobody/splunk_httpinput/data/inputs/http -d name=smeltr-ack -d token=00000000-0000-0000-0000-00000000a0c4 -d useACK=1 -d index=main -d indexes=main -o /dev/null
    # The Event Hubs emulator has no healthcheck of its own.
    timeout 120 sh -c 'until curl -sf http://localhost:5300/health -o /dev/null; do sleep 2; done'
    # Gitea's one user. Refused as a duplicate once it exists.
    -docker compose -f tests/live/compose.live.yaml exec -T -u git gitea gitea admin user create --admin --username smeltr --password smeltr-live-1 --email smeltr@example.com --must-change-password=false
    docker compose -f tests/live/compose.live.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic budget-a --partitions 4 --replication-factor 1
    docker compose -f tests/live/compose.live.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic budget-b --partitions 4 --replication-factor 1
    docker compose -f tests/live/compose.live.yaml exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --if-not-exists --topic budget-c --partitions 4 --replication-factor 1

# `SMELTR_LIVE_TESTS=required`: without it a missing service makes the
# live tests skip, and a skipped test reports `ok`. Only the suites that
# assert behavior; the measurement suites assert on timings. The tests
# share one broker, hence `--test-threads` and one suite at a time.
# Services stay up afterward; `just down` clears them.

# `check-all`, plus every live test that asserts behavior.
[group("dev")]
[env("KAFKA_TEST_BROKERS", "localhost:19092")]
[env("ES_TEST_URL", "http://localhost:9200")]
[env("SPLUNK_TEST_HEC", "http://localhost:8088")]
[env("SPLUNK_TEST_TOKEN", "00000000-0000-0000-0000-000000000001")]
[env("SPLUNK_TEST_ACK_TOKEN", "00000000-0000-0000-0000-00000000a0c4")]
[env("SPLUNK_TEST_API", "http://localhost:8089")]
[env("SPLUNK_TEST_AUTH", "admin:smeltr-live-1")]
[env("S3_TEST_ENDPOINT", "http://localhost:4566")]
[env("ICEBERG_TEST_CATALOG", "http://localhost:8181")]
[env("SCHEMA_REGISTRY_TEST_URL", "http://localhost:8082/apis/ccompat/v7")]
[env("GCS_TEST_ENDPOINT", "http://localhost:4443")]
[env("AZURE_TEST_ENDPOINT", "http://127.0.0.1:10000/devstoreaccount1")]
[env("AZURE_QUEUE_TEST_ENDPOINT", "http://127.0.0.1:10001/devstoreaccount1")]
[env("PUBSUB_TEST_ENDPOINT", "http://127.0.0.1:8085")]
[env("EVENT_HUBS_TEST_BROKERS", "localhost:9092")]
[env("GIT_TEST_HOST", "http://127.0.0.1:3000")]
[env("SFTP_TEST_HOST", "127.0.0.1:2222")]
[env("POSTGRES_TEST_HOST", "127.0.0.1:15432")]
[env("MYSQL_TEST_HOST", "127.0.0.1:13306")]
[env("MARIADB_TEST_HOST", "127.0.0.1:13307")]
[env("GIT_TEST_AUTH", "smeltr:smeltr-live-1")]
[env("KEYCLOAK_TEST_ISSUER", "http://localhost:8180/realms/smeltr")]
[env("PEBBLE_TEST_DIRECTORY", "https://localhost:14000/dir")]
[env("PEBBLE_DNS_TEST_DIRECTORY", "https://localhost:14001/dir")]
[env("CHALLTESTSRV_TEST_DNS", "127.0.0.1:18053")]
[env("CHALLTESTSRV_TEST_MANAGEMENT", "http://127.0.0.1:18055")]
[env("S2S_TEST_FORWARDER", "target/s2s-forwarder")]
[env("OTEL_COLLECTOR_TEST_IMAGE", "otel/opentelemetry-collector-contrib:0.135.0")]
[env("PROMETHEUS_TEST_IMAGE", "prom/prometheus:v3.5.0")]
[env("FILEBEAT_TEST_IMAGE", "docker.elastic.co/beats/filebeat:8.17.0")]
[env("FLUENT_BIT_TEST_IMAGE", "fluent/fluent-bit:3.2.10")]
[env("TELEMETRYGEN_TEST_IMAGE", "ghcr.io/open-telemetry/opentelemetry-collector-contrib/telemetrygen:v0.135.0")]
[env("SYSLOG_SENDER_TEST_IMAGE", "debian:bookworm-slim")]
[env("S2S_FORWARDER_TEST_IMAGE", "splunk/universalforwarder:10.6.0.5@sha256:944fd52a8da3cb5709532a8d8e349732fdccaf6e09c165d1877be7c41f1c88e3")]
[env("SMELTR_LIVE_TESTS", "required")]
check-live: live-up check-all
    cargo test --test integration kafka_tests:: -- --test-threads=4
    cargo test --test integration kafka_fuzz::
    cargo test --test integration kafka_destination_fuzz::live_
    cargo test --test integration kafka_avro_fuzz::live_
    cargo test --test integration worker_tests:: -- --test-threads=4
    cargo test --test integration elastic_live_tests:: -- --test-threads=4
    cargo test --test integration splunk_live_tests:: -- --test-threads=4
    cargo test --test integration elastic_fuzz::live_
    cargo test --test integration splunk_fuzz::live_
    cargo test --test integration http_fuzz::live_
    cargo test --test integration s2s_live_tests::
    cargo test --test integration s2s_receiver_fuzz::live_
    cargo test --test integration objects_live_tests:: -- --test-threads=4
    cargo test --test integration objects_fuzz::live_ -- --test-threads=4
    cargo test --test integration object_source_fuzz::live_ -- --test-threads=7
    cargo test --test integration iceberg_live_tests::
    cargo test --test integration iceberg_live_fuzz::
    cargo test --test integration delta_live_tests::
    cargo test --test integration delta_live_fuzz::
    cargo test --test integration event_hubs_live_fuzz::
    cargo test --lib io::sftp::tests::live_ -- --test-threads=4
    cargo test --test integration git_remote_live_tests:: -- --test-threads=4
    cargo test --test integration postgres_live_tests:: -- --test-threads=4
    cargo test --test integration mysql_live_tests:: -- --test-threads=4
    cargo test --test integration sql_live_fuzz::
    cargo test --test integration receiver_tests::an_opentelemetry_
    cargo test --test integration otlp_receiver_fuzz::live_
    cargo test --test integration receiver_tests::prometheus_
    cargo test --test integration remote_write_tests::prometheus_
    cargo test --test integration prometheus_receiver_fuzz::live_
    cargo test --test integration remote_write_fuzz::prometheus_
    cargo test --test integration hec_receiver_fuzz::live_
    cargo test --test integration http_receiver_fuzz::live_
    cargo test --test integration receiver_tests::elastic_
    cargo test --test integration elastic_receiver_fuzz::live_
    cargo test --test integration syslog_receiver_tests::a_real_
    cargo test --test integration syslog_receiver_fuzz::live_
    cargo test --test integration okta_live_tests:: -- --ignored
    cargo test --test integration log_analytics_fuzz::live_ -- --ignored
    cargo test --test integration gate_tests::live_ -- --test-threads=4
    cargo test --test integration acme_live_tests:: -- --test-threads=1

# A KDC and a broker that demands GSSAPI, for the Kerberos tests. Needs
# `krb5-kdc krb5-admin-server krb5-user` and a JRE; the scripts say what
# they write, all of it under /tmp.

# Start the KDC and a Kerberized broker. `up.sh` clears what a last run left first.
[private]
kerberos-up:
    ./tests/kerberos/up.sh

# The Kerberos tests, against a KDC and broker started fresh.
[group("dev")]
check-kerberos: kerberos-up
    set -a && . /tmp/krb/env && set +a && SMELTR_LIVE_TESTS=required cargo test --test integration kerberos_tests::

# Reads a real Okta org's System Log, by API token and as a service app with
# DPoP: the one check of what Okta itself takes. `check-live` runs it too;
# `check` never does. Needs OKTA_ORG, OKTA_TOKEN, OKTA_CLIENT_ID and
# OKTA_PRIVATE_KEY (PEM, or that PEM in base64 on one line), from an Okta org
# with an API Services app granted okta.logs.read and the Read-only
# Administrator role. OKTA_KEY_ID names its key when it holds several.
check-okta:
    SMELTR_LIVE_TESTS=required cargo test --test integration okta_live_tests:: -- --ignored

# What a real Log Analytics workspace takes from smeltr: rows
# sent to a data collection rule, queried back. `check-live` runs it too;
# `check` never does. Needs LOG_ANALYTICS_TEST_ENDPOINT (the rule's logs ingestion
# endpoint), LOG_ANALYTICS_TEST_RULE_ID, LOG_ANALYTICS_TEST_STREAM, LOG_ANALYTICS_TEST_TABLE
# (a table with TimeGenerated, Tag and Message columns, which the stream fills
# as it is), LOG_ANALYTICS_TEST_WORKSPACE (the workspace's id), and
# LOG_ANALYTICS_TEST_TENANT_ID, LOG_ANALYTICS_TEST_CLIENT_ID and
# LOG_ANALYTICS_TEST_CLIENT_SECRET, a service principal holding Monitoring Metrics
# Publisher on the rule and Log Analytics Reader on the workspace.
check-log-analytics:
    SMELTR_LIVE_TESTS=required cargo test --test integration log_analytics_fuzz::live_ -- --ignored

# A broker that serves TLS and demands a client certificate, for the Kafka
# half of the identity tests. Needs openssl and a JRE; the scripts say what
# they write, all of it under /tmp. The Elasticsearch half needs no service
# and runs in `check`: `elastic-tls-tests` serves its own TLS.

# Start a broker that demands a client certificate. `up.sh` clears what a last run left first.
[private]
tls-up:
    ./tests/tls/up.sh

# The Kafka identity tests, against a broker started fresh.
[group("dev")]
check-tls: tls-up
    set -a && . /tmp/smeltr-tls/env && set +a && SMELTR_LIVE_TESTS=required cargo test --test integration kafka_tls_tests::

# Every profiling suite, with its numbers. One at a time, since they time a
# thread against a whole machine. The suites that need no services run first,
# so their numbers print before Kafka is started. tick-profile and flush-cost measure the
# Kafka hop. panos-ecs-profile is the reference workload, PAN-OS syslog
# normalized to ECS, stage by stage and against thread count. logs-cost is what
# a call site pays to raise a log, and memory-budget-cost what the memory
# budget's accounting costs, record-cost a typed record against the same data
# as `_raw` JSON, and guard-cost a guarded write by how many rows its guard
# takes. Builds past a stale GUI, which is what
# `SMELTR_ALLOW_STALE_GUI` is for: a benchmark does not embed the frontend.
[doc("Every profiling suite, and its numbers.")]
[group("dev")]
[env("KAFKA_TEST_BROKERS", "localhost:19092")]
[env("ES_TEST_URL", "http://localhost:9200")]
[env("SMELTR_ALLOW_STALE_GUI", "1")]
measure:
    cargo test --profile dist --test panos-ecs-profile --test logs-cost --test memory-budget-cost --test record-cost --test guard-cost -- --ignored --nocapture --test-threads=1
    just live-up
    cargo test --profile dist --test tick-profile --test flush-cost -- --test-threads=1 --nocapture

# A row per reference pipeline in the log `BENCH_LOG` names, HEAD against the
# pinned reference, and the ratio chart beside it as an .svg. Run daily.
# Needs no services, and perf from linux-tools-generic.
[doc("A row per reference pipeline in the log BENCH_LOG names, HEAD against the pinned reference.")]
[group("dev")]
bench-log:
    bench/bench-log.sh

# Each source and each destination timed alone through the shipped worker, on one transform thread: rows/s, MB/s
# and what saturated, over the link bench/network describes and over none. Needs a kernel with sch_netem; the
# services' containers are shaped and kept off the transform's core. `CONNECTOR` picks the rows whose name holds it,
# `CONNECTOR_ROWS` (1,000,000) sets their size, and `CONNECTOR_LINK=bare` runs only the bare link.
[doc("Each source and destination timed alone through the worker, over a shaped link and a bare one.")]
[group("dev")]
[env("KAFKA_TEST_BROKERS", "localhost:19092")]
[env("ES_TEST_URL", "http://localhost:9200")]
[env("SPLUNK_TEST_HEC", "http://localhost:8088")]
[env("SPLUNK_TEST_TOKEN", "00000000-0000-0000-0000-000000000001")]
[env("SPLUNK_TEST_ACK_TOKEN", "00000000-0000-0000-0000-00000000a0c4")]
[env("SPLUNK_TEST_API", "http://localhost:8089")]
[env("SPLUNK_TEST_AUTH", "admin:smeltr-live-1")]
[env("S3_TEST_ENDPOINT", "http://localhost:4566")]
[env("ICEBERG_TEST_CATALOG", "http://localhost:8181")]
[env("GCS_TEST_ENDPOINT", "http://localhost:4443")]
[env("AZURE_TEST_ENDPOINT", "http://127.0.0.1:10000/devstoreaccount1")]
[env("AZURE_QUEUE_TEST_ENDPOINT", "http://127.0.0.1:10001/devstoreaccount1")]
[env("PUBSUB_TEST_ENDPOINT", "http://127.0.0.1:8085")]
[env("EVENT_HUBS_TEST_BROKERS", "localhost:9092")]
[env("SFTP_TEST_HOST", "127.0.0.1:2222")]
[env("POSTGRES_TEST_HOST", "127.0.0.1:15432")]
[env("MYSQL_TEST_HOST", "127.0.0.1:13306")]
[env("MARIADB_TEST_HOST", "127.0.0.1:13307")]
[env("SMELTR_LIVE_TESTS", "required")]
[env("SMELTR_ALLOW_STALE_GUI", "1")]
connector-speed: live-up
    cargo test --profile dist --test connector-speed -- --ignored --exact speed --nocapture

# Stop the live services and the Kerberos and TLS brokers, and discard their
# data. A line that fails, for a rig that is not up or a Docker that is not
# running, does not stop the rest. The Kerberos and TLS rigs are bash scripts,
# so on Windows those two lines fail.
[doc("Stop every service the tests started, and discard their data.")]
[group("dev")]
down:
    -docker compose -f tests/live/compose.live.yaml down -v
    -./tests/kerberos/down.sh
    -./tests/tls/down.sh

# Reformat in place.
[group("dev")]
fmt: need-rust deps-gui deps-website
    cargo fmt --all
    npm --prefix gui-frontend run format
    npm --prefix website run format

# `build-dev`'s binary, started.
[group("dev")]
run-dev: build-dev
    cargo run --bin smeltr

# Frontend, then an optimized binary, started.
[unix]
run: gui
    cargo run --release --bin smeltr

# Windows loads the AWS-LC FIPS module from a DLL beside the binary (`tools/fips-dll.ps1`).
[windows]
run: gui
    cargo build --release --bin smeltr
    powershell -NoProfile -ExecutionPolicy Bypass -File tools/fips-dll.ps1 "{{ env('CARGO_TARGET_DIR', 'target') }}/release"
    cargo run --release --bin smeltr

# Each test starts its own leader and workers from empty data directories, so
# they run side by side. Arguments go to Playwright: a file, `-g <title>`,
# `--headed`. A failure leaves a trace and a screenshot in `tests/gui/results`.
[doc("Every user story the GUI serves, driven in Chromium against the debug binary.")]
[group("dev")]
[unix]
[positional-arguments]
check-gui *args: build-dev
    npm --prefix tests/gui install
    npm --prefix tests/gui run format:check
    cd tests/gui && npx playwright test "$@"

# A leader, four workers and a change history, staged from nothing against
# `tests/live/compose.live.yaml`'s Kafka and Elasticsearch. The fleet stays up afterward:
# `tools/screenshots/run.sh capture` shoots again, `run.sh stop` ends it.
# `tools/screenshots/README.md` has the one-time setup. A bash script, hence
# unix only.
[doc("Every major GUI page, in light and dark, from a demo fleet staged from nothing.")]
[group("dev")]
[unix]
screenshots: build
    ./tools/screenshots/run.sh

# The website demo's leader answers from these. They come from a staged fleet,
# as `just screenshots` stages one, so they change only when this runs again.
# It needs the one-time setup in `tools/screenshots/README.md`.
[doc("Record the staged demo fleet's answers for the website demo.")]
[group("dev")]
[unix]
demo-fixtures: build
    ./tools/screenshots/run.sh demo

# The binary is statically linked, so every crate and every vendored C
# library in it is distributed and has to be attributed. `cargo about` sees
# only crates, so the vendored ones live in `tools/notices/about.hbs` and survive a
# regeneration.
# Regenerate THIRD-PARTY-NOTICES.md.
[group("dev")]
notices: need-rust (need "cargo-about" "Run: cargo install cargo-about --locked")
    cargo about generate -c tools/notices/about.toml tools/notices/about.hbs -o THIRD-PARTY-NOTICES.md
