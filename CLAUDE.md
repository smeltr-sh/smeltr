# Conventions

## Agents

A thread does its own work, in order. Threads already run side by side; agents within a thread would share its one
`target/`, queue on its lock and rebuild smeltr for each other. Use an agent only for work that builds nothing:
reading, searching, reviewing a diff.

## Scoping a change

`docs/smeltr-design.md` tells what must be true. It carries no status. `docs/status.md` tells what is built. `src/` is the truth about what is.

A change needs a scoping plan first. Write it for a chief architect.

1. Say it in chat. Touch no files.
2. Say it is ready. Wait for agreement. A review is not agreement.
3. Then build it. In its own commit.

The plan says what must be true. Then what is already there. Then, and only then, what to build. Get the answers from the code. Always present multiple solutions, and evaluate each in the context of the overall design, a solution that uses existing architecture with small changes is better than a new mechanism with big changes.

Name the systems in `docs/smeltr-design.md` the change touches. Say what it costs.

Fixing or extending a mechanism that already exists is ordinary work and needs none of this.

## Writing

- Say what a thing is and what must hold. Do not list what it is not.
- One name for one thing. A second name makes it look like a second thing.
- Do not tell the document's own history. No "this used to say."
- A picture is better than text. When the shape is the point — how it flows, what comes after what — draw it. Mermaid diagrams are our friend.
- Let the code say what it does. The comment says why, when why is not plain.
- A function's comment gives the caller what he needs to know. A line comment says why that line is the way it is.

### Architecture documents

- Open with the whole shape, in one paragraph, before any of its parts.
- Name each section for what it holds, not for the argument that got you there.
- Costs and trade-offs are facts. State them.
- Let the content set its own shape. Do not borrow another document's frame.
- Put your reasoning only where order is the point — what must come before what, and why.
- Say what must be true. A signature or a default value, copied into a design doc, goes stale the day the code changes, because code cannot go stale against itself. Name the thing. Let `src/` carry the detail.

## Stored formats

What a leader stores and what builds send each other outlive the build that wrote them
(`docs/architecture/control-plane.md`, Upgrades).

- A change to a store's schema or meaning appends a step to it. Never edit a released step.
- A change to `system.json`'s format adds a converter from the old version to the new.
- A field added to a heartbeat or enrollment message is optional. Never refuse unknown fields there.
- Cutting a release runs `just cut-release <release>`, which records it, commits that and tags it.

## Checks

Run `just check` once, at the end of the work, before its push. Commit each task of the work without checking
between them, then run `just check` and put what it finds in one last commit. John runs `just check-all` before a
merge to main. Nothing runs on a PR. After a change to `Cargo.toml`, `Cargo.lock` or the
Kerberos tests, also run `just check-kerberos`.

A dependency change also needs `just notices`: nothing else keeps
`THIRD-PARTY-NOTICES.md` current.

`just check` runs what the files changed since origin/nightly can break, as `tools/check-scope` decides: a document,
the citation checks; the GUI or the site, their own checks; a Rust file, the tests of its module and of the
integration modules that use it. It can miss a test, and anything it cannot place runs `just check-all`, which is every
check but the fuzzers.

Live checks are expensive. Run one only when a change touches a live
integration directly and `just check` cannot cover it. Never run the whole
`just check-live`: run only the lines of its recipe the change affects, with
the `SMELTR_LIVE_TESTS=required` and service variables the recipe sets for
them. Start the Docker daemon first, then `just live-up`. Windows is checked
nowhere.

After a change to the GUI or the API it calls, also run `just check-gui`. While building, run only the specs the change
touches (`just check-gui specs/<name>.spec.ts`), and the whole of it once, before the PR leaves draft. A base merged
into a branch without a conflict needs only `just check`.

While building, run the app with `just build-dev` or `just run-dev`, and take screenshots from it. `just build` and
`just run` are for checking how a release build behaves.

A machine builds into one `target/`, the main checkout's. A worktree or second clone sets `CARGO_TARGET_DIR` to
it, so dependencies build once and a new checkout recompiles only smeltr. Cargo commands on one `target/` run one
at a time: a second waits on the first's lock, so a waiting build is queued, not hung. Clippy builds into
`target/clippy`, so it runs beside the tests rather than waiting on them. Disk is tight: free it by
removing checkouts you are done with, then stale binaries in `target/`, and never while a build runs.

## Fuzzers

Every source and destination has two fuzzers that exercise every option and config and try to break it: an
offline one, and a live one against the real system or its emulator wherever one can run. Both are seeded: they
draw from `src/fuzz.rs` (`crate::fuzz` in the crate's tests, `harness::fuzz` under `tests/`), run their seeds with
`fuzz::run`, and assert with `Coverage` that every option was reached. `SMELTR_FUZZ_SEED=n` replays a failing
seed.

The offline fuzzers run in `just check-fuzz`, which picks them by the `fuzz` in their test names, so a fuzzer's
module or file is named for it. They run weekly, in the scheduled check that also runs `cargo deny check advisories`
and `npm audit`, and after a change to anything a fuzzer exercises: a source, a destination, a parser or the
compiler. The live one is a live test like any other: it takes its service
from `tests/live`, so it skips without one, and it gets its own line in `just check-live`.

The GUI has two fuzzers, seeded the same way (`tests/gui/fuzz.ts` ports `src/fuzz.rs`), and both check after every
step that what is on screen works (`tests/gui/onscreen.ts`). `tests/gui/specs/gui-fuzz.spec.ts` walks the GUI as
each kind of person, by what is on screen. `tests/gui/specs/journey-fuzz.spec.ts` takes an administrator from a
fresh install to rows flowing and on, by moves (`tests/gui/moves/journey.ts`) each checked against the leader's API,
the workers and the rows that arrive. A new thing an administrator does is a new move. `just check-gui` runs a seed
for each kind of person and one journey, which makes only some of the moves; `just fuzz-gui` and `just fuzz-journey`
run enough seeds to reach everything, and `just check-fuzz` runs both after the crate's fuzzers.

## Pull requests

Open one when the feature stands on its own. Watch it for reviews, do not trust the review. Discuss each non-trivial (>3 lines) fix with me before proceeding.
