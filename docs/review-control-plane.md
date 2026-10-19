# Review: the control plane

The decision record for the control plane: what was chosen, what each
choice costs, and the order the rest lands in.
The design document's [Control plane](architecture/control-plane.md),
[Config plane](architecture/config-plane.md) and [Security](architecture/security.md)
state the design; this says why it is that shape and what it gave up. What of it is built is
[`status.md`](status.md)'s to say.

## Requirements

The worker is stateless and coordinated by the source; the leader is the
second tier beside it, and the split must not blur: the leader owns all
new state itself and pushes none of it onto workers.

- Runs in an enterprise and on a laptop or edge box, and standalone
  means smeltr plus a source plus a destination. No git host, no other
  tool.
- The GUI is the authoring surface. The JSON bundle is its output.
- Several people own several pipelines. Two of them must never
  overwrite each other, an in-progress change must never be dragged into
  someone else's deploy, and a change can be validated against real data
  before anyone approves it.
- A data engineer edits and submits their own pipelines and nothing
  else.
- Git for rollback, a commit-and-deploy workflow, and an approval step.

## The store: one git repository, held by the leader

**Architecture:** config plane, control plane.

One repository is the source of truth, on the leader's own disk or on an
external host it is pointed at. Every group's bundle is a subtree of it,
`groups/<group-id>/`, beside the `shared/` one they all draw on, so
logic used by several groups is one file rather than a copy per group. A
group tracks a ref, `main`, rather than a hash: deploy is whatever moves
`main`, and rollback is deploying an earlier commit, applied so history
stays linear and a worker's "the commit changed, redeploy" logic is
untouched.

Pointing the leader at a host it does not own changes the URL a worker
is given and nothing else. What it changes is who can move `main`, which
is why smeltr reconciles rather than enforces (design doc, Config plane).

The alternative shapes both fail on shared logic. A repository per
group gives shared config no home: copied into each, it drifts, or a
cross-repository mechanism is bolted on to undo that. A `main` per group
is worse — long-lived branches never meant to converge, colliding with
the author's own branch, since "which `main` is this off?" is a question
nobody should have to answer. One tree makes a shared pipeline one file,
so drift is not discouraged but impossible.

**Reuses:** `leader::git`. Nothing is named to a worker: it tracks `main`
and redeploys when its own subtree or `shared/` moved, so the heartbeat
carries a group's policy and not a commit.

**Worse:** with the repository on the leader, leader HA is
restore-from-backup until a shared store exists; a second replica has
nothing to share. Large lookup files live beside history, in a lookup
store a commit points into, so an mmdb update grows the repository by a
pointer; the store keeps a few versions per file name, and a rollback
reaches no further back than they do. A change to
`shared/` reaches every group that uses it at once: two regions that must
diverge are two pipelines, written and named as such, rather than one
pipeline quietly running at two versions.

## Distribution: plain git

**Architecture:** control plane (bundle distribution).

Workers pull and the leader never pushes. A worker clones the repository
and fetches `main`; the leader serves it with `git http-backend`, git's
own server, behind the same worker credential everything else uses.
Read-only is the server's own doing — `http.receivepack` is false —
rather than a route that happens not to exist.

A worker holds the whole repository. The cluster is the security
boundary, so an enrolled worker is trusted with every group's
configuration. A changed pipeline is one small object and an unchanged
lookup table is nothing. A worker compares the tree git stores for its own subtree and
`shared/` against what it is running, so a commit that touched only
another group never redeploys it.

**Reuses:** the authenticated worker API, `load_bundle_dir`, git itself.

**Worse:** every host downloads every group's objects. Lookup files would make
that unaffordable, so they travel beside git, fetched by hash and only by
the workers whose deployment names them.

## Authoring: a branch per thing in flight, a commit per save

**Architecture:** config plane.

Every GUI save is a commit on a branch, and the first edit puts an
author on one: `main` is never editable. Two people editing one pipeline
are on two branches, so nothing is overwritten and the collision moves
to merge time, where it is visible and file-scoped. Uploading a lookup
file is a commit like any other.

Branches are not owned. What protects one is the access its changes need
to land, so a reviewer commits a fix to an author's branch under the
same rule the author writes to it, and an author holds as many branches
as they have things in flight — park one, start another off `main`,
come back.

A save onto a branch that moved is refused with both versions shown. One
file per pipeline is what makes two people editing different pipelines
merge cleanly by construction.

**Reuses:** one file per pipeline, which was split for exactly this;
`leader::git`.

A branch is deleted by whoever created it or by anyone holding `deploy`
on the objects it touches, warning when it carries someone else's
commits. There is no upload endpoint and no bundle archive: configuration
reaches a leader through the GUI, which commits to a branch, or through
an external git store, which is the same mechanism with somebody else's
host holding `main`.

**Worse:** every save is a commit, so a branch accumulates history a
user never chose to write. That is the cost of never losing work, and
squash-on-merge keeps `main` readable.

## Review and deploy

**Architecture:** config plane, control plane (deployment lifecycle).

A change is a whole branch opened for review against `main`. Its tests
run and travel with it; a change that breaks one can still be opened,
carrying the failure.

If `main` has moved, the branch is brought current and its tests re-run
before it can be deployed: merging is deploying, and a stale green
result would put untested config in front of live data. A conflict
belongs to the author, who wrote the content, rather than the reviewer,
who may hold only `deploy` over it — refuse and reopen is honest until
there is somewhere to resolve one.

Approval merges to `main` as one commit and deletes the branch. There is
no separate deploy step: `main` moving is what workers pick up. An
author may deploy their own change, and the history records when author
and deployer are the same person, as it records a deploy over a failing
test. Closing a change keeps the branch, so no work is lost.

A deploy squashes the branch away, so the commit it leaves carries both:
the author in git's own author field, the deployer in a trailer. `main`'s
reflog is the deploy history, so there is no separate audit table.

**Reuses:** git's own history and merge.

**Worse:** a merge UI is real work if refuse-and-reopen turns out not to
be enough.

## Validation: capture on workers, preview on the leader

**Architecture:** control plane (live sampling), runtime stages
(transform).

Workers own sources and destinations. A capture is a worker tapping its
own ingest: copy up to N raw records for session S, POST them to the
leader, outbound only. The invitation rides in the heartbeat response,
as the design doc's Control plane describes. The leader compiles the branch and runs the *transform*
over those records in process, with capture destinations, and the GUI
shows output rows, which route matched and where each row would have
gone, and per-function metrics from the leader's own registry. A
reviewer runs the same preview on a change.

Two ways to get records. A tap on live ingest works for any source a
worker is running. A read that does not advance the source is something a
source declares it supports, the same way it declares whether it can
report lag, and is the only way to sample a source nothing consumes yet.

**Reuses:** `Transform` without a `Reader`: a `Handoff` fed by the
capture endpoint, stepped by `Transform` with capture targets.
`/api/preview` becomes "this branch over this session". The leader needs
no source or destination credential.

**Removes:** `live.rs` and its hand-built worker.

**Worse:** sampled production data reaches the control plane and sits in
leader memory for the session: over TLS, never persisted, bounded by
count and time, taking editor in the source's area, and said out loud
in the enterprise story. Leader CPU goes to previews, bounded by sample
size. A bounded sample can miss a rare record shape.

## Identity and access: roles per area

**Architecture:** control plane.

The leader validates OIDC tokens itself for the enterprise, and runs
with a local user or no auth at all on a laptop. Either way the access
decision is the leader's. Nothing in the data plane learns a person
exists.

An area is `shared` or one worker group. A person holds viewer, editor
or deployer in each area they may see, from roles that map the
provider's groups to a level per area. Objects carry nothing: an object
is in the area it was made in, so access is set once per team rather
than once per object.

**Worse:** an OIDC implementation is security-sensitive code the project
owns. It is bounded, and it is the price of standalone.

## Heartbeat: a poll, not a stream

**Architecture:** control plane.

A standing stream per worker was rejected: it pins a worker to one
connection for its lifetime and gives the leader connection-affinity
state to route commands through. A poll interval on the order of 1–2 s
gives equivalent command latency without it.

## Trust between worker and leader

**Architecture:** control plane.

A different concern from identity and access: that is a person proving
who they are to the GUI; this is two unattended processes on a network
that may include hosts nobody trusts. The threat model: prevent a worker
from attacking the legitimate leader, and prevent a worker from taking
commands from a leader impersonating the real one. Forcibly stopping a
worker the leader has decided is compromised is the infrastructure
control plane's job, and the leader does not duplicate it.

## Credentials: a version, not a fingerprint

**Architecture:** secrets boundary.

A leader-stored credential is referenced by name and version rather than
by a fingerprint of its value: a fingerprint would tell anyone reading
the repository which objects share a password, and would let a weak one
be attacked offline. A version leaks nothing and diffs as well.

## Startup: check in before running

**Architecture:** control plane (offline behavior).

A worker runs nothing until it has received one directive from a leader.
Startup is check in, fetch `main`, run. The local clone buys speed, not
independence: a `main` already on disk starts instantly. After the first
directive the worker runs indefinitely without a leader, subject to the
group's `stop_after_unreachable_secs`, which travels in the bundle -- so
a deployment always runs under the policy it started with.

**Worse:** a worker that restarts while the leader is down runs nothing
until the leader returns. Chosen: a worker that started from a clone the
leader had moved past is the one case where "the leader's view is what
workers last said" is false. A per-group "may start from cache" policy
for an edge site behind a flaky link is the knob, deliberately not added
until a deployment asks.

## Removed from the control-plane design

- **Auto-balancing, and the reassign endpoint.** Both existed because a
  hot partition pinned the thread that held it. With transform a pool
  sized by cores, a hot partition's batches spread across every core
  whatever reader polled it; the imbalance that remains is between
  hosts, which is the group protocol's. The endpoint breaks the worker
  it targets.
- **The DB-backed version table and its propose→approve→deploy state
  machine.** Git is the store.
- **The OPA-style policy engine.** A role's level per area is the only
  new state.
- **The separate audit log.** Commits, merges and the reflog.
- **The Postgres store.** Until a shared git host exists, a second
  leader replica has nothing to share.

## What this settles in the runtime

The reader/transform split finishes as **one reader per source and a
transform pool sized by cores**, not the thread pair PR #31 built.
Deleting auto-balancing depends on it, and the leader's preview is a
second consumer of the seam. `thread_limit` means the pool's size and
nothing else; the reader budget's arithmetic goes with it. The memory
question was settled separately: flow memory stays implicitly bounded,
the budget stays a per-record safety net.

## Order

Branches, review and deploy landed, and the GUI is a commit path
(`config_api`, `leader::git::deploy_change`). Capture and preview landed
with them, and `live.rs` went. What remains:

1. Roles, then OIDC. Everything else works on a laptop
   without them.
