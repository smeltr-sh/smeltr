# Smeltr — user stories

Who uses smeltr, what they need, and why. This document holds needs. A
story that names a mechanism has been written wrong; the mechanisms live
in [`smeltr-design.md`](smeltr-design.md), and what is built lives in
[`status.md`](status.md).

Numbering here supersedes any earlier list.

## Personas

- **Data engineer** — operates the flow. Is the data moving, is it the
  right volume, is it falling behind.
- **Data analyst** — authors pipelines. Is the data arriving in the
  expected shape, is it leaving in the right one.
- **System administrator** — runs smeltr itself: workers, credentials,
  deployments, and who may do what.

A **reviewer** is not a fourth persona. It is a right held over some set
of objects, exercised through the same pages an author uses.

## Data engineer

**E1 — Fleet at a glance.** One view showing every worker in every group
as healthy, warning, or failed, so an engineer can tell in seconds whether
anything needs them. Warnings and errors are the content; partitions and
thread detail are drill-down.

**E2 — Flow over time.** Rows in per source and out per destination over
a window the engineer chooses: sixty minutes by default, out to thirty
days. A stall that started five minutes ago and a slow bleed over three
weeks are one question at two scales.

**E3 — Told, not asked.** Alerted when the volume flowing in from a source
is abnormal without having to define abnormal for each one. There are too
many to hand-tune, and the right number changes by hour and by day.

**E4 — A stalled source, with a cause.** Which source, since when, and
why, in words that name the fix.

**E5 — A struggling destination.** Rejecting rows, failing sends, or
backing up is its own condition, not something inferred from a missing
line on a chart.

**E6 — Lag where lag exists.** Per-partition lag over E2's window, with a
warning when it grows monotonically. Lag is a property of the source
kind: a source declares whether it can report it, and only those that can
show it.

**E7 — Where the rows went.** When out is less than in, attribute the
difference: dropped by a step, filtered by a route, unrouted, errored, or
never arrived.

**E8 — Read again from a point.** Once a fix is deployed, reread a Kafka
source from the time the bug went in; when a source is far behind, skip to
the newest, by changing where the source starts. Each partition reads from
that point once, whatever its group has committed.

## Data analyst

**A1 — Real records to work from.** Samples obtained by reading a source,
by tapping what it is delivering now, by upload, or by paste. Reading a
source for a sample never advances its position and never takes records
from what is running, so a source can be sampled before anything consumes
it and from any point in its history — the answer to a rare shape a live
tap would miss. Not every kind of source can do this; a source declares
what it can do, and one that cannot offers upload and paste. Samples are
saved and named, so an analyst can return to one and a colleague can use
it. A saved sample is cut down to the records that matter, and a record is
fixed or added, without losing where each was read from.

**A2 — Schema from a sample.** The parser's declared fields and their
types are proposed from a sample, for the analyst to accept and edit.

**A3 — Where the sample is thin.** Fields absent from some rows, fields
whose rows disagree about their type, and shapes the sample
suggests but cannot pin down.

**A4 — The whole trip.** For a sample: the output rows, which route
matched, and which destinations each row would reach.

**A5 — Blast radius.** Before changing anything: what feeds this object,
what it feeds, and whose work shares it.

**A6 — Samples become tests.** A sample is promoted to a test that locks
input and expected output. Saving a config that breaks a test is always
allowed — authoring stays interruptible. Opening it for review is allowed
too, with a warning; the failing tests travel with the change.

**A7 — Is it even reachable.** Before building on a new source or
destination, an analyst wants to know it is reachable and its credentials
work. Only a worker can answer: it holds the connection, and the leader
never opens one. A destination is checked for reach and authentication
and nothing more — verifying it by writing means writing to production.

**A8 — Bad rows go somewhere.** A row a parser or a step cannot handle is
not lost. It is delivered marked with what went wrong, or sent where the
analyst says such rows go, and the analyst sees which rows failed and why
while authoring, before anything is deployed.

**A9 — Reference data.** Records are enriched from tables the analyst
keeps beside the pipeline: an uploaded file, rows typed in, or a
geolocation database, matched exactly, by network or by pattern.

**A10 — Sources and destinations from a form.** An analyst adds a source
or a destination by filling in its form, and what the form saves is what
the leader accepts. For a source one worker reads at a time, the analyst
sees which worker holds it, and how long since that worker was last heard.

## Change and deploy

Shared by analysts, reviewers and administrators. Every configuration
change reaches workers through this path and no other.

**W1 — Editing happens on a branch, and the analyst names it.** Main is
never editable. The first edit aimed at it stops and asks which branch to
go on, so nobody finds their work somewhere they did not choose; saying no
leaves the edit unsaved. An analyst has as many branches as they have
things in flight, and can park one and start another. Branches are not
owned: what protects a branch is the access its changes need to land.

**W2 — Open a change for review.** When the work is ready it becomes a
review request against main, from the shell, wherever the analyst is:
the branch bar under the nav names the branch, says it is not deployed,
says what is saved on it, and unfolds into the form that opens the change. The form
is filled from what changed — a subject over a body with a line for each object — and
shows the files touched and how the tests do; the author edits it before opening, and
renames it later on the review page, where the diff is.
Tests run on it, and their result is part of what the reviewer sees.

**W3 — Review a change.** The reviewer sees the diff, every pipeline the
change impacts with each of their test results, and can run their own
samples against it. For a pipeline change that is one pipeline; for a
shared object — source, destination, route, lookup table — it is every
pipeline that reads it, because the author's tests only prove what the
author thought of. A reviewer may comment, and may commit fixes to the
branch directly.

**W4 — Current with main before deploy.** Tests ran against the main that
existed when the change was opened. If main has moved, the change is
brought current and its tests re-run before it can be deployed — merging
is deploying, and a stale green result would put untested config in front
of live data. A conflict belongs to the author, not the reviewer, who
would otherwise be silently editing content they only hold deploy over.

**W5 — Approve and deploy.** Approval merges to main as one commit and
deletes the branch; main's movement is what reaches workers. There is no
separate deploy step, and nothing smeltr deploys is unapproved. An author
may deploy their own change — a single administrator has to be able to
work alone — and the history records when author and deployer are the
same person. Deploying over a failing test is likewise recorded, not
forbidden.

**W6 — Reject.** Closing a change keeps the branch, so no work is lost.
A reason is offered and encouraged, so the author has something to act on,
but never required: a lone author reviewing their own change should not
have to write notes to themselves.

**W7 — Roll back.** Deploying any earlier commit, by whoever holds deploy
on the objects it touches. Not a separate mechanism: a rollback is a
deploy that happens to move backward.

**W8 — Main is what is running.** A read-only view of main answers "what
is live right now", distinct from whatever branch is being edited. Review
is how smeltr moves main; it is not the only way main can move, so the
view says which commit is deployed and how it got there rather than
implying only one route exists.

**W11 — Told when what is deployed fails its tests.** As an engineer or an
administrator, I want to know when what the fleet runs fails its own tests,
however it reached main. Not refused: a git repository is a directory on a
disk, an external git host has its own workflow, and a control plane that
pretended otherwise would be wrong quietly the first time. Deploy it, and
tell me.

**W9 — Nothing is silently overwritten.** A save onto a branch that moved
is refused, with both versions shown. An editor notices when anything it
is working from moves: if nothing has been typed since it loaded, it
reloads itself; if something has, it says so loudly and waits. An edit
left behind on one page and come back to is checked the same way, against
what the file says on return.

**W10 — Branch housekeeping.** Every branch and open change is listed
with its state, so nothing is lost or forgotten. A branch is deleted by
whoever created it or by anyone holding deploy on the objects it touches,
with a warning when it carries someone else's commits.

**W12 — From a fresh install to rows flowing.** An administrator who has
just installed a leader does the whole job from the GUI and nowhere else:
signs in and chooses their own password, adds a source, a destination, a
pipeline and a route between them, saving each, opens the change, comments
on it, approves and deploys it, and sees rows arrive at the destination.

## System administrator

**S1 — Bring a worker in.** A worker enrolls in the fleet with credentials,
without hand-editing anything on the host.

**S2 — Take a worker's trust away**, and see that it is gone.

**S3 — Control what a group runs.** Which commit is deployed, and what a
worker does when it cannot reach the leader.

**S4 — Read the history.** Who changed what, who deployed it, and when it
reached the fleet. Read-only: an administrator holds no special authority
over configuration.

**S5 — Roles.** Say what each of the provider's groups may do in each
area, once, so nobody has to label objects one at a time.

**S6 — Configure the service.** Address, TLS, retention.

**S7 — Smeltr's own health.** Is the leader up, is it out of memory or
disk, and which release each machine runs. Distinct from the health of
the data.

**S8 — Hold and rotate credentials.** As an administrator, I want to set a
credential once and see everything that uses it. Nobody reads a value
back, including me.

Rotating one is an ordinary change: the value goes to the store, and what
the change carries is the reference to it moving forward a version. So a
rotation is reviewed, deployed and rolled back like anything else — the
diff shows that a credential moved without showing what it moved to, and
a rollback restores the credential the previous deploy ran with. A
credential taking effect on a schedule nobody chose, invisible to the
history everyone reads to see what changed, is an outage nobody can
attribute.

**S9 — The first minute.** A fresh install is usable without a separate
setup step, and only whoever can read the leader's console can claim it.
The first sign-in requires choosing a password of one's own.

**S10 — Know whether we owe.** As an administrator, I want to see how
much data this leader read this year, and whether an organization our
size needs a license. Nothing stops either way.

**S12 — Turn AI agents on.** As an administrator, I want to turn the
leader's AI agents on by naming the model endpoint they think with:
Anthropic, OpenAI or another OpenAI-compatible service, its model and its
key. Until I do, nobody is offered the agent's screens, and a key or model
that cannot work is refused when I save it.

## Groups

**G5 — Bring your own git.** As an administrator, I want the option of
keeping the configuration on the git host we already run, so it sits with
our review tooling, our access control and our backups rather than beside
them. Smeltr deploys what lands on main either way; what changes is who
can put it there.

**G1 — A group per boundary.** As an administrator, I want a group per
region, enclave, or environment, each running its own configuration, so a
change to one cannot touch the others. Groups differ in what they can
reach: a source in one may not exist in another at all.

**G2 — One fleet, many groups.** As an engineer, I want the whole fleet in
one view, grouped by group. Seeing all of it is the point of one leader,
and a group is a filter on that view rather than a mode that hides the
rest.

**G3 — Shared logic written once.** As an analyst, I want transform logic
used by several groups to exist once, not copied per group. Copies drift,
and the day someone updates three of four, the fourth quietly runs last
month's logic. A change to shared logic reaches every group that uses it;
an author who wants two regions to diverge writes two pipelines and says
so.

**G4 — Records stay where they belong.** As the administrator of an
enclave, I want to control whether records from its sources may leave it
at all. Sampling moves production data to the leader, which may sit
outside the boundary the enclave exists to enforce.

## Permissions

**P1 — Roles per area.** An area is `shared` or one worker group. A
person holds one level in each area they may see, and each level includes
the ones before it:

| level        | what it is                                                       |
| ------------ | ---------------------------------------------------------------- |
| **viewer**   | read the area's configuration                                    |
| **editor**   | change it on a branch, and capture, preview and read its samples |
| **deployer** | approve and deploy a change to it                                |

A role is the level it gives in each area, and the provider groups that
give it. Holding several roles holds the highest level any of them gives.
Holding any area reads `shared`, since a group is read merged with it.

**P2 — Nothing to set on an object.** An object is in the area it was
made in, and the area decides who may do what with it. A new source
asks nothing about access.

**P3 — A change needs a deployer in every area it touches.** A change to
`shared` is judged as `shared`, though every group runs it, so a
`shared` deployer can change what every group runs.

**P4 — A full administrator holds everything.** Anyone holding the
built-in administrator role is one, given it by an administrator or by a
provider group, so one person can run the whole system alone. Only
administrators reach Admin, and at least one local administrator
remains.

**P5 — Records are editor work.** Capture, preview and samples show
production records, so they take editor in the area, as changing its
configuration does.

**P6 — A team sees its own areas.** A change, a commit, the map and the
status pages show a person only the areas they hold; a change reaching an
area they do not hold is not shown to them at all.

**P7 — Roles follow the provider, or the administrator.** What an OIDC
account holds comes from the groups its provider last named, and changes
within minutes of a change there. What a local or key account holds is the
roles an administrator gave it. A change to a role reaches everyone holding
it on their next request.

**P8 — Agent permissions.** A person chooses what their agent may use
when it acts for them at their prompt: up to a level in each area, and, for
an administrator, the administrator role. The agent holds the lower of that
and what the person holds, and no agent changes its own permissions.

**P9 — Talk to my agent.** A person asks their agent, on a page of their
own, to look into or change something for them. They see each request it
made of the leader and what it was answered, can stop it, and can delete a
conversation. Nobody else sees their conversations.

## Open questions

- **Access within one worker group.** A group is the smallest area, so
  two teams sharing one group share its access.
- **Where the key that protects stored credentials comes from.** It cannot
  live in the store it protects.
- **Whether G4 is a real constraint or expressed by roles.** Roles decide
  who may see records; they do not stop records crossing a boundary.
