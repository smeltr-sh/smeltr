# Control plane

A system of the [design document](../smeltr-design.md).

A worker runs exactly one deployment: one bundle at one commit. Which one is
not its own decision; it is told, and this is the plane that tells it.

## The leader

The leader is one process holding a git repository, a small store for
what is not configuration (worker credentials, grants, sealed secrets), an
in-memory registry assembled from heartbeats, and the GUI. One leader owns a
data directory: a second started on it is refused, and the refusal names the
directory and says to stop the other leader or give this one a directory of
its own. It validates people, distributes commits, previews changes over
captured data and shows the fleet. It keeps each person's conversations with their AI agent, and
deletes one nobody has added to for the days an administrator sets. What the model is sent of a conversation stays
bounded: past a size, a summary stands in for its older turns, written by the provider where it can and by the
model at the leader's ask where it cannot, while the conversation is kept whole. Standalone means smeltr plus a source plus a destination. A smeltr
process is a leader or a worker; both roles compile into one binary, and
setting a leader address is what makes a process a worker. Running both in
one process spawns a worker that talks to its own leader over the same API an
external one uses ([api.md](api.md)).

```text
worker                                                   leader

  |--- an enrollment code ------------------------------>|
  |<-- a durable credential, stored locally -------------|  enroll

  |--- what I am doing, and what has happened since ---->|
  |<-- what only the leader knows -----------------------|  heartbeat

  |--- fetch what is deployed --------------------------->|
  |<-- objects ------------------------------------------|  config

                      no arrow starts on the right
```

Every exchange is worker-initiated. Workers behind NAT work identically to
public ones, and the leader's picture of the fleet is what workers last said,
never what it observed. A worker that stops talking stays in the view as it
last spoke, until the leader forgets it.

## Groups

Configuration belongs to a group, not to a worker. A group is a named fleet
with its own subtree of the one repository, drawn in its own color; `shared/`
beside the groups holds what several use, and the deployed pointer is the ref
every group deploys by. A group exists on a commit exactly when its subtree there holds
anything, so deleting a group removes the whole subtree. A worker serves one
group, named in its own startup file along with the settings it must hold
before it has heard from a leader. The group is the addressing unit for
health, sampling and access.

## Heartbeat

A short-interval poll, not a standing connection. Up: the worker's identity,
the commit it runs, its threads and what each holds, the metrics
accumulated since the last acknowledged heartbeat, and the progress of every
single-worker source it holds. Down: only what the leader
alone knows, such as every capture it still wants from this worker, the
single-worker sources it holds, a
certificate authority it has just minted, or an operator's directive to restart
its deployment or shut down. Every answer carries the heartbeat interval. A
worker uses a built-in default until its first heartbeat is answered. A directive names no commit: what to
run is the deployed pointer, and the worker fetches it for itself over plain git and runs
its own group's subtree merged with `shared/`. Each answer also names the commit the
pointer is at, as a cue: a worker that has fetched that commit already does not fetch
again, and one whose beat went unanswered fetches on its own schedule. A commit that moves nothing the
worker runs is not a redeploy. The leader answers that fetch with git's
protocol, and a lookup file by its SHA-256 from its lookup store, and nothing
else: no path a worker sends returns a file git keeps to itself or one beside
the repository, such as the secret store or the key of the leader's own authority.

A lookup file's bytes do not travel in git. A commit holds a pointer naming
them by SHA-256; the worker fetches the files its own deployment names, checks
each against its pointer and keeps only those. The leader keeps the newest few
versions of each uploaded file name and whatever a branch or a running
deployment names, so a rollback reaches as far back as that.

Git itself stays compact on both sides. The leader packs its repository once
git's own limits are passed, and drops what no ref reaches once git's grace
has passed. A deploy-log or change marker holds the commit it names as a
parent, so what a marker names is kept with it. A worker has no git binary
and gains a pack per fetch, so past the same limit it writes what its fetched
and running commits reach as one pack and removes the rest.

Every exchange with the leader gives up once it has stalled for a time budget,
the first included: the fetch of the root an enrollment code names by
fingerprint, made before the worker can verify the leader at all. A leader
that accepts a connection and never answers costs the worker one failed
attempt, and the worker reports it.

## Holds

A single-worker source is read by one worker of its group at a time. Every
worker running it says so on its heartbeat, and the leader holds the source
for one of them, sticky, spreading a group's sources across its workers. The
holder reads; the others read nothing. Its progress is what every destination
has acknowledged: a **cursor**, one position, where the source's own protocol
orders what it holds, or a **ledger**, the items it has read, where nothing
does. The holder reports progress on its next heartbeat, and the leader keeps
it in its store, taking it only from the holder.

```mermaid
flowchart LR
  run[a worker runs the source] --> beat[its heartbeat says so]
  beat --> held{held by another worker the leader still hears?}
  held -->|yes| idle[it reads nothing]
  held -->|no| take[the leader holds it for this worker]
  take --> resume[it fetches the ledger, resumes, reads]
  resume --> report[progress on each heartbeat]
```

A holder loses the hold once the leader has gone three heartbeat intervals, without hearing from it, or without it listing the source, so a redeploy
that briefly runs nothing keeps its holds. One that says it is stopping gives
them up once its last progress is kept. The next worker resumes from the last
progress the leader kept: what was delivered since is delivered again. A leader that restarts counts
every stored holder as heard at its own start. A holder that cannot reach the
leader keeps reading, as every worker keeps running its deployment, so a
partition between the two can leave two readers until it heals. That costs
duplicates, never lost rows. A hold that moves opens or closes one reader
inside the running deployment and restarts nothing else.

Progress belongs to a source in its group at the place it reads. The leader
keeps it while any bundle it may run reads the source there: the deployed
commit's, `main`'s head's, or one a worker still runs, and while a worker it
hears lists the source. Once neither holds, the leader forgets the hold and
its progress, so its store holds what is deployed
rather than all that ever was. A source that comes back afterwards, by a new
deploy or a rollback, starts afresh, and one that keeps what it reads reads it
all again.

## Deploying moves a pointer

What workers fetch is a pointer to one commit of `main`'s history, not `main`
itself. Approving a change moves `main` and the pointer together. Rolling back
moves only the pointer, to an earlier commit, and writes nothing: the fleet is
then *held*, and stays held while `main` moves on, until somebody releases it by
deploying `main`'s head or approves another change. A hold is a warning for as
long as it lasts, naming who held the fleet, when, and how many commits on
`main` it does not run, since a fleet that does not take what was deployed after
it looks to everyone else like a deploy that failed. Every move of the pointer
is a marker in a deploy log the leader keeps beside it, which is where who held
it comes from.

```text
  approve ──► main and the pointer move together          not held
  roll back ──► the pointer moves back, main stays        held
  main moves on ──► the pointer stays                     still held
  release, or approve ──► the pointer moves to main       not held
```

## Lifecycle

A worker runs nothing until it has checked in once; its clone
buys speed, not independence. After that first directive it runs indefinitely
without a leader, subject only to its own stop-after-unreachable setting.
Redeployment is wholesale: fetch, fetch the lookup files it names, load,
check named secrets, stop the old, start the new, so every refusal comes
before anything stops. A panic while reading a commit is one of those
refusals. Nothing is
checked out -- a commit is already an immutable snapshot, and a bundle is read
out of one.

```mermaid
flowchart LR
  read[read and check the commit] -->|refused or panicked| keep[old keeps running; retried on backoff]
  read --> stop[stop the old] --> start[start the new]
  start -->|ready| run[runs the new]
  start -->|failed| again[start the last good commit once]
  again -->|ready| held[runs the last good; waits for a new commit or a restart]
  again -->|failed| none[runs nothing; retried on backoff]
```

A commit that fails only once started cannot be refused before the old
deployment stops, so the worker starts the last commit that ran here again.
It reports why the target did not start, and does not try the target again
on a timer: each try would stop the deployment that works.

## A worker asked to stop ends its deployment, then says so, then exits

```text
  asked to stop ──► deployment ends ──► last heartbeat ──► exit
                    drained, and        says it is stopping
                    confirmed dead,
                    as for a redeploy
```

Ending a deployment is one drain, bounded like every other step. Every
destination finishes what it holds at once rather than at its own interval, a
receiver answers new requests busy and hands over what it already took, and
each reader commits what lands. What has not landed when the drain ends is
read again by the next deployment, or sent again by its sender.

The notice comes last so that no beat lands after it to say the worker is
back, and so that it carries what the deployment counted and said to its end.
Each step is bounded: a deployment that will not end, or a leader that does
not answer, delays the exit and never prevents it. A notice that does not land
leaves a worker that stopped without one. A second request to stop exits at
once.

## A worker is a supervisor and a deployment, and the deployment is a process

Talking to the leader must never wait on what the worker is running, because
what it is running is the thing that can refuse to stop: a stalled connector
cannot be interrupted, a thread cannot be killed, and a second deployment
beside an abandoned reader is two consumers on one source. So heartbeat,
discovery and deployment run on schedules of their own, and the deployment
runs in a child process the supervisor can end.

```text
  supervisor process                         runtime process
    heartbeat  ── every interval ──► leader
    discovery  ── every interval ──► deployed    one deployment
    deployment ── start, stop, kill ───────────► readers, transform, egress
```

A receiver's port outlives the runtime serving it. The supervisor binds the
address of every receiver its deployment names and hands the socket to each
runtime it starts, so a sender that connects while one runtime stops and the
next starts waits in the socket's queue and is answered by the next, rather
than being refused. A receiver's UDP socket is held the same way, and a
datagram sent meanwhile waits in its buffer until that buffer fills. That is
unix only, since it hands the socket over as an
inherited descriptor; on Windows each runtime binds its own, and a sender that
connects while one runtime is replaced is refused and sends again.

## The invariant: no deployment state can stop a worker from being given a new one

A target that will not load, will not start or will not stop is a
bounded attempt followed by a retry, and the newest observation always wins --
ordering is by when this worker looked, never by commit ancestry, so a
rollback is eligible the moment it is seen. Nothing takes a runtime's place
until that runtime is confirmed dead.

It holds while the supervisor, the operating system and the network do. Three
things are outside it, and each is reported rather than recovered from: a host
that is gone, a supervisor that is gone, and a process the operating system
will not confirm dead -- which leaves the worker blocked from deploying, still
heartbeating, and saying so. In all-in-one the supervisor is a thread of the
leader, so a runtime hang is recoverable there and a supervisor hang is not.

A supervisor that is gone confirmed nothing, so the one thing that must not
happen anyway is enforced without it: a runtime holds an advisory lock on its
worker's directory for as long as it lives, and a runtime that cannot take that
lock refuses to start. The operating system releases it when the holder exits,
crash included. So an orphan draining alone keeps it, a replacement supervisor's
first runtime is refused while that lasts, and two readers of one source is the
state the system has no way to reach rather than one it tries to avoid.

## Sampling

Only a worker ever opens a connection to a broker or cluster.
On the leader's invitation a worker taps its live ingest, or reads a source
without advancing it where the source supports that, or taps the rows its
pipelines finish for a destination, into a bounded buffer. Every capture takes
a filter, read where it taps: the record as it arrived at a source, the row as
its pipeline finished it at a destination. A capture of a source a block holds
is shown the records that blocked, once, each with why. So is a capture at a
destination the rows it refused and keeps, whatever its filter. A capture of a stream source
starts where it asks, at the latest unless it names another start; one that
names another start reads the source itself on every worker, a worker whose
deployment already reads the source included. For a source, the leader runs the
transform over them in process, with preview destinations in place of real
ones. A capture of a receiver may take what arrives **as sent** in place of its
records: each read of a sender's connection, after TLS, and each datagram,
before the receiver frames any of it, with who sent it, how, and why it closed
a connection unread. It takes no filter, since there is no record yet to read.
A worker running the receiver taps it; one that is not listens on its address
itself, answers nothing, and is refused a port a receiver there holds. One such
request, from invitation to end, is a **capture**. A worker
fills its buffer only while the leader still wants the capture from it. A
credential lent for a capture stays only while the capture is wanted from that
worker or the worker's answer to it is still owed. What the leader holds for
one capture has a fixed ceiling, however many workers answer. Rows a block
holds fill it before any other, and a capture does not end on its total
before every worker that held such rows when it opened has reported. A worker
holding them takes no other row for the capture, and reports nothing, until it
has shown them. Once a capture
is over at the leader, what the leader holds of it is final. What it holds,
saved, is a **sample**: a bundle file on the branch it was saved to, which
reaches `main` when that branch deploys (`config-plane.md`).

A capture belongs to whoever opened it, and a person holds one at a time:
opening another drops the last, running or not. Its owner may preview over it
unsaved. A finished capture is held while it is read, and dropped once nobody
has read it for a while; no capture is held past a fixed time from its
opening. What every capture holds together has a ceiling, which counts a
running capture at the most it may grow to. A new capture drops the oldest until it
fits.

## Warnings

The leader raises every warning, because the sharpest one, a worker gone
silent, is one no worker can report about itself. A worker that said it was
stopping is not silent, whatever its kind: going without notice is the one
thing about a gone worker that warns. An ephemeral worker that is gone -- by
its notice, or by a silence longer than the operator allows -- is forgotten,
and leaves the view, the roster and every wait that counts the fleet. Silence
is what the leader was up to hear: a fleet that ran on through the leader's
own outage is not forgotten for it. A permanent one stays in the view, stopped
or silent. Most read the facts workers report; the rest read what the fleet is
running, since a certificate in the deployed bundle runs out on a date no
heartbeat mentions. A warning is derived on every heartbeat, never stored, and
clears when its fact stops. The catalog is fixed: each entry names a subject,
the fact it reads and the sentence that says what to do. The leader serves the
warnings a reader may see to a scraper too, each a series while it holds, so an
alert manager pages on what the Issues page shows and resolves it when the
series goes.

The leader also tells alert channels itself, on a timer of its own, so a fleet
whose workers have all gone quiet is still told. Each channel, a webhook or a
mailbox, takes the kinds of warning ticked for it, in the groups chosen for it.
It is told when one is raised and again when it clears. What each channel was
told is kept in the leader's store and written only once the channel took it,
so a failed send is made again and a restarted leader neither repeats one nor
drops one. A channel muted until a time is told nothing meanwhile; what still
holds when the mute ends is told then. A channel that does not take a send is
itself a warning about the leader.

An entry is a failure, a warning or info. Info is a fact worth knowing that
asks nothing of anyone, such as a worker on another release of the leader's
major. The Issues page, a scraper and the alert channels that take it are given
info, marked as such. The views that judge health leave info out, so none of
them counts it as trouble.

A blocked source warns by what holds it, because each releases differently:
rows only a new config lets through, rows a destination refused, or a
destination not taking records. The last releases on its own once the
destination is back, so it is a warning where the other two are failures.

A source reading fewer rows than it normally does warns, and one reading more
raises info. Its rows are judged at four widths: the last 5 minutes, 30 minutes,
6 hours and 24 hours. At each, the usual window is the same window at the same
time of day on the last seven days learned, and the range is the usual window
scaled by how far that width's learned windows strayed from theirs, a week of
them for the short widths and four weeks for the long, and widened by the
source's preset. A width judges a source only where its usual window holds ten
rows, so a busy source is judged at every width and a sparse one at the long
ones. A source too sparse for any width is judged by the gap: how long it has
gone without a row, against half again the longest quiet spell it made in the
past year. A source warns once some width reads below its range for as many
readings in a row as its preset requires, or once it outlasts the gap, and the
warning holds until some width judges the source and none reads it below its
range, or, where only the gap judges it, until the first row after it began that
ends a quiet spell the gap read below. Rows above normal are judged at 30
minutes alone.

A source's preset sets how soon a drop warns against how often a brief dip does:
how much wider than its learned spread the range is, and how many readings in a
row below it warn. Fast warns at the first reading and on the most dips;
Sensitive and Balanced wait longer; Quiet waits longest and warns on the fewest.
Each is a point the detector was measured at
(`docs/research/normal-detector/paper.md`), so an editor picks one of four
measured trade-offs, never a value in between. A stop reads below every range,
so the wait alone sets how soon one is seen. A source is at Balanced until an
editor in its worker group picks another on its History tab on the Map, and the
leader keeps the pick with who made it. A pick applies from the next reading, to
everything already learned. The gap is the same at every preset.

What a below-normal warning read, and the time a stop held, are not learned, so
an outage does not widen the normal. Nor is a window learned until a whole width
has passed since it ended: a warning that rises in that time leaves the window
unlearned. So a drop that takes its width to show is judged against the normal
before it, and is not taught to that normal while it unfolds. Days not learned
are passed over, not counted, so a lasting drop is still judged against the days
before it. It keeps warning until the source reads inside its range again or an
editor says otherwise. A window with more rows is learned, so more rows become
the new normal. A source with too little history has no normal yet. A source
that is held, unreachable or on a silent worker is not judged, because a warning
that names the cause is already up. After that warning ends, the source is not
judged at a width while that width's window still contains the stop and the
catch-up after it. The leader keeps for a year when each warning about a source
was up, so its end is known across a restart and the gap learns a year of quiet
spells less those that warned. The Status page ranks sources by how far each is
from the same normal.

An editor in the source's worker group may say any of three things about it.
Lower turns the warning that is up into info until it ends, and it stays on the
Issues page for everyone. Reset history forgets what the source read before now,
so it has no normal until it has enough history again. Normal says a period was
normal, so it is learned though it warned. Outage says a period was an outage,
so it is not learned though it did not warn. The source's History tab on the Map
lists its warnings, including the one that is up, and its longest quiet spells of
the past year, and each can be marked Normal or Outage. A warning marked Normal
is learned whole. A window that contains rows marked Normal is learned without
waiting a width. So marking the warning that is up teaches the level the source
reads now, and the warning ends once that normal is learned, within the hour.
The leader keeps each of the three, with who said it.

A source's senders are judged as the source is. A sender is one value of one
field of the source's records, such as a firewall's name, or of its stream key
(Observability, Streams). The leader sums what workers count in each five-minute
bucket and writes the bucket once it ends, into a day row for each field and
each value: the buckets the field was counted in, the buckets the value held a
row in, and for a watched sender the rows it held in each. A bucket's count of
rows takes one to three bytes of the day row, so four weeks of one sender's rows
take 10 to 30 kilobytes.

```mermaid
flowchart LR
  W[workers: rows by value of each field] --> B[leader: each five-minute bucket, written once it ends]
  B --> D[a day row per field and per value: its buckets, and a watched sender's rows in each]
  D -- once a day --> P{sent steadily on seven days in a row}
  P -- yes: watched --> J[judged at the four widths against its own normal]
  D -- a watched sender's rows --> J
  J -- below its range at a width, as many readings in a row as its preset requires --> X[Senders below normal]
```

Each day the leader picks which values to watch: a value is watched once it went
no more than an hour without a row, counting only the time its field was
counted, on each of seven days in a row, and it stays watched for a week after.
A field that held one value names no sender, and neither does one that passed
its cap of values on a worker. The pick reads the last two weeks of day rows. A
watched sender keeps its rows from the day it is picked, in day rows kept for
the four weeks its normal is learned from. A value the pick stops watching loses
its rows, and its day rows once no pick reads them. A newly watched sender is
judged once it has three days of rows.

A watched sender is judged at the four widths, below normal only and never by
the gap, at the preset picked for its source's senders. That preset is Quiet
until an editor picks another on the Senders tab: each sender warns on its own,
so a source with many senders has that many chances for a dip to warn. It is
judged only where its field was counted, so a field that a pipeline change takes
out leaves its senders unjudged rather than all below, and so does a bucket the
leader lost to a restart. It is not judged while its source warns that its rows
are below normal or that it is blocked, refused, waiting or unreachable, nor on
the reading its source crosses: the source's own warning says more. Senders of
one source that cross on one reading raise one warning that names them, and a
sender that crosses later raises another. The warning says which of its senders
are back within their range, and ends once all of them are.

An editor in the source's worker group lowers one sender's warning to info, and
a warning all of whose senders were lowered is info. An editor also stops
watching a field, or adds one the pick cannot offer, such as a number or a
nested path. New fields are watched until an editor turns off Watch new fields;
while it is off, a field that comes later is counted but watched only once an
editor checks it. The source's Senders tab on the Map lists its fields, what the
pick read of each, and each watched sender's last day against its range. A field
first counted since the day's pick is listed the same day, as learning.

## Upgrades

A released smeltr starts on whatever an earlier supported release left, and
carries it forward with no manual step. A worker takes a deployment from any
leader of its own major release, so besides the leader's own stores the
configuration and the messages between leader and workers cross builds too.

```mermaid
flowchart TD
    start[leader starts] --> stores[leader.db, registry.db, config.git: run the steps each has not had]
    stores --> serve[serve]
    serve --> fetch{a worker fetches}
    fetch -->|one major release, or one commit| load{it loads the commit}
    fetch -->|anything else| keep[refused: it keeps running what it has, and the leader raises WorkerOnAnotherBuild]
    load -->|yes| deploy[it takes the deployment]
    load -->|no: something newer than it knows| hold[it keeps running what it has, and the leader raises WorkerNotDeployingItsTarget]
    newer[a store a newer build broke] --> refuse[the leader refuses to start, and says why]
```

- **A store has steps.** Each store's version is how many of its ordered
  steps it has had. A step is appended, never edited once released, and a
  test holds every released step to the hash it shipped with. A rebuild that
  adds no step leaves every store as it was.
- **Only a breaking step stops a rollback.** A step is additive when a build
  without it can still use the store: a new table, a plain index, a column a
  row may leave empty. Anything else is breaking, including a change to what
  stored values mean, which is a step of its own. A store keeps the fewest
  steps a build must know to use it, and only a breaking step raises that; an
  older build is refused only below it.
- **A build is its commit.** The date in the version changes on every rebuild
  and does not count. A build with uncommitted edits to its sources is its
  commit and those edits. A build that cannot read its commit fails to build.
- **A release is a tag on `main`.** `v1.2.0`, semver. A build from that commit
  with no edits carries the release name.
- **Releases of one major work together.** A worker takes deployments from a
  leader of its own major, in either direction. Any other pair, a pre-release
  or a build that is not a release included, must be one commit. Within a
  major, a newer release reads what an older one wrote with the same meaning,
  and an older one refuses what it does not know and never reads it otherwise.
- **A major release is one that breaks that.** It also raises the upgrade
  floor or ships a breaking store step; a minor adds only additive steps, a
  patch adds none, and `just cut-release` refuses a release named otherwise.
- **Messages between builds only grow.** Heartbeat and enrollment messages
  gain optional fields and ignore ones they do not know, so a leader and a
  worker one release apart still hear each other.
- **Config files are upgraded as they are read.** The leader never rewrites a
  file nobody saved.
- **Each release is kept.** `just cut-release` stores the data directory
  and the messages a release writes, and every later build must start on them,
  load the deployment they hold, and read every message.
- **Each release says what it changed.** Its notes are written in
  `website/src/lib/releases.json`, which the release pages on smeltr.sh show.
  `just cut-release` and the release workflow refuse a release without dated
  notes there. A GitHub release carries the same notes, and a pre-release has
  none.
- **Nothing from before the first release is upgraded.** A store with no
  record of its steps is refused, with a message to move it aside.
- **A leader says when a newer release is out.** A release leader reads the
  releases published on GitHub, through smeltr.sh, when it starts and once a
  day after, unless an administrator turns that off. The highest release that is
  no pre-release, when it is newer than the leader's, is announced as info
  that links that release's page on smeltr.sh. The request
  names the leader's release, operating system and CPU architecture and nothing
  else about the install. smeltr.sh counts leaders by those, and by the country
  and network the request comes from, never keeping its address. A look that fails raises nothing, so a
  leader that cannot reach smeltr.sh is never noisy.

## Damaged stores

Every store is checked when its process starts, before anything acts on it. A
store that can be had again is had again; one that cannot stops the process
with what is damaged and how to restore it. Nothing runs on the part of a
store that still reads.

```mermaid
flowchart TD
    start[a process starts] --> check{each store reads back whole}
    check -->|yes| run[the steps, then serve]
    check -->|a cache: worker clone, lookup file, registry.db| again[set aside or removed, and had again]
    again --> run
    check -->|config.git or leader.db| refuse[refuse to start, naming the file and the copy to restore]
```

- **Reading back whole** is SQLite's own check for a database, and for a
  repository every ref, every commit in their history, and every object under
  each ref's tree, each hashed against its id. A lookup file is hashed against
  its name.
- **`leader.db` has no other copy**, so each start that finds it sound keeps
  one beside it. The refusal names that copy and its age. Restoring it loses
  what changed since, and sources resume from the positions it held.
- **`config.git` is not rebuilt.** A git host holds its `main` and pushed
  branches but not unpushed saves, changes or the deploy log, so what to
  restore it from is the operator's call. The refusal names the host.
- **A lookup file comes back from where it came:** a worker fetches it from
  the leader, and a leader with a git host from the host's LFS server, for
  `main` and for what runs.

## Seams

The [config plane](config-plane.md) defines what a commit holds; the control
plane decides which commit runs where. The
[secrets boundary](security.md#secrets-boundary) rides the authenticated worker
channel. The heartbeat is [observability](observability.md)'s first consumer.
