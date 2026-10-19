# Exception taxonomy

A system of the [design document](../smeltr-design.md).

Six things go wrong, kept separate because the right response differs. Each
names who decides, what stops, and what bounds memory while it is stopped.

| what | who decides | what happens |
|---|---|---|
| the destination cannot be reached | nobody: waiting is correct | egress retries, reports the stall, commits nothing past it; queues fill in order, then the reader holds |
| the destination looked and refused | the `Refused` answer: the destination's over the group's | under block, the refused rows are kept and sent again, at most a minute apart, until the destination takes them, and a capture at the destination is shown them, once, each with why; under deliver, the rows wait until every target's queue has room, and the commit waits for every target; under drop, nothing buffers |
| no route placed the row | the `Unmatched` answer: the group's for a row no route took, the route's over the group's for one no delivery rule of its route took | as the answer says; a row no route took runs no pipeline, and one its rules declined goes as its pipeline left it |
| the pipeline marked a row | the answer to each of its kinds: the route's over the group's | under block, the batch in hand is kept until the config changes; otherwise nothing buffers, and the row leaves on this tick |
| the code panicked on a batch | the group's `Unprocessable` answer takes the batch: the same records meet the same code again, so a second attempt has nothing to find out | as the answer says |
| a batch's own materialization exceeds the memory budget | the runtime: split the route's range and re-run, down to one record | only a record that will not fit alone meets the `Unprocessable` answer, alone |

## One answer list for every exception

An exception is anything that loses or repairs a value, or a row no pipeline
answers for. Each has a kind, and each kind belongs to one category: parse,
math, limits, encoding or delivery. An encoding exception repaired its value
and kept it: invalid UTF-8 replaced, an encoding guessed. A parse, math or
limits exception lost or repaired a value, as its kind says. The delivery category
holds the kinds no pipeline answers on its own row: `Unmatched`, a row no
route took or no delivery rule of its route took; `Unprocessable`, a record no pipeline could finish; `Refused`, a
record a destination refused.

Every kind is answered from one list:

| answer | what happens | what releases it |
|---|---|---|
| ignore | the row goes on as it is | nothing is held |
| tag | the row goes on with its tag field set, and the delivery rules place it like any other row | nothing is held |
| drop | the row is counted under its own name | nothing is held |
| deliver | the row goes to each destination named, instead of where it was going, in the format that destination writes | the commit waits for every destination named |
| block | the batch in hand is kept, nothing past it commits, and the source blocks and says why | a new config, or for `Refused`, the destination taking the rows |

Only a destination names a format: the record, one field's text, or one
field's bytes, by default `__source.bytes`, the payload as the source read it. The record is the pipeline's output, or where
no pipeline finished the row, the record object as its source handed it:
`{"_raw": ...}` holding a byte record's decoded text, `{}` where its source's
`encoding` is `none`, or a typed record's fields. A row without the field a
text destination writes is refused there. The bytes go once per source record, however many rows it became. A source
that does not declare the field a bytes destination writes cannot reach it:
loading refuses the config. A row keeps the
form each destination down its chain of refusals writes, so a refused row
reaches a DLQ in that DLQ's own format. An answer that needs what its kind
does not have is refused at save, naming what is missing:

| kind | ignore | tag | drop | block | deliver |
|---|---|---|---|---|---|
| every other category | ✓ | ✓ | ✓ | ✓ | ✓ |
| `Unmatched` | no route | no route | ✓ | ✓ | ✓ |
| `Unprocessable` | no record | no record | ✓ | ✓ | ✓ |
| `Refused` | no destination | no destination | ✓ | ✓ | ✓ |

A group's `exceptions` setting answers each category, and each kind it
names; a kind beats its category. A group that names neither ignores
encoding, blocks delivery, and tags every other category: losing data is
something an operator chooses. A route's setting lays over the group's, so
its answer for a category replaces the group's for every kind in it. Of the
delivery kinds a route answers only `Unmatched`, for the rows its delivery
rules decline: `Unprocessable` never reaches one, and `Refused` is the
destination's. A destination's answer for `Refused`
lays over the group's. A pipeline has no setting of its own, so a call inlines
another pipeline's steps and nothing else.

A row takes the strongest answer of its uncaught kinds: block, then deliver,
then drop, then tag, then ignore. A row with several deliver answers goes to
each destination they name, once. A row with an exception not ignored gets
the tag field, whatever its answer; under drop and block it is not sent, so
the tag reaches a destination only on a tagged or delivered row. The tag
says where the row failed and why: the route, the pipeline, and each of its
exceptions not ignored, in the order they happened, with the step
that raised it, its kind and its message. One raised reading the record names
no step. The field is `smeltr.exception` unless the group names another, and a
route may name its own. Its shape is fixed, so a destination's schema types
each of its fields. It is written over whatever that row holds there,
a field the pipeline binds or a scalar on its path included. A tagged row is a row: nothing but
delivery rules decides where it goes. A rule reads the row as it will be sent:
the tag field is the tag on a tagged row, and on any other whatever the row
holds there. A batch runs every row to the end.

A `try` catches every category but encoding and delivery
([Language](language.md#the-statements)). An exception a `catch` took leaves
no mark and is not counted; every other is counted, whatever its answer.

## Refused or unreachable

The line between the first two is whether the destination looked at the
record. Unreachable is transient and waited for; refused is refused
identically for as long as nothing changes, so retrying it in a loop wedges
the partition. Only the operator knows whether the record or the destination
is wrong, so refusal is answered rather than decided by a predicate.
Classification is by allowlist and deliberately narrow: a wrong "refused"
loses a record, a wrong "transient" costs a visible stall.

A connector draws that line with one question: would the same bytes, sent
again to a healthy destination, succeed? Yes is unreachable. So a failed
credential is unreachable, since the credential gets fixed, and so is
throttling. A malformed document, a mapping conflict and a record over the
destination's own limit are refused. A destination that acknowledges a batch
at once reports each refused record beside that acknowledgment, never as the
batch's failure: a failed batch commits nothing, so one bad record would hold
the stream. A refused record delivered elsewhere goes once to each
destination its answer names, and the commit waits for every one of them. A
destination that refuses it too answers with its own `Refused` answer, so
refusals chain; a chain that leads back to where it started is refused at
save, since a record refused along it would go round forever.

## Releasing a block

A block answers for a destination, never for a source: a source's own commit
refused across a rebalance is dropped and counted. Releasing a block is a
separate obligation, since a blocked source cannot deliver its way back to
health; a block that kept nothing releases on an empty poll once nothing is
outstanding. A block on unmatched rows releases only with a new config: the
same rows meet the same routes, so it is decided before any pipeline runs. A
block on failed or unprocessable rows is the same: the same rows fail the same
way. A block on refused rows releases once the destination takes them. The fix for a refusal
is usually the destination's own, and nothing tells the worker it is done, so
the refused rows are sent again until they land. A source blocked
by rows it cannot deliver anywhere, with no rows refused or held, releases once
the destination takes records again. A blocked source reports which of these
holds it, so its warning says what will release it.

## What is counted

A row that matched no route, a row its pipeline failed and dropped, a row no
pipeline could process, a row whose pipeline ran and whose every delivery rule
then declined it, a row without an identity under a policy that wants one, and a
row dropped after a destination refused it are each counted under their own name,
because each is a number that must not round into "sent".

## Seams

The [runtime stages](runtime-stages.md) carry the exceptions and the
answers. The [config plane](config-plane.md) refuses at save an answer its
kind cannot take, a format a destination does not offer, a deliver answer
naming no destination, and a chain of refusals that leads back to where it
started. Every disposition is a counter in the
[observability](observability.md) registry.
