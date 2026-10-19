# Compiler

A system of the [design document](../smeltr-design.md).

Configuration is declarative; the runtime executes instructions. The compiler
is the one-way bridge, run once per deployment and again in the leader for
preview. It resolves every name to a slot, interns shared resources to
indices, and emits an instruction list over those slots. Compilation is
pure: the same configuration yields the same plan, so one plan is shared by
every thread running it and the compiler is tested without a runtime.

```text
  resolved topology ──► slots ──► plan
  calls inlined         every name to a    one per pipeline
                        slot, in SSA form  and source metadata
```

## Every step is code

The compiler has one front end, the code language. It lowers each step to the
code it stands for as it reaches it (`plan::lower`) and compiles those
statements under the step's guard, so nothing a step does is done a second
way, and every compile error names the step the author wrote.

```text
  parser   a.b: i64;  …one per declared field the name does not hold at that type;  Object.assign($, json(_raw))
  lookup   Object.assign($, lookup("geo", key))
  rename   move(a.b, a.d)
  drop     if (cond) drop()            a blank condition lowers to nothing
  unroll   unroll(field, keep)
  eval     target = expr
  code     its own text
  if       if (…) { … }
```

## Names and slots

The compiler binds names to slots in one pass and emits instructions in static
single assignment form: an assignment allocates a fresh slot and rebinds. So
reassigning a field never changes a value an earlier step read from it: after
`y = x; x = x + 1`, `y` holds the old `x`. A name still resolves to one slot
for every row: a guarded parser parses into fresh slots and blends each into
the name's prior value, exactly like a guarded assignment. Every field that
holds a value when the pipeline ends is output, under the name it ends with.

The compiler holds the record as a name tree, at compile time only. A name is
a leaf, one slot, or an object: its fields and the slot of its unclaimed
fields. The record is the root, an object like any other.

```text
  {"status": 200, "source": {"ip": "10.0.0.1", "port": 443}, "tags": "a"}
  with status and source.ip declared

  (record)              object
  ├─ status             leaf        200
  ├─ source             object
  │  ├─ ip              leaf        "10.0.0.1"
  │  └─ *               unclaimed   {"port": 443}
  └─ *                  unclaimed   {"tags": "a"}

  each leaf and each * is one slot
```

Resolution walks the tree: `a.b.c` is the leaf `c` under the object `a.b`. A
read of a name nothing bound is a compile error, wherever it appears, and so
is a write under a scalar or a variant. A write under a key of an object's
unclaimed fields makes that key an object of its own, whose unclaimed fields
are the key's other keys. A parser's slots are its declared fields, so a
record key a step reads is a field its parser declares, and a declared object
is its leaves. A parser that discovers keys lays every other key into the
unclaimed fields of the deepest object at its name. The plan carries the tree
to the serializer, which writes each object's fields over its unclaimed
fields, so a step that writes a name claims the record's key of that name. No
guard reaches the unclaimed fields, so a dynamic parser under a guard is a
compile error.

An object is open where it has unclaimed fields, which is one fact in the
tree, not a flag beside them. A path or a literal key past its fields reads
a variant out of its unclaimed fields; a closed object refuses it. A key
only the row knows is a path too. A read by one builds the object whole, as
a copy lays it down, and finds the path in it on each row, at a cost of the
object's size per row. A write by one is a guarded write of each leaf at any
depth where the key names it, and at each open object a set, in its
unclaimed fields, of the rest of a key that runs under it and that none of
its fields takes. A loop
that writes by key to a name nothing binds, or binds to null, has an open
object with nothing in it bound before the loop, and an open object its
unclaimed fields, so each trip adds to the last's.

## One plan per source kind and pipeline

A plan is compiled once per distinct pair of source metadata and pipeline: a
pipeline two Kafka sources route to compiles once, and one two source kinds
route to compiles once for each, because the names in scope differ. Those
names are the record's fields the source declares, `_raw` for a byte source
that decodes and none for one whose `encoding` is `none`, and its kind's
`__source` fields, `__source.bytes` among a byte source's. A name the source
does not declare is a compile error.

## The field a bytes destination writes

A destination writing bytes writes one field. Loading refuses a config where a
source reaches such a destination, by its routes, its group's answers to
unmatched and unprocessable rows, or a DLQ chain, and does not declare that
field as `bytes`. The error names the source, the destination and the field
(`check_bytes_fields`).

## Seams

The [config plane](config-plane.md) hands the compiler a resolved topology with
pipeline calls already inlined. The compiler hands the
[runtime stages](runtime-stages.md) one plan per pipeline, its row barriers and
live fields named. The leader runs the same compiler over a branch to preview
and to test.
