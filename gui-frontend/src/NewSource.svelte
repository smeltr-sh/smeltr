<!--
  Adding a source (`NewObject`), its kind asked first, with a source's fields.
-->
<script lang="ts">
  import NewObject from './NewObject.svelte';
  import SourceFields from './SourceFields.svelte';
  import { SOURCE_KINDS } from './sourceKinds';
  import { SOURCE_TYPES, blankSource, isSql, typeLabel, typeOf } from './sources';
  import type { Area } from './areas';
  import type { KafkaFacts, SecretDto, SourceDto } from './api';

  let {
    open,
    unavailable,
    secrets,
    kafka,
    readAt,
    onsaved,
    from,
    areas,
    takenIn,
    onadd,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    /** Every id the new one may not take. */
    unavailable: string[];
    secrets: SecretDto[];
    kafka: KafkaFacts;
    /** The commit the caller read, for what `SourceFields` saves on its own. */
    readAt: string | null;
    onsaved: (commit: string) => void;
    /** A source to copy (`NewObject`). */
    from?: SourceDto;
    /** Where a copy may go (`NewObject`). */
    areas?: Area[];
    takenIn?: (area: string) => Promise<string[]>;
    /** Takes the source, and the area picked for a copy. The dialog stays open until its owner closes it. */
    onadd: (source: SourceDto, area?: string) => void | Promise<void>;
    oncancel: () => void;
  } = $props();

  /** A database source with no columns has no record: Run finds them. */
  const ready = (draft: SourceDto) => !(isSql(draft) && draft.fields.length === 0);
</script>

<NewObject
  {open}
  noun="source"
  {unavailable}
  types={SOURCE_TYPES}
  kinds={SOURCE_KINDS}
  blank={blankSource}
  {typeLabel}
  {typeOf}
  {ready}
  {from}
  {areas}
  {takenIn}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft, area)}
    <SourceFields source={draft} {secrets} {kafka} {readAt} {onsaved} {area} />
  {/snippet}
</NewObject>
