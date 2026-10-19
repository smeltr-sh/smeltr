<!--
  Adding a source (`NewObject`), its kind asked first, with a source's fields.
-->
<script lang="ts">
  import NewObject from './NewObject.svelte';
  import SourceFields from './SourceFields.svelte';
  import { SOURCE_KINDS } from './sourceKinds';
  import { SOURCE_TYPES, blankSource, isSql, typeLabel } from './sources';
  import type { IdentityDto, SecretDto, SourceDto } from './api';

  let {
    open,
    unavailable,
    identities,
    secrets,
    mechanisms,
    readAt,
    onsaved,
    onadd,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    /** Every id the new one may not take. */
    unavailable: string[];
    identities: IdentityDto[];
    secrets: SecretDto[];
    mechanisms: string[];
    /** The commit the caller read, for what `SourceFields` saves on its own. */
    readAt: string | null;
    onsaved: (commit: string) => void;
    /** Takes the source. The dialog stays open until its owner closes it. */
    onadd: (source: SourceDto) => void | Promise<void>;
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
  {ready}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft)}
    <SourceFields source={draft} {identities} {secrets} {mechanisms} {readAt} {onsaved} />
  {/snippet}
</NewObject>
