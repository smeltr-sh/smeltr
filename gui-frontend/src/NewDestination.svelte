<!--
  Adding a destination (`NewObject`), with a destination's fields.
-->
<script lang="ts">
  import NewObject from './NewObject.svelte';
  import DestinationFields from './DestinationFields.svelte';
  import { DESTINATION_TYPES, blankDestination, typeLabel } from './destinations';
  import type { DestinationDto, SecretDto } from './api';

  let {
    open,
    unavailable,
    secrets,
    mechanisms,
    onadd,
    oncancel,
  }: {
    /** Kept mounted and closed rather than removed, so the dialog closes and focus goes back to what opened it. */
    open: boolean;
    /** Every id the new one may not take. */
    unavailable: string[];
    secrets: SecretDto[];
    mechanisms: string[];
    /** Takes the destination. The dialog stays open until its owner closes it. */
    onadd: (destination: DestinationDto) => void | Promise<void>;
    oncancel: () => void;
  } = $props();
</script>

<NewObject
  {open}
  noun="destination"
  {unavailable}
  types={DESTINATION_TYPES}
  blank={blankDestination}
  {typeLabel}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft)}
    <DestinationFields destination={draft} {secrets} {mechanisms} />
  {/snippet}
</NewObject>
