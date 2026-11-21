<!--
  Adding a destination (`NewObject`), with a destination's fields.
-->
<script lang="ts">
  import NewObject from './NewObject.svelte';
  import DestinationFields from './DestinationFields.svelte';
  import { DESTINATION_TYPES, blankDestination, typeLabel } from './destinations';
  import type { Area } from './areas';
  import type { DestinationDto, KafkaFacts, SecretDto } from './api';

  let {
    open,
    unavailable,
    secrets,
    kafka,
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
    /** A destination to copy (`NewObject`). */
    from?: DestinationDto;
    /** Where a copy may go (`NewObject`). */
    areas?: Area[];
    takenIn?: (area: string) => Promise<string[]>;
    /** Takes the destination, and the area picked for a copy. The dialog stays open until its owner closes it. */
    onadd: (destination: DestinationDto, area?: string) => void | Promise<void>;
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
  {from}
  {areas}
  {takenIn}
  {onadd}
  {oncancel}
>
  {#snippet fields(draft)}
    <DestinationFields destination={draft} {secrets} {kafka} />
  {/snippet}
</NewObject>
