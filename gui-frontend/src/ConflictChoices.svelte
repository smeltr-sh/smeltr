<!--
  What bringing a branch current needs a side chosen for. Each conflict is a
  file, or one object of a section file, that main and this branch both
  changed differently; the author keeps one side. Nothing is written until
  every conflict has a side and Apply sends them, against the same two tips
  they were shown at.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import Panel from './Panel.svelte';
  import type { MergeConflicts, MergeSide } from './api';
  import { conflictLabel } from './conflictLabel';

  let {
    conflicts,
    busy = false,
    onapply,
    oncancel,
  }: {
    conflicts: MergeConflicts;
    busy?: boolean;
    onapply: (sides: MergeSide[]) => void;
    oncancel: () => void;
  } = $props();

  let sides = $state<(MergeSide | '')[]>([]);
  $effect.pre(() => {
    sides = conflicts.conflicts.map(() => '');
  });
  let ready = $derived(sides.length > 0 && sides.every((s) => s !== ''));
</script>

<Panel title="Conflicts">
  {#snippet actions()}
    <div class="actions">
      <button type="button" disabled={busy} onclick={oncancel}>Cancel</button>
      <button
        type="button"
        class="primary"
        disabled={!ready || busy}
        onclick={() => onapply(sides as MergeSide[])}>{busy ? 'Applying…' : 'Apply'}</button
      >
    </div>
  {/snippet}
  {#each conflicts.conflicts as conflict, i (`${conflict.path}\u0000${conflict.id ?? ''}`)}
    <section class="conflict">
      <h3>{conflictLabel(conflict)}</h3>
      <div class="versions">
        {#each [['Base', conflict.base], ['Main', conflict.main], ['This branch', conflict.work]] as [label, text] (label)}
          <div class="version" role="group" aria-label={label}>
            <h4>{label}</h4>
            {#if text === null}<p class="absent">Absent</p>{:else}<pre>{text}</pre>{/if}
          </div>
        {/each}
      </div>
      <Field label="Keep">
        <select bind:value={sides[i]} disabled={busy}>
          <option value="" disabled>Choose</option>
          <option value="main">Main</option>
          <option value="work">This branch</option>
        </select>
      </Field>
    </section>
  {/each}
</Panel>

<style>
  .actions {
    display: flex;
    gap: 0.5rem;
  }
  .conflict {
    display: grid;
    gap: 0.6rem;
  }
  .conflict + .conflict {
    border-top: 1px solid var(--border);
    padding-top: 0.9rem;
  }
  h3,
  h4 {
    margin: 0;
  }
  pre {
    font-family: var(--mono);
  }
  .absent {
    margin: 0;
    color: var(--dim);
  }
  .versions {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.75rem;
  }
  pre {
    margin: 0;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    max-height: 20rem;
    overflow: auto;
  }
  @media (max-width: 800px) {
    .versions {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
