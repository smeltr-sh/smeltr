<!--
  One id that an import or a move finds taken where it goes: what the two versions differ in, both versions in full,
  and the answer, which keeps the incoming one, keeps the existing one, or keeps both with the incoming one renamed.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import { PART_NAMES, type ImportListed } from './api';
  import { diffObjects, formatDiffValue } from './diff';

  let {
    listed,
    index,
    keep = $bindable(),
    rename = $bindable(),
    disabled,
    incoming,
    existing,
    keepExisting,
    sharedNote,
  }: {
    listed: ImportListed;
    /** Its place among the taken ids, which names its list for a screen reader. */
    index: number;
    keep: '' | 'imported' | 'here' | 'renamed';
    rename: string | undefined;
    disabled: boolean;
    /** What the incoming one is called: `imported`, or `moved`. */
    incoming: string;
    /** Where the existing one is, as the diff's heading says it: `here`, or the area moved to. */
    existing: string;
    /** The answer that keeps the existing one: `Here`, or the area moved to's. */
    keepExisting: string;
    /** Why only the existing one or both can be kept, when shared holds it and the objects go into a group. */
    sharedNote: string;
  } = $props();

  const capitalized = (word: string) => word.charAt(0).toUpperCase() + word.slice(1);
  /** A taken object by its name and the id that is taken, or by its id when that is its name. */
  const label = $derived(
    listed.name && listed.name !== listed.id
      ? `${PART_NAMES[listed.part]} ${listed.name} (${listed.id})`
      : `${PART_NAMES[listed.part]} ${listed.id}`,
  );
  /** Each top-level field the two versions differ in, from the existing one to the incoming one. */
  const differs = $derived.by(() => {
    const parsed = (text: string | undefined) =>
      text === undefined ? undefined : JSON.parse(text);
    return diffObjects(parsed(listed.here), parsed(listed.imported)).filter(
      (e) => e.kind !== 'unchanged',
    );
  });
</script>

<section class="taken-id" aria-label={label}>
  <h4>{label}</h4>
  {#if listed.inShared}
    <p class="dim">{sharedNote}</p>
  {/if}
  <h5 id="differs-{index}">What differs, {existing} &rarr; {incoming}</h5>
  <ul class="differs" aria-labelledby="differs-{index}">
    {#each differs as entry (entry.key)}
      <li class="mono">
        <span class="diff-key">{entry.key}:</span>
        {#if entry.kind !== 'added'}
          <span class="removed">{formatDiffValue(entry.before)}</span>
        {/if}
        {#if entry.kind === 'changed'}
          <span class="dim">&rarr;</span>
        {/if}
        {#if entry.kind !== 'removed'}
          <span class="added">{formatDiffValue(entry.after)}</span>
        {/if}
      </li>
    {:else}
      <li class="dim">Only the contents of the lookup file differ.</li>
    {/each}
  </ul>
  <details>
    <summary>Both versions</summary>
    <div class="versions">
      {#each [[capitalized(existing), listed.here], [capitalized(incoming), listed.imported]] as [side, text] (side)}
        <div role="group" aria-label={side}>
          <h5>{side}</h5>
          <pre>{text}</pre>
        </div>
      {/each}
    </div>
  </details>
  <div class="answer">
    <Field label="Keep">
      <select bind:value={keep} {disabled}>
        <option value="" disabled>Choose</option>
        {#if !listed.inShared}
          <option value="imported">{capitalized(incoming)}</option>
        {/if}
        <option value="here">{keepExisting}</option>
        <option value="renamed">Both, rename {incoming}</option>
      </select>
    </Field>
    {#if keep === 'renamed'}
      <Field label="New id" required>
        <input class="mono" bind:value={rename} {disabled} />
      </Field>
    {/if}
  </div>
</section>

<style>
  .mono,
  pre {
    font-family: var(--mono);
  }
  h4,
  h5,
  p {
    margin: 0;
  }
  .taken-id {
    display: grid;
    gap: 0.6rem;
  }
  :global(.taken-id + .taken-id) {
    border-top: 1px solid var(--border);
    padding-top: 0.9rem;
  }
  .differs {
    margin: 0;
    padding: 0;
    list-style: none;
    overflow-wrap: anywhere;
  }
  .diff-key {
    color: var(--dim);
    margin-right: 0.4rem;
  }
  .added {
    color: var(--added);
  }
  .removed {
    color: var(--removed);
  }
  summary {
    cursor: pointer;
  }
  .versions {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 0.75rem;
  }
  pre {
    margin: 0;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    max-height: 20rem;
    overflow: auto;
  }
  .answer {
    display: flex;
    gap: 1rem;
    align-items: flex-end;
  }
  @media (max-width: 800px) {
    .versions {
      grid-template-columns: minmax(0, 1fr);
    }
  }
</style>
