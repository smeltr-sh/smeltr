<!--
  The records a workspace runs over, chosen from one menu: a saved sample, the capture the leader
  holds unsaved, or a new sample, which Add sample makes. It has a search, since a fleet's samples
  run to the hundreds.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import { count, dateTime } from './time';
  import { capturedAt, type SampleSummary } from './api';
  import { HELD_NAME } from './sampleChoices.svelte';

  let {
    samples,
    selected,
    lastUsed = '',
    records,
    held,
    tests = [],
    test = '',
    ontest,
    onpick,
    onadd,
  }: {
    samples: SampleSummary[];
    /** The sample the records on screen came from. */
    selected: string;
    /** The sample this pipeline last ran over, which the list puts first. */
    lastUsed?: string;
    /** How many records it holds, once run. */
    records: number | null;
    /** The capture the leader holds for the person signed in, listed first among the samples. Picking it passes
     * `choice` to `onpick`. */
    held?: { choice: string; records: number; source?: string };
    /** The tests that run here, each over its sample: a pipeline's in its editor, the end-to-end ones in Route a
     * sample. */
    tests?: { id: string; name: string; sample: string }[];
    /** The test on screen, if there is one: the grid shows its sample's records, with the fields and rows the test
     * checks. */
    test?: string;
    ontest?: (id: string) => void;
    onpick: (id: string) => void;
    /** Opens Add sample to make a new sample; absent where the person may make none. */
    onadd?: () => void;
  } = $props();

  let open = $state(false);
  let query = $state('');

  let selectedName = $derived(
    held?.choice === selected
      ? HELD_NAME
      : (samples.find((s) => s.id === selected)?.name ?? selected),
  );

  // -- samples -------------------------------------------------------------

  let saved = $derived(
    [...samples].sort(
      (a, b) =>
        Number(b.id === lastUsed) - Number(a.id === lastUsed) ||
        (b.written ?? 0) - (a.written ?? 0),
    ),
  );
  /** A row of the list, saying after its name what it holds and where it is from. */
  const row = (
    id: string,
    name: string,
    records: number,
    source: string | undefined,
    ...notes: (string | false)[]
  ) => ({
    id,
    name,
    source,
    said: [count(records, 'record'), source, ...notes].filter(Boolean).join(' · '),
  });
  /** The held capture, then the saved samples. */
  let listed = $derived([
    ...(held ? [row(held.choice, HELD_NAME, held.records, held.source, 'not saved')] : []),
    ...saved.map((s) => {
      const at = capturedAt(s);
      return row(
        s.id,
        s.name,
        s.records,
        s.source,
        at !== undefined && dateTime(at / 1000),
        s.id === lastUsed && 'last used',
      );
    }),
  ]);

  let q = $derived(query.trim().toLowerCase());
  let testName = $derived(tests.find((t) => t.id === test)?.name);
  const sampleName = (id: string) => samples.find((s) => s.id === id)?.name ?? id;
  let foundTests = $derived(
    tests.filter((t) => `${t.name} ${sampleName(t.sample)}`.toLowerCase().includes(q)),
  );
  let found = $derived(
    listed.filter((s) => `${s.name} ${s.source ?? ''}`.toLowerCase().includes(q)),
  );
</script>

<ChipPicker
  kind="columns"
  align="left"
  bind:open
  ariaLabel="Choose a sample"
  onopen={() => (query = '')}
>
  {#snippet face()}
    {#if testName}
      <span class="dim">Test</span>
      <strong>{testName}</strong>
    {:else}
      <span class="dim">Sample</span>
      <strong>{selectedName}</strong>
    {/if}
    {#if records !== null}<span class="dim">{count(records, 'record')}</span>{/if} ▾
  {/snippet}
  {#snippet children()}
    <div class="menu">
      {#if onadd}
        <button
          type="button"
          class="add"
          onclick={() => {
            open = false;
            onadd();
          }}>Add sample</button
        >
      {/if}
      <!-- svelte-ignore a11y_autofocus -->
      <input
        type="search"
        bind:value={query}
        placeholder={tests.length ? 'Find a test or sample' : 'Find a sample'}
        aria-label={tests.length ? 'Find a test or sample' : 'Find a sample'}
        autofocus
      />
      {#if foundTests.length}
        <div class="list" role="listbox" aria-label="Tests">
          <span class="group">Tests</span>
          {#each foundTests as t (t.id)}
            <button
              type="button"
              class="item"
              role="option"
              aria-selected={t.id === test}
              onclick={() => {
                open = false;
                if (t.id !== test) ontest?.(t.id);
              }}
            >
              <span class="name ellipsis">{t.name}</span>
              <span class="dim">{sampleName(t.sample)}</span>
            </button>
          {/each}
        </div>
      {/if}
      <div class="list" role="listbox" aria-label="Samples">
        {#if foundTests.length}<span class="group">Samples</span>{/if}
        {#each found as s (s.id)}
          <button
            type="button"
            class="item"
            role="option"
            aria-selected={!testName && s.id === selected}
            onclick={() => {
              open = false;
              // Picking the sample already chosen changes nothing. While a test is chosen, picking any sample, the
              // test's own included, closes the test and shows that sample.
              if (s.id !== selected || testName) onpick(s.id);
            }}
          >
            <span class="name ellipsis">{s.name}</span>
            <span class="dim">{s.said}</span>
          </button>
        {/each}
      </div>
    </div>
  {/snippet}
</ChipPicker>

<style>
  .menu {
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
    width: min(30rem, 80vw);
    padding: 0.5rem;
  }

  .dim {
    color: var(--dim);
    font-weight: 400;
  }

  .add {
    align-self: flex-end;
  }

  .list {
    display: flex;
    flex-direction: column;
    max-height: 22rem;
    overflow-y: auto;
  }

  .group {
    padding: 0.3rem 0.4rem 0.1rem;
    color: var(--dim);
    font-size: var(--text-label);
    font-weight: 600;
  }

  .item {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
    border: none;
    border-radius: var(--radius);
    background: none;
    padding: 0.3rem 0.4rem;
    font: inherit;
    font-size: var(--text-label);
    color: var(--ink);
    text-align: left;
    cursor: pointer;
  }

  .item:hover,
  .item[aria-selected='true'] {
    background: color-mix(in srgb, var(--ink) 8%, transparent);
  }

  .item[aria-selected='true'] .name {
    font-weight: 600;
  }

  .name {
    flex: 1;
    min-width: 0;
  }
</style>
