<!--
  What is unsaved and not on screen. An edit outlives the page that made it
  (`edits.ts`), so without this a reader has no way to find work they left
  behind, and the leave warning speaks about files they cannot name.

  Shown only when there is something to act on, the call STYLE_GUIDE.md, "The
  branch" makes for the shell's other row: a control that is always there and
  usually dead teaches people to stop reading it.

  A row sets the group before the hash. `nav.ts` has the rule -- landing on the
  right editor in the wrong group is the mistake the rail exists to catch -- and
  a record carries the group it belongs to, which need not be the one in hand.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import { EDITS_EVENT, heldLabel, listHeld, type Held } from './edits';
  import { editorForHeld } from './nav';
  import { setScope, SHARED_SCOPE } from './api';

  let open = $state(false);
  let held: Held[] = $state(listHeld());

  $effect(() => {
    const sync = () => (held = listHeld());
    addEventListener(EDITS_EVENT, sync);
    return () => removeEventListener(EDITS_EVENT, sync);
  });

  /** What is held, by the same names the list below gives: the first two,
   * then how many more, so the shell's row keeps its width. */
  let named = $derived(
    held.slice(0, 2).map(heldLabel).join(', ') + (held.length > 2 ? ` +${held.length - 2}` : ''),
  );

  /** Opens the page the edit belongs to, in the group it belongs to. */
  function go(record: Held) {
    const hash = editorForHeld(record.file);
    if (!hash) return;
    open = false;
    setScope(record.scope, hash);
  }
</script>

{#if held.length > 0}
  <ChipPicker bind:open kind="scope" align="left" title="unsaved work">
    {#snippet face()}
      <span class="dot"></span>
      <span class="dim">Unsaved</span>
      <span>{named}</span>
      <span class="caret" aria-hidden="true">&#9662;</span>
    {/snippet}

    {#each held as record (record.branch + record.scope + record.file)}
      <div class="row">
        <button type="button" class="open" onclick={() => go(record)}>
          <span class="mono">{heldLabel(record)}</span>
          <span class="dim">{record.scope === SHARED_SCOPE ? 'shared' : record.scope}</span>
        </button>
      </div>
    {/each}
  </ChipPicker>
{/if}

<style>
  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--accent);
  }

  .row {
    display: flex;
  }

  .open {
    flex: 1;
    display: flex;
    gap: 0.6rem;
    align-items: baseline;
    background: none;
    border: none;
    color: inherit;
    text-align: left;
    padding: 0.4rem 0.6rem;
    cursor: pointer;
    font-size: var(--text-body);
  }

  .open:hover {
    background: var(--hover, rgb(255 255 255 / 0.06));
  }

  .mono {
    font-family: var(--mono);
  }
</style>
