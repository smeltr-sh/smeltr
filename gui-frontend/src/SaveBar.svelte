<!--
  A persistent Save affordance, fixed to the bottom of the viewport so it
  survives a page long enough to scroll its own header out of view.
  STYLE_GUIDE.md, "Bottom save bar".

  Shown only while there is something to save: a control that is always on
  screen and usually inert teaches people to stop reading it (STYLE_GUIDE.md,
  "The branch" makes the same call for the branch chip's own waiting-work
  row).
-->
<script lang="ts">
  import { currentBranch, currentScope } from './api';
  import { editable } from './access';
  import { keepClear } from './keepClear';

  let {
    dirty,
    saving = false,
    label = 'Save',
    refusals = [],
    onsave,
    ondiscard,
  }: {
    dirty: boolean;
    /** What the load would refuse in this save, each in a sentence. Any one the saved config lacks holds Save back. */
    refusals?: string[];
    saving?: boolean;
    label?: string;
    onsave: () => void;
    /** Reverts to what was last saved. Omitted where there is nothing to revert to yet
     * (a pristine "new" page with no save of its own to fall back on). */
    ondiscard?: () => void;
  } = $props();

  /** Which file this Save writes: the branch and the group. An id is only unique within a
   * group, and a group's bundle only exists on a branch, so the same page and the same row
   * name a different file under each pair -- and nothing else on screen says which pair is in
   * hand once the chips are scrolled past.
   *
   * Read from `api.ts`, not handed down: those two are the values `branched()` puts on the
   * call this button makes, so this cannot say one thing while the write does another. Read
   * again each time the bar appears, which is the moment the question is live. */
  const writesTo = $derived(dirty ? `${currentBranch()} · ${currentScope()}` : '');

  /** What the saved config already holds: the server takes a save that adds no refusal, so only a new one holds Save
   * back. */
  let saved: string[] = $state([]);
  $effect.pre(() => {
    if (!dirty) saved = refusals;
  });
  const added = $derived(refusals.filter((r) => !saved.includes(r)));
</script>

<!-- Never where the scope is not the person's to change: the leader would refuse the save. -->
{#if dirty && editable()}
  <div class="save-bar" {@attach keepClear('bottom')}>
    {#if added.length > 0}
      <div class="save-bar-row refusals" role="alert">
        <strong>Save would be refused</strong>
        {#each added as r (r)}<span>{r}</span>{/each}
      </div>
    {/if}
    <div class="save-bar-row">
      <span class="grow"></span>
      <span class="writes-to">Saving to <code>{writesTo}</code></span>
      <span class="dirty-flag"><span class="dot"></span>Unsaved</span>
      {#if ondiscard}
        <button type="button" onclick={ondiscard} disabled={saving}>Discard</button>
      {/if}
      <button
        type="button"
        class="primary"
        data-guide="save"
        onclick={onsave}
        disabled={saving || added.length > 0}>{saving ? 'Saving…' : label}</button
      >
    </div>
  </div>
{/if}

<style>
  .save-bar {
    position: fixed;
    left: 0;
    right: 0;
    /* Above the agent panel while it is open. */
    bottom: var(--agent-panel, 0);
    z-index: 15;
    background: var(--panel);
    border-top: 1px solid var(--border);
  }

  .save-bar-row {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    max-width: var(--measure, var(--width-page));
    margin-inline: auto;
    padding: 0.75rem 1.5rem;
  }

  .refusals {
    flex-direction: column;
    align-items: start;
    gap: 0.2rem;
    padding-bottom: 0;
    font-size: var(--text-label);
  }
  .refusals strong {
    color: var(--error);
  }

  .grow {
    flex: 1;
  }

  .dirty-flag {
    display: inline-flex;
    align-items: center;
    gap: 0.4rem;
    color: var(--dim);
    font-size: var(--text-note);
  }

  /* Quiet: a fact to check before pressing Save, not a warning. Truncates rather than pushing
     the buttons off a narrow viewport -- the same call `ScopePicker` makes for a long group
     name. */
  .writes-to {
    color: var(--dim);
    font-size: var(--text-note);
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .writes-to code {
    font-family: var(--mono);
    color: var(--ink);
  }

  .dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--accent);
  }
</style>
