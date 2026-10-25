<!--
  Which branch an edit goes on (W1). `main` is what deploys move, so a write
  aimed at it stops here and asks: the writer names a branch, or says no and
  nothing is written.

  Drawn by `Dialog`, so the focus trap, Escape and the top layer come from
  the platform. It is a question with one answer, which is what a dialog is
  for -- unlike a settings modal, whose OK would be a second commit point
  beside the page's own Save (STYLE_GUIDE.md, "Advanced settings").
-->
<script lang="ts">
  import { untrack } from 'svelte';
  import Dialog from './Dialog.svelte';
  import Field from './Field.svelte';

  let {
    open = false,
    suggested = '',
    onanswer,
  }: {
    open?: boolean;
    suggested?: string;
    onanswer: (branch: string | null) => void;
  } = $props();

  let name = $state('');

  // Each time it opens, so a second edit on main starts from its own suggestion rather than from
  // what was typed the last time; a suggestion that moves while it is open leaves the typing alone.
  $effect(() => {
    if (open) name = untrack(() => suggested);
  });

  function submit() {
    if (name.trim()) onanswer(name.trim());
  }
</script>

<Dialog {open} title="branch required" oncancel={() => onanswer(null)} onsubmit={submit}>
  <div class="body">
    <p>
      <code class="mono">main</code> is what deploys move, so it is never edited directly. Name the branch
      to put this on &mdash; an existing one carries on where it left off.
    </p>
    <Field required label="branch">
      <!-- svelte-ignore a11y_autofocus -->
      <input bind:value={name} autofocus spellcheck="false" aria-label="branch to edit on" />
    </Field>
  </div>
  {#snippet actions()}
    <button type="button" onclick={() => onanswer(null)}>Don't save</button>
    <button type="submit" class="primary" disabled={!name.trim()}>Edit on this branch</button>
  {/snippet}
</Dialog>

<style>
  .body {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }

  p {
    margin: 0;
    color: var(--ink);
    font-size: var(--text-body);
    line-height: 1.5;
  }

  input {
    font-family: var(--mono);
  }
</style>
