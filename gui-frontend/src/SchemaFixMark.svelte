<!--
  A warning a field's type answers, marked on the field's own line: hovering or pressing the mark shows the warning,
  and a button that gives the field the type it asks for.
-->
<script module lang="ts">
  let seq = 0;
</script>

<script lang="ts">
  import Button from './Button.svelte';
  import Popover from './Popover.svelte';
  import StateMark from './StateMark.svelte';
  import Ticked from './Ticked.svelte';

  let {
    stated,
    label,
    onfix,
  }: {
    /** The warning, without the sentence that says to write the type. */
    stated: string;
    /** The button's text. */
    label: string;
    onfix: () => void;
  } = $props();

  const id = `schema-fix-${++seq}`;
  let mark: HTMLSpanElement;
  let pop: Popover;
  let leaving: ReturnType<typeof setTimeout> | undefined;

  function show() {
    clearTimeout(leaving);
    pop.toggle(true);
  }
  // Long enough to cross from the mark to the note.
  function hide() {
    leaving = setTimeout(() => pop.toggle(false), 250);
  }
</script>

<span
  bind:this={mark}
  class="mark"
  role="button"
  tabindex="0"
  aria-label={stated}
  aria-controls={id}
  onmouseenter={show}
  onmouseleave={hide}
  onclick={show}
  onkeydown={(e) => {
    if (e.key !== 'Enter' && e.key !== ' ') return;
    e.preventDefault();
    pop.toggle();
  }}><StateMark state="warning" size={15} /></span
>
<Popover bind:this={pop} {id} anchor={() => mark}>
  <div class="note" role="presentation" onmouseenter={show} onmouseleave={hide}>
    <span><Ticked text={stated} /></span>
    <Button
      type="button"
      class="compact"
      onclick={() => {
        pop.toggle(false);
        onfix();
      }}>{label}</Button
    >
  </div>
</Popover>

<style>
  .mark {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    padding: 0 0.25rem;
    cursor: pointer;
  }

  .note {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    gap: 0.5rem;
  }
</style>
