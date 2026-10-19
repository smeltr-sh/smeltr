<!--
  Why a save would refuse `pattern` as a line breaker, said under the box it is typed in, or nothing while it would
  take it. The server judges, as the save does.
-->
<script lang="ts">
  import { errorText, lineBreakerVerdict } from './api';
  import { untrack } from 'svelte';
  import { SvelteMap } from 'svelte/reactivity';

  let { pattern }: { pattern: string } = $props();

  // Each pattern's verdict, by its text: why a save would refuse it, or `null` when it would take it. Asked once a
  // pattern has sat still a moment, since every keystroke before that is a half-typed one. An answer is kept under the
  // pattern it was asked for, so one arriving after a later keystroke cannot land on the wrong text. An unreachable
  // server leaves no verdict, and the save still checks.
  let verdicts = new SvelteMap<string, string | null>();
  $effect(() => {
    const asking = pattern;
    if (asking.trim() === '' || untrack(() => verdicts.has(asking))) return;
    const timer = setTimeout(() => {
      lineBreakerVerdict(asking)
        .then((v) => verdicts.set(asking, v.ok ? null : errorText(v.errors)))
        .catch(() => {});
    }, 400);
    return () => clearTimeout(timer);
  });
</script>

{#if verdicts.get(pattern)}
  <div class="refused">{verdicts.get(pattern)}</div>
{/if}

<style>
  .refused {
    color: var(--error);
    font-size: var(--text-note);
    line-height: 1.4;
    max-width: 34rem;
  }
</style>
