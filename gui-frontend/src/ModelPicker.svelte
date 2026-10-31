<!--
  The model and effort the next answer is made with, as one chip under the message box. Its menu offers the
  newest few models, the effort, and every other model the key reaches, the way a chat app does: the common choice
  is one press, the rest one press further.
-->
<script lang="ts">
  import ChipPicker from './ChipPicker.svelte';
  import type { AgentModelDto, Effort } from './api';

  let {
    models,
    model = $bindable(),
    effort = $bindable(),
  }: { models: AgentModelDto[]; model: string; effort: Effort | '' } = $props();

  /** Each effort, by what a person picks. */
  const EFFORTS: { value: Effort; label: string }[] = [
    { value: 'low', label: 'Low' },
    { value: 'medium', label: 'Medium' },
    { value: 'high', label: 'High' },
    { value: 'xhigh', label: 'Extra high' },
    { value: 'max', label: 'Max' },
  ];

  let open = $state(false);
  let view = $state<'models' | 'effort' | 'more'>('models');

  /** A model's name without its maker's, which every one of them shares. */
  const short = (m: AgentModelDto) => m.name.replace(/^Claude /, '');
  const picked = $derived(models.find((m) => m.id === model));
  /** A model's line and version, from its name: "Claude Opus 5.5" is line "Claude Opus #", version [5, 5]. */
  function parts(m: AgentModelDto): { line: string; version: number[] } {
    const found = m.name.match(/\d+(?:\.\d+)*/);
    return found
      ? { line: m.name.replace(found[0], '#'), version: found[0].split('.').map(Number) }
      : { line: m.name, version: [] };
  }
  const newer = (a: number[], b: number[]) => {
    for (let i = 0; i < Math.max(a.length, b.length); i++)
      if ((a[i] ?? 0) !== (b[i] ?? 0)) return (a[i] ?? 0) > (b[i] ?? 0);
    return false;
  };
  /** Each line's newest model; an older version of one is folded under More models, and so is nothing else. */
  const first = $derived.by(() => {
    const newest = new Map<string, AgentModelDto>();
    for (const m of models) {
      const { line, version } = parts(m);
      const held = newest.get(line);
      if (!held || newer(version, parts(held).version)) newest.set(line, m);
    }
    const top = models.filter((m) => newest.get(parts(m).line) === m);
    // The model picked stays in view even when a newer one of its line is listed.
    return picked && !top.includes(picked) ? [...top, picked] : top;
  });
  /** The efforts the picked model takes, where its provider says; every one where it does not. */
  const efforts = $derived(
    picked?.efforts ? EFFORTS.filter((e) => picked.efforts!.includes(e.value)) : EFFORTS,
  );
  const effortLabel = $derived(EFFORTS.find((e) => e.value === effort)?.label ?? 'Default');

  // A model that does not take the effort picked falls back to its provider's default.
  $effect(() => {
    if (effort && !efforts.some((e) => e.value === effort)) effort = '';
  });

  function choose(id: string) {
    model = id;
    open = false;
  }
</script>

<ChipPicker
  bind:open
  kind="model"
  compact
  ariaLabel="Model and effort"
  onopen={() => (view = 'models')}
>
  {#snippet face()}<span>{picked ? short(picked) : model}</span><span class="dim"
      >{effortLabel} effort</span
    ><span class="caret" aria-hidden="true">&#9662;</span>{/snippet}

  <div class="menu">
    {#if view === 'models'}
      {#each first as m (m.id)}
        <button type="button" class="pick" class:on={m.id === model} onclick={() => choose(m.id)}>
          <span class="grow">{short(m)}</span>{#if m.id === model}<span aria-hidden="true">✓</span
            >{/if}
        </button>
      {/each}
      <div class="seam"></div>
      <button type="button" class="pick" onclick={() => (view = 'effort')}>
        <span class="grow">Effort</span><span class="dim">{effortLabel}</span><span
          aria-hidden="true">›</span
        >
      </button>
      {#if models.length > first.length}
        <div class="seam"></div>
        <button type="button" class="pick" onclick={() => (view = 'more')}>
          <span class="grow">More models</span><span aria-hidden="true">›</span>
        </button>
      {/if}
    {:else if view === 'effort'}
      <button type="button" class="pick back" onclick={() => (view = 'models')}>
        <span aria-hidden="true">‹</span><span class="grow">Effort</span>
      </button>
      <div class="pick-note">How long the model thinks before it answers.</div>
      <button
        type="button"
        class="pick"
        class:on={effort === ''}
        onclick={() => ((effort = ''), (open = false))}
      >
        <span class="grow">Default</span>{#if effort === ''}<span aria-hidden="true">✓</span>{/if}
      </button>
      {#each efforts as e (e.value)}
        <button
          type="button"
          class="pick"
          class:on={effort === e.value}
          onclick={() => ((effort = e.value), (open = false))}
        >
          <span class="grow">{e.label}</span>{#if effort === e.value}<span aria-hidden="true"
              >✓</span
            >{/if}
        </button>
      {/each}
    {:else}
      <button type="button" class="pick back" onclick={() => (view = 'models')}>
        <span aria-hidden="true">‹</span><span class="grow">More models</span>
      </button>
      <div class="all">
        {#each models as m (m.id)}
          <button type="button" class="pick" class:on={m.id === model} onclick={() => choose(m.id)}>
            <span class="grow">{short(m)}</span>{#if m.id === model}<span aria-hidden="true">✓</span
              >{/if}
          </button>
        {/each}
      </div>
    {/if}
  </div>
</ChipPicker>

<style>
  .menu {
    min-width: 14rem;
    padding-block: 0.25rem;
  }
  .grow {
    flex: 1 1 auto;
  }
  .seam {
    margin: 0.25rem 0.7rem;
    border-top: 1px solid var(--border);
  }
  .back {
    font-weight: 600;
  }
  .all {
    max-height: 16rem;
    overflow: auto;
  }
</style>
