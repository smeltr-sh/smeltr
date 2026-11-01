<!--
  Which bundle an authoring page is editing: `shared`, the one every group draws on, or one
  group's own (design doc, Control plane). Switching mounts the page afresh (`App.svelte`): every
  config call carries the scope, and a page holds what it read.

  `SectionShell` draws this at the head of its rail, whose seam past the rail carries the color
  set here, so the control sits on its own indicator. It renders only where a scope applies
  (`nav.ts`'s `showsScope`), which is what unsets the color everywhere else: the hue is this
  component's to paint and to take away again when it goes.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import ChipPicker from './ChipPicker.svelte';
  import LevelMark from './LevelMark.svelte';
  import WorkerGroupIcon from './WorkerGroupIcon.svelte';
  import {
    SCOPE_EVENT,
    SHARED_SCOPE,
    currentScope,
    leaveScope,
    listWorkerGroups,
    setScope,
    type WorkerGroupDto,
  } from './api';
  import { isAdministrator, levelIn } from './access';
  import { lastScopeRead, rememberScopeRead, scopeHue } from './scope';
  import { dropScope } from './edits';

  let scope = $state(currentScope());
  let picking = $state(false);
  // Seeded from the last read, so a navigation between Build pages redraws the color it already knew rather
  // than dropping it until this mount's own read lands.
  let groups = $state<WorkerGroupDto[]>(lastScopeRead().groups);
  let sharedHue: number | null = $state(lastScopeRead().sharedHue);

  // Somebody who is not an administrator is offered only the areas they hold, each with the level
  // they hold it at, so the chip says what this page lets them do.
  const administrator = isAdministrator();
  // The face shortens it to the glyph and "Workers"; hover and a screen reader get it whole.
  const named = $derived(scope === SHARED_SCOPE ? 'Shared configuration' : `Worker group ${scope}`);
  const offered = $derived(administrator ? groups : groups.filter((g) => levelIn(g.id) !== null));

  // A scope set from a page -- a review's file link -- moves the chip and the rail with it.
  $effect(() => {
    const sync = () => (scope = currentScope());
    addEventListener(SCOPE_EVENT, sync);
    return () => removeEventListener(SCOPE_EVENT, sync);
  });

  /** What the picker lists, and where the hue on the chrome comes from -- so it is read when a
   * scoped page opens rather than only when the picker does. */
  async function load() {
    const res = await listWorkerGroups();
    groups = res.ok ? res.groups : [];
    sharedHue = res.ok ? (res.sharedHue ?? null) : null;
    if (res.ok) rememberScopeRead(groups, sharedHue);
    // A scope names a group, not a name of its own -- unlike an unsaved branch, there is no
    // "not started yet" reading for one that is gone.
    if (scope !== SHARED_SCOPE && !groups.some((g) => g.id === scope)) {
      dropScope(scope);
      leaveScope(scope);
    }
  }
  // A read refused because the session ended has already put the sign-in screen up.
  load().catch(reported);

  // The group's color, on the root so the rail's seam and this chip's dot read the same one
  // (STYLE_GUIDE.md, "Group hues"). Nothing rather than a guess before the groups arrive; and
  // taken away when this unmounts, which is how a page with no scope stays uncolored.
  $effect(() => {
    const hue = scopeHue(scope, groups, sharedHue);
    const root = document.documentElement.style;
    if (hue === null) root.removeProperty('--scope-h');
    else root.setProperty('--scope-h', String(hue));
    // `shared/`'s own, published beside it: a group's page marks what it draws on from `shared/`, and that mark
    // has to be the color `shared/` is everywhere else rather than a second one meaning the same thing.
    if (sharedHue === null) root.removeProperty('--shared-h');
    else root.setProperty('--shared-h', String(sharedHue));
    return () => {
      root.removeProperty('--scope-h');
      root.removeProperty('--shared-h');
    };
  });

  function switchScope(name: string) {
    picking = false;
    if (name !== scope) setScope(name);
  }
</script>

<ChipPicker
  bind:open={picking}
  kind="scope"
  align="left"
  title={named}
  ariaLabel={administrator ? named : `${named}, ${levelIn(scope) ?? 'no access'}`}
  onopen={load}
>
  {#snippet face()}
    {#if scope === SHARED_SCOPE}<span class="scope-name">Shared configuration</span
      >{:else}<WorkerGroupIcon /><span class="dim">Workers</span>
      <span class="scope-name">{scope}</span>{/if}{#if !administrator}{@const level =
        levelIn(scope)}{#if level}<LevelMark {level} named={false} />{:else}<span>no access</span
        >{/if}{/if}
    <span class="scope-dot" aria-hidden="true"></span>
  {/snippet}

  {#if administrator || levelIn(SHARED_SCOPE)}
    {@const level = levelIn(SHARED_SCOPE)}
    <button
      type="button"
      class="pick"
      class:on={scope === SHARED_SCOPE}
      onclick={() => switchScope(SHARED_SCOPE)}
    >
      <span>Shared configuration</span>
      <span class="dim"
        >{#if administrator}every worker group draws on this{:else if level}<LevelMark
            {level}
          />{/if}</span
      >
    </button>
  {/if}
  {#each offered as g (g.id)}
    {@const level = levelIn(g.id)}
    <button type="button" class="pick" class:on={scope === g.id} onclick={() => switchScope(g.id)}>
      <span class="mono">{g.id}</span>
      <span class="dim"
        >{#if administrator}{g.name} — {g.deployed
            ? 'live'
            : 'pending, not deployed yet'}{:else if level}<LevelMark {level} />{/if}</span
      >
    </button>
  {/each}
</ChipPicker>

<style>
  /* Truncates on its own -- the dot beside it is a fixed-size sibling, not part of the text an
     overlong group name clips into. */
  .scope-name {
    max-width: 9rem;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* The group whose bundle a Build page is editing. The page carries the same hue at the seam
     between `SectionShell`'s rail and its content. */
  .scope-dot {
    width: 8px;
    height: 8px;
    border-radius: 999px;
    background: oklch(var(--scope-l) var(--scope-c) var(--scope-h));
    flex: none;
  }
</style>
