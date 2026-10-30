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
  import {
    MAIN_BRANCH,
    SHARED_SCOPE,
    configAt,
    type MergeConflict,
    type MergeConflicts,
    type MergeSide,
  } from './api';
  import { conflictLabel } from './conflictLabel';
  import { scopeOf } from './objectChanges';
  import { routeNameIn } from './routeFlow';

  let {
    branch,
    conflicts,
    busy = false,
    onapply,
    oncancel,
  }: {
    branch: string;
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

  const key = (c: MergeConflict) => `${c.path}\u0000${c.id ?? ''}`;

  /** Each conflicting route's `routeName`, by `key`: as this branch has it, or as main does where the branch removed
   * it. */
  let routeNames = $state<Map<string, string>>(new Map());
  $effect(() => {
    nameRoutes(conflicts);
  });

  async function nameRoutes(at: MergeConflicts) {
    // Without a bundle the label is just `Route`; the JSON beneath it still shows which.
    const read = (onBranch: string, scope: string, commit: string) =>
      configAt(onBranch, scope, commit).then(
        (res) => res.config,
        () => null,
      );
    /** The route's name in `scope` at `commit`, its group's counted after `shared/`'s, as the group runs them. */
    const nameAt = async (onBranch: string, commit: string, scope: string, id: string) => {
      const [own, shared] = await Promise.all([
        read(onBranch, scope, commit),
        scope === SHARED_SCOPE ? null : read(onBranch, SHARED_SCOPE, commit),
      ]);
      return own ? routeNameIn(own, id, shared) : undefined;
    };
    const named = await Promise.all(
      at.conflicts.map(async (c) => {
        if (c.id === undefined || !c.path.endsWith('/routes.json')) return null;
        const scope = scopeOf(c.path);
        const name =
          (await nameAt(branch, at.work, scope, c.id)) ??
          (await nameAt(MAIN_BRANCH, at.main, scope, c.id));
        return name === undefined ? null : ([key(c), name] as const);
      }),
    );
    if (at === conflicts) routeNames = new Map(named.filter((n) => n !== null));
  }
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
  {#each conflicts.conflicts as conflict, i (key(conflict))}
    <section class="conflict">
      <h3>{conflictLabel(conflict, routeNames.get(key(conflict)))}</h3>
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
