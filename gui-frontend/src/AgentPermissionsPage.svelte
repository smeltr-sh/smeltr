<!--
  What the person signed in lets their own agent use: everything they hold, or up to a level in every area and
  another in the areas they name. The leader holds the agent to the lower of this and what the person holds, so
  each area shows what they hold beside it.
-->
<script lang="ts">
  import { reported } from './errors.svelte';
  import SectionShell from './SectionShell.svelte';
  import PageHeader from './PageHeader.svelte';
  import Field from './Field.svelte';
  import Segmented from './Segmented.svelte';
  import { LEVELS, isAdministrator, levelOf } from './access';
  import { scopeHue } from './scope';
  import {
    SHARED_SCOPE,
    getAgentPermissions,
    listWorkerGroups,
    putAgentPermissions,
    refused,
    type AgentPermissionsDto,
  } from './api';
  import { session, type AccessLevel } from './session';

  type Kind = AgentPermissionsDto['kind'];

  let loading = $state(true);
  let kind = $state<Kind>('upto');
  let everyArea = $state<AccessLevel>('viewer');
  /** `''` is an area left at the level every area gets. */
  let byArea = $state<Record<string, AccessLevel | ''>>({});
  let groups = $state<{ id: string; hue: number | null }[]>([]);
  let sharedHue = $state<number | null>(null);

  let busy = $state(false);
  let message = $state('');

  function held(area: string): AccessLevel | null {
    const s = session();
    return levelOf(s.access ?? { administrator: s.administrator, areas: {} }, area);
  }

  /** The areas the person holds, `shared` first, and any the saved permissions name that they no longer hold, so a
   * save does not drop it unseen. */
  const areas = $derived.by(() => {
    const known = [{ id: SHARED_SCOPE, hue: sharedHue }, ...groups];
    const mine = known.filter((a) => isAdministrator() || held(a.id) !== null);
    const gone = Object.keys(byArea).filter((a) => !mine.some((x) => x.id === a));
    return [...mine, ...gone.sort().map((id) => ({ id, hue: null }))];
  });

  function show(permissions: AgentPermissionsDto) {
    kind = permissions.kind;
    if (permissions.kind === 'upto') {
      everyArea = permissions.default;
      byArea = { ...permissions.areas };
    }
  }

  async function load() {
    try {
      const [permissions, listed] = await Promise.all([
        getAgentPermissions(),
        listWorkerGroups().catch(() => null),
      ]);
      show(permissions);
      if (listed?.ok) {
        sharedHue = listed.sharedHue ?? null;
        groups = listed.groups.map((g) => ({
          id: g.id,
          hue: scopeHue(g.id, listed.groups, sharedHue),
        }));
      }
    } catch (e) {
      reported(e);
    } finally {
      loading = false;
    }
  }

  $effect(() => {
    void load();
  });

  /** What the form would save: the areas given a level of their own. */
  const draft = $derived<AgentPermissionsDto>(
    kind === 'everything'
      ? { kind }
      : {
          kind,
          default: everyArea,
          areas: Object.fromEntries(
            Object.entries(byArea).filter((e): e is [string, AccessLevel] => e[1] !== ''),
          ),
        },
  );

  async function save(event: SubmitEvent) {
    event.preventDefault();
    if (busy) return;
    busy = true;
    message = '';
    try {
      const result = await putAgentPermissions(draft);
      if (refused(result)) return;
      show(result);
      message = 'agent permissions saved';
    } catch (e) {
      reported(e);
    } finally {
      busy = false;
    }
  }
</script>

<SectionShell>
  <PageHeader title="Agent permissions">
    {#snippet help()}
      <p>
        What your agent may do when it acts for you at your prompt. It never holds more than you do:
        in each area it holds the lower of your level and the one you give it here.
      </p>
    {/snippet}
  </PageHeader>

  {#if loading}
    <p>Reading your agent permissions…</p>
  {:else}
    <form class="panel" onsubmit={save}>
      <Segmented
        label="Agent permissions"
        options={[
          { value: 'upto', label: 'Up to a level' },
          { value: 'everything', label: 'Everything you hold' },
        ]}
        bind:value={kind}
      />
      {#if kind === 'upto'}
        <Field label="Every area">
          <select bind:value={everyArea}>
            {#each LEVELS as level (level)}<option value={level}>{level}</option>{/each}
          </select>
        </Field>
        <Field label="By area" group>
          {#snippet help()}
            An area set here takes its own level instead of the one every area gets.
          {/snippet}
          <div class="levels">
            {#each areas as area (area.id)}
              <span class="area-name"
                ><span
                  class="dot"
                  class:hued={area.hue !== null}
                  style:--h={area.hue}
                  aria-hidden="true"
                ></span>{area.id}</span
              >
              <span class="dim">you hold {held(area.id) ?? 'nothing'}</span>
              <select aria-label="{area.id} level" bind:value={byArea[area.id]}>
                <option value="">as every area</option>
                {#each LEVELS as level (level)}<option value={level}>{level}</option>{/each}
              </select>
            {/each}
          </div>
        </Field>
      {/if}
      {#if message}<p class="message" role="status">{message}</p>{/if}
      <div class="actions">
        <button class="primary" type="submit" disabled={busy}>Save</button>
      </div>
    </form>
  {/if}
</SectionShell>

<style>
  .panel {
    background: var(--bg);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    padding: 1rem;
    max-width: 34rem;
    display: grid;
    gap: 0.8rem;
    justify-items: start;
  }

  .panel p {
    margin: 0;
  }

  .levels {
    display: grid;
    grid-template-columns: max-content max-content 9rem;
    gap: 0.4rem 1rem;
    align-items: center;
  }

  .area-name {
    font-family: var(--mono);
    font-size: var(--text-body);
  }

  .dim {
    color: var(--dim);
    font-size: var(--text-body);
  }

  /* An area in the hue it is drawn in everywhere else (STYLE_GUIDE.md, "Group hues"). */
  .dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 999px;
    margin-right: 0.35rem;
    vertical-align: 1px;
    background: var(--dim);
  }

  .dot.hued {
    background: oklch(var(--scope-l) var(--scope-c) var(--h));
  }

  .actions {
    justify-self: end;
  }

  .message {
    color: var(--ink);
    font-size: var(--text-body);
  }

  @media (max-width: 720px) {
    .levels {
      grid-template-columns: 1fr;
    }
  }
</style>
