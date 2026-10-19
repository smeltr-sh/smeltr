<!--
  Where one credential's value lives: a variable on a worker's own host, or
  a secret the leader holds. Never the value itself -- this control writes
  a reference into the bundle, and the bundle is committed.

  Which is which was decided once, when the secret was declared on Admin's
  Secrets page: this picks a name from that catalog, not a custody. A
  leader-stored pick quotes the version it is at now -- that number is what
  says which value the deployment was built for; bumping it later is what
  redeploys the connections using it. A worker-local pick names nothing
  more than a variable the leader knows of.
-->
<script lang="ts">
  import { currentVersion, isLeaderStored, type SecretDto, type SecretRef } from './api';

  let {
    value,
    secrets,
    onchange,
    label = 'credential',
  }: {
    value: SecretRef | undefined;
    /** The full catalog -- every declared secret, either custody. */
    secrets: SecretDto[];
    onchange: (v: SecretRef | undefined) => void;
    label?: string;
  } = $props();

  /** The name this reference names, regardless of its shape. A blank name,
   * which a new connection starts with, names nothing. */
  const nameOf = (r: SecretRef | undefined): string | undefined =>
    r === undefined || r === '' ? undefined : isLeaderStored(r) ? r.secret : r;

  /** The select's own value. A reference naming a secret no longer in the
   * catalog still shows its name, so editing another field cannot
   * silently drop it. */
  let choice = $derived(nameOf(value) ?? '');

  function choose(name: string) {
    if (name === '') {
      onchange(undefined);
      return;
    }
    const picked = secrets.find((s) => s.name === name);
    if (!picked) return; // re-selecting the "not listed" fallback: nothing changes
    if (picked.custody === 'worker-local') {
      onchange(name);
      return;
    }
    // The version it is at now: a reference written today runs today's
    // value, and moving it later is what deploys a rotation.
    onchange({ secret: name, version: currentVersion(picked)?.version ?? 1 });
  }
</script>

<div class="ref">
  <select
    value={choice}
    onchange={(e) => choose(e.currentTarget.value)}
    aria-label="{label} source"
  >
    <option value="">none</option>
    {#each secrets as s (s.name)}
      <option value={s.name}>{s.name}</option>
    {/each}
    {#if nameOf(value) !== undefined && !secrets.some((s) => s.name === nameOf(value))}
      <option value={nameOf(value)}>{nameOf(value)} (not listed)</option>
    {/if}
  </select>
  {#if isLeaderStored(value)}
    <span class="at mono">v{value.version}</span>
  {/if}
</div>

<style>
  .ref {
    display: flex;
    gap: 0.35rem;
    align-items: center;
  }

  .at {
    color: var(--dim);
    font-size: var(--text-note);
  }

  .mono {
    font-family: var(--mono);
  }
</style>
