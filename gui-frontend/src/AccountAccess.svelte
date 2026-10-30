<!--
  What a local or key account holds, as the fields that choose it: everything
  as an administrator, or what the roles it is given give.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import Toggle from './Toggle.svelte';
  import type { RoleDto } from './api';

  let {
    roles,
    administrator = $bindable(false),
    chosen = $bindable([]),
  }: { roles: RoleDto[]; administrator?: boolean; chosen?: string[] } = $props();

  function toggle(name: string) {
    chosen = chosen.includes(name) ? chosen.filter((r) => r !== name) : [...chosen, name];
  }
</script>

<label class="check"><Toggle bind:checked={administrator} /> Administrator</label>
{#if !administrator}
  <Field label="Roles" group>
    {#snippet help()}It holds what these roles give.{/snippet}
    {#if roles.length === 0}
      <p class="dim">No roles yet. Add one under Roles.</p>
    {:else}
      <div class="roles">
        {#each roles as role (role.name)}
          <label class="check mono"
            ><input
              type="checkbox"
              checked={chosen.includes(role.name)}
              onchange={() => toggle(role.name)}
            />
            {role.name}</label
          >
        {/each}
      </div>
    {/if}
  </Field>
{/if}

<style>
  .check {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: var(--text-body);
  }

  .mono {
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .dim {
    color: var(--dim);
    font-size: var(--text-body);
  }

  .roles {
    display: grid;
    gap: 0.3rem;
  }
</style>
