<!--
  What a local or key account holds, as the field that chooses it: the roles
  it is given, the administrator role among them.
-->
<script lang="ts">
  import Field from './Field.svelte';
  import type { RoleDto } from './api';

  let { roles, chosen = $bindable([]) }: { roles: RoleDto[]; chosen?: string[] } = $props();

  function toggle(name: string) {
    chosen = chosen.includes(name) ? chosen.filter((r) => r !== name) : [...chosen, name];
  }
</script>

<Field label="Roles" group>
  {#snippet help()}It holds what these roles give. The administrator role holds everything.{/snippet}
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
</Field>

<style>
  .check {
    display: flex;
    align-items: center;
    gap: 0.4rem;
  }

  .mono {
    font-family: var(--mono);
    font-size: var(--text-note);
  }

  .roles {
    display: grid;
    gap: 0.3rem;
  }
</style>
