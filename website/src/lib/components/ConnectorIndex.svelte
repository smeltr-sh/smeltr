<script lang="ts">
  import { byCategory, type Connection, type SourceGroup } from '#lib/connections.ts';
  /** A step has no category: its index is one table with no heading. */
  type Entry = Omit<Connection, 'category'> & { category?: string };
  /** `groups` orders the categories and gives each heading its anchor and help. Without it, they are in order of
   * name. */
  let { entries, groups }: { entries: Entry[]; groups?: SourceGroup[] } = $props();
  const grouped = $derived(
    byCategory(
      entries,
      groups?.map((g) => g.category),
    ).map((g) => ({ ...g, group: groups?.find(({ category }) => category === g.category) })),
  );
</script>

{#each grouped as { category, kinds, group } (category)}
  {#if category}<h3 id={group?.id}>{category}</h3>{/if}
  {#if group}<p>{group.help}</p>{/if}

  <div class="table-scroll">
    <table>
      <thead
        ><tr><th scope="col">Kind</th><th scope="col">type</th><th scope="col">What it does</th></tr
        ></thead
      >
      <tbody>
        {#each kinds as entry (entry.id)}
          <tr
            ><td><a href={entry.href}>{entry.name}</a></td><td><code>{entry.type}</code></td><td
              >{entry.summary}</td
            ></tr
          >
        {/each}
      </tbody>
    </table>
  </div>
{/each}
