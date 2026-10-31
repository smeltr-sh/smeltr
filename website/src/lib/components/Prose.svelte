<script lang="ts">
  import { inline } from '#lib/connector-reference.ts';
  let { text }: { text: string } = $props();
</script>

{#each inline(text) as p, i (i)}{#if p.kind === 'code'}<code>{p.text}</code
    >{:else if p.kind === 'strong'}<strong>{p.text}</strong>{:else if p.kind === 'link'}<a
      href={p.href}
      >{#if p.code}<code>{p.text}</code>{:else}{p.text}{/if}</a
    >{:else}{p.text}{/if}{/each}
