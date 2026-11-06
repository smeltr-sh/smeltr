<script lang="ts">
  import DocRedirect from '#lib/components/DocRedirect.svelte';
  import { GROUPS, anchor } from '#lib/reference.ts';
  const destination = '/docs/code-reference';
  const topics = [
    { id: 'how-it-works', title: 'How it works' },
    { id: 'types', title: 'Types' },
    { id: 'fields', title: 'Fields' },
    ...GROUPS.flatMap((group) => [
      { id: group.id, title: group.title },
      ...group.functions.map((fn) => ({ id: anchor(fn), title: fn.name })),
    ]),
  ];
  const links = topics.map((topic) => ({
    ...topic,
    href: `${destination}#${encodeURIComponent(topic.id)}`,
  }));
</script>

<DocRedirect title="Language reference" {destination} {links} preserveHash />
