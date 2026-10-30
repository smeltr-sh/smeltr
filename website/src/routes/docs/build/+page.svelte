<script lang="ts">
  import DocRedirect from '#lib/components/DocRedirect.svelte';
  const links = [
    { id: 'prepare', title: 'Prepare a change', href: '/docs/first-pipeline#prepare' },
    { id: 'preview', title: 'Build against a sample', href: '/docs/first-pipeline#transform' },
    { id: 'tests', title: 'Save regression tests', href: '/docs/steps#verify' },
    { id: 'ready', title: 'Ready for review', href: '/docs/review#check' },
  ];
</script>

<DocRedirect title="Build and validate" destination="/docs/first-pipeline" {links} />
