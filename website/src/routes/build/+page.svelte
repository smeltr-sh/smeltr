<script lang="ts">
  import DocRedirect from '#lib/components/DocRedirect.svelte';
  const links = [
    { id: 'test-heading', title: 'Save the result as a test', href: '/docs/steps#verify' },
    { id: 'filtering', title: 'Drop the noise', href: '/siem-costs#drop-heading' },
    { id: 'filtering-heading', title: 'Drop the noise', href: '/siem-costs#drop-heading' },
  ];
</script>

<DocRedirect title="How it works" destination="/how-it-works#build-heading" {links} />
