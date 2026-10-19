<script lang="ts">
  import DocRedirect from '#lib/components/DocRedirect.svelte';
  const links = [
    { id: 'startup', title: 'Startup versus live settings', href: '/docs/reference#startup' },
    { id: 'logs', title: 'Change log verbosity', href: '/docs/operations#logs' },
    { id: 'secrets', title: 'Secrets & credentials', href: '/docs/secrets#secrets' },
    { id: 'groups', title: 'Worker groups', href: '/docs/enroll-worker/host#before' },
  ];
</script>

<DocRedirect title="Configure Smeltr" destination="/docs/secrets" {links} />
