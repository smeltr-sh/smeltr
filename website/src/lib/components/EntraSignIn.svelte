<!--
  How an Azure connector signs in with Entra ID, as a list item's opening and its choices. A connector with another
  way in, such as Event Hubs' connection string, passes it as the first choice. Every connector that signs in with
  Entra ID draws this, so they all name the same choices and the same limits.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';

  let { who, children }: { who: 'source' | 'destination'; children?: Snippet } = $props();
</script>

Choose how the {who} signs in, with exactly one of:
<ul>
  {@render children?.()}
  <li>
    a service principal: set <code>tenantId</code>, <code>clientId</code> and
    <code>clientSecret</code>, with the secret in a <a href="/docs/secrets">credential</a>.
  </li>
  <li>
    a user-assigned managed identity: set <code>managedIdentityClientId</code> to its client id. Set it
    when the host has more than one managed identity.
  </li>
  <li>
    none: the {who} signs in as the host's
    <a href="https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview"
      >managed identity</a
    >, or as an AKS pod's
    <a href="https://learn.microsoft.com/azure/aks/workload-identity-overview">workload identity</a
    >. The managed identities of App Service, Container Apps and Functions are not supported.
  </li>
</ul>
