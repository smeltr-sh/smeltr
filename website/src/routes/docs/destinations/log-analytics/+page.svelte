<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import EntraSignIn from '#lib/components/EntraSignIn.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import { DESTINATIONS } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Log Analytics workspace destination"
  description="Send records to a Log Analytics workspace through a data collection rule: an example, every setting, and what to have ready."
  {sections}
>
  <h1>Log Analytics workspace</h1>
  <p class="lede">Sends each record to a Log Analytics table, through a data collection rule.</p>

  <h2 id="example">Example</h2>
  <p>
    Sends to the stream <code>Custom-Events_CL</code> of a data collection rule, signed in as the host's
    managed identity.
  </p>
  <pre><code>{@html highlight(json(examples.logAnalyticsDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Create the table and a data collection rule with a stream that sends to it. See Microsoft's
      <a href="https://learn.microsoft.com/azure/azure-monitor/logs/tutorial-logs-ingestion-portal"
        >Logs Ingestion API tutorial</a
      >.
    </li>
    <li>
      <EntraSignIn who="destination" />
      See Microsoft's
      <a href="https://learn.microsoft.com/azure/azure-monitor/logs/logs-ingestion-api-overview"
        >Logs Ingestion API docs</a
      > for the role it needs.
    </li>
    <li>Allow workers to reach the endpoint on port 443.</li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="logAnalytics" />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination sends each batch as one gzipped JSON array to the rule's stream, signed in
    through Entra ID. It reads no <code>__destination</code> field. It handles each answer as the
    <a href="/docs/destinations/http#answers">HTTP destination</a> does.
  </p>

  <h2 id="verify">Verify</h2>
  <p>Send a record you can recognize through the source, then query its table in the workspace.</p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The destination stalls, saying its credentials were refused</strong></dt>
    <dd>Check that the identity it signs in as has the role the rule needs.</dd>
    <dt><strong>The destination stalls on a 404</strong></dt>
    <dd>Check the rule id, the stream, and that the endpoint is the rule's.</dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/http">HTTP destination</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
