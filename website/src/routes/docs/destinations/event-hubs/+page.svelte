<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import EntraSignIn from '#lib/components/EntraSignIn.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-reads', title: 'Fields it reads' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Azure Event Hubs destination"
  description="Write records to Azure Event Hubs through its Kafka endpoint: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Azure Event Hubs</h1>
  <p class="lede">
    Writes each record to the Event Hubs namespace through its Kafka endpoint only.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Writes to the event hub <code>alerts</code> in the namespace <code>acme-logs</code>, with the
    connection string from the worker-host variable <code>EVENT_HUBS_CONNECTION_STRING</code>.
  </p>
  <pre><code>{@html highlight(json(examples.eventHubsDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Use a namespace with a
      <a href="https://learn.microsoft.com/azure/event-hubs/azure-event-hubs-apache-kafka-overview"
        >Kafka endpoint</a
      >. Microsoft
      <a href="https://learn.microsoft.com/azure/event-hubs/event-hubs-create"
        >recommends the Standard tier</a
      > for Kafka.
    </li>
    <li>Create each event hub it writes to.</li>
    <li>
      <EntraSignIn who="destination">
        <li>
          a connection string: copy one from a
          <a href="https://learn.microsoft.com/azure/event-hubs/event-hubs-get-connection-string"
            >shared access policy</a
          >
          on the namespace with the Send right, and put it in a
          <a href="/docs/secrets">credential</a>. Use the namespace's, not one that ends in
          <code>EntityPath=</code>.
        </li>
      </EntraSignIn>
      Give a service principal or identity the
      <a href="https://learn.microsoft.com/azure/event-hubs/authorize-access-azure-active-directory"
        >Azure Event Hubs Data Sender</a
      >
      role on the namespace.
    </li>
    <li>
      Allow workers to reach the namespace on port 9093, such as
      <code>acme-logs.servicebus.windows.net:9093</code>.
    </li>
    <li>
      Set <code>compression</code> to <code>gzip</code> only on a namespace that takes it. See
      Microsoft's
      <a href="https://learn.microsoft.com/azure/event-hubs/apache-kafka-configurations"
        >Kafka client settings for Event Hubs</a
      >.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="eventHubs" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <p><code>__destination.topic</code> names the event hub a record goes to.</p>
  <FieldsTable names={kind(DESTINATIONS, 'eventHubs').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination writes through the namespace's Kafka endpoint, as the
    <a href="/docs/destinations/kafka">Kafka destination</a> does, over TLS, signed in with the
    connection string, the service principal or the host's identity. A batch is acknowledged once
    Event Hubs has every record in it. A record Event Hubs will not take is
    <a href="/docs/delivery#refused">refused</a>.
  </p>

  <h2 id="verify">Verify</h2>
  <p>Send a record you can recognize through the source, then find it in the event hub.</p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Authentication fails</strong></dt>
    <dd>
      Check that the namespace has a Kafka endpoint. With a connection string, check that it is the
      namespace's and that its policy has the Send right. Otherwise, check that the service
      principal or identity has the Azure Event Hubs Data Sender role.
    </dd>
    <dt><strong>Connecting times out</strong></dt>
    <dd>
      Check that the worker host reaches port 9093 on the namespace, through its firewall and any
      network rules on the namespace.
    </dd>
    <dt><strong>The destination stalls saying the namespace does not take gzip</strong></dt>
    <dd>
      The namespace does not take gzip. Set <code>compression</code> to <code>none</code>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/event-hubs">Azure Event Hubs source</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
