<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import EntraSignIn from '#lib/components/EntraSignIn.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import StartAndStop from '#lib/components/StartAndStop.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'start-and-stop', title: 'Where it starts and stops' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Azure Event Hubs source"
  description="Read Azure Event Hubs through its Kafka endpoint: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Azure Event Hubs</h1>
  <p class="lede">Reads events from the Event Hubs namespace through its Kafka endpoint only.</p>
  <p>
    Workers reading in one <strong>consumer group</strong> share the work, and a source in a different
    group reads the same events independently. It reads the Azure logs a diagnostic setting streams to
    an event hub.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads the sign-in logs Entra ID streams to the hub <code>insights-logs-signinlogs</code>, with
    the connection string from the worker-host variable <code>EVENT_HUBS_CONNECTION_STRING</code>.
  </p>
  <pre><code>{@html highlight(json(examples.eventHubsSource))}</code></pre>
  <ProductScreenshot
    source="event-hubs-source-form"
    height={2100}
    crop={{ x: 608, y: 379, width: 1664, height: 1344 }}
    alt="The Azure Event Hubs form on the Sources page: id entra-logs, namespace acme-logs, event hub insights-logs-signinlogs, consumer group smeltr."
    caption="The same source in the Sources page's add dialog, before you pick a connection string."
  />

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
    <li>
      To collect Azure's logs, add a
      <a href="https://learn.microsoft.com/azure/azure-monitor/essentials/diagnostic-settings"
        >diagnostic setting</a
      >
      that streams them to the namespace. Each log category goes to an event hub named after it, such
      as <code>insights-logs-signinlogs</code>, unless the setting names one hub for all of them.
    </li>
    <li>
      Choose a consumer group name for this source. The leader refuses two sources that read one
      event hub in one consumer group of one namespace.
    </li>
    <li>
      <EntraSignIn who="source">
        <li>
          a connection string: copy one from a
          <a href="https://learn.microsoft.com/azure/event-hubs/event-hubs-get-connection-string"
            >shared access policy</a
          >
          on the namespace with the Listen right, and put it in a
          <a href="/docs/secrets">credential</a>. Use the namespace's, not one that ends in
          <code>EntityPath=</code>.
        </li>
      </EntraSignIn>
      Give a service principal or identity the
      <a href="https://learn.microsoft.com/azure/event-hubs/authorize-access-azure-active-directory"
        >Azure Event Hubs Data Receiver</a
      >
      role on the namespace.
    </li>
    <li>
      Allow workers to reach the namespace on port 9093, such as
      <code>acme-logs.servicebus.windows.net:9093</code>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="eventHubs" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p><code>__source.topic</code> holds the event hub's name.</p>
  <FieldsTable names={kind(SOURCES, 'eventHubs').fields ?? []} />

  <h2 id="start-and-stop">Where it starts and stops</h2>
  <StartAndStop topic="event hub" topics="event hubs" record="event" />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The source reads through the namespace's Kafka endpoint, as the
    <a href="/docs/sources/kafka">Kafka source</a> does, over TLS, signed in with the connection string,
    the service principal or the host's identity. Smeltr commits the consumer group's progress as it delivers
    events, so a restarted worker resumes where its group left off.
  </p>
  <p>
    Event Hubs keeps events only for the hub's retention period, so a group that falls further
    behind than that loses the oldest.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send an event you can recognize, such as with the event hub's Data Explorer in the portal, then
    find it at the destination.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Authentication fails</strong></dt>
    <dd>
      Check that the namespace has a Kafka endpoint. With a connection string, check that it is the
      namespace's, not an event hub's, and that its policy has the Listen right. Otherwise, check
      that the service principal or identity has the Azure Event Hubs Data Receiver role.
    </dd>
    <dt><strong>Connecting times out</strong></dt>
    <dd>
      Check that the worker host reaches port 9093 on the namespace, through its firewall and any
      network rules on the namespace.
    </dd>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the diagnostic setting streams to this namespace and that the event hub's metrics
      show incoming messages.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/kafka">Kafka source</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
