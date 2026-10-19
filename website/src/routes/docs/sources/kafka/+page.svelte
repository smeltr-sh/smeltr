<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
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
  title="Kafka source"
  description="Read Kafka topics as records arrive, or a range of them: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Kafka</h1>
  <p class="lede">
    Reads records from Kafka topics, as they arrive or as a range of existing records.
  </p>
  <p>
    Workers reading in one <strong>consumer group</strong> share the work, and a source in a different
    group reads the same records independently.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads UTF-8 records from a plaintext test broker. Replace <code>localhost:9092</code> with an address
    reachable from the worker; localhost means the worker's own host.
  </p>
  <pre><code>{@html highlight(json(examples.kafkaSource))}</code></pre>
  <ProductScreenshot
    source="kafka-source-form"
    height={2100}
    crop={{ x: 608, y: 243, width: 1664, height: 1614 }}
    alt="The Kafka form on the Sources page: id events-in, broker localhost:9092, topic smeltr-events-in."
    caption="The same source in the Sources page's add dialog."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Create the input topic.</li>
    <li>
      Leave the group id blank unless this source needs offsets of its own. Blank reads as its
      worker group's consumer group, <code>smeltr.&lt;worker group&gt;</code>, so every worker group
      reads every record and rebalances only among its own workers. The leader refuses two sources
      that read one topic in one consumer group on the same brokers.
    </li>
    <li>
      Allow workers to reach the initial broker addresses you enter and every broker address Kafka
      returns when they connect.
    </li>
    <li>Give the Kafka account permission to read the topics and use the consumer group.</li>
    <li>
      For an authenticated broker, configure <a href="/docs/secrets#kafka">Kafka security</a>.
      Select a
      <a href="/docs/secrets#identities">connection identity</a> for a private CA or a client certificate.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="kafka" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 'kafka').fields ?? []} />

  <h2 id="start-and-stop">Where it starts and stops</h2>
  <StartAndStop topic="topic" topics="topics" record="record" />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Smeltr manages the consumer's commits. The extra properties refuse Kafka settings Smeltr sets
    itself, such as auto-commit.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Produce a record you can recognize to the input topic, then find it at the destination. The
    <a href="/docs/first-pipeline#verify">tutorial's delivery check</a> shows the commands.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Kafka connects, then times out</strong></dt>
    <dd>
      Check the brokers' advertised addresses from the worker host, plus DNS, firewall rules and TLS
      names.
    </dd>
    <dt><strong>Authentication or authorization fails</strong></dt>
    <dd>
      Check the security protocol, mechanism, secret availability, and topic and group permissions.
      See <a href="/docs/secrets#kafka">Kafka security</a>.
    </dd>
    <dt><strong>Old records do not replay</strong></dt>
    <dd>
      Check the group's saved positions and the topic's retention. To read old records again, <a
        href="#move">move</a
      > the source.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/destinations/kafka">Kafka destination</a></li>
    <li><a href="/docs/secrets#kafka">Kafka security</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
