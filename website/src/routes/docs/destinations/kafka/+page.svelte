<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-reads', title: 'Fields it reads' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Kafka destination"
  description="Write records to Kafka topics: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Kafka</h1>
  <p class="lede">Writes each record to a Kafka topic.</p>
  <p>
    Smeltr manages the producer, including its protection against duplicates caused by producer
    retries.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Writes to a plaintext test broker. Replace its address with one reachable from the worker, and
    keep input and output topics separate unless you mean to build a feedback path.
  </p>
  <pre><code>{@html highlight(json(examples.kafkaDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Create the output topic.</li>
    <li>Allow workers to reach every broker address Kafka advertises.</li>
    <li>Give the Kafka account permission to write to the topic.</li>
    <li>
      For an authenticated broker, configure <a href="/docs/secrets#kafka">Kafka security</a>.
      Select a
      <a href="/docs/secrets#identities">connection identity</a> for a private CA or a client certificate.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="kafka" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'kafka').reads ?? []} />

  <h2 id="verify">Verify</h2>
  <p>
    Send a record you can recognize through the source, consume the output topic, and check the
    record's fields, not just that the topic has messages. The
    <a href="/docs/first-pipeline#verify">tutorial's delivery check</a> shows the commands.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Timeout or authorization error</strong></dt>
    <dd>
      Check advertised addresses, that the topic exists, write permissions and <a
        href="/docs/secrets#kafka">Kafka security</a
      >.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/sources/kafka">Kafka source</a></li>
    <li><a href="/docs/secrets#kafka">Kafka security</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
