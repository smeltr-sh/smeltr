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
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Splunk HEC destination"
  description="Send events to Splunk HTTP Event Collector: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Splunk HEC</h1>
  <p class="lede">Sends each record to a Splunk HTTP Event Collector (HEC).</p>

  <h2 id="example">Example</h2>
  <p>
    Reads the token from the worker-host variable <code>SPLUNK_HEC_TOKEN</code>. The URL is the
    collector's base address; the destination adds the endpoint path itself.
  </p>
  <pre><code>{@html highlight(json(examples.splunkDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>Create a HEC token in Splunk and allow it to write to the target indexes.</li>
    <li>Allow workers to reach the collector's port.</li>
    <li>
      Put the token alone, without the <code>Splunk</code> prefix, in a
      <a href="/docs/secrets">credential</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="splunkHec" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'splunkHec').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <h3 id="event-and-raw-endpoints">Event and raw endpoints</h3>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">endpoint</th><th scope="col">What Splunk receives</th></tr></thead>
      <tbody>
        <tr
          ><th scope="row">event (default)</th><td
            >Each record is the event of its own HEC envelope. A record's top-level
            <code>_time</code>, <code>host</code>, <code>source</code> and
            <code>sourcetype</code> file the event; the destination's settings fill in any it lacks.
            <code>_time</code>
            is Unix seconds or a datetime. <code>__destination.index</code> picks the index.</td
          ></tr
        >
        <tr
          ><th scope="row">raw</th><td
            >Each record is sent as one line of text. Every line gets the destination's index, host,
            source and sourcetype.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <h3 id="acknowledgement">Acknowledgement</h3>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col">acknowledgement</th><th scope="col">When a batch counts as delivered</th
          ></tr
        ></thead
      >
      <tbody>
        <tr
          ><th scope="row">indexer (default)</th><td
            >Once Splunk reports the events indexed. Turn on indexer acknowledgement for the token.
            A record resent after a failure can arrive more than once.</td
          ></tr
        >
        <tr
          ><th scope="row">receipt</th><td
            >Once Splunk accepts the request. Smeltr does not send it again. Turn indexer
            acknowledgement off for the token.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    If the token's acknowledgement setting does not match, the destination reports it and delivers
    nothing until one of them changes. If Splunk refuses one event in a request, only that event is
    refused.
    <a href="/docs/delivery#refused">Choose what happens to it →</a>
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then search the index for it in Splunk.
    Check the event's time, host, source and sourcetype.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>The URL is refused</strong></dt>
    <dd>
      Give the base address, such as <code>https://splunk.example.com:8088</code>, not one ending in
      <code>/services/collector</code>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/delivery#refused">Handle refused records</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
