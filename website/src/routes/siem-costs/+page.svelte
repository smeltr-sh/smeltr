<script lang="ts">
  import SiemSteps from '#lib/components/SiemSteps.svelte';
  import SiemCutExample from '#lib/components/SiemCutExample.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
</script>

<svelte:head
  ><title>Cut SIEM costs — smeltr</title><meta
    name="description"
    content="Drop the records nobody searches and the fields nobody reads before they reach your SIEM, and keep a full copy in cheaper storage."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">Cut SIEM costs</p>
    <h1>Send your SIEM only what you search.</h1>
    <p class="lede">
      Smeltr sits between your sources and your SIEM. Your pipelines drop the records nobody
      searches and the fields nobody reads, and you can send a full copy to cheaper storage.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/demo/try/" rel="external">Try the demo</a>
      <a class="btn large" href="/get">Get smeltr</a>
    </div>
  </section>
  <section aria-labelledby="drop-heading">
    <h2 id="drop-heading">Drop the noise before it is indexed.</h2>
    <p>
      Ten PAN-OS traffic logs, run through two steps. A Parser names the line's 68 columns, skips
      the 48 columns it does not need, and rebuilds the line with their columns left empty, so a
      SIEM still reads every column in its place. A Drop step drops allowed DNS lookups, for a team
      that already keeps them in its DNS servers' own logs. The destination sends each line as text.
      Sizes are measured from this run; your cut depends on what you drop.
    </p>
    <SiemSteps />
    <SiemCutExample />
  </section>
  <section aria-labelledby="copy-heading">
    <h2 id="copy-heading">Keep everything, somewhere cheaper.</h2>
    <p>Send the same records to your SIEM trimmed and to object storage whole.</p>
    <WorkflowDiagram kind="siem" />
    <p>
      <a href="/docs/destinations/s3">Amazon S3</a> ·
      <a href="/docs/destinations/gcs">Google Cloud Storage</a> ·
      <a href="/docs/destinations/azure-blob">Azure Blob Storage</a> ·
      <a href="/docs/destinations/iceberg">Apache Iceberg</a> ·
      <a href="/docs/destinations/delta">Delta Lake</a>
    </p>
  </section>
  <section aria-labelledby="fit-heading">
    <h2 id="fit-heading">Fits in front of the SIEM you have.</h2>
    <p>
      Point your senders at Smeltr instead of the SIEM. They keep their own protocol and change only
      where they send: Splunk forwarders, HEC senders, Filebeat and Fluent Bit. Smeltr sends on to
      Splunk, Elasticsearch or a Log Analytics workspace.
    </p>
    <p>
      <a href="/docs/sources/s2s-receiver">S2S receiver</a> ·
      <a href="/docs/sources/hec-receiver">HEC receiver</a> ·
      <a href="/docs/sources/elastic-receiver">Elasticsearch receiver</a> ·
      <a href="/docs/destinations/splunk">Splunk HEC</a> ·
      <a href="/docs/destinations/elasticsearch">Elasticsearch</a> ·
      <a href="/docs/destinations/log-analytics">Log Analytics workspace</a>
    </p>
  </section>
  <section class="next-step">
    <h2>Start with one noisy source.</h2>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a><a
        class="btn large"
        href="/docs/getting-started">Read the getting-started guide</a
      >
    </div>
  </section>
</div>
