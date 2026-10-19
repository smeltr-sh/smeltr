<script lang="ts">
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
</script>

<svelte:head
  ><title>Operate the fleet — smeltr</title><meta
    name="description"
    content="Trace telemetry flow, investigate warnings, and inspect each worker’s deployed commit."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">Operate</p>
    <h1>Find where the flow needs attention.</h1>
    <p class="lede">
      Follow sources through pipelines to destinations. Inspect warnings, throughput, logs, and
      worker state to understand what is happening.
    </p>
  </section>

  <section class="demo-section">
    <div class="section-intro">
      <p class="eyebrow">Trace the path</p>
      <h2>Trace sources through pipelines to destinations.</h2>
      <p>
        Throughput and warnings help you find the source, pipeline, or destination to investigate.
        Search by name or select a group to narrow the view.
      </p>
    </div>
    <ProductScreenshot
      source="fleet-map"
      height={2440}
      crop={{ x: 40, y: 290, width: 2800, height: 2000 }}
      alt="Smeltr Map showing sources connected through pipelines to destinations in the Applications and cloud and Network security groups."
      caption="A running installation with sample traffic. Follow each source through its pipeline to the destinations it feeds."
    />
  </section>

  <section class="demo-section">
    <div class="section-intro">
      <p class="eyebrow">Check the running version</p>
      <h2>Check worker health and running versions.</h2>
      <p>
        Compare a worker's commit with main to see which configuration it is running. Open a worker
        in Status for its owned partitions and thread detail.
      </p>
    </div>
    <ProductScreenshot
      source="fleet-attention"
      height={1800}
      crop={{ x: 680, y: 233, width: 1960, height: 1082 }}
      alt="Fleet status with one active issue: fw-collector-02 has stopped sending heartbeats. Its last-seen time is highlighted beside three online workers."
      caption="Example fleet: one worker needs attention. Check its last-seen time and the active issue before investigating its host."
    />
  </section>

  <section>
    <h2>See what needs action.</h2>
    <p>
      Warnings name the affected object and the condition behind it. Logs retain what was reported
      once a condition clears.
    </p>
  </section>

  <section aria-labelledby="delivery-path">
    <div class="section-intro">
      <p class="eyebrow">Delivery and recovery</p>
      <h2 id="delivery-path">Keep control when delivery fails.</h2>
      <p>
        If a destination becomes unreachable, Smeltr retries and holds the sources feeding it as
        buffers fill. If the destination refuses a record, choose whether to hold it, send it to
        another destination for investigation, or deliberately drop it.
      </p>
    </div>
    <WorkflowDiagram kind="delivery" />
    <p>
      Keep source records long enough to cover the outage and the time needed to catch up. If the
      destination chosen to keep refused records also fails, the affected input streams can still
      pause.
      <a href="/docs/delivery#hold">Hold and retry delivery →</a>
    </p>
  </section>
  <section class="detail-grid">
    <div><h2>Workers connect. The leader coordinates.</h2></div>
    <div>
      <p>
        Workers hold the data connections and initiate communication with the leader. The leader
        provides authoring, review, and a shared operating view.
      </p>
      <p>
        Worker groups let separate environments run their own configuration while sharing reusable
        pipeline logic.
      </p>
    </div>
  </section>
  <section class="next-step">
    <p><a href="/docs/operations">Read the step-by-step operations guide →</a></p>
    <h2>Start operating Smeltr.</h2>
    <div class="actions">
      <a class="btn large primary" href="/docs">Read the documentation →</a><a
        class="btn large"
        href="/">Back to the start</a
      >
    </div>
  </section>
</div>
