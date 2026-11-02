<script lang="ts">
  import EditorScreenshot from '#lib/components/EditorScreenshot.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
</script>

<svelte:head
  ><title>How it works — smeltr</title><meta
    name="description"
    content="Build a pipeline against records from your own sources, review and deploy it through Git, and watch it run."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">How it works</p>
    <h1>Build it on real data. Review it. Watch it run.</h1>
    <div class="actions">
      <a class="btn large primary" href="/demo/try/" rel="external">Try the demo</a>
      <a class="btn large" href="/get">Get smeltr</a>
    </div>
  </section>
  <section aria-labelledby="fits-heading">
    <h2 id="fits-heading">A leader and its workers.</h2>
    <p>
      You sign in to the leader, where you build and deploy pipelines. Workers run them, near your
      sources, and move the data.
    </p>
    <WorkflowDiagram kind="system" />
  </section>
  <section aria-labelledby="build-heading">
    <h2 id="build-heading">Build against records from your own sources.</h2>
    <p>
      Capture a sample from a live source and see what each step adds, changes or drops, beside the
      pipeline. Save the result as a test.
    </p>
    <EditorScreenshot />
    <p><a href="/docs/first-pipeline">Build your first pipeline →</a></p>
  </section>
  <section aria-labelledby="review-heading">
    <h2 id="review-heading">Review every change before it runs.</h2>
    <p>
      Each change is a Git branch. Review its diff and test results, then approve and deploy it. Any
      earlier version is one deploy away.
    </p>
    <ProductScreenshot
      source="build-changes"
      height={1800}
      crop={{ x: 452, y: 288, width: 2400, height: 520 }}
      alt="Branches page with a pending firewall change, one passing test, and one untested item."
      caption="Each open change shows its tests before it is deployed."
    />
    <p><a href="/docs/review">Review and deploy →</a></p>
  </section>
  <section aria-labelledby="run-heading">
    <h2 id="run-heading">See where every record goes.</h2>
    <p>
      Follow each source through its pipelines to its destinations, with throughput and warnings.
    </p>
    <ProductScreenshot
      source="fleet-map"
      height={2440}
      crop={{ x: 40, y: 290, width: 2800, height: 2000 }}
      alt="Smeltr Map showing sources connected through pipelines to destinations in the Applications and cloud and Network security worker groups."
      caption="A running installation with sample traffic."
    />
    <p><a href="/docs/operations">See where every record goes →</a></p>
  </section>
  <section class="next-step">
    <h2>Try it on your own data.</h2>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a><a
        class="btn large"
        href="/docs/getting-started">Read the getting-started guide</a
      >
    </div>
  </section>
</div>
