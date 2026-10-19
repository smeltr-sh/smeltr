<script lang="ts">
  import EditorScreenshot from '#lib/components/EditorScreenshot.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  import FilteringExample from '#lib/components/FilteringExample.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
</script>

<svelte:head
  ><title>Build &amp; validate — smeltr</title><meta
    name="description"
    content="Keep useful fields, drop unwanted records, and test the result against saved samples before deployment."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">Build &amp; validate</p>
    <h1>See the result while you build.</h1>
    <p class="lede">
      Choose which records and fields to keep. Work with saved samples beside the pipeline to see
      what each step adds, changes, or drops before the data reaches a destination.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/demo/try/" rel="external">Try smeltr in your browser →</a>
      <a class="btn large" href="/docs/first-pipeline">Follow the first-pipeline tutorial</a>
    </div>
  </section>
  <section id="walkthrough" aria-labelledby="walkthrough-heading">
    <div class="section-intro" id="editor">
      <p class="eyebrow">Inside Smeltr</p>
      <h2 id="walkthrough-heading">From raw records to a reviewed change.</h2>
      <p>
        Explore parsing, enrichment, tests, and review using sample web and firewall logs from a
        running Smeltr installation. Open any screenshot for a closer look.
      </p>
    </div>
    <article aria-labelledby="parse-heading">
      <p class="eyebrow">1 · Parse</p>
      <h3 id="parse-heading">Turn a line of text into useful fields.</h3>
      <p>
        This web-log parser extracts the client address, request method, status, and other fields.
        Its settings sit beside the sample output. Green additions show exactly what the step
        produced.
      </p>
      <ProductScreenshot
        source="build-parse"
        height={2000}
        alt="Web-log parser settings beside an expanded sample showing added client_ip, method, status, and other fields."
        caption="Web-log example: one raw line becomes fields you can filter, change, and route."
      />
    </article>
    <article aria-labelledby="enrich-heading">
      <p class="eyebrow">2 · Enrich</p>
      <h3 id="enrich-heading">Add the context your logs are missing.</h3>
      <p>
        In this firewall pipeline, a directory lookup uses the recorded username to find a
        department and full name. Select the lookup step to see the fields it adds to each matching
        record.
      </p>
      <EditorScreenshot />
    </article>
    <article aria-labelledby="test-heading">
      <p class="eyebrow">3 · Test</p>
      <h3 id="test-heading">Save what the output should look like.</h3>
      <p>
        Choose <strong>Lock this as a test</strong> in the editor to save the expected result. A regression
        test checks that later edits still produce that result, including which records the pipeline drops.
        Here, both saved firewall tests are passing.
      </p>
      <ProductScreenshot
        source="build-tests"
        height={1800}
        alt="Samples page with two passing firewall pipeline tests. One expects ten records in and nine out; the other expects ten in and ten out."
        caption="A passing test can include dropped records: the first test expects 10 inputs and 9 outputs."
      />
    </article>
    <article aria-labelledby="review-heading">
      <p class="eyebrow">4 · Review</p>
      <h3 id="review-heading">Check the change before it goes live.</h3>
      <p>
        Work on a Git branch, then open a change to review its diff and test results. This firewall
        change adds one step and passes its test. Choose <strong>Approve &amp; deploy</strong> to
        merge into <code>main</code>, then check Status to confirm each intended worker runs the new
        commit.
      </p>
      <ProductScreenshot
        source="build-change"
        height={2200}
        alt="An open change with its conversation, its saves and tests, and the pipeline step it adds, ready to approve and deploy."
        caption="Review the pipeline diff, test results, and discussion before deployment."
      />
      <p><a href="/review">See how review and deployment work →</a></p>
    </article>
  </section>
  <section id="filtering" aria-labelledby="filtering-heading">
    <h2 id="filtering-heading">Keep useful events. Leave out the noise.</h2>
    <p>
      Suppose your team needs order and error events, but not routine health checks or debug fields.
      A Drop step removes health-check records. An Eval step clears the unused fields from the
      records you keep. The result is two smaller records ready for delivery.
    </p>
    <FilteringExample />
    <p>
      <a href="/docs/first-pipeline#filtering"
        >Try these three samples and save their results as tests →</a
      >
    </p>
  </section>
  <section aria-labelledby="data-path">
    <h2 id="data-path">Keep what matters. Send it where it belongs.</h2>
    <WorkflowDiagram kind="build" />
    <p>
      <a href="/docs/sources">Configure sources</a> ·
      <a href="/docs/destinations">Configure destinations</a>
    </p>
  </section>
  <section class="next-step">
    <p class="eyebrow">Put it into practice</p>
    <h2>Finish with a record at the destination.</h2>
    <p>
      In an existing Smeltr installation, build a small pipeline, lock its output as a test, review
      and deploy it, then verify the worker's commit and the delivered record. The tutorial supplies
      the input, expressions, and checks for each step.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/docs/first-pipeline"
        >Build your first verified pipeline →</a
      ><a class="btn large" href="/review">Explore review &amp; deployment</a>
    </div>
    <p>
      <a href="/docs/first-pipeline#next">Test your own data</a> ·
      <a href="/docs/code-reference">Expression reference</a>
    </p>
  </section>
</div>

<style>
  article {
    margin-top: 3.5rem;
    min-width: 0;
  }
  article > p:not(.eyebrow) {
    max-width: 48rem;
    margin-bottom: 1.5rem;
  }
  h3 {
    font-size: clamp(1.3rem, 3vw, 1.8rem);
  }
</style>
