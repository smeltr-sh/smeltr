<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';

  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'matches', title: 'Every matching route runs' },
    { id: 'split', title: 'Split records between pipelines' },
    { id: 'delivery', title: 'Choose destinations' },
    { id: 'preview', title: 'Preview the result' },
  ];
</script>

<GuideLayout
  title="Routing records"
  description="Connect sources to pipelines, understand overlapping routes, and choose destinations for the results."
  {sections}
>
  <h1>Routing records</h1>
  <p class="lede">
    A route connects a source to a pipeline. Its condition decides which input records enter that
    pipeline. Delivery rules then choose destinations for the result.
  </p>
  <h2 id="matches">Every matching route runs</h2>
  <p>
    Routes have no first-match order. If two routes match a record, both pipelines run. A route with
    no condition matches every record from its source.
  </p>
  <p>
    For example, give one route no condition to archive all records, and a second route the
    condition <code>includes(_raw, "ERROR")</code> to process errors. An error record runs through both
    pipelines. If both deliver to the same destination, that destination can receive both outputs.
  </p>
  <h2 id="split">Split records between pipelines</h2>
  <p>
    To send each record through only one of two pipelines, make their conditions mutually exclusive:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Pipeline</th><th scope="col">Route condition</th></tr></thead>
      <tbody>
        <tr><td>Errors</td><td><code>includes(_raw, "ERROR")</code></td></tr>
        <tr><td>Other records</td><td><code>!includes(_raw, "ERROR")</code></td></tr>
      </tbody>
    </table>
  </div>
  <p>
    Route conditions run before pipeline steps. Use the raw text or fields the source supplies. To
    choose destinations using parsed or calculated fields, use delivery conditions instead.
  </p>
  <p>
    A record that matches no route is <code>Unmatched</code>; set its answer in the
    <strong>Exceptions</strong> on Routes. It blocks by default. Choose its answer deliberately; a
    record your routes overlook can pause the source. See
    <a href="/docs/routing/exceptions#unmatched">handling unmatched records</a>.
  </p>
  <h2 id="delivery">Choose destinations</h2>
  <p>
    Delivery conditions read the pipeline's output. For example, after parsing <code>status</code>
    as an integer, <code>status &gt;= 500</code> can send server errors to an error destination.
  </p>
  <p>
    Check delivery conditions for overlap too. Smeltr can send a record to more than one
    destination. A null condition counts as false. Route and delivery conditions do not support
    array <code>map</code>
    or <code>filter</code> calls.
  </p>
  <h2 id="preview">Preview the result</h2>
  <p>
    Preview records that match each route, more than one route, and no route. Then check actual
    destination output after deployment. The <a href="/docs/first-pipeline#check"
      >first-pipeline tutorial</a
    > walks through connecting and verifying a route.
  </p>
  <ProductScreenshot
    source="route-preview"
    height={1800}
    crop={{ x: 720, y: 403, width: 1870, height: 660 }}
    alt="Route preview for 12 storefront access records: all enter nginx-access, ten would go to es-web, and two are dropped by the pipeline."
    caption="This preview accounts for all 12 sample records: Smeltr would send 10, and the pipeline drops 2. Preview does not deliver data."
  />
</GuideLayout>
