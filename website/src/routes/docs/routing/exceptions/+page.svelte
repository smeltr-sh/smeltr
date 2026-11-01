<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  const sections = [
    { id: 'choose', title: 'Find the right answer' },
    { id: 'answers', title: 'Answer exceptions' },
    { id: 'tag', title: 'Tag and route' },
    { id: 'unmatched', title: 'When no route matches' },
    { id: 'unprocessable', title: 'When processing cannot finish' },
  ];
</script>

<GuideLayout
  title="Exceptions"
  description="Choose exception answers, tag records with their exceptions, and handle unmatched and unprocessable records in Smeltr."
  {sections}
>
  <h1>Exceptions</h1>
  <p class="lede">
    Follow a record from routing through processing and delivery. Whatever stops a record on the way
    is an exception, and you choose each exception's answer.
  </p>
  <h2 id="choose">Find the right answer</h2>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col">What happened?</th><th scope="col">Exception</th><th scope="col"
            >Where to answer it</th
          ><th scope="col">Default</th></tr
        ></thead
      >
      <tbody>
        <tr
          ><td>A pipeline lost a value</td><td>a parse, math, limits or encoding kind</td><td
            >Routes → Exceptions</td
          ><td>tag, or ignore for encoding</td></tr
        >
        <tr
          ><td>No route or delivery condition took it</td><td><code>Unmatched</code></td><td
            >Routes → Exceptions</td
          ><td>block</td></tr
        >
        <tr
          ><td>A batch could not be processed</td><td><code>Unprocessable</code></td><td
            >Routes → Exceptions</td
          ><td>block</td></tr
        >
        <tr
          ><td>A destination refused a record</td><td><code>Refused</code></td><td
            >Routes → Exceptions</td
          ><td>block</td></tr
        >
        <tr
          ><td>A destination could not be reached</td><td>none</td><td
            ><a href="/docs/delivery#hold">Repair its connection or service</a></td
          ><td>Retry; pause input when buffers fill</td></tr
        >
      </tbody>
    </table>
  </div>

  <h2 id="answers">Answer exceptions</h2>
  <p>
    Each exception has a kind, and each kind belongs to a category: parse, math, limits, encoding or
    delivery. The <strong>Exceptions</strong> on the Routes page give every kind in the group one of five
    answers:
  </p>
  <ul>
    <li>
      <strong>ignore:</strong> the record goes on unmarked. Smeltr still counts the exception.
    </li>
    <li>
      <strong>tag:</strong> the record goes on with its exceptions written to the
      <a href="#tag">tag field</a>.
    </li>
    <li><strong>drop:</strong> Smeltr counts the record and discards it.</li>
    <li>
      <strong>deliver:</strong> the record goes to the destinations you pick, with its exceptions in
      the <a href="#tag">tag field</a>, each in its own
      <a href="/docs/destinations#formats">format</a>.
    </li>
    <li>
      <strong>block:</strong> the source stops. Processing waits for a new deployment; Smeltr sends
      a refused record again until the destination takes it. Status → Issues shows it as Source
      blocked, or Blocked by destination refusals;
      <a href="/docs/capture-sample#blocked">capture it</a> to see the records.
    </li>
  </ul>
  <p>
    An answer for a category covers every kind in it, and a kind's own answer beats its category's.
    A record with several exceptions takes the strongest answer: block, then deliver, then drop,
    then tag, then ignore. The delivery kinds have no record on a route to tag, so you cannot ignore
    or tag them.
  </p>
  <ProductScreenshot
    source="exception-answers"
    height={2800}
    crop={{ x: 460, y: 830, width: 2380, height: 1100 }}
    alt="The Exceptions on the Routes page: math is dropped, limits block the source, and Overflow, a math kind, is tagged."
    caption="The worker group's Exceptions on the Routes page. Overflow's answer beats its category's, so Overflow is tagged while the rest of math is dropped."
  />
  <p>
    A route or a destination can answer differently from the worker group, under
    <strong>Exceptions</strong> in its card. A destination answers only the records it
    <a href="/docs/delivery#refused">refuses</a>.
  </p>

  <h2 id="tag">Tag and route</h2>
  <p>
    A tagged record carries its exceptions in <code>smeltr.exception</code>. Delivery conditions
    read that field to choose a destination. Use <code>smeltr.exception != null</code> to send
    tagged records to an investigation destination, and <code>smeltr.exception == null</code> to send
    only clean records to the normal one.
  </p>
  <p>
    The <strong>Tag field</strong>, under <strong>Exceptions</strong> →
    <strong>Advanced</strong>, names another field for the worker group or for one route. If you
    rename it, update both conditions. See
    <a href="/docs/steps/parser#unparsable">unparsable records</a> for examples you can test.
  </p>

  <h2 id="unmatched">When no route matches</h2>
  <p>
    A record that matches none of its source's routes is <code>Unmatched</code>. Blocking holds the
    records and pauses the source until a new deployment: add or correct a route, or change the
    answer, then review and deploy. Delivering keeps them in another destination for investigation
    or later processing.
  </p>
  <p>
    Use <a href="/docs/routing/records">route conditions</a> to decide what belongs in each pipeline.
    An unconditional route matches all records from its source, so no record of that source is left without
    a route.
  </p>
  <p>
    Every record a pipeline finishes goes to a destination, is dropped by your pipeline, or is
    <code>Unmatched</code>: if no delivery condition takes it, it gets the <code>Unmatched</code>
    answer, delivered as the pipeline left it.
  </p>

  <h2 id="unprocessable">When processing cannot finish</h2>
  <p>
    A batch Smeltr cannot process, including a record that exceeds its memory budget, is
    <code>Unprocessable</code>. Blocking waits for a new deployment. No pipeline finished the
    record, so a destination that writes the record writes <code>{'{"_raw": …}'}</code>.
  </p>
</GuideLayout>
