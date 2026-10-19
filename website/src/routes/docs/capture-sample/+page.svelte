<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'prepare', title: 'Choose a capture target' },
    { id: 'capture', title: 'Collect records' },
    { id: 'save', title: 'Save and test' },
    { id: 'blocked', title: 'Capture a blocked source' },
    { id: 'empty', title: 'If no records arrive' },
  ];
</script>

<GuideLayout
  title="Capture a sample"
  description="Collect real records from a source, inspect them, and save a sample for pipeline tests."
  {sections}
>
  <h1>Capture a sample</h1>
  <p class="lede">
    Capture a few real records to build and test your pipeline. You can inspect the records as they
    arrive and stop as soon as you have enough.
  </p>
  <h2 id="prepare">Choose a capture target</h2>
  <ol>
    <li>
      Work on your draft branch. Configure a <a href="/docs/sources">source</a> and its credentials.
    </li>
    <li>
      Open Build → Resources → Samples, choose <strong>+ Add sample</strong>, then
      <strong>Capture</strong>. The pipeline editor has the same <strong>Capture</strong> button above
      its list of samples. The sample you capture there becomes the editor's sample.
    </li>
    <li>
      Choose the worker <strong>Group</strong> and <strong>Capture target</strong>.
    </li>
  </ol>
  <p>
    The capture uses the source configuration on your branch. A worker already reading that source
    takes samples from the records flowing through it. Otherwise it opens a separate reader without
    saving a new reading position.
  </p>
  <p>
    A Kafka or Event Hubs capture reads in real time by default. To read older records, open
    <strong>Advanced</strong> and set <strong>Start at</strong>. Existing source reading positions
    will remain unchanged.
  </p>
  <ProductScreenshot
    source="capture-form"
    height={1800}
    crop={{ x: 608, y: 516, width: 1664, height: 768 }}
    alt="Capture setup with the Network security group, panw-firewalls source, a raw-text filter, a 200-record limit, a 30-second limit, and Start."
    caption="Choose a capture target, narrow the sample with an optional filter, and set record and time limits before starting."
  />
  <h2 id="capture">Collect records</h2>
  <ol>
    <li>
      Leave <strong>Keep records where</strong> empty for all records, or enter a condition such as
      <code>_raw.includes("error")</code>.
    </li>
    <li>
      Set <strong>At most</strong> to the total number of records you want, up to 1,000, and choose a
      time limit. Workers share the total.
    </li>
    <li>
      Select <strong>Start</strong>. Inspect the incoming records and worker counts. Select a worker
      chip to view only that worker's records.
    </li>
    <li>
      Wait for the capture to finish, or select <strong>Stop capture</strong> to keep what has arrived
      so far.
    </li>
  </ol>
  <p>
    The filter runs before the pipeline. It can read <code>_raw</code> and fields the source
    provides; fields your parser creates are not available yet. The filter does not support array
    <code>map</code> and <code>filter</code> calls.
  </p>
  <ProductScreenshot
    source="capture-running"
    height={2200}
    crop={{ x: 992, y: 1204, width: 896, height: 872 }}
    alt="Capture in progress, showing incoming firewall records from two workers, the record limit, time remaining, and Stop capture button."
    caption="Capture in progress using sample traffic. Worker counts show where the records came from; select a worker to inspect its records."
  />
  <h2 id="save">Save and test</h2>
  <p>
    Enter a name under <strong>Save as</strong> and select <strong>Save sample</strong>. Open the
    saved sample in your pipeline, run it, and inspect the output. Save an expected result as a
    test, following <a href="/build#test-heading">the Build page</a>. Choose
    <strong>Edit</strong> on a sample's row under Build → Resources → Samples to change it.
  </p>
  <p>
    You need at least one captured record to save. Choose <strong>Discard</strong> if the records are
    not useful. Capture only enough to test the case you care about: saved samples become part of the
    versioned configuration.
  </p>
  <h2 id="blocked">Capture a blocked source</h2>
  <p>
    Capture works on a blocked source or destination. Select it on the Map and choose
    <strong>Capture</strong>. A blocked worker shows the records holding it up with
    <code>__exception.message</code> first, and other workers still show new records.
  </p>
  <h2 id="empty">If no records arrive</h2>
  <ul>
    <li>
      Read the reason shown for each worker. Check its connection and credentials if it failed.
    </li>
    <li>Try without a filter, send a known record, and capture again.</li>
    <li>
      Check that the selected group has online workers and that the source has records available
      during the capture window.
    </li>
  </ul>
  <p>
    You can sample Smeltr metrics and logs even before you deploy that source. They describe the
    process serving the capture. The worker may refuse a logs capture when another reader already
    holds the log buffer; the worker's reason explains this.
  </p>
</GuideLayout>
