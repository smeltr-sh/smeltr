<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'prepare', title: 'Choose a capture target' },
    { id: 'capture', title: 'Collect records' },
    { id: 'save', title: 'Save and test' },
    { id: 'blocked', title: 'Capture a blocked source' },
    { id: 'as-sent', title: 'Capture what a receiver was sent' },
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
      <strong>Capture</strong>. In the pipeline editor, choose <strong>Add sample</strong> in the
      sample menu, then <strong>Capture</strong>. The sample you capture there becomes the editor's
      sample.
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
    At a source, the filter runs before the pipeline. It can read <code>_raw</code> and fields the source
    provides; fields your parser creates are not available yet.
  </p>
  <p>
    When you capture at a destination, the filter reads each record as the route's pipeline
    delivered it, so it can use fields your parsers created. When several routes deliver there, the
    capture keeps records only from routes where the filter can run. Before you start, the dialog
    lists the other routes and why each is skipped.
  </p>
  <p>
    The filter does not support array <code>map</code> and <code>filter</code> calls.
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
    test, as <a href="/docs/steps#verify">Verify</a> describes. Choose
    <strong>Edit</strong> on a sample's row under Build → Resources → Samples to change it.
  </p>
  <p>
    You need at least one captured record to save. Choose <strong>Discard</strong> if the records
    are not useful. Capture only enough to test the case you care about. A saved sample deploys with
    your branch, whether or not a test uses it. To keep one off <code>main</code>, delete it before
    you deploy.
  </p>
  <h2 id="blocked">Capture a blocked source</h2>
  <p>
    Capture works on a blocked source or destination. Select it on the Map and choose
    <strong>Capture</strong>. A blocked worker shows the records holding it up with
    <code>__exception.message</code> first, and other workers still show new records.
  </p>
  <h2 id="as-sent">Capture what a receiver was sent</h2>
  <p>
    When a receiver makes no records from what arrives, capture what its senders wrote instead.
    Choose the receiver as the <strong>Capture target</strong>, open <strong>Advanced</strong>, and
    turn on <strong>Capture as sent</strong>. Each record is one read of a sender's connection,
    after TLS, or one datagram. It holds the sender's address in <code>__source.remote</code> and
    the exact bytes in <code>__source.bytes</code>. A sender that <strong>allowFrom</strong>
    refuses, or whose TLS handshake fails, shows why in <code>__exception.message</code>. A capture
    as sent takes no filter.
  </p>
  <p>
    A worker that runs the receiver keeps serving its senders during the capture. A worker that
    doesn't run the receiver listens on its port itself and sends nothing back, so senders get no
    answer from it. It can't listen on a port another receiver on that worker uses.
  </p>
  <p>
    Select <strong>Download</strong> to keep the records as a file, one JSON record per line, to inspect
    or send on. The bytes are kept as sent, so they may hold passwords or tokens. Check them before you
    save or share them.
  </p>
  <h2 id="empty">If no records arrive</h2>
  <ul>
    <li>
      Read the reason shown for each worker. Check its connection and credentials if it failed.
    </li>
    <li>Try without a filter, send a known record, and capture again.</li>
    <li>
      For a receiver, <a href="#as-sent">capture as sent</a> to see whether anything arrives at all.
    </li>
    <li>
      At a destination, read the message under the filter. If it says no route can use the filter,
      change or clear the filter.
    </li>
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
