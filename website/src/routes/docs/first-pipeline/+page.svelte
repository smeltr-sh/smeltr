<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import FilteringExample from '#lib/components/FilteringExample.svelte';
  import filtering from '#lib/filtering-example.json';
  import GuideLayout from '#lib/components/GuideLayout.svelte';

  const sections = [
    { id: 'prepare', title: '1. Before you start' },
    { id: 'route', title: '2. Start a route' },
    { id: 'source', title: '3. Add the source' },
    { id: 'pipeline', title: '4. Make the pipeline from a capture' },
    { id: 'transform', title: '5. Shape the records' },
    { id: 'destination', title: '6. Add the destination' },
    { id: 'check', title: '7. Check where records go' },
    { id: 'deploy', title: '8. Review and deploy' },
    { id: 'verify', title: '9. Send a real record' },
    { id: 'filtering', title: 'Try filtering rows and fields' },
    { id: 'next', title: 'Try your own data' },
  ];
  const assignments = [
    { target: 'message', expr: 'message.toUpperCase()' },
    { target: 'event.dataset', expr: '"smeltr.tutorial"' },
  ];
  const expected = JSON.stringify(
    { service: 'checkout', message: 'PAYMENT DECLINED', event: { dataset: 'smeltr.tutorial' } },
    null,
    2,
  );
  const topics = `export BROKERS='localhost:9092'
kafka-topics.sh --bootstrap-server "$BROKERS" --create \\
  --topic smeltr-tutorial-in --partitions 1 --replication-factor 1
kafka-topics.sh --bootstrap-server "$BROKERS" --create \\
  --topic smeltr-tutorial-out --partitions 1 --replication-factor 1`;
  const seed = `printf '%s\\n' \\
  '{"service":"checkout","message":"payment declined"}' \\
  '{"service":"search","message":"query timed out"}' \\
  '{"service":"checkout","message":"order placed"}' |
kafka-console-producer.sh --bootstrap-server "$BROKERS" --topic smeltr-tutorial-in`;
  const send = `export RUN_ID="smeltr-tutorial-$(date +%s)"
printf '{"service":"checkout","message":"%s"}\\n' "$RUN_ID" | kafka-console-producer.sh \\
  --bootstrap-server "$BROKERS" --topic smeltr-tutorial-in`;
  const receive = `kafka-console-consumer.sh --bootstrap-server "$BROKERS" \\
  --topic smeltr-tutorial-out --from-beginning \\
  --timeout-ms 30000 > delivered.jsonl`;
  const verify = `python3 - <<'PY'
import json, os
expected = os.environ["RUN_ID"].upper()
with open("delivered.jsonl") as records:
    found = any(
        row.get("message") == expected
        and row.get("event", {}).get("dataset") == "smeltr.tutorial"
        for line in records if line.strip()
        for row in [json.loads(line)]
    )
assert found, f"Expected record not delivered: {expected}"
print(f"Verified delivery: {expected}")
PY`;
</script>

<GuideLayout
  title="Your first pipeline"
  description="Build one route from a Kafka topic to another, check where its records go, review the change, and see the result arrive."
  {sections}
>
  <p class="eyebrow">Tutorial</p>
  <h1>Your first pipeline</h1>
  <p class="lede">
    Read JSON records from one Kafka topic, uppercase each message, add a field, and send them to
    another topic. You'll build it all from one route and review the change before it reaches a
    worker.
  </p>
  <p>
    You'll need a running Smeltr installation, administrator access, and an online worker in a group
    you can use for testing. If you're still setting up, start with
    <a href="/docs/getting-started">the setup guide</a>.
  </p>

  <h2 id="prepare">1. Before you start</h2>
  <p>
    We'll use two Kafka topics: one for incoming records and one for the result. Keeping them
    separate prevents the output from feeding back into the pipeline.
  </p>
  <p>
    In a terminal with Kafka's command-line tools available, replace <code>localhost:9092</code>
    with your test broker's address and create the topics:
  </p>
  <pre><code>{topics}</code></pre>
  <p>
    These commands assume a single test broker with a plaintext listener. For an authenticated
    broker, see the <a href="/docs/sources/kafka">Kafka source</a> and
    <a href="/docs/destinations/kafka">Kafka destination</a> guides. If the topics already exist, make
    sure they're yours to use for this exercise.
  </p>
  <p>Send three records to the input topic, so there is something to capture:</p>
  <pre><code>{seed}</code></pre>

  <h2 id="route">2. Start a route</h2>
  <p>
    A route takes records from a source, runs them through a pipeline, and delivers them to
    destinations. You'll make all three from the route itself.
  </p>
  <p>
    In Smeltr, select your test worker group and open <strong>Build</strong>. It opens on Routes.
    Choose <strong>+ Add route</strong>. The new route shows <strong>from source</strong>,
    <strong>pipeline</strong> and <strong>destination</strong>.
  </p>

  <h2 id="source">3. Add the source</h2>
  <p>
    In <strong>from source</strong>, choose <strong>+ New source…</strong>. Choose
    <strong>Pull, all workers</strong>, then Kafka. Fill in the settings below and choose
    <strong>Add</strong>.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Setting</th><th>Value</th></tr></thead>
      <tbody>
        <tr><td>id</td><td><code>tutorial-in</code></td></tr>
        <tr><td>brokers</td><td>Your broker address</td></tr>
        <tr><td>topics</td><td><code>smeltr-tutorial-in</code></td></tr>
      </tbody>
    </table>
  </div>
  <p>
    Use broker addresses the worker can reach, including the addresses Kafka advertises to clients.
    The worker needs permission to read the input topic and write to the output topic.
  </p>
  <p>
    Smeltr saves the source at once. The first save asks for a branch, because <code>main</code> is
    only changed by deploying. Call it <code>tutorial-first-pipeline</code> and choose
    <strong>Edit on this branch</strong>. Everything you save from here on goes on that branch.
  </p>

  <h2 id="pipeline">4. Make the pipeline from a capture</h2>
  <p>
    In <strong>pipeline</strong>, choose <strong>+ New pipeline…</strong>. The pipeline
    <code>tutorial-in-parse</code> opens and asks for a capture of your source. A capture reads records
    through a worker without deploying anything.
  </p>
  <p>
    Open <strong>Advanced</strong>, set <strong>Start at</strong> to earliest, and choose
    <strong>Start</strong>. When the three records appear, choose <strong>Stop capture</strong>.
    Enter <code>tutorial-message</code> under <strong>Save as</strong> and choose
    <strong>Save sample</strong>.
  </p>
  <p>
    Smeltr reads the sample and proposes the steps. It reports that 3 of 3 records read as JSON, and
    adds a Parser step that declares <code>service</code> and <code>message</code>. The parsed
    records show beside the steps.
  </p>

  <h2 id="transform">5. Shape the records</h2>
  <p>
    After the Parser step, choose <strong>+ step</strong>, then Eval. An Eval step sets fields to
    the values of expressions. Fill in the first row, then choose <strong>+ assignment</strong> for the
    second:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Target field</th><th>Expression</th></tr></thead>
      <tbody>
        {#each assignments as assignment}
          <tr
            ><td><code>{assignment.target}</code></td><td
              ><code>{@html highlight(assignment.expr)}</code></td
            ></tr
          >
        {/each}
      </tbody>
    </table>
  </div>
  <p>
    The first row uppercases the parsed message. The second labels it as part of this tutorial. A
    dot in the field name puts <code>dataset</code> inside an <code>event</code> object. The output updates
    as you type. Its first record should have these fields, along with any it kept from the input:
  </p>
  <pre><code>{@html highlight(expected)}</code></pre>
  <p>
    Choose <strong>Save</strong>. Smeltr saves the pipeline and takes you back to the route, with
    <code>tutorial-in-parse</code> picked.
  </p>

  <h2 id="destination">6. Add the destination</h2>
  <p>
    Under <strong>destination</strong>, choose <strong>+ destination</strong>, then
    <strong>+ New destination…</strong>, then Kafka. Fill in the settings below and choose
    <strong>Add</strong>. If the route already sends to another destination, choose its ✕ to stop
    sending there.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Setting</th><th>Value</th></tr></thead>
      <tbody>
        <tr><td>id</td><td><code>tutorial-out</code></td></tr>
        <tr><td>brokers</td><td>The same broker</td></tr>
        <tr><td>default topic</td><td><code>smeltr-tutorial-out</code></td></tr>
      </tbody>
    </table>
  </div>
  <p>
    The route now reads <code>tutorial-in</code> → <code>tutorial-in-parse</code> →
    <code>tutorial-out</code>. Choose <strong>Save</strong> to save the route.
  </p>

  <h2 id="check">7. Check where records go</h2>
  <p>
    Open the <strong>Route a sample</strong> tab. Select <code>tutorial-message</code> and choose
    <strong>Run</strong>. Smeltr runs the sample through your routes. Nothing is delivered.
  </p>
  <p>
    All three records should reach <code>tutorial-out</code>, with none unrouted or dropped. Choose
    the <code>tutorial-out</code> count to see the documents as they would be sent.
  </p>

  <h2 id="deploy">8. Review and deploy</h2>
  <p>
    Nothing you've saved runs until it's deployed. In the branch bar, choose
    <strong>Open for review</strong>. Smeltr fills in the title from what changed. Give it a
    description and choose <strong>Open for review</strong>.
  </p>
  <p>
    On the review page, read through <strong>Saves</strong> and make sure it holds only your tutorial
    changes.
  </p>
  <p>
    If someone has changed <code>main</code> while you were working, choose
    <strong>Bring current</strong> and check the saves again. Then choose
    <strong>Approve &amp; deploy</strong> and confirm. Smeltr opens Status, where you can watch your workers
    pick up the change. If a worker reports a deployment failure, resolve it before sending a record.
  </p>

  <h2 id="verify">9. Send a real record</h2>
  <p>
    Back in the same terminal, send a fresh message to the input topic. The timestamp in it will
    help you tell it apart from earlier attempts.
  </p>
  <pre><code>{send}</code></pre>
  <p>
    Read the output topic into a file, then open it. The command stops after 30 seconds without
    another record.
  </p>
  <pre><code
      >{receive}
cat delivered.jsonl</code
    ></pre>
  <p>
    Look for the message you just sent, now in uppercase, with <code>event.dataset</code> set to
    <code>smeltr.tutorial</code>. The three records you sent first are there too: the source starts
    at the earliest record. Open <strong>Map</strong> to see your route with records flowing through it.
  </p>
  <details>
    <summary>Check the result with a script</summary>
    <p>
      This Python 3 snippet finds the record from your latest attempt and checks both fields. Run it
      in the same terminal so it can read <code>RUN_ID</code>.
    </p>
    <pre><code>{verify}</code></pre>
    <p>
      It prints <code>Verified delivery:</code> followed by your uppercase message when it finds a match.
    </p>
  </details>
  <div class="table-scroll">
    <table>
      <thead><tr><th>If you don't see the result</th><th>What to try</th></tr></thead>
      <tbody>
        <tr
          ><td>The file is empty</td><td
            >Check for an error from the producer. Then look at Status → Issues and Logs for
            connection, permission, or deployment errors.</td
          ></tr
        >
        <tr
          ><td>The message isn't uppercase or has the wrong dataset</td><td
            >Check the saved Eval expressions, and make sure you're looking at the right worker
            group and running commit.</td
          ></tr
        >
        <tr
          ><td>You only see older messages</td><td
            >Check for a backlog or worker errors, then read the topic again. If you're using the
            script, keep the same terminal so <code>RUN_ID</code> still identifies the message you sent.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    When you're finished, remove the tutorial route, pipeline, sample, source and destination in a
    new reviewed change. Once the workers are running that change, you can remove the two Kafka
    topics too.
  </p>
  <h2 id="filtering">Try filtering rows and fields</h2>
  <p>
    Use a separate pipeline to try this example. Keep order and error events, drop routine health
    checks, and remove the debug field. Copy each input below into its own saved sample.
  </p>
  <ol>
    <li>
      Add a JSON Parser step with <code>_raw</code> as its input. Declare <code>event_type</code>,
      <code>message</code>, and <code>debug</code> as <code>str</code> fields.
    </li>
    <li>
      Add a Drop step with condition <code>{filtering.condition}</code>. A true condition removes
      the whole record.
    </li>
    <li>
      Add an Eval step with the assignments below. Setting these fields to <code>null</code> leaves
      them out of the delivered JSON. <code>_raw</code> is the original input text.
    </li>
  </ol>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Target field</th><th>Expression</th></tr></thead>
      <tbody
        >{#each filtering.assignments as assignment}<tr
            ><td><code>{assignment.target}</code></td><td
              ><code>{@html highlight(assignment.expr)}</code></td
            ></tr
          >{/each}</tbody
      >
    </table>
  </div>
  <FilteringExample />
  <p>Run each sample and check the result shown above.</p>

  <h2 id="next">Try your own data</h2>
  <p>
    Pick a few records from your source and remove sensitive values before saving them as samples.
    Work on a branch and check which routes use the pipeline; an edit to shared logic can affect
    other worker groups. Add one step at a time and inspect the fields and types in the preview.
  </p>
  <p>
    Include records that are awkward to handle. If your next pipeline converts a status code to a
    number, try these cases:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Input</th><th>What to check</th></tr></thead>
      <tbody>
        <tr><td><code>"200"</code></td><td>The result is a number.</td></tr>
        <tr
          ><td><code>"unknown"</code></td><td
            >The pipeline handles an invalid value as you intend.</td
          ></tr
        >
        <tr><td>A missing field</td><td>The pipeline handles an absent value as you intend.</td></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    Test filtering in both directions too. For a Drop step, save one record the step should drop and
    another that should pass through. When removing fields, check that unwanted columns are absent
    and the fields you need are still present.
  </p>
  <p>The preview checks your transformation; send a real record to check destination access too.</p>
  <p>
    Use the <a href="/docs/code-reference">language reference</a> as you build, then follow the
    <a href="/docs/review#check">review checklist</a> before deploying.
  </p>
</GuideLayout>

<style>
  details {
    margin: 1.5rem 0;
    padding: 1rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
  }
  summary {
    cursor: pointer;
    font-weight: 600;
    color: var(--ink);
  }
</style>
