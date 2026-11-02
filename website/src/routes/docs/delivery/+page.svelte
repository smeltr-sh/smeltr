<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  const sections = [
    { id: 'delivery', title: 'Delivery at a glance' },
    { id: 'hold', title: 'Hold and retry' },
    { id: 'refused', title: 'Handle refused records' },
    { id: 'dlq', title: 'Keep refused records' },
    { id: 'recovery', title: 'Recover and replay' },
  ];
</script>

<GuideLayout
  title="Delivery"
  description="How Smeltr confirms delivery, retries unreachable destinations, handles refused records, and replays saved records."
  {sections}
>
  <h1>Delivery</h1>
  <p class="lede">
    Smeltr advances a source's reading position after every chosen destination accepts the record or
    completes the configured action for a refused record. Smeltr retries unreachable destinations.
    These rules apply across <a href="/docs/destinations">destination types</a>.
  </p>

  <h2 id="delivery">Delivery at a glance</h2>
  <WorkflowDiagram kind="delivery" />
  <p>
    Each destination writes the record in its own <a href="/docs/destinations#formats">format</a>.
    An unreachable destination can keep the source paused until delivery succeeds. For records a
    destination refuses, the <a href="#refused">configured action</a> determines what happens next.
  </p>

  <h2 id="hold">Hold and retry</h2>
  <p>
    When a destination is unreachable, delivery retries. Connection failures, authentication
    failures, and throttling need attention at the receiving service or its connection settings. As
    buffers fill, Smeltr pauses the input streams sending records to that destination. Sources with
    independent delivery paths can continue.
  </p>
  <p>
    For Kafka inputs, Smeltr saves its reading position only after the required deliveries finish or
    the <strong>drop</strong> action discards the record. While the source is paused, Smeltr reads
    nothing more from Kafka, so records it has not read wait there for as long as the topic's
    <a href="https://kafka.apache.org/documentation/#topicconfigs_retention.ms">retention</a> keeps them.
    Allow enough for the outage and the time needed to catch up. Worker memory has a fixed limit and does
    not replace this storage. Smeltr cannot reread local logs and metrics from a retained Kafka topic.
  </p>
  <p>
    An unreachable destination does not automatically switch to a standby destination. Dead-letter
    delivery handles records that a destination refuses, using its
    <a href="#refused">configured refusal action</a>.
  </p>
  <h2 id="refused">Handle refused records</h2>
  <p>
    A record a destination cannot accept is <code>Refused</code>. Examples include a mapping
    conflict, an oversized message, or a text record missing its required field. An
    <strong>answer</strong> is the configured action for that refusal: block, deliver, or drop. Set
    the worker group's answer under <strong>Exceptions</strong> on the Routes page, or one
    destination's under <strong>Exceptions</strong> in its card.
  </p>
  <ProductScreenshot
    source="destination-overrides"
    height={3000}
    crop={{ x: 520, y: 1855, width: 1280, height: 450 }}
    alt="A destination's Exceptions: a refused record is delivered to kafka-dlq."
    caption="A destination's Exceptions hold only Refused. Here they send the record to kafka-dlq."
  />
  <ul>
    <li>
      <strong>block (default):</strong> keep the record and send it again while the source waits.
    </li>
    <li>
      <strong>deliver:</strong> send it to the destinations you pick, where it stays for investigation
      and later replay. Each must be configured and reachable.
    </li>
    <li><strong>drop:</strong> count the refused record and continue.</li>
  </ul>
  <p>
    A connection or authentication failure needs its own fix. A refusal action does not repair an
    unreachable service. Use pipeline filtering for records you intentionally want to discard.
  </p>

  <h2 id="dlq">Keep refused records for investigation</h2>
  <p>
    You can send a refused record to another destination, such as a Kafka topic, so you can inspect
    it and try sending it again after fixing the problem. This is often called a
    <strong>dead-letter queue</strong>. Any normal Smeltr destination can serve this role: you
    designate it by selecting it for the <strong>deliver</strong> action. Prepare the destination and
    its credentials before deploying the change.
  </p>
  <p>
    If you pick several destinations, all of them must confirm acceptance. For a Kafka source,
    Smeltr waits for these confirmations before saving its reading position past that record. You
    still need to investigate why the original destination refused it.
  </p>
  <h3>If the destination for refused records also fails</h3>
  <p>
    If it is unreachable, Smeltr retries and can pause the input streams sending records to it as
    buffers fill. If it refuses the record, its own configured action applies: block, deliver
    elsewhere, or drop. You can pass records through several destinations, but the path must never
    loop back to an earlier one. Each destination along the path writes the record in its own
    format.
  </p>
  <p>
    Inspect a saved record before planning replay. It holds the record in the
    <a href="/docs/destinations#formats">format</a> of the destination that kept it.
  </p>

  <h2 id="recovery">Recover and replay</h2>
  <p>
    After fixing a connection or receiving service, watch delivery resume and the source backlog
    drain. Records saved for investigation stay there until you arrange to send them through again,
    a process called replay. Smeltr does not automatically send them back to the original
    destination.
  </p>
  <ol>
    <li>
      Inspect the refusal and a saved record. Fix the mapping, pipeline, permissions, or capacity
      that caused it.
    </li>
    <li>
      Choose which saved records to retry. If they are in Kafka, use a separate source and consumer
      group, and select an explicit range of record positions or times. See <a
        href="/docs/sources/kafka">Kafka source reads</a
      >. Keep the retained records until you verify recovery.
    </li>
    <li>
      Build the recovery route on a branch. Use saved samples to test its output and delivery
      conditions. Send records to the intended destination and check that the route cannot feed them
      back into the same input topic.
    </li>
    <li>
      Plan for redelivery. Preserve stable document IDs when using Elasticsearch dedupe or
      overwrite, and check how downstream consumers handle duplicates.
    </li>
    <li>
      Review and deploy the recovery route. Verify the worker commit, actual destination records,
      rejection counts, and remaining backlog before widening the replay window.
    </li>
    <li>Retire the temporary recovery route after you have accounted for its selected records.</li>
  </ol>

  <p>
    For missing output, follow <a href="/docs/troubleshooting#no-output">No records arriving</a>. To
    check a working path end to end, use
    <a href="/docs/first-pipeline#verify">the tutorial's delivery check</a>.
  </p>
</GuideLayout>
