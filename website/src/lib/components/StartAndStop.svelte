<!--
  Where a Kafka-protocol source starts and stops reading. Shared by the Kafka and Event Hubs pages, which differ
  only in what they call a topic and a record.
-->
<script lang="ts">
  let { topic, topics, record }: { topic: string; topics: string; record: string } = $props();
</script>

<p>
  <code>start</code> and <code>stop</code> apply to each partition of every {topic} the source reads.
  The group's workers share the partitions, whatever the start and the stop.
</p>
<ul>
  <li>
    <strong>Start.</strong> Where the source reads from: beginning, end, or a specific time.
  </li>
  <li>
    <strong>Stop.</strong> When to stop reading: never, or a specific time. Once every partition has reached
    it, the source waits, idle, until it is changed.
  </li>
  <li>Progress is saved and restarted sources continue from where they left off.</li>
</ul>
<p>
  To read the {topics} as a stream, leave both out. To read between 4 and 8 AM yesterday, start at 4 AM
  and stop at 8 AM.
</p>

<h3 id="move">Moving a source</h3>
<p>
  To reread or skip {record}s, change <code>start</code> and deploy. Each partition moves there once,
  then reads on.
</p>
<ul>
  <li>Setting the same <code>start</code> again moves nothing.</li>
  <li>Rolling back a deploy does not move the source back.</li>
</ul>
