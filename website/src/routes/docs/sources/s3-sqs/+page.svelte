<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Amazon S3 via SQS source"
  description="Read the objects S3 announces on an SQS queue: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Amazon S3 via SQS</h1>
  <p class="lede">
    Reads each object S3 announces on an SQS queue, splits it into records, and deletes the message
    once Smeltr delivers every record.
  </p>
  <p>
    The source receives a bucket's object-created notifications from an SQS queue and reads the
    objects they name, from whichever bucket each one names. It reads a notification that reaches
    the queue directly from the bucket, through an SNS topic, or through an EventBridge rule.
  </p>

  <h2 id="example">Example</h2>
  <p>Reads each announced object one line per record, with the worker host's AWS credentials.</p>
  <pre><code>{@html highlight(json(examples.s3SqsSource))}</code></pre>
  <ProductScreenshot
    source="s3-sqs-source-form"
    height={2100}
    crop={{ x: 608, y: 512, width: 1664, height: 1076 }}
    alt="The S3 via SQS form on the Sources page: id bucket-logs, its queue URL, framing lines."
    caption="The same source on the Sources page."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Create an SQS queue in the bucket's region, and have the bucket's object-created events reach
      it by one of these:
      <ul>
        <li>
          <strong>Directly</strong>: AWS describes it in
          <a
            href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/ways-to-add-notification-config-to-bucket.html"
            >Configuring a bucket for notifications</a
          >.
        </li>
        <li>
          <strong>Through SNS</strong>: send the bucket's events to an SNS topic and
          <a href="https://docs.aws.amazon.com/sns/latest/dg/subscribe-sqs-queue-to-sns-topic.html"
            >subscribe the queue to the topic</a
          >. Raw message delivery may be on or off.
        </li>
        <li>
          <strong>Through EventBridge</strong>:
          <a
            href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/enable-event-notifications-eventbridge.html"
            >turn on EventBridge</a
          >
          for the bucket, and add a rule that sends its <code>Object Created</code> events to the queue.
        </li>
      </ul>
    </li>
    <li>
      Give the queue a
      <a
        href="https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html"
        >dead-letter queue</a
      >, so a message the source cannot use stops returning: one whose object it cannot read, or one
      that is not about S3 at all, such as another message on the same SNS topic.
    </li>
    <li>
      Allow the credentials Smeltr uses <code>sqs:ReceiveMessage</code>,
      <code>sqs:ChangeMessageVisibility</code> and <code>sqs:DeleteMessage</code> on the queue, and
      <code>s3:GetObject</code> on the buckets. A versioned bucket also needs
      <code>s3:GetObjectVersion</code>. Allow <code>sqs:GetQueueAttributes</code> too for the source to
      report the queue's depth as its lag; without it the source reads the same, and reports no lag.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="s3Sqs" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 's3Sqs').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The source reads the object version the notification names, and decodes a URL-encoded key. The
    source deletes a message that announces anything other than a created object without reading
    anything. A message that is not about S3 at all stays on the queue, like one whose object the
    source cannot read.
  </p>
  <p>
    The source decompresses a gzip, zstd, framed snappy or framed lz4 object first and reads any
    other as it is.
  </p>
  <p>
    The source keeps a message hidden while it reads the message's objects and Smeltr delivers them,
    and deletes it once Smeltr commits every record of every object it names. It never deletes a
    message whose object it cannot read.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload an object with a line you can recognize to the bucket, then find that line at the
    destination. The queue's message count returns to zero once Smeltr delivers it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the bucket's object-created events reach this queue, and that the queue's access
      policy lets the sender in: S3 when the bucket sends directly, SNS when it goes through a
      topic, whose own access policy must also let S3 publish to it, EventBridge when it goes
      through a rule. For EventBridge, also check that the bucket has EventBridge turned on.
    </dd>
    <dt><strong>The same object is read again and again</strong></dt>
    <dd>
      The source cannot read the object: check <code>s3:GetObject</code> on its bucket and the worker
      log for the reason. A dead-letter queue stops the repeats.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
