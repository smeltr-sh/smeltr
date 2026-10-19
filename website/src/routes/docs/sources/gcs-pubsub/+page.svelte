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
  title="Google Cloud Storage via Pub/Sub source"
  description="Read the objects Cloud Storage announces on a Pub/Sub subscription: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Google Cloud Storage via Pub/Sub</h1>
  <p class="lede">
    Reads each object Cloud Storage announces on a Pub/Sub subscription, splits it into records, and
    acknowledges the message once Smeltr delivers every record.
  </p>
  <p>
    A bucket's <strong>Pub/Sub notifications</strong> publish a message to a topic for each change to
    its objects. The source pulls those messages from a subscription on the topic and reads the objects
    they name, from whichever bucket each one names.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads each announced object one line per record, with the service account key from the
    worker-host variable <code>GCS_SERVICE_ACCOUNT_KEY</code>.
  </p>
  <pre><code>{@html highlight(json(examples.gcsPubSubSource))}</code></pre>
  <ProductScreenshot
    source="gcs-pubsub-source-form"
    height={2100}
    crop={{ x: 608, y: 512, width: 1664, height: 1076 }}
    alt="The Google Cloud Storage via Pub/Sub form on the Sources page: id bucket-logs, its subscription, framing lines."
    caption="A Google Cloud Storage via Pub/Sub source on the Sources page, signed in with the host's Application Default Credentials."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      <a href="https://cloud.google.com/storage/docs/reporting-changes"
        >Configure Pub/Sub notifications</a
      >
      for the bucket to a topic. Either payload format works, and you may limit the event type to
      <code>OBJECT_FINALIZE</code>.
    </li>
    <li>
      <a href="https://cloud.google.com/pubsub/docs/create-subscription"
        >Create a pull subscription</a
      >
      on the topic, and give it a
      <a href="https://cloud.google.com/pubsub/docs/handling-failures">dead-letter topic</a>, so a
      message the source cannot use stops returning: one whose object it cannot read, or one that is
      not a Cloud Storage notification at all. Exactly-once delivery may be on or off.
    </li>
    <li>
      Give the service account it reads as the Pub/Sub Subscriber role on the subscription and the
      Storage Object Viewer role on the buckets. See
      <a href="https://cloud.google.com/storage/docs/access-control/iam-roles"
        >Cloud Storage IAM roles</a
      >.
    </li>
    <li>
      Choose how it signs in. With no key, it uses the worker host's
      <a href="https://cloud.google.com/docs/authentication/application-default-credentials"
        >Application Default Credentials</a
      >. Otherwise put a
      <a href="https://cloud.google.com/iam/docs/keys-create-delete">service account key</a>, the
      whole JSON file, in a <a href="/docs/secrets">credential</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="gcsPubSub" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 'gcsPubSub').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The source reads a notification from its message's attributes, so both payload formats read the
    same. It reads the object generation an <code>OBJECT_FINALIZE</code> notification names. The source
    acknowledges a notification of any other event, such as a deleted object, without reading anything.
    A message that is not a Cloud Storage notification stays on the subscription, like one whose object
    the source cannot read.
  </p>
  <p>
    The source decompresses a gzip, zstd, framed snappy or framed lz4 object first and reads any
    other as it is.
  </p>
  <p>
    The source extends a message's ack deadline while it reads the message's objects and Smeltr
    delivers them, and acknowledges the message once Smeltr commits every record of every object it
    names. It never acknowledges a message whose object it cannot read.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload an object with a line you can recognize to the bucket, then find that line at the
    destination. The subscription's unacknowledged message count returns to zero once Smeltr
    delivers it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the bucket has a notification configuration for this topic, that Cloud Storage's
      service agent may publish to the topic, and that the subscription is on the same topic.
    </dd>
    <dt><strong>The same object is read again and again</strong></dt>
    <dd>
      The source cannot read the object at the generation announced: check Storage Object Viewer on
      its bucket and the worker log for the reason. A dead-letter topic stops the repeats.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/destinations/gcs">Google Cloud Storage destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
