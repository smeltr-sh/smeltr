<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { DESTINATIONS, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-reads', title: 'Fields it reads' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Google Cloud Storage destination"
  description="Write batches as objects in a Cloud Storage bucket: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Google Cloud Storage</h1>
  <p class="lede">Writes batches of records as objects in a Cloud Storage bucket.</p>

  <h2 id="example">Example</h2>
  <p>
    Reads the service account key from the worker-host variable <code>GCS_SERVICE_ACCOUNT_KEY</code
    >.
  </p>
  <pre><code>{@html highlight(json(examples.gcsDestination))}</code></pre>
  <ProductScreenshot
    source="gcs-destination-form"
    height={2500}
    crop={{ x: 608, y: 644, width: 1664, height: 1212 }}
    alt="The Google Cloud Storage form on the Destinations page: id archive, bucket acme-logs, format ndjson."
    caption="A Google Cloud Storage destination on the Destinations page, signed in with the host's Application Default Credentials."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      <a href="https://cloud.google.com/storage/docs/creating-buckets">Create the bucket</a>.
    </li>
    <li>
      Give the service account the destination writes as permission to create objects in the bucket,
      such as the Storage Object Creator role. See
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
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="gcs" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'gcs').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination keeps one object open per prefix, and for Parquet, Avro and CSV per record type
    of the routes feeding it, across batches, named <code
      >&lt;prefix&gt;&lt;random id&gt;.&lt;extension&gt;</code
    >, and finishes it when it reaches <code>maxObjectBytes</code> on disk, when
    <code>fileIntervalSecs</code>
    passes, when <code>bufferBytes</code> fills on the worker, or when a 65th object opens and this one
    was written least recently. A batch is acknowledged once every object holding its records has landed.
    Two objects never share a key, so records written again after a failure make new objects.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then list the bucket under the prefix for
    a new object and look for the marker in it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Permission denied</strong></dt>
    <dd>Check that the service account may create objects in the bucket.</dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/destinations/azure-blob">Azure Blob Storage destination</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
