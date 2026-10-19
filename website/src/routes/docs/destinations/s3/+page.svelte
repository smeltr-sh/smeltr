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
  title="Amazon S3 destination"
  description="Write batches as objects in an S3 bucket: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Amazon S3</h1>
  <p class="lede">
    Writes batches of records as objects in an S3 bucket, or a bucket in an S3-compatible store.
  </p>

  <h2 id="example">Example</h2>
  <p>Writes with the worker host's AWS credentials.</p>
  <pre><code>{@html highlight(json(examples.s3Destination))}</code></pre>
  <ProductScreenshot
    source="s3-destination-form"
    height={2500}
    crop={{ x: 608, y: 644, width: 1664, height: 1212 }}
    alt="The Amazon S3 form on the Destinations page: id archive, bucket acme-logs, format ndjson."
    caption="The same destination on the Destinations page. Region sits under Endpoint and delivery."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html"
        >Create the bucket</a
      >.
    </li>
    <li>
      Give the identity the destination writes as permission to put objects in the bucket (<code
        >s3:PutObject</code
      >). See
      <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/example-policies-s3.html"
        >S3 policy examples</a
      >. With <code>kmsKeyId</code>, it also needs to use that key; see
      <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html"
        >SSE-KMS</a
      >.
    </li>
    <li>
      Choose how it signs in. With no access key, it uses the worker host's own AWS credentials,
      such as an instance role; see
      <a href="https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html"
        >AWS credential providers</a
      >. Otherwise put the access key's id and secret in
      <a href="/docs/secrets">credentials</a>.
    </li>
    <li>
      For a bucket in another account, give a role to assume in <code>roleArn</code>, and its
      external id if it requires one. See
      <a
        href="https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_common-scenarios_aws-accounts.html"
        >cross-account roles</a
      >.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="s3" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 's3').reads ?? []} />

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
    <dt><strong>Access denied</strong></dt>
    <dd>
      Check that the identity may put objects in the bucket, and may use the KMS key when <code
        >kmsKeyId</code
      > is set.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/gcs">Google Cloud Storage destination</a></li>
    <li><a href="/docs/destinations/azure-blob">Azure Blob Storage destination</a></li>
    <li><a href="/docs/destinations/iceberg">Apache Iceberg destination</a></li>
    <li><a href="/docs/destinations/delta">Delta Lake destination</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
