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
    { id: 'security-lake', title: 'Amazon Security Lake' },
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

  <h2 id="security-lake">Amazon Security Lake</h2>
  <p>
    Security Lake reads OCSF records as Parquet objects that a custom source writes to its bucket.
  </p>
  <ol>
    <li>
      <a href="https://docs.aws.amazon.com/security-lake/latest/userguide/custom-sources.html"
        >Add a custom source</a
      > in Security Lake for each OCSF class you send. Each gives a location in S3, and a role to assume
      with its external id.
    </li>
    <li>
      <a href="/docs/schemas#ocsf">Add a schema from the OCSF class</a> at the OCSF release Security
      Lake reads for custom sources, which the
      <a href="https://docs.aws.amazon.com/security-lake/latest/userguide/custom-sources.html"
        >Add a custom source</a
      >
      page names, and choose it in the pipeline's <a href="/docs/schemas#produces">Produces bar</a>.
      <ProductScreenshot
        source="security-lake-schema"
        height={1800}
        crop={{ x: 608, y: 478, width: 1664, height: 851 }}
        alt="The New schema from an OCSF class dialog: id authentication, OCSF 1.3.0 built in, the Authentication class."
        caption="For example, the Authentication class, read from OCSF 1.3.0."
      />
      <ProductScreenshot
        source="security-lake-produces"
        height={1800}
        crop={{ x: 470, y: 420, width: 1170, height: 640 }}
        alt="The okta-auth pipeline with authentication in its Produces bar, and the fields the rows still lack."
        caption="The pipeline produces authentication. Until its steps fill the fields OCSF requires, the editor lists them."
      />
    </li>
    <li>
      Add an S3 destination for each custom source, with its location's bucket, its role in <code
        >roleArn</code
      >
      and its external id in <code>externalId</code>, format <code>parquet</code> and compression
      <code>zstd</code>. Set <code>prefix</code> to the rest of its location and the folders the
      <a href="https://docs.aws.amazon.com/security-lake/latest/userguide/custom-sources.html"
        >Add a custom source</a
      >
      page lists below it, with <code>%Y%m%d</code> for the day.
    </li>
    <li>Route each class to its own destination.</li>
  </ol>
  <p>
    For example, Okta sign-ins mapped to OCSF's Authentication class, sent to a custom source named
    <code>okta-auth</code> in account 123456789012:
  </p>
  <pre><code>{@html highlight(json(examples.securityLakeDestination))}</code></pre>
  <ProductScreenshot
    source="security-lake-destination"
    height={3000}
    crop={{ x: 608, y: 64, width: 1664, height: 2496 }}
    alt="The Amazon S3 form on the Destinations page: id lake-okta-auth, its bucket and prefix, format parquet, and under Endpoint and delivery region us-east-1, the role ARN, external id acme-okta-auth and compression zstd."
    caption="The same destination on the Destinations page. The role, its external id and compression sit under Endpoint and delivery."
  />
  <p>
    A file it writes on 3 November 2026 lands at
    <code>ext/okta-auth/region=us-east-1/accountId=123456789012/eventDay=20261103/</code>, named by
    a random id with <code>.parquet</code> at the end.
  </p>
  <p>
    <code>accepts</code> names the schema from step 2, so review flags a route whose pipeline does not
    produce it.
  </p>
  <p>
    <code>%Y%m%d</code> is the day the file is written, in UTC, so a record written after midnight
    gets the next day's <code>eventDay</code>. Security Lake expects <code>eventDay</code> to be the
    record's own day. To give each record its own day, set <code>__destination.prefix</code> from
    its
    <code>time</code> in a
    <a href="/docs/steps/code">Code step</a>:
  </p>
  <pre><code
      >{'__destination.prefix = "ext/okta-auth/region=us-east-1/accountId=123456789012/eventDay="\n  + fromEpochMillis(time).toString().slice(0, 10).replaceAll(/-/, "") + "/";'}</code
    ></pre>

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
