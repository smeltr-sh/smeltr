<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import { DESTINATIONS } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'permissions', title: 'Storage permissions' },
    { id: 'unity-catalog', title: 'Unity Catalog' },
    { id: 'columns', title: 'Columns' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
  const policy = {
    Version: '2012-10-17',
    Statement: [
      {
        Effect: 'Allow',
        Action: ['s3:ListBucket'],
        Resource: 'arn:aws:s3:::lake',
        Condition: { StringLike: { 's3:prefix': 'tables/access_log/_delta_log/*' } },
      },
      {
        Effect: 'Allow',
        Action: ['s3:GetObject', 's3:PutObject', 's3:DeleteObject', 's3:AbortMultipartUpload'],
        Resource: 'arn:aws:s3:::lake/tables/access_log/*',
      },
    ],
  };
  const layout = `s3://lake/tables/access_log/
  _delta_log/
    00000000000000000000.json                 the table, created
    00000000000000000001.json                 a commit: a version of the table
    …
    00000000000000000010.checkpoint.parquet   the table's whole state at version 10
    _last_checkpoint
  host=web-1/
    5f0c…e1.parquet
  host=__HIVE_DEFAULT_PARTITION__/
    9a42…07.parquet                           rows whose host is null or empty`;
</script>

<GuideLayout
  title="Delta Lake destination"
  description="Commit records as Parquet files to a Delta table in S3 through its _delta_log: an example, every setting, the storage permissions it needs, and the columns it writes."
  {sections}
>
  <h1>Delta Lake</h1>
  <p class="lede">
    Writes records as Parquet files to a Delta table in S3, or a store that speaks its API, and
    commits them as the next entry of the table's <code>_delta_log</code>. The table is reached by
    its location, or by its name in Databricks Unity Catalog. Each commit is a version of the table.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Writes the table at <code>s3://lake/tables/access_log</code> with the worker host's AWS
    credentials, and partitions it by <code>host</code> when it creates it.
  </p>
  <pre><code>{@html highlight(json(examples.deltaDestination))}</code></pre>
  <ProductScreenshot
    source="delta-destination-form"
    width={832}
    height={608}
    alt="The Delta form on the Destinations page: id lake-access, location s3://lake/tables/access_log, partition by host, the host's credentials."
    caption="A Delta destination on the Destinations page. Region, endpoint and role sit under Storage."
  />
  <p>
    Writes the same table in a MinIO bucket, signed with the access key in the worker-host variables <code
      >MINIO_ACCESS_KEY</code
    >
    and <code>MINIO_SECRET_KEY</code>.
  </p>
  <pre><code>{@html highlight(json(examples.deltaMinioDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Have a bucket in S3, or in a store that speaks its API, including conditional writes: a write
      with <code>If-None-Match: *</code> must succeed only when no object is at its key.
    </li>
    <li>
      The table is the one at <code>location</code>, and the destination creates a missing one
      there. Or it is the external table <code>unityCatalog</code> names, which must exist: see
      <a href="#unity-catalog">Unity Catalog</a>. A table that Unity Catalog manages is not written.
    </li>
    <li>
      A table that exists is one this writer can append to:
      <ul>
        <li>
          its protocol needs reader version 3 or lower and writer version 7 or lower, and each
          writer feature it names is one of <code>appendOnly</code>, <code>invariants</code>,
          <code>checkConstraints</code>, <code>changeDataFeed</code>,
          <code>columnMapping</code>, <code>v2Checkpoint</code>, <code>domainMetadata</code>,
          <code>deletionVectors</code>, <code>vacuumProtocolCheck</code>,
          <code>timestampNtz</code>, <code>clustering</code>, <code>rowTracking</code>,
          <code>typeWidening</code>, <code>identityColumns</code> and
          <code>inCommitTimestamp</code>;
        </li>
        <li>its files are Parquet;</li>
        <li>it maps columns by name or by id, or not at all;</li>
        <li>it sets no check constraint;</li>
        <li>
          no column is generated, has a default or carries an invariant, and every column is
          nullable, as is every array's element, except an
          <a href="#identity-columns">identity column</a> the record does not write;
        </li>
        <li>it has a column a data file can hold, one that is not <code>void</code> alone;</li>
        <li>
          its checkpoints keep file statistics as JSON: <code
            >delta.checkpoint.writeStatsAsJson</code
          >
          is not <code>false</code> and <code>delta.checkpoint.writeStatsAsStruct</code> is not
          <code>true</code>;
        </li>
        <li>
          it is partitioned by columns of type <code>boolean</code>, <code>byte</code>,
          <code>short</code>, <code>integer</code>, <code>long</code>, <code>double</code>,
          <code>date</code>, <code>timestamp</code> or <code>string</code>.
        </li>
      </ul>
      The destination refuses to start on any other, and says why.
    </li>
    <li>
      Give whatever signs for storage the permissions <a href="#permissions">Storage permissions</a> lists:
      the access key or role the destination names, else the worker host's AWS credentials. Through Unity
      Catalog, the credentials it vends must carry them.
    </li>
    <li>
      Send this destination records of one type: the routes writing to it write the same record,
      which is the table's schema. See <a href="#columns">Columns</a>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="delta" />

  <h2 id="permissions">Storage permissions</h2>
  <p>Whatever signs for storage needs, under the table's location:</p>
  <ul>
    <li><code>s3:ListBucket</code> on the table's <code>_delta_log/</code>;</li>
    <li><code>s3:GetObject</code>, <code>s3:PutObject</code> and <code>s3:DeleteObject</code>;</li>
    <li><code>s3:AbortMultipartUpload</code>.</li>
  </ul>
  <p>A policy for the table in the example:</p>
  <pre><code>{@html highlight(json(policy))}</code></pre>
  <p>
    With <code>roleArn</code>, the role carries this policy and the credentials the destination
    starts from may assume it.
  </p>

  <h2 id="unity-catalog">Unity Catalog</h2>
  <p>
    A Delta table registered in Databricks Unity Catalog is named by <code>unityCatalog</code>, in
    place of <code>location</code>. The destination signs in to the workspace as a service
    principal, takes the table's location and storage credentials from Unity Catalog, and renews the
    credentials before they expire, so it holds no AWS credentials of its own. The table is an
    external Delta table in S3, and it must exist. Everything after that is as for a table named by
    <code>location</code>.
  </p>
  <p>
    Writes the external table <code>main.logs.web</code>, signed in to its workspace as the service
    principal whose OAuth secret is in the worker-host variable
    <code>DATABRICKS_CLIENT_SECRET</code>. The table's bucket is in <code>us-west-2</code>.
  </p>
  <pre><code>{@html highlight(json(examples.deltaUnityCatalogDestination))}</code></pre>

  <h3 id="unity-catalog-databricks">Set up Databricks</h3>
  <p>
    Create the service principal and its OAuth secret as Databricks'
    <a href="https://docs.databricks.com/aws/en/dev-tools/auth/oauth-m2m"
      >OAuth machine-to-machine authentication</a
    >
    describes, and give it access to the table as
    <a href="https://docs.databricks.com/aws/en/external-access/credential-vending"
      >credential vending for external system access</a
    >
    describes. Then:
  </p>
  <ul>
    <li><code>workspace</code> is the workspace's URL;</li>
    <li><code>table</code> is the table's name, <code>catalog.schema.table</code>;</li>
    <li><code>clientId</code> is the service principal's client id;</li>
    <li>
      <code>clientSecret</code> is the <a href="/docs/secrets">credential</a> holding its OAuth secret;
    </li>
    <li>
      <code>region</code>, beside <code>unityCatalog</code>, is the region of the table's bucket.
    </li>
  </ul>

  <h3 id="unity-catalog-refused">What is refused</h3>
  <p>The configuration is refused when a Delta destination:</p>
  <ul>
    <li>names both <code>location</code> and <code>unityCatalog</code>, or neither;</li>
    <li>
      gives a <code>workspace</code> that is not an <code>https://</code> address with nothing after
      its host, or an <code>http://</code> one off this host;
    </li>
    <li>names a <code>table</code> that is not three parts, <code>catalog.schema.table</code>;</li>
    <li>names no <code>clientId</code>;</li>
    <li>names no <code>region</code> beside <code>unityCatalog</code>;</li>
    <li>
      names <code>accessKeyId</code>, <code>secretAccessKey</code>, <code>roleArn</code> or
      <code>authEndpoint</code> beside <code>unityCatalog</code>: the destination takes its storage
      credentials from Unity Catalog.
    </li>
  </ul>
  <p>
    The destination refuses to start on a table that is not Delta, not external, or not in S3, and
    on one reached through a multi-region access point.
  </p>

  <h2 id="columns">Columns</h2>
  <p>
    The table's schema is the record the routes write here, its fields the table's columns, nested
    fields as structs and arrays as arrays. A missing table is created with that schema, as a plain
    Delta table, with <code>partitionBy</code> as its partition columns. A table that exists keeps
    its columns and its partitioning: a field it lacks, at any depth, is added as a nullable column
    before any file is written, and a column the record lacks reads as null in the rows written. A
    field that can hold no value,
    <code>null</code> or a list or closed struct of nothing else, has no column. Each field is
    written as the Delta type of its
    <a href="/docs/code-reference#types">record type</a>:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Record type</th><th scope="col">Delta type</th></tr></thead>
      <tbody>
        <tr><th scope="row"><code>bool</code></th><td><code>boolean</code></td></tr>
        <tr><th scope="row"><code>i64</code></th><td><code>long</code></td></tr>
        <tr
          ><th scope="row"><code>duration</code></th><td
            ><code>long</code>, its count of microseconds</td
          ></tr
        >
        <tr><th scope="row"><code>f64</code></th><td><code>double</code></td></tr>
        <tr><th scope="row"><code>str</code></th><td><code>string</code></td></tr>
        <tr
          ><th scope="row"><code>variant</code></th><td><code>string</code>, the value as JSON</td
          ></tr
        >
        <tr><th scope="row"><code>datetime</code></th><td><code>timestamp</code>, in UTC</td></tr>
        <tr><th scope="row"><code>bytes</code></th><td><code>binary</code></td></tr>
        <tr><th scope="row"><code>array&lt;T&gt;</code></th><td><code>array</code></td></tr>
        <tr
          ><th scope="row"><code>{'{name: T, ...}'}</code></th><td
            ><code>struct</code>; an open object's other keys go in a <code>string</code> column
            named <code>rest</code></td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    It refuses to start when the table holds a field as another type, or has two columns of one
    struct whose names differ only in case. A partition column is a top-level column of a primitive
    type other than <code>bytes</code>.
  </p>

  <h3 id="identity-columns">Identity columns</h3>
  <p>
    The destination generates the values of each top-level identity column the record does not
    write. Each row takes the next value in the column's sequence, which starts at
    <code>delta.identity.start</code>, moves by <code>delta.identity.step</code> and lies past
    <code>delta.identity.highWaterMark</code>. No value is given twice, by this writer or another.
    It reserves values a block at a time, in commits that raise the high-water mark, so the mark can
    stand past the largest value in the table. A row that would take a value past a
    <code>long</code>'s range is <a href="/docs/delivery#refused">refused</a>. The record may write
    an identity column only if the column sets <code>delta.identity.allowExplicitInsert</code>; its
    values are then written as they are.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    When it starts, the destination reads the table, or creates it, and adds the record's new
    columns. It writes records as Parquet files under the table's root, in a directory per partition
    value:
  </p>
  <pre><code>{layout}</code></pre>
  <p>
    A null or empty partition value is written as <code>__HIVE_DEFAULT_PARTITION__</code> in the
    path. A row whose partition values would make a file's key longer than 1024 bytes is
    <a href="/docs/delivery#refused">refused</a>.
  </p>
  <p>
    The destination commits its files as the table's next version when
    <code>fileIntervalSecs</code> passes, or sooner when the records it holds near
    <code>bufferBytes</code>. A batch is acknowledged once the commit holding its records lands.
    Other writers may write the same table. A failed write is tried again until
    <code>stallAfterSecs</code>, and then the batches are written again. When storage's answer to a
    commit is lost past that point, its rows may be in the table twice. Where the table sets
    <code>delta.enableInCommitTimestamps</code> to <code>true</code>, each commit records the time
    it was made, or, when the commit before it is dated at that time or later, one millisecond after
    that commit, so the table's history stays in order when clocks differ.
  </p>
  <p>
    Every <code>delta.checkpointInterval</code> commits, 10 when the table does not set it, the
    destination writes a checkpoint, and it deletes log entries older than
    <code>delta.logRetentionDuration</code>, 30 days when the table does not set it, unless
    <code>delta.enableExpiredLogCleanup</code> is <code>false</code>.
  </p>
  <p>
    The destination only appends. Compact small files, and vacuum the files no version needs, with
    the table's own maintenance. Large files go up as multipart uploads, and a worker that stops
    without shutting down can leave one unfinished. Give the bucket a
    <a
      href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html"
      >lifecycle rule that aborts incomplete multipart uploads</a
    > after a day.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source. After <code>fileIntervalSecs</code>, look
    for a new commit under <code>_delta_log/</code>, or a new version in the table's history, and
    query the table for the marker with any engine that reads Delta.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt>
      <strong>the routes writing here write different records, and a table has one schema</strong>
    </dt>
    <dd>
      Two routes feeding the destination write records of different types. Make them write the same
      record, or give each table a destination of its own.
    </dd>
    <dt><strong>… is … in the table, and the record writes … there as …</strong></dt>
    <dd>
      The table holds a column as another type than the record's field. Rename the field in the
      pipeline or the column in the table.
    </dd>
    <dt><strong>the table's column … is not nullable …</strong></dt>
    <dd>
      Every column the destination writes is nullable. Make the column nullable in the table, or
      send the records to a table of their own.
    </dd>
    <dt>
      <strong>the table needs writer version …</strong>, <strong>reader version …</strong>, or
      <strong>the writer feature …</strong>
    </dt>
    <dd>
      The table's protocol asks for more than this writer does. Write to a table whose protocol
      <a href="#before-you-start">Before you start</a> lists.
    </dd>
    <dt><strong>the table maps columns by …, which this writer does not</strong></dt>
    <dd>Write to a table that maps columns by name or by id, or not at all.</dd>
    <dt><strong>the table has the check constraint …</strong></dt>
    <dd>
      The destination does not check constraints. Drop the constraint, or write another table.
    </dd>
    <dt><strong>the table's column … carries …, which this writer cannot honour</strong></dt>
    <dd>
      The column is generated, has a default, carries an invariant, or is an identity column inside
      a struct, which the destination does not compute or check. Write to a table without such a
      column.
    </dd>
    <dt>
      <strong
        >the record writes …, an identity column the table generates and lets no writer set</strong
      >
    </dt>
    <dd>
      Drop the field in the pipeline, so the destination generates the column's values, or set
      <code>delta.identity.allowExplicitInsert</code> on the column.
    </dd>
    <dt><strong>the table is partitioned by its identity column …</strong></dt>
    <dd>
      A partition value is the record's, and the destination generates this column's values. Write
      to a table partitioned by other columns.
    </dd>
    <dt>
      <strong>the identity column … has no value left in a long's range</strong>
    </dt>
    <dd>
      The column's sequence has reached the end of a <code>long</code>'s range, and the row is
      refused. Give the table a new identity column.
    </dd>
    <dt><strong>the table is partitioned by …, a … column …</strong></dt>
    <dd>
      The partition column is of a type the destination writes no partition value of. Partition by
      another column.
    </dd>
    <dt><strong>the row's partition values make a key of … bytes …</strong></dt>
    <dd>
      The row's partition values make the file's key too long. Partition by a shorter value, or
      shorten it in the pipeline.
    </dd>
    <dt>
      <strong>another writer dropped a column the record writes</strong>,
      <strong>changed the table's partitioning</strong>, or
      <strong>another table replaced the one at its location</strong>
    </dt>
    <dd>
      The table changed under the destination, and its files no longer fit it. Commits wait until
      they do. Restore the column or the partitioning, or the destination takes the table as it then
      is when it next starts.
    </dd>
    <dt><strong>commit lost to another writer 8 times over</strong></dt>
    <dd>
      Other writers took every version the destination tried. Lower how often they commit, or raise
      <code>fileIntervalSecs</code>.
    </dd>
    <dt><strong>Access denied writing to storage</strong></dt>
    <dd>
      Check that whatever signs for storage has every permission
      <a href="#permissions">Storage permissions</a> lists, under the table's location.
    </dd>
    <dt>
      <strong>names both a location and a unityCatalog table</strong>, or
      <strong>names no table</strong>
    </dt>
    <dd>
      Name the table's root in S3 as <code>location</code>, or its <code>unityCatalog</code> table.
    </dd>
    <dt><strong>names no region beside a unityCatalog table</strong></dt>
    <dd>
      Name the region of the table's bucket as <code>region</code>.
    </dd>
    <dt><strong>names … beside a unityCatalog table</strong></dt>
    <dd>
      The destination takes its storage credentials from Unity Catalog. Take the access key, the
      role or the auth endpoint out.
    </dd>
    <dt><strong>… /oidc/v1/token refused the sign-in (…): …</strong></dt>
    <dd>
      The workspace refused the service principal's client id or secret. Check
      <code>clientId</code> and the secret <code>clientSecret</code> names.
    </dd>
    <dt>
      <strong
        >the delta destination takes its client secret from …, which this worker does not have</strong
      >
    </dt>
    <dd>
      The worker cannot read the <a href="/docs/secrets">credential</a> <code>clientSecret</code> names.
      Set the variable on the worker host, or store the secret.
    </dd>
    <dt><strong>Unity Catalog refused the table … (…): …</strong></dt>
    <dd>
      Unity Catalog answered with the status and the error the message names. On 403, check the
      service principal's access to the table: see <a href="#unity-catalog-databricks"
        >Set up Databricks</a
      >. On 404, check <code>table</code>.
    </dd>
    <dt><strong>the table is …, not Delta</strong></dt>
    <dd>The table is in another format. Name a Delta table.</dd>
    <dt>
      <strong>the table is …; only an external table is written</strong>
    </dt>
    <dd>The table is managed, or of another type. Name an external Delta table.</dd>
    <dt><strong>Unity Catalog refused credentials for the table's storage (…): …</strong></dt>
    <dd>
      Check the service principal's access to write the table. See
      <a href="#unity-catalog-databricks">Set up Databricks</a>.
    </dd>
    <dt>
      <strong
        >Unity Catalog vended no AWS credentials, or no expiry, for the table's storage; the table
        must be in S3</strong
      >
    </dt>
    <dd>The destination writes only tables in S3.</dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/destinations/iceberg">Apache Iceberg destination</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
