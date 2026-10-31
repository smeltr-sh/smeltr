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
    { id: 'sign-in', title: 'Signing in' },
    { id: 'columns', title: 'Columns' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="Apache Iceberg destination"
  description="Commit records as Parquet files to an Iceberg table in S3 through its REST catalog: an example, every setting, how it signs in, and the columns it writes."
  {sections}
>
  <h1>Apache Iceberg</h1>
  <p class="lede">
    Writes records as Parquet files to an Iceberg table in S3, or a store that speaks its API, and
    commits them to the table through its REST catalog. Each commit is a snapshot of the table.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Signs in to a Polaris catalog with OAuth client credentials, the client's secret in the
    worker-host variable <code>CATALOG_CLIENT_SECRET</code>, and writes the table
    <code>analytics.web.access_log</code>, partitioned by the day of <code>ts</code> and 16 buckets
    of <code>user.id</code> when it creates it. Storage is signed with what the catalog vends.
  </p>
  <pre><code>{@html highlight(json(examples.icebergDestination))}</code></pre>
  <ProductScreenshot
    source="iceberg-destination-form"
    width={832}
    height={808}
    alt="The Apache Iceberg form on the Destinations page: catalog https://polaris.example.com/api/catalog, namespace analytics.web, table access_log, partition by day(ts) and bucket(16, user.id), catalog sign-in OAuth client credentials, client id smeltr."
    caption="An Iceberg destination on the Destinations page. Choose the catalog sign-in, and its fields follow. Warehouse and scope sit under Catalog and storage."
  />
  <p>
    Signs in to AWS Glue's Iceberg endpoint with AWS signatures, as the worker host's AWS
    credentials, in the region the address names. Set the warehouse to the account's id, and the
    namespace to a Glue database.
  </p>
  <pre><code>{@html highlight(json(examples.icebergGlueDestination))}</code></pre>

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Have an Iceberg REST catalog the worker hosts can reach, such as
      <a href="https://polaris.apache.org/">Apache Polaris</a>,
      <a href="https://docs.aws.amazon.com/glue/latest/dg/iceberg-rest-endpoint.html"
        >AWS Glue's Iceberg endpoint</a
      >, or
      <a
        href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-open-source.html"
        >S3 Tables</a
      >. The table's files are in S3 or a store that speaks its API.
    </li>
    <li>
      Create the namespace in the catalog. The destination creates a missing table in it, but not
      the namespace.
    </li>
    <li>
      A table that exists is of
      <a href="https://iceberg.apache.org/spec/#format-versioning">format version</a> 2 or 3 and not encrypted.
      A table the destination creates is of format version 2.
    </li>
    <li>
      Give the principal the destination signs in to the catalog as permission to load and update
      the table, and to create it when it is missing. <a href="#sign-in">Signing in</a> lists the ways.
    </li>
    <li>
      Give whatever signs for storage permission to write and delete objects under the table's data
      location, and to read and write objects under its metadata location: the credentials the
      catalog vends, the access key or role the destination names, or the worker host's own. The
      destination deletes no metadata.
    </li>
    <li>
      Keep the table's location, its <code>write.data.path</code> and its
      <code>write.metadata.path</code> in one bucket. The destination refuses to start on a table whose
      paths are in another bucket than its location.
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
  <KindSettings group={DESTINATIONS} type="iceberg" />

  <h2 id="sign-in">Signing in</h2>
  <p>
    The destination signs in to the catalog one way, and to the table's storage another. The GUI
    form's catalog sign-in chooses the first.
  </p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col">Catalog sign-in</th><th scope="col">Settings</th><th scope="col"
            >What it sends</th
          ></tr
        ></thead
      >
      <tbody>
        <tr
          ><th scope="row">none</th><td>none of these</td><td
            >Calls with no sign-in, until the catalog gives a token of the table's own when it loads
            it.</td
          ></tr
        >
        <tr
          ><th scope="row">A bearer token</th><td><code>token</code></td><td
            >The token from the credential on every call, until the catalog gives a token of the
            table's own when it loads it.</td
          ></tr
        >
        <tr
          ><th scope="row">OAuth client credentials</th><td
            ><code>clientId</code>, <code>clientSecret</code>, and <code>tokenUrl</code> and
            <code>scope</code> when the catalog needs them</td
          ><td
            >A token it obtains from the token URL with the client's id and secret, replaced before
            it expires, and again when the catalog refuses it.</td
          ></tr
        >
        <tr
          ><th scope="row">AWS signatures</th><td><code>signingName</code></td><td
            >Every call signed with AWS Signature Version 4 for the service it names: <code
              >glue</code
            >
            for Glue's Iceberg endpoint, <code>s3tables</code> for S3 Tables. It signs with the
            access key or role the destination names for storage, else the worker host's AWS
            credentials, for the region the catalog's AWS address names, else
            <code>region</code>. When the address names no region, <code>region</code> is required.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>Storage is signed with the first of these the destination has:</p>
  <ol>
    <li>
      The access key in <code>accessKeyId</code> and <code>secretAccessKey</code>, or the role in
      <code>roleArn</code>.
    </li>
    <li>
      The credentials the catalog vends for the table. The destination asks for them when it loads
      the table, and asks again five minutes before they expire.
    </li>
    <li>The worker host's own AWS credentials, such as an instance role.</li>
  </ol>
  <p>
    The table's bucket is in <code>region</code>, else the region the catalog gives the table, else
    the one the catalog's AWS address names, else the worker host's, else <code>us-east-1</code>. It
    is reached at <code>endpoint</code>, else the endpoint the catalog gives the table, else, with
    no access key named, the one the worker host's AWS settings name, else AWS's own address for the
    region.
  </p>

  <h2 id="columns">Columns</h2>
  <p>
    The table's schema is the record the routes write here, its fields the table's columns, nested
    fields as structs and arrays as lists. A missing table is created with that schema and with
    <code>partitionBy</code> as its partition spec. A table that exists keeps its columns and its
    spec: a field it lacks, at any depth, is added as an optional column before any file is written,
    and a column the record lacks reads as null in the rows written. A field that can hold no value,
    <code>null</code> or a list or closed struct of nothing else, has no column. Each field is
    written as the Iceberg type of its
    <a href="/docs/code-reference#types">record type</a>:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Record type</th><th scope="col">Iceberg type</th></tr></thead>
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
        <tr><th scope="row"><code>datetime</code></th><td><code>timestamptz</code></td></tr>
        <tr><th scope="row"><code>bytes</code></th><td><code>binary</code></td></tr>
        <tr><th scope="row"><code>array&lt;T&gt;</code></th><td><code>list</code></td></tr>
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
    Every column the destination writes is optional. It refuses to start when the table holds a
    field as another type, or has a required column the record may leave null or does not write.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    When it starts, the destination loads the table from the catalog, or creates it, and adds the
    record's new columns. It then writes each batch's records into Parquet files under the table's
    data location, <code>write.data.path</code> when the table sets it, partitioned by the table's
    own spec: identity, <code>year</code>, <code>month</code>, <code>day</code>, <code>hour</code>,
    <code>bucket</code> and <code>truncate</code>. Each file carries the table's column ids. A file
    stays open across batches, one per partition, at most 64 at once; a 65th closes the one written
    least recently. A file also closes at <code>maxObjectBytes</code>.
  </p>
  <p>
    The destination commits when <code>fileIntervalSecs</code> passes, when the records it holds
    near <code>bufferBytes</code>, and whenever the worker waits for its destinations to acknowledge
    everything they hold. A commit closes every open file, writes a manifest of the new files and a
    manifest list under the table's metadata location, merging the parent's small manifests as the
    table's
    <code>commit.manifest.*</code> properties say, and asks the catalog to append them as the
    table's next snapshot on top of the one it read. In a table of format version 3, the snapshot
    gives its manifests row ids from the table's <code>next-row-id</code>, as the spec's
    <a href="https://iceberg.apache.org/spec/#row-lineage">row lineage</a> asks. A batch is acknowledged
    once the commit holding its records lands.
  </p>
  <p>
    A commit another writer beat to the table is made again at once on top of the new snapshot, up
    to 8 times. A failed call is tried again with backoff until <code>stallAfterSecs</code>. When
    the catalog's answer to a commit is lost, the destination looks for that commit in the table
    before it tries again, so it does not make it twice. Past <code>stallAfterSecs</code> the destination
    aborts its open uploads and writes the batches again. It deletes the files it finished unless a lost
    answer may have committed them; then they stay, and their rows may be in the table twice. A file storage
    keeps after a failure is an orphan for the table's own cleanup.
  </p>
  <p>
    Each commit is a snapshot the table keeps until it is expired. Expire snapshots and compact
    small files with the table's own maintenance.
  </p>
  <p>
    A file larger than 8 MiB goes up as a multipart upload, which the destination completes when the
    file closes and aborts when it fails. Two things leave one unfinished: the worker stops without
    shutting down (it is killed, it crashes, or its host is lost) while the file is open, or storage
    does not answer the abort within <code>stallAfterSecs</code>. A file stays open at most
    <code>fileIntervalSecs</code>, so give the bucket a
    <a
      href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-abort-incomplete-mpu-lifecycle-config.html"
      >lifecycle rule that aborts incomplete multipart uploads</a
    > after a day: longer than any file stays open.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source. After <code>fileIntervalSecs</code>, look
    for a new snapshot in the table's history, and query the table for the marker with any engine
    that reads Iceberg.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>the catalog refused …</strong></dt>
    <dd>
      The catalog answered with the status the message names. On 401 or 403, check the sign-in: the
      token, the client's id, secret and scope, or the AWS credentials and the signing name. On 404,
      check <code>catalog</code>, <code>warehouse</code> and that the namespace exists.
    </dd>
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
    <dt><strong>the table's column … is required …</strong></dt>
    <dd>
      Every column the destination writes is optional. Make the column optional in the table, or
      send the records to a table of their own.
    </dd>
    <dt><strong>it is of format version …, and versions 2 and 3 are written</strong></dt>
    <dd>
      Upgrade a version 1 table in place in Spark with
      <code
        >ALTER TABLE &lt;namespace&gt;.&lt;table&gt; SET TBLPROPERTIES ('format-version'='2')</code
      >, which rewrites no data file. A table of a later version is not written: write to a table of
      format version 2 or 3.
    </dd>
    <dt>
      <strong>the table's column … has a write default, and the record does not write it</strong>
    </dt>
    <dd>
      The table expects a writer to fill this column with its write default when a row leaves it
      out, and the destination does not. Write the field from the pipeline, or drop the column's
      write default.
    </dd>
    <dt>
      <strong
        >its partition field … is a transform of several columns, and only a transform of one column
        is written</strong
      >
    </dt>
    <dd>Write to a table whose partition fields each transform one column.</dd>
    <dt><strong>it is encrypted, and an encrypted table is not written</strong></dt>
    <dd>Write to a table that is not encrypted.</dd>
    <dt>
      <strong>the catalog vended no single credential for both the table's data location …</strong>
    </dt>
    <dd>
      The catalog's credentials cover one of the table's locations and not the other. Name an access
      key or a role for storage, or have the catalog vend one credential for both.
    </dd>
    <dt><strong>the catalog places the table's storage at …, plain HTTP off this host</strong></dt>
    <dd>
      The catalog gives the table an <code>http://</code> endpoint on another host. Name an
      <code>https://</code> <code>endpoint</code>.
    </dd>
    <dt><strong>Access denied writing to storage</strong></dt>
    <dd>
      Check that whatever signs for storage may write and delete objects under the table's data
      location, and read and write objects under its metadata location.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/delivery">Delivery</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
