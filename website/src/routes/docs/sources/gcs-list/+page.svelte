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
  title="Google Cloud Storage source"
  description="Read the objects under a prefix of a Cloud Storage bucket by listing it: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Google Cloud Storage</h1>
  <p class="lede">
    Lists a prefix of one Cloud Storage bucket every poll interval, reads each new or changed object
    it finds, and splits it into records. Only one worker of each group reads this source at a time.
  </p>
  <p>
    Use it for a bucket that cannot send Pub/Sub notifications, or one you may not configure. When
    the bucket can notify, use <a href="/docs/sources/gcs-pubsub"
      >Google Cloud Storage via Pub/Sub</a
    >: every worker of the group reads, and nothing lists the bucket.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads each object under <code>app/</code> one line per record, signed in with the worker host's
    Application Default Credentials, and moves each object under <code>done/</code> once Smeltr delivers
    every record.
  </p>
  <pre><code>{@html highlight(json(examples.gcsListSource))}</code></pre>
  <ProductScreenshot
    source="gcs-list-source-form"
    height={2800}
    crop={{ x: 608, y: 556, width: 1664, height: 1688 }}
    alt="The GCS listing form on the Sources page: id bucket-export, bucket acme-logs, prefix app/, after commit move to done/, framing lines."
    caption="The same source in the Sources page's add dialog, as it is added."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Choose what happens to an object once Smeltr delivers it, <code>afterCommit</code>.
      <code>keep</code> leaves the bucket as it is. <code>delete</code> and <code>move</code> drain it,
      so each listing finds only what is still to read.
    </li>
    <li>
      Give the service account it reads as a role on the bucket. <code>keep</code> needs to list and
      read objects: Storage Object Viewer. <code>delete</code> also needs to delete them, and
      <code>move</code> to create them as well, since a move copies the object and then deletes it:
      Storage Object User covers both. See
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
  <KindSettings group={SOURCES} type="gcsList" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 'gcsList').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Every <code>pollIntervalSecs</code>, counted from the start of the last listing, the source
    lists the objects whose names start with <code>prefix</code>. It reads each object the listing
    finds new, or changed since it last read it, and reads a changed object again whole. An empty
    object holds no record, and the source does not read it. The source decompresses a gzip, zstd,
    framed snappy or framed lz4 object first and reads any other as it is.
  </p>
  <p>
    One worker of the group holds the source and reads it; every other worker running it reads
    nothing. The leader chooses the holder from the workers' heartbeats. The leader gives the hold
    to another worker after three heartbeat intervals without hearing from the holder, the holder's
    interval or the leader's, whichever is longer, and that worker resumes from the last progress
    the leader kept. That worker delivers again what the previous holder delivered after that
    progress.
  </p>
  <p>
    With <code>afterCommit</code> <code>keep</code>, the progress is a <strong>ledger</strong>: each
    object read, by name and version. The holder records an object in it once Smeltr delivers every
    record of the object and reports it to the leader on its heartbeats, so the next holder reads
    only what is not in it. An object that leaves the bucket leaves the ledger. Every listing walks
    the whole prefix, so a large prefix costs a full listing each poll.
  </p>
  <p>
    With <code>delete</code> or <code>move</code>, the bucket itself says what is still to read and
    there is no ledger. Once Smeltr delivers every record of an object, the source deletes it, or
    copies it to <code>movePrefix</code> in place of <code>prefix</code> and deletes the original. An
    object overwritten since the source read it stays in place, and the source reads it again. It deletes
    or moves an empty object unread.
  </p>
  <p>
    <code>start</code> <code>everything</code> reads every object the first listing finds.
    <code>new</code> records the first listing as read and reads only what arrives or changes after
    it, so it needs <code>keep</code>.
  </p>
  <p>
    The source's progress belongs to its name and to its bucket and prefix. Renamed, or pointed at
    another bucket or prefix, it starts afresh.
  </p>
  <p>
    Once you remove the source and deploy, the leader forgets its progress, so a rollback that
    brings the source back starts it afresh.
  </p>
  <p>
    An object handed out five times and never delivered is no longer offered until it changes. With
    <code>keep</code> it is recorded in the ledger as read; with <code>delete</code> or
    <code>move</code> it stays in the bucket. The worker log names it. With <code>delete</code> or
    <code>move</code> only the holder remembers it, so a restart or a new holder hands it out again.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload an object with a line you can recognize under the prefix, then find that line at the
    destination after the next listing. On the <strong>Sources</strong> page, the source's
    <strong>worker</strong> column names the worker holding it, and with <code>keep</code> how many
    objects its ledger holds. With <code>delete</code> or <code>move</code>, the object leaves the
    prefix once it is delivered.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the object's name starts with <code>prefix</code>, and that the poll interval has
      passed. With <code>start</code> <code>new</code>, the source does not read what was there at
      the first listing until it changes. The worker log of the holder says why a listing failed,
      such as a missing role.
    </dd>
    <dt><strong>The worker column is empty</strong></dt>
    <dd>
      No worker holds the source: check that a worker of its group runs the deployment that has it.
    </dd>
    <dt><strong>The holder was last heard some time ago</strong></dt>
    <dd>
      The leader has not heard from that worker. It gives the hold to another worker after three
      heartbeat intervals without hearing from the holder, the holder's interval or the leader's,
      whichever is longer.
    </dd>
    <dt><strong>An object stays under the prefix</strong></dt>
    <dd>
      The source could not read it or could not delete it: check the role on the bucket and the
      worker log for the reason.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/gcs-pubsub">Google Cloud Storage via Pub/Sub</a></li>
    <li><a href="/docs/sources/s3-list">Amazon S3</a></li>
    <li><a href="/docs/sources/azure-blob-list">Azure Blob Storage</a></li>
    <li><a href="/docs/sources/sftp">SFTP</a></li>
    <li><a href="/docs/destinations/gcs">Google Cloud Storage destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
