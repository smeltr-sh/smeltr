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
  title="Amazon S3 source"
  description="Read the objects under a prefix of an S3 bucket by listing it: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Amazon S3</h1>
  <p class="lede">
    Lists a prefix of one bucket every poll interval, reads each object it finds new or changed, and
    splits it into records.
  </p>
  <p>
    It reads a bucket that cannot announce its objects. When the bucket can send event
    notifications, use <a href="/docs/sources/s3-sqs">Amazon S3 via SQS</a> instead: it reads an object
    as soon as the bucket announces it, lists nothing, and every worker of the group reads from the queue.
    Only one worker of each group reads this source at a time.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads each object under <code>app/</code> one line per record, with the worker host's AWS
    credentials, and moves each object under <code>done/</code> once Smeltr delivers it.
  </p>
  <pre><code>{@html highlight(json(examples.s3ListSource))}</code></pre>
  <ProductScreenshot
    source="s3-list-source-form"
    height={2800}
    crop={{ x: 608, y: 556, width: 1664, height: 1688 }}
    alt="The S3 listing form on the Sources page: id bucket-export, bucket acme-logs, prefix app/, after commit move to done/, framing lines."
    caption="The same source in the Sources page's add dialog, before you set its region."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Choose what happens to an object once Smeltr delivers it: kept, deleted, or moved under
      another prefix (<code>afterCommit</code>). Deleting or moving keeps each listing short.
    </li>
    <li>
      Allow the credentials Smeltr uses <code>s3:ListBucket</code> on the bucket and
      <code>s3:GetObject</code> on the objects under the prefix. To delete, also allow
      <code>s3:DeleteObject</code> under the prefix. To move, which copies each object and then
      deletes it, also allow <code>s3:DeleteObject</code> under the prefix and
      <code>s3:PutObject</code> under <code>movePrefix</code>.
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="s3List" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <FieldsTable names={kind(SOURCES, 's3List').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Every poll interval the source lists every key that starts with <code>prefix</code>, and reads
    each object it has not read before or that has changed since it last read it. It reads an object
    whole, so it reads a changed object again from its start. The source decompresses a gzip, zstd,
    framed snappy or framed lz4 object first and reads any other as it is. An empty object holds no
    record, and the source does not read it.
  </p>
  <p>
    One worker of the group reads the source at a time: the leader <strong>holds</strong> it for that
    worker, and the others run it and read nothing. The leader gives the hold to another worker after
    three heartbeat intervals without hearing from the holder, the holder's interval or the leader's,
    whichever is longer, and that worker resumes from the last progress the leader kept. That worker delivers
    again what the previous holder delivered after that progress. A holder cut off from the leader keeps
    reading, so until that heals two workers can read the same objects: that costs duplicates, never lost
    records.
  </p>
  <p>
    <code>afterCommit</code> says what the source does with an object once Smeltr delivers every record
    of it:
  </p>
  <ul>
    <li>
      <strong><code>keep</code></strong> leaves it in the bucket and records its key and version in
      a <strong>ledger</strong>. The holder reports the ledger to the leader, and the next holder
      reads only what it does not hold. A key gone from the bucket leaves the ledger. Every listing
      lists everything under the prefix, and the ledger holds one entry per object there.
    </li>
    <li>
      <strong><code>delete</code></strong> deletes it, and <strong><code>move</code></strong> copies
      it under <code>movePrefix</code> in place of <code>prefix</code> and then deletes it. The bucket
      then holds only what is left to read, and the source keeps no ledger. An object replaced since the
      source read it stays in place, and the source reads it again. It deletes or moves an empty object
      unread.
    </li>
  </ul>
  <p>
    <code>start</code> says what the source reads first. <code>everything</code> reads every object
    its first listing finds. <code>new</code> records what its first listing finds in the ledger as
    read, and reads only objects that arrive or change after it; it needs
    <code>afterCommit</code> <code>keep</code>, the only setting that keeps a ledger.
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
    An object handed out five times and never delivered counts as unreadable, and the worker logs a
    warning naming it. With <code>keep</code> it is recorded as read; with <code>delete</code> or
    <code>move</code> it is left in the bucket. It is read again only once it changes. With
    <code>delete</code>
    or <code>move</code> only the holder remembers it, so a restart or a new holder hands it out again.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload an object with a line you can recognize under the prefix, then find that line at the
    destination within a poll interval. With <code>delete</code> the object is then gone from the
    bucket; with <code>move</code> it is under <code>movePrefix</code>.
  </p>
  <p>
    On the Sources page, the <strong>worker</strong> column names the worker holding the source, and
    with <code>keep</code> how many objects its ledger holds.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the object's key starts with <code>prefix</code>, which does not start with
      <code>/</code>. Check the worker log for a listing that fails, and that the credentials allow
      <code>s3:ListBucket</code>. With <code>start</code> <code>new</code>, the source does not read
      an object already there at the first listing until it changes.
    </dd>
    <dt><strong>The worker column is empty</strong></dt>
    <dd>
      No worker of the group holds the source: check that a worker of the group is running and
      reaches the leader.
    </dd>
    <dt><strong>Objects stay in the bucket with delete or move</strong></dt>
    <dd>
      Check <code>s3:DeleteObject</code> under the prefix, and for <code>move</code>
      <code>s3:PutObject</code> under <code>movePrefix</code>, then the worker log for the reason.
      An object the source could not read stays too, until it changes.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/s3-sqs">Amazon S3 via SQS</a></li>
    <li><a href="/docs/sources/gcs-list">Google Cloud Storage</a></li>
    <li><a href="/docs/sources/azure-blob-list">Azure Blob Storage</a></li>
    <li><a href="/docs/sources/sftp">SFTP</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
