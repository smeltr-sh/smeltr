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
  title="Azure Blob Storage source"
  description="Read the blobs under a prefix of one container by listing it: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Azure Blob Storage</h1>
  <p class="lede">
    Lists a prefix of one container every poll interval, reads each blob it finds new or changed,
    and splits it into records. Only one worker of each group reads this source at a time.
  </p>
  <p>
    Use it where the storage account cannot notify. Where it can, <a
      href="/docs/sources/azure-blob-queue">Azure Blob via Storage queue</a
    > reads each blob as the storage account announces it, with no listing, and any number of workers
    share the work.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Reads each blob under <code>app/</code> one line per record, as the worker host's managed
    identity, and moves it under <code>done/</code> once Smeltr delivers it.
  </p>
  <pre><code>{@html highlight(json(examples.azureBlobListSource))}</code></pre>
  <ProductScreenshot
    source="azure-blob-list-source-form"
    height={2800}
    crop={{ x: 608, y: 490, width: 1664, height: 1820 }}
    alt="The Azure Blob listing form on the Sources page: id container-export, account acmelogs, container logs, prefix app/, after commit move to done/, framing lines."
    caption="The same source in the Sources page's add dialog, as it is added."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Choose what it does with a blob once Smeltr delivers it, <code>afterCommit</code>. That sets
      the permission it needs: <code>keep</code> only lists and reads; <code>delete</code> also
      deletes;
      <code>move</code> also writes the blob under <code>movePrefix</code>, then deletes it.
    </li>
    <li>
      Choose how it signs in, and give that identity that permission on the container. A key,
      signature or client secret goes in a <a href="/docs/secrets">credential</a>.
      <ul>
        <li>
          the worker host's
          <a
            href="https://learn.microsoft.com/entra/identity/managed-identities-azure-resources/overview"
            >managed identity</a
          >, or a
          <a
            href="https://learn.microsoft.com/entra/identity-platform/howto-create-service-principal-portal"
            >service principal</a
          >, given Storage Blob Data Reader on the container for <code>keep</code>, or Storage Blob
          Data Contributor for <code>delete</code> and <code>move</code> (see
          <a href="https://learn.microsoft.com/azure/storage/blobs/assign-azure-role-data-access"
            >assign a role for data access</a
          >);
        </li>
        <li>
          an
          <a href="https://learn.microsoft.com/azure/storage/common/storage-account-keys-manage"
            >account key</a
          >; or
        </li>
        <li>
          a
          <a href="https://learn.microsoft.com/azure/storage/common/storage-sas-overview"
            >shared access signature</a
          >
          for the blob service, allowed to list and read, and also to delete for
          <code>delete</code>, or to create, write and delete for <code>move</code>.
        </li>
      </ul>
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="azureBlobList" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    <code>__source.bucket</code> holds the container, and <code>__source.key</code> the blob's name.
  </p>
  <FieldsTable names={kind(SOURCES, 'azureBlobList').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Every <code>pollIntervalSecs</code> it lists the blobs whose names start with
    <code>prefix</code>. The source reads a blob once two listings in a row find it unchanged, since
    an append blob grows while something writes it. It reads the blob whole: it decompresses a gzip,
    zstd, framed snappy or framed lz4 blob first and reads any other as it is. An empty blob holds
    no record, and the source does not read it.
  </p>
  <p>
    One worker of the group reads it at a time, the one the leader holds it for; the others read
    nothing. The leader gives the hold to another worker after three heartbeat intervals without
    hearing from the holder, the holder's interval or the leader's, whichever is longer. With <code
      >keep</code
    >, its progress is a <strong>ledger</strong>: the blobs it has read, each with the version it
    read. The holder reports it on its heartbeat and the leader keeps it; the next holder fetches
    the ledger and reads only what is not in it. The next holder delivers again what the previous
    holder delivered after the last progress the leader kept. With <code>delete</code> or
    <code>move</code> the source keeps no progress: the next holder reads what the container still holds,
    so it delivers again a blob delivered but not yet deleted or moved.
  </p>
  <p>
    <code>afterCommit</code> says what it does with a blob once Smeltr delivers every record.
  </p>
  <ul>
    <li>
      <code>keep</code> leaves the blob and records it in the ledger. When a blob changes after the source
      reads it, the source reads it again whole. A blob that is gone from the listing leaves the ledger.
      Every listing lists the whole prefix, so the cost of each poll grows with what the prefix holds.
    </li>
    <li>
      <code>delete</code> deletes the blob, and <code>move</code> copies it under
      <code>movePrefix</code> in place of <code>prefix</code> and deletes it. The container is then its
      own record of what is left to read, and the source keeps no ledger. A blob that changed after the
      source read it stays in place, and the source reads it again. It deletes or moves an empty blob
      unread.
    </li>
  </ul>
  <p>
    A blob handed out five times and never delivered is no longer read until it changes. With
    <code>keep</code> it is recorded in the ledger as read; with <code>delete</code> or
    <code>move</code> it stays in the container. The worker log says which blob. With
    <code>delete</code>
    or <code>move</code> only the holder remembers it, so a restart or a new holder hands it out again.
  </p>
  <p>
    <code>start</code> says what the first listing reads. <code>everything</code> reads every blob
    it finds. <code>new</code> records every blob it finds as read and reads only what arrives or
    changes after it; it needs <code>afterCommit</code> <code>keep</code>, since only the ledger
    remembers what was skipped.
  </p>
  <p>
    The source's progress belongs to its name and to its storage account, container and prefix.
    Renamed, or pointed at another container or prefix, it starts afresh.
  </p>
  <p>
    Once you remove the source and deploy, the leader forgets its progress, so a rollback that
    brings the source back starts it afresh.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload a blob with a line you can recognize under the prefix, then find that line at the
    destination after two listings. On the Sources page, the source's <strong>worker</strong> column
    names the worker that holds it, and with <code>keep</code> how many blobs its ledger holds. With
    <code>delete</code> or <code>move</code>, the blob leaves the prefix once it is delivered.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      The source reads a blob only on the second listing that finds it unchanged, so wait two
      <code>pollIntervalSecs</code>. Check that its name starts with <code>prefix</code>, which does
      not start with <code>/</code>, and the worker log for a listing that failed.
    </dd>
    <dt><strong>The worker column is empty</strong></dt>
    <dd>
      No worker of the group runs the source, or none has reported it to the leader. Check that the
      group's workers are enrolled and running the deployment.
    </dd>
    <dt><strong>A blob is read again</strong></dt>
    <dd>
      It changed after the source read it, or the source moved to another worker before its last
      progress reached the leader. Both deliver its records again.
    </dd>
    <dt><strong>Blobs stay in the container with delete or move</strong></dt>
    <dd>
      The identity cannot delete or write there: check its role or signature, and the worker log for
      the reason.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/azure-blob-queue">Azure Blob via Storage queue</a></li>
    <li><a href="/docs/sources/s3-list">Amazon S3</a></li>
    <li><a href="/docs/sources/gcs-list">Google Cloud Storage</a></li>
    <li><a href="/docs/sources/sftp">SFTP</a></li>
    <li><a href="/docs/destinations/azure-blob">Azure Blob Storage destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
