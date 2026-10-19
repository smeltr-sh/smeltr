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
  title="Azure Blob via Storage queue source"
  description="Read the blobs Event Grid announces on a Storage queue: an example, every setting, the fields it gives, and what to have ready."
  {sections}
>
  <h1>Azure Blob via Storage queue</h1>
  <p class="lede">
    Reads each blob Event Grid announces on a Storage queue, splits it into records, and deletes the
    message once Smeltr delivers every record.
  </p>
  <p>
    The source receives a storage account's Event Grid <code>BlobCreated</code> events from a Storage
    queue and reads the blobs they name.
  </p>

  <h2 id="example">Example</h2>
  <p>Reads each announced blob one line per record, as the worker host's managed identity.</p>
  <pre><code>{@html highlight(json(examples.azureBlobQueueSource))}</code></pre>
  <ProductScreenshot
    source="azure-blob-queue-source-form"
    height={2100}
    crop={{ x: 608, y: 444, width: 1664, height: 1212 }}
    alt="The Azure Blob via Storage queue form on the Sources page: id blob-logs, its queue URL, account acmelogs, framing lines."
    caption="The same source on the Sources page."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      <a href="https://learn.microsoft.com/azure/storage/queues/storage-quickstart-queues-portal"
        >Create a Storage queue</a
      >
      in the storage account that holds the blobs.
    </li>
    <li>
      <a href="https://learn.microsoft.com/azure/event-grid/blob-event-quickstart-portal"
        >Subscribe to the account's events</a
      >
      with the event type <strong>Blob Created</strong> and the queue as the
      <a href="https://learn.microsoft.com/azure/event-grid/handler-storage-queues">endpoint</a>.
      The Event Grid schema and CloudEvents 1.0 both work.
    </li>
    <li>
      Choose how it signs in, and give that identity permission to read the blobs and to work the
      queue. A key, signature or client secret goes in a <a href="/docs/secrets">credential</a>.
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
          >, given Storage Blob Data Reader on the blobs and Storage Queue Data Contributor on the
          account's queues (see
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
          an account
          <a href="https://learn.microsoft.com/azure/storage/common/storage-sas-overview"
            >shared access signature</a
          > for the blob and queue services, allowed to read blobs, create queues, and read, add, update
          and process messages.
        </li>
      </ul>
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="azureBlobQueue" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    <code>__source.bucket</code> holds the blob's container, and <code>__source.key</code> its name.
  </p>
  <FieldsTable names={kind(SOURCES, 'azureBlobQueue').fields ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    A message may hold one event or an array of them, as plain text or base64. The source reads the
    blob a <code>BlobCreated</code> event names, at its latest version. For a Data Lake file it
    skips <code>CreateFile</code> and reads the file at <code>FlushWithClose</code>. The source
    deletes a message that announces no blob without reading anything. A message that is not an
    Event Grid event, or names a blob in another account, stays on the queue, like one whose blob
    the source cannot read.
  </p>
  <p>
    The source decompresses a gzip, zstd, framed snappy or framed lz4 blob first and reads any other
    as it is.
  </p>
  <p>
    The source keeps a message hidden while it reads the message's blobs and Smeltr delivers them,
    and deletes it once Smeltr commits every record of every blob it names. It never deletes a
    message whose blob it cannot read, and moves a message received more than <code
      >maxDeliveries</code
    >
    times to the queue named after this one with <code>-poison</code> added, making that queue if it is
    not there. A message on the poison queue stays until someone deletes it.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Upload a blob with a line you can recognize to the account, then find that line at the
    destination. The queue's message count returns to zero once Smeltr delivers it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that the event subscription filters for Blob Created, sends to this queue, shows
      delivered events in its metrics, and has no subject filter that leaves out your blobs.
    </dd>
    <dt><strong>Messages pile up on the poison queue</strong></dt>
    <dd>
      The source cannot read their blobs: check the identity's role on the container, or that the
      blob's account is the source's <code>account</code>, and the worker log for the reason.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/destinations/azure-blob">Azure Blob Storage destination</a></li>
    <li><a href="/docs/delivery#recovery">Recover and replay</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
