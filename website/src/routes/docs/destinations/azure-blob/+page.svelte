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
  title="Azure Blob Storage destination"
  description="Write batches as blobs in an Azure container: an example, every setting, the fields it reads, and what to have ready."
  {sections}
>
  <h1>Azure Blob Storage</h1>
  <p class="lede">Writes batches of records as blobs in an Azure Blob Storage container.</p>

  <h2 id="example">Example</h2>
  <p>Writes as the worker host's managed identity.</p>
  <pre><code>{@html highlight(json(examples.azureDestination))}</code></pre>
  <ProductScreenshot
    source="azure-blob-destination-form"
    height={2500}
    crop={{ x: 608, y: 578, width: 1664, height: 1344 }}
    alt="The Azure Blob Storage form on the Destinations page: id archive, account acmelogs, container events, format ndjson."
    caption="The same destination on the Destinations page."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      <a href="https://learn.microsoft.com/azure/storage/blobs/storage-quickstart-blobs-portal"
        >Create the storage account and container</a
      >.
    </li>
    <li>
      Choose how it signs in, and give that identity permission to write blobs to the container. A
      key, signature or client secret goes in a <a href="/docs/secrets">credential</a>.
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
          >, either given a role such as Storage Blob Data Contributor (see
          <a href="https://learn.microsoft.com/azure/storage/blobs/assign-azure-role-data-access"
            >assign a role for blob data</a
          >);
        </li>
        <li>
          an
          <a href="https://learn.microsoft.com/azure/storage/common/storage-account-keys-manage"
            >account key</a
          >; or
        </li>
        <li>
          a <a href="https://learn.microsoft.com/azure/storage/common/storage-sas-overview"
            >shared access signature</a
          > allowed to create and write blobs.
        </li>
      </ul>
    </li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>
    It also takes the settings <a href="/docs/destinations#settings">every destination</a> takes.
  </p>
  <KindSettings group={DESTINATIONS} type="azureBlob" />

  <h2 id="fields-it-reads">Fields it reads</h2>
  <FieldsTable names={kind(DESTINATIONS, 'azureBlob').reads ?? []} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    The destination keeps one blob open per prefix, and for Parquet, Avro and CSV per record type of
    the routes feeding it, across batches, named <code
      >&lt;prefix&gt;&lt;random id&gt;.&lt;extension&gt;</code
    >, and finishes it when it reaches <code>maxObjectBytes</code> on disk, when
    <code>fileIntervalSecs</code>
    passes, when <code>bufferBytes</code> fills on the worker, or when a 65th blob opens and this one
    was written least recently. A batch is acknowledged once every blob holding its records has landed.
    Two blobs never share a key, so records written again after a failure make new blobs.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Send a record with a unique marker through the source, then list the container under the prefix
    for a new blob and look for the marker in it.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Authorization fails</strong></dt>
    <dd>
      Check that the identity has a role such as Storage Blob Data Contributor on the container, or
      that the SAS token allows creating and writing blobs.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/destinations">Destinations</a></li>
    <li><a href="/docs/destinations/s3">Amazon S3 destination</a></li>
    <li><a href="/docs/destinations/gcs">Google Cloud Storage destination</a></li>
    <li><a href="/docs/secrets">Secrets & credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
