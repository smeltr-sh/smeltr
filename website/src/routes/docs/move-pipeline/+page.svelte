<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'export', title: 'Export' },
    { id: 'import', title: 'Import' },
    { id: 'taken', title: 'Taken ids' },
    { id: 'credentials', title: 'Secrets and certificates' },
  ];
</script>

<GuideLayout
  title="Move a pipeline between leaders"
  description="Export a pipeline with what it reads, and import it on another leader."
  {sections}
>
  <h1>Move a pipeline between leaders</h1>
  <p>
    An export is a zip file that holds a pipeline and everything it reads: the pipelines it calls,
    its lookup tables with their files, its grok patterns, the schemas it produces, and the samples
    and tests that lock them. Importing the file on another leader saves those objects on your
    branch. You then review and deploy them like any other change.
  </p>
  <h2 id="export">Export</h2>
  <ol>
    <li>Open <strong>Build → Pipelines</strong>.</li>
    <li>
      Select <strong>Export</strong> on the pipeline's row, or in the pipeline editor. The dialog lists
      what the file will hold.
    </li>
    <li>
      To include the routes that run the pipeline, with their sources and destinations, turn on
      <strong>Include routes, sources and destinations</strong>.
    </li>
    <li>
      Every object is exported at first. Untick <strong>Export</strong> on each row you want to leave
      out. A sample goes with the tests that run over it.
    </li>
    <li>Select <strong>Download</strong>.</li>
  </ol>
  <p>
    The file holds secret and certificate names, never their values. The worker group's own
    exception answers and name stay behind. The importing group uses its own.
  </p>
  <h2 id="import">Import</h2>
  <ol>
    <li>On the other leader, open the branch to import into.</li>
    <li>Open <strong>Build → Pipelines</strong> and select <strong>Import</strong>.</li>
    <li>
      Under <strong>Import into</strong>, choose shared or a worker group. This leader's groups need
      not be the ones the export came from.
    </li>
    <li>
      Choose the export file. The dialog lists each object it holds as <strong>New</strong>,
      <strong>Same as here</strong> or <strong>Taken</strong>.
    </li>
    <li>
      Every object is imported at first. Untick <strong>Import</strong> on each row you want to leave
      out.
    </li>
    <li>
      Answer each taken id and match each secret and certificate, then select <strong>Import</strong
      >.
    </li>
  </ol>
  <p>
    An object that is the same as one here is not written again. The import is one save on your
    branch, and it is refused if it would add errors to the configuration. So when an object you
    import uses one you leave out, that one must already exist in shared or in the group you import
    into.
  </p>
  <h2 id="taken">Taken ids</h2>
  <p>
    An id is taken when this leader holds a different object of the same kind under it. The dialog
    shows both versions. Under <strong>Keep</strong>, choose one:
  </p>
  <ul>
    <li><strong>Imported</strong> replaces the object here with the imported one.</li>
    <li><strong>Here</strong> keeps the object here, and the imported one is not written.</li>
    <li>
      <strong>Both, rename imported</strong> writes the imported object under the new id you give it.
      Every reference to it inside the import is changed to the new id.
    </li>
  </ul>
  <p>
    An import into a worker group cannot replace an object that shared holds, so for one of those
    only <strong>Here</strong> and <strong>Both, rename imported</strong> are offered. A lookup table
    whose file holds the same content as the one here is the same, whatever its file is named.
  </p>
  <h2 id="credentials">Secrets and certificates</h2>
  <p>
    Each secret and certificate that an imported source or destination names must be matched to one
    on this leader's <a href="/docs/secrets">Secrets</a> or
    <a href="/docs/certificates">Certificates</a> page. Add any that are missing there first. When this
    leader lists one under the same name, the dialog chooses it for you.
  </p>
</GuideLayout>
