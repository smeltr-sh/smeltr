<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import { highlight } from '#lib/highlight/highlight.ts';
  // The importers' code is the code `src/leader/schema_catalog.rs` tests, read from the same place.
  import examples from '../../../../../docs/examples.md?raw';

  /** The pipeline code of `docs/examples.md`'s section `n`. */
  function importer(n: number): string {
    const section = examples.split(`## ${n}.`)[1] ?? '';
    return section.split('```ts\n')[1]?.split('\n```')[0] ?? '';
  }
  const ecs = importer(9);
  const ocsf = importer(10);

  const sections = [
    { id: 'add', title: 'Add a schema' },
    { id: 'import', title: 'Import ECS or OCSF' },
    { id: 'produces', title: 'Hold a pipeline to a schema' },
    { id: 'findings', title: 'Read the findings' },
    { id: 'export', title: 'Export an Elasticsearch mapping' },
  ];
</script>

<GuideLayout
  title="Schemas"
  description="Declare the fields a pipeline's records should hold, import ECS or OCSF, and export an Elasticsearch mapping."
  {sections}
>
  <h1>Schemas</h1>
  <p class="lede">
    A schema lists the fields a pipeline's records should hold. Each field has a type, and may be
    required. Smeltr tells you where a pipeline's records do not fit. It never blocks a deploy.
  </p>

  <h2 id="add">Add a schema</h2>
  <ol>
    <li>Work on your draft branch.</li>
    <li>
      Open Build → Resources → Schemas and choose <strong>+ Add schema</strong>.
    </li>
    <li>
      Choose how it starts. <strong>Import</strong> reads a file through one of your pipelines.
      <strong>JSON Schema</strong> takes a JSON Schema you paste or upload. <strong>empty</strong>
      starts with no fields.
    </li>
    <li>Select <strong>Add</strong>, then <strong>Save</strong>.</li>
  </ol>
  <p>
    Select a schema's field count to open its fields. Change them with <strong>Edit JSON</strong>. A
    schema in
    <code>shared/</code> can be named by every group.
  </p>

  <h2 id="import">Import ECS or OCSF</h2>
  <p>
    An import runs one of your pipelines over a file. Each record the pipeline writes becomes a
    field of the schema. A record holds <code>field</code>, the field's path with dots, and
    <code>type</code>. It may also hold <code>required</code>, <code>description</code>, and
    <code>elasticsearch</code>, the field's Elasticsearch mapping. Any list of fields can come in
    this way, such as a spreadsheet saved as CSV.
  </p>
  <h3 id="ecs">Elastic Common Schema</h3>
  <ol>
    <li>
      Download ECS's
      <a href="https://github.com/elastic/ecs/blob/main/generated/csv/fields.csv"
        ><code>generated/csv/fields.csv</code></a
      >.
    </li>
    <li>
      Add a pipeline named ECS importer. Give it a Drop step that drops
      <code>_raw.startsWith("ECS_Version,") || _raw.includes(".text,match_only_text,")</code>, then
      a Code step with the code below.
    </li>
    <li>
      Add a schema. Choose <strong>Import</strong>, the ECS importer, and the file. Set the line
      breaker to <code>([\r\n]+)\d+\.\d+\.\d+</code>, since some of the file's descriptions span
      lines.
    </li>
  </ol>
  <ProductScreenshot
    source="schema-import"
    height={1800}
    crop={{ x: 610, y: 524, width: 1660, height: 752 }}
    alt="The New import schema dialog with the id ecs, the ECS importer, the file fields.csv, and the line breaker filled in."
    caption="An import names the pipeline that reads the file, the file, and where the file breaks into records."
  />
  <pre><code>{@html highlight(ecs)}</code></pre>

  <h3 id="ocsf">OCSF</h3>
  <ol>
    <li>
      Compile the <a href="https://github.com/ocsf/ocsf-schema">OCSF schema</a> into one file with
      <a href="https://pypi.org/project/ocsf-schema-compiler/"><code>ocsf-schema-compiler</code></a
      >: <code>ocsf-schema-compiler ocsf-schema &gt; schema.json</code>.
    </li>
    <li>
      Add a pipeline named OCSF importer, with a Code step holding the code below. Set
      <code>cls</code> on its first line to the class you want, such as
      <code>network_activity</code>.
    </li>
    <li>
      Add a schema. Choose <strong>Import</strong>, the OCSF importer, and
      <code>schema.json</code>. Set the line breaker to <code>(\x00)</code>, so the whole file is
      one record.
    </li>
  </ol>
  <p>
    The schema holds every field OCSF requires or recommends for the class, about a hundred. Add a
    schema for each class you send.
  </p>
  <pre><code>{@html highlight(ocsf)}</code></pre>

  <h2 id="produces">Hold a pipeline to a schema</h2>
  <p>
    Open the pipeline. In the <strong>Produces</strong> bar under its name, choose
    <strong>Add a schema it produces</strong>. A pipeline may produce several schemas.
  </p>
  <p>
    A destination can name the schemas it accepts. Open the destination and add them under
    <strong>accepts</strong>. A route whose pipeline produces none of them is a finding. Leave it
    empty to accept any pipeline.
  </p>

  <h2 id="findings">Read the findings</h2>
  <p>
    Run a sample in the pipeline editor. The Produces bar says <strong>the rows fit</strong>, or how
    many findings there are. A field a schema requires and the records lack shows as a column of its
    own, marked with the schema that requires it.
  </p>
  <p>
    In review, the change's <strong>Schemas</strong> tab lists the findings by pipeline, with the step
    that wrote each field. It shows only what the change adds. None of them block the deploy.
  </p>

  <h2 id="export">Export an Elasticsearch mapping</h2>
  <p>
    Open a schema's fields and choose <strong>Export Elasticsearch mapping</strong>. Load it into
    the index template of the indexes the pipeline writes to. <strong>Export JSON Schema</strong>
    gives the same fields to other tools.
  </p>
  <p>
    Each field maps to the Elasticsearch type its type maps to. To change one, set
    <strong>Fields shown as</strong> to <strong>Elasticsearch</strong> and choose its type, such as
    <code>ip</code> for an address held as a string. Save the change.
  </p>
  <ProductScreenshot
    source="schema-elasticsearch"
    height={1800}
    crop={{ x: 460, y: 540, width: 2380, height: 1260 }}
    alt="The ecs schema's fields searched for .ip and shown as Elasticsearch, each address field mapped as ip."
    caption="The imported ECS schema maps its address fields as ip. Search narrows the fields to the ones you want."
  />
</GuideLayout>
