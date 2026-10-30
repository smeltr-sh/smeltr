<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';

  const sections = [
    { id: 'add', title: 'Add a schema' },
    { id: 'ecs', title: 'Read ECS' },
    { id: 'ocsf', title: 'Read an OCSF class' },
    { id: 'csv', title: 'Read a CSV of fields' },
    { id: 'produces', title: 'Hold a pipeline to a schema' },
    { id: 'findings', title: 'Read the findings' },
    { id: 'export', title: 'Export an Elasticsearch mapping' },
  ];
</script>

<GuideLayout
  title="Schemas"
  description="Declare the fields a pipeline's records should hold, read ECS or OCSF, and export an Elasticsearch mapping."
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
    <li>Choose how it starts, and its file if it reads one.</li>
    <li>Select <strong>Add</strong>, then <strong>Save</strong>.</li>
  </ol>
  <ul>
    <li><strong>ECS</strong> reads the Elastic Common Schema.</li>
    <li><strong>OCSF</strong> reads one class of the Open Cybersecurity Schema Framework.</li>
    <li><strong>CSV of fields</strong> reads a CSV file with a row per field.</li>
    <li><strong>JSON Schema</strong> takes a JSON Schema you paste or upload.</li>
    <li>
      <strong>Import</strong> runs one of your pipelines over a file in any other format. Each
      record the pipeline writes becomes a field: <code>field</code> and <code>type</code>, and
      optionally
      <code>required</code>, <code>description</code> and <code>elasticsearch</code>.
    </li>
    <li><strong>Empty schema</strong> starts with no fields.</li>
  </ul>
  <p>
    Select a schema's field count to open its fields. Change them with <strong>Edit JSON</strong>. A
    schema in
    <code>shared/</code> can be named by every group.
  </p>

  <h2 id="ecs">Read ECS</h2>
  <p>
    Download <code>fields.csv</code> or <code>ecs_flat.yml</code> from the
    <a href="https://github.com/elastic/ecs/tree/main/generated"><code>generated/</code></a> folder
    of the ECS repository, at the ECS version you use. Choose <strong>ECS</strong> and the file.
    Each field keeps the Elasticsearch type ECS gives it, such as <code>ip</code> for an address.
  </p>

  <h2 id="ocsf">Read an OCSF class</h2>
  <ol>
    <li>
      Compile the <a href="https://github.com/ocsf/ocsf-schema">OCSF schema</a> into one file with
      <a href="https://pypi.org/project/ocsf-schema-compiler/"><code>ocsf-schema-compiler</code></a
      >: <code>ocsf-schema-compiler ocsf-schema &gt; schema.json</code>.
    </li>
    <li>Choose <strong>OCSF</strong> and <code>schema.json</code>.</li>
    <li>Choose the <strong>Class</strong>, such as <code>authentication</code>.</li>
    <li>
      Leave <strong>Depth</strong> at 2 and <strong>Include fields that are</strong> at required and recommended.
      That is the list OCSF asks a producer to fill. Add optional, or a greater depth, only if your pipeline
      fills those fields.
    </li>
  </ol>
  <ProductScreenshot
    source="schema-ocsf"
    height={1800}
    crop={{ x: 612, y: 474, width: 1656, height: 856 }}
    alt="The New schema from an OCSF class dialog: the compiled file, the authentication class, depth 2, and required and recommended checked."
    caption="An OCSF class, read two levels deep, with the fields OCSF requires or recommends."
  />
  <p>Add a schema for each class you send.</p>

  <h2 id="csv">Read a CSV of fields</h2>
  <p>
    Choose <strong>CSV of fields</strong> and the file. The file needs a header row. Choose the column
    that holds each field's name and the one that holds its type. Columns for required, description, example
    and elasticsearch are optional. If a type value is not one Smeltr knows, choose how to read it.
  </p>

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
    the index template of the indexes the pipeline writes to. <strong>Export JSON Schema</strong> gives
    the same fields to other tools.
  </p>
  <p>
    Each field maps to the Elasticsearch type that matches its Smeltr type. To change one, set
    <strong>Fields shown as</strong> to <strong>Elasticsearch</strong> and choose its type, such as
    <code>ip</code> for an address held as a string. Save the change.
  </p>
  <ProductScreenshot
    source="schema-elasticsearch"
    height={1800}
    crop={{ x: 460, y: 540, width: 2380, height: 1260 }}
    alt="The ecs schema's fields searched for .ip and shown as Elasticsearch, each address field mapped as ip."
    caption="The ECS schema maps its address fields as ip. Search narrows the fields to the ones you want."
  />
</GuideLayout>
