<script lang="ts">
  import EditorScreenshot from '#lib/components/EditorScreenshot.svelte';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';

  const sections = [
    { id: 'editor', title: 'Leader pipeline editor' },
    { id: 'fleet', title: 'Leader and workers' },
    { id: 'worker', title: 'Inside a worker' },
    { id: 'does', title: 'What it does' },
    { id: 'run', title: 'How you run it' },
  ];
</script>

<GuideLayout
  title="What is Smeltr?"
  description="Smeltr is data engineering pipeline telemetry software written in Rust for Linux and Windows."
  {sections}
>
  <h1>What is Smeltr?</h1>
  <p class="lede">
    Smeltr is data engineering pipeline telemetry software written in Rust for Linux and Windows.
  </p>

  <h2 id="editor">Leader pipeline editor</h2>
  <EditorScreenshot />

  <h2 id="fleet">Leader and workers</h2>
  <WorkflowDiagram kind="workerFleet" />

  <h2 id="worker">Inside a worker</h2>
  <WorkflowDiagram kind="dataPath" />

  <h2 id="does">What it does</h2>
  <p>
    Smeltr can filter and reshape records before delivering them. A route for application logs might
    work like this.
  </p>
  <ol>
    <li><strong>Source.</strong> Read application logs from Kafka.</li>
    <li><strong>Pipeline.</strong> Drop routine health checks and standardize field names.</li>
    <li>
      <strong>Destinations.</strong> Send the remaining logs to Elasticsearch for search and Amazon S3
      for archiving.
    </li>
  </ol>

  <h2 id="run">How you run it</h2>
  <p>
    You run Smeltr on your own infrastructure. The <strong>leader</strong> provides a browser
    interface to build pipelines, test them on sample records, and review changes.
    <strong>Workers</strong> run those pipelines and move records directly from sources to destinations.
  </p>
  <p>
    Workers load the deployed configuration from Git and report which commit they're running. You
    can verify that a rollout reached the intended workers or roll back to an earlier commit.
  </p>
</GuideLayout>
