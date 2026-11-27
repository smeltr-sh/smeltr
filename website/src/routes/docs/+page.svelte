<script lang="ts">
  import DocsLayout from '#lib/components/DocsLayout.svelte';
  import EditorScreenshot from '#lib/components/EditorScreenshot.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';

  const sections = [
    { id: 'what-is-smeltr', title: 'What is Smeltr?' },
    { id: 'start', title: 'Get started' },
    { id: 'workflow', title: 'Build and deploy pipelines' },
    { id: 'manual', title: 'Operate Smeltr' },
    { id: 'reference', title: 'Reference' },
  ];
</script>

<svelte:head>
  <title>Documentation — smeltr</title>
  <meta
    name="description"
    content="Learn how Smeltr fits into your telemetry stack, then set it up, build a pipeline, or find operations guides and reference material."
  />
</svelte:head>

<DocsLayout {sections}>
  <header>
    <h1>Smeltr documentation</h1>
  </header>

  <section id="what-is-smeltr">
    <h2>What is Smeltr?</h2>
    <p class="lede">
      Smeltr is data engineering pipeline telemetry software written in Rust for Linux and Windows.
    </p>

    <h3>Leader pipeline editor</h3>
    <EditorScreenshot />

    <h3>Leader and workers</h3>
    <WorkflowDiagram kind="workerFleet" />

    <h3>Inside a worker</h3>
    <WorkflowDiagram kind="dataPath" />

    <h3>What it does</h3>
    <p>
      Smeltr can filter and reshape records before delivering them. A route for application logs
      might work like this.
    </p>
    <ol>
      <li><strong>Source.</strong> Read application logs from Kafka.</li>
      <li><strong>Pipeline.</strong> Drop routine health checks and standardize field names.</li>
      <li>
        <strong>Destinations.</strong> Send the remaining logs to Elasticsearch for search and Amazon
        S3 for archiving.
      </li>
    </ol>

    <h3>How you run it</h3>
    <p>
      You run Smeltr on your own infrastructure. The <strong>leader</strong> provides a browser
      interface to build pipelines, test them on sample records, and review changes.
      <strong>Workers</strong> run those pipelines and move records directly from sources to destinations.
    </p>
    <p>
      Workers load the deployed configuration from Git and report which commit they're running. You
      can verify that a rollout reached the intended workers or roll back to an earlier commit.
    </p>
  </section>

  <section id="start" aria-labelledby="start-heading">
    <h2 id="start-heading">Get started</h2>
    <ul class="topics">
      <li>
        <a href="/docs/getting-started">Set up Smeltr</a>
        <p>
          Choose an installation method, start the leader, and confirm your first worker is online.
        </p>
      </li>
      <li>
        <a href="/docs/first-pipeline">Build your first pipeline</a>
        <p>Use a test Kafka broker to transform records and verify delivery to an output topic.</p>
      </li>
    </ul>
  </section>

  <section id="workflow">
    <h2>Build and deploy pipelines</h2>
    <ol class="topics">
      <li>
        <a href="/docs/capture-sample">Capture records</a>
        <p>Collect a sample from a source and save records for testing.</p>
      </li>
      <li>
        <a href="/docs/steps#verify">Test the transformation</a>
        <p>Check the output and lock the fields and rows you want future edits to preserve.</p>
      </li>
      <li>
        <a href="/docs/routing/records">Check routing</a>
        <p>See how records are matched to destinations.</p>
      </li>
      <li>
        <a href="/docs/review">Review and deploy</a>
        <p>Publish the reviewed change and confirm workers are running the deployed commit.</p>
      </li>
    </ol>
    <p>
      <a href="/docs/schemas">Share a schema across pipelines</a> ·
      <a href="/docs/git/configuration">How workers get configuration from Git</a>
    </p>
  </section>

  <section id="manual">
    <h2>Operate Smeltr</h2>
    <ul class="topics">
      <li>
        <a href="/docs/troubleshooting">Troubleshoot a problem</a>
        <p>
          Start with a symptom such as missing output, an offline worker, or a certificate error.
        </p>
      </li>
      <li>
        <a href="/docs/operations">Monitor workers and manage rollbacks</a>
        <p>Read fleet status, set up alerts, and verify a rollback.</p>
      </li>
      <li>
        <a href="/docs/delivery">Recover stalled delivery</a>
        <p>Check retries, refused records, and whether you can replay data.</p>
      </li>
      <li>
        <a href="/docs/backup-recovery">Back up and restore the leader</a>
        <p>Preserve the leader state and the matching secrets encryption key.</p>
      </li>
    </ul>
    <p>
      <a href="/docs/accounts">Accounts and access</a> ·
      <a href="/docs/certificates">Certificates and TLS</a> ·
      <a href="/docs/secrets">Secrets and credentials</a>
    </p>
  </section>

  <section id="reference">
    <h2>Reference</h2>
    <ul class="topics">
      <li><a href="/docs/sources">Sources</a> for reading or receiving records</li>
      <li id="steps"><a href="/docs/steps">Pipeline steps</a> for transforming records</li>
      <li><a href="/docs/destinations">Destinations</a> for delivering records</li>
      <li><a href="/docs/code-reference">Language reference</a> for expressions and functions</li>
      <li><a href="/docs/reference">Administrator settings</a> for startup configuration</li>
    </ul>
  </section>
</DocsLayout>

<style>
  header {
    padding: 0.5rem 0 1.5rem;
  }
  h1 {
    font-size: clamp(2.2rem, 4vw, 3.5rem);
    letter-spacing: -0.03em;
  }
  .lede {
    font-size: 1.15rem;
  }
  p {
    color: var(--ink);
    max-width: 45rem;
  }
  section {
    padding: 1.5rem 0;
    border-top: 1px solid var(--border);
  }
  h2 {
    font-size: clamp(1.6rem, 2.5vw, 2rem);
    letter-spacing: -0.02em;
  }
  .topics {
    padding-inline-start: 1.25rem;
  }
  .topics li {
    margin-block: 0.75rem;
  }
  .topics p {
    margin: 0.2rem 0 0;
  }
</style>
