<script lang="ts">
  import DocsLayout from '#lib/components/DocsLayout.svelte';

  const sections = [
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
    content="Set up Smeltr, build and deploy a telemetry pipeline, or find operations guides and reference material."
  />
</svelte:head>

<DocsLayout {sections}>
  <header>
    <h1>Smeltr documentation</h1>
    <p class="lede">
      Smeltr is a self-hosted tool for filtering, transforming, and routing logs, metrics, and
      events. Build and test pipelines in the browser, review changes in Git, and deploy them to
      workers that process and deliver the records.
    </p>
  </header>

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
    <p>
      A route connects a source, a pipeline, and one or more destinations. A sample lets you check
      the result before deploying the route to a worker group.
    </p>
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
