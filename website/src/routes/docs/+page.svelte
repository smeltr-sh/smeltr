<script lang="ts">
  import DocsLayout from '#lib/components/DocsLayout.svelte';
  import { stepPages } from '#lib/step-pages.ts';

  const sections = [
    { id: 'start', title: 'Start here' },
    { id: 'manual', title: 'Administrator manual' },
    { id: 'workflow', title: 'Pipeline workflow' },
    { id: 'steps', title: 'Pipeline steps' },
    { id: 'reference', title: 'Reference' },
  ];

  const manual = [
    {
      href: '/docs/getting-started',
      title: 'Get started',
      body: 'Choose where to run Smeltr, install the leader, and connect a worker.',
    },
    {
      href: '/docs/certificates',
      title: 'Certificates & TLS',
      body: 'Secure browser and worker connections without bypassing trust checks.',
    },
    {
      href: '/docs/install-leader/kubernetes',
      title: 'Kubernetes',
      body: 'Run a persistent leader from the published container image.',
    },
    {
      href: '/docs/operations',
      title: 'Operate the fleet',
      body: 'Read health, warnings and deployed commits in the running system.',
    },
    {
      href: '/docs/backup-recovery',
      title: 'Backup & recovery',
      body: 'Protect leader state and restore it as a consistent unit.',
    },
  ];
  const startingPoints = [
    {
      href: '/docs/getting-started',
      title: 'Set up Smeltr',
      body: 'Install a leader, connect your first worker, and get ready to process data.',
      action: 'Start setup',
    },
    {
      href: '/docs/first-pipeline',
      title: 'Build your first pipeline',
      body: 'Build from a sample, save a test, and follow your change through review and deployment.',
      action: 'Follow the tutorial',
    },
    {
      href: '/docs/troubleshooting',
      title: 'Troubleshoot a deployment',
      body: 'Find help for startup errors, worker enrollment, certificates, and unexpected deployment state.',
      action: 'Find a fix',
    },
  ];
</script>

<svelte:head>
  <title>Documentation — smeltr</title>
  <meta
    name="description"
    content="Installation, administration, operations, pipeline workflow, and expression-language documentation for Smeltr."
  />
</svelte:head>

<DocsLayout {sections}>
  <header>
    <p class="eyebrow">Documentation</p>
    <h1>What do you want to do?</h1>
    <p class="lede">
      Start with a task below, or search the manual for a setting, an error, or a function.
    </p>
  </header>

  <section id="start" class="start" aria-labelledby="start-heading">
    <h2 id="start-heading">Start here</h2>
    <div class="starting-points">
      {#each startingPoints as task (task.href)}
        <a class="guide task" href={task.href}>
          <h3>{task.title}</h3>
          <p>{task.body}</p>
          <span>{task.action} <span aria-hidden="true">→</span></span>
        </a>
      {/each}
    </div>
  </section>

  <section id="manual">
    <h2>Administrator manual</h2>
    <div class="guides">
      {#each manual as guide (guide.href)}
        <a class="guide" href={guide.href}>
          <h3>{guide.title}</h3>
          <p>{guide.body}</p>
          <span>Read guide <span aria-hidden="true">→</span></span>
        </a>
      {/each}
    </div>
    <p class="more">
      <a href="/docs/install-leader/binary">Leader install</a> ·
      <a href="/docs/enroll-worker/host">Worker enrollment</a>
      · <a href="/docs/sources">Sources</a>
      · <a href="/docs/destinations">Destinations</a>
      · <a href="/docs/getting-started#complete">Installation checklist</a>
      · <a href="/docs/accounts">Accounts & access</a> ·
      <a href="/docs/secrets">Secrets &amp; credentials</a>
      · <a href="/docs/troubleshooting">Troubleshooting</a>
    </p>
  </section>

  <section id="workflow">
    <h2>Build, review &amp; deploy</h2>
    <p>Follow the pipeline lifecycle from saved samples to a running worker group.</p>
    <p>
      Start with the diagrams:
      <a href="/docs/sources#flow">the data path</a>,
      <a href="/docs/review">how changes reach workers</a>, and
      <a href="/docs/delivery#delivery">what happens when delivery fails</a>.
    </p>
    <div class="workflow-links">
      <a href="/docs/first-pipeline"
        >First pipeline: sample to verified delivery <span aria-hidden="true">→</span></a
      >
      <a href="/docs/capture-sample">Capture a sample <span aria-hidden="true">→</span></a>
      <a href="/docs/schemas">Schemas <span aria-hidden="true">→</span></a>
      <a href="/docs/routing/records">Routing <span aria-hidden="true">→</span></a>
      <a href="/docs/git/configuration">Git &amp; configuration <span aria-hidden="true">→</span></a
      >
      <a href="/docs/review">Review &amp; deploy <span aria-hidden="true">→</span></a>
      <a href="/docs/operations">Operate the fleet <span aria-hidden="true">→</span></a>
      <a href="/docs/delivery">Delivery <span aria-hidden="true">→</span></a>
    </div>
  </section>

  <section id="steps">
    <h2>Pipeline steps</h2>
    <p>Find each pipeline step's behavior, settings, and worked examples.</p>
    <div class="workflow-links">
      {#each stepPages as link (link.href)}
        <a href={link.href}>{link.title} <span aria-hidden="true">→</span></a>
      {/each}
    </div>
  </section>

  <section id="reference">
    <h2>Reference</h2>
    <p>
      Look up startup settings, expression operators and functions, or source and destination
      settings.
    </p>
    <p>
      <a href="/docs/reference">Administrator reference →</a> &nbsp;
      <a href="/docs/code-reference">Expression reference →</a> &nbsp;
      <a href="/docs/sources">Sources →</a> &nbsp;
      <a href="/docs/destinations">Destinations →</a>
    </p>
  </section>
</DocsLayout>

<style>
  header {
    padding: 0.5rem 0 2.5rem;
  }
  .eyebrow {
    margin: 0 0 0.75rem;
    color: var(--dim);
    font-family: var(--mono);
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }
  h1 {
    max-width: 20ch;
    font-size: clamp(2.2rem, 4vw, 3.5rem);
    letter-spacing: -0.03em;
  }
  .lede {
    font-size: 1.15rem;
    max-width: 45rem;
  }
  p {
    color: var(--ink);
    max-width: 45rem;
  }
  section {
    padding: 2.5rem 0;
    border-top: 1px solid var(--border);
  }
  h2 {
    font-size: clamp(1.6rem, 2.5vw, 2rem);
    letter-spacing: -0.02em;
  }
  h3 {
    font-size: 1.28rem;
    color: var(--ink);
  }
  .start {
    padding-top: 0;
    border-top: 0;
  }
  .starting-points {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(min(100%, 14rem), 1fr));
    gap: 1rem;
    margin-top: 1.25rem;
  }
  .task {
    background: var(--panel);
  }
  .guides {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1rem;
  }
  .guide {
    display: flex;
    flex-direction: column;
    padding: 1.25rem;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    text-decoration: none;
  }
  .guide p {
    font-size: 0.92rem;
  }
  .guide > span {
    margin-top: auto;
    font-size: 0.88rem;
    font-weight: 600;
  }
  .guide:hover {
    border-color: var(--ink);
    background: var(--panel);
  }
  .more {
    margin-top: 1.25rem;
    font-size: 0.92rem;
  }
  .workflow-links {
    display: grid;
    gap: 0;
    max-width: 42rem;
  }
  .workflow-links a {
    display: flex;
    justify-content: space-between;
    padding: 0.85rem 0;
    border-bottom: 1px solid var(--border);
    text-decoration: none;
  }
  @media (max-width: 46rem) {
    header {
      padding-bottom: 1.25rem;
    }
    h1 {
      font-size: 2rem;
    }
    section {
      padding-block: 1.5rem;
    }
    .guides {
      grid-template-columns: 1fr;
    }
  }
</style>
