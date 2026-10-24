<script lang="ts">
  import { image } from '#lib/release.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  import TopologyDiagrams from '#lib/components/TopologyDiagrams.svelte';
  const sections = [
    { id: 'overview', title: 'How Smeltr fits together' },
    { id: 'what-you-need', title: 'What you need' },
    { id: 'connections', title: 'Connect your data systems' },
    { id: 'topology', title: 'Choose where to run Smeltr' },
    { id: 'quick-start', title: 'Quick start' },
    { id: 'complete', title: 'Check the installation' },
    { id: 'next', title: 'Next steps' },
  ];
</script>

<GuideLayout
  title="Get started"
  description="Install a leader, connect a worker, and begin operating Smeltr."
  {sections}
>
  <p class="eyebrow">Getting started</p>
  <h1>Set up a small Smeltr deployment.</h1>
  <p class="lede">
    Use the leader's browser interface to build, test, and deploy configuration. Workers run that
    configuration and move records from sources to destinations.
  </p>

  <h2 id="overview">How Smeltr fits together</h2>
  <WorkflowDiagram kind="system" />
  <p>
    A <strong>worker group</strong> is a set of workers that run the same group configuration.
    Groups let you manage different environments separately while sharing reusable pipelines.
    <a href="/docs/git/configuration">Read how workers follow main →</a>
  </p>

  <h2 id="what-you-need">What you need</h2>
  <ul>
    <li>
      A Smeltr binary, or the <a href="/docs/install-leader/container#pull">container image</a> from
      <code>{image}</code>, with its version and checksum or digest.
    </li>
    <li>
      One host for a standalone install, or a leader host and separate worker hosts for a
      distributed deployment.
    </li>
    <li>Each separate worker can reach the leader over the network.</li>
    <li>
      Git installed on the leader host to serve the configuration repository. Workers use a built-in
      Git client. See <a href="/docs/git/external-host#hosting">Repository hosting</a>.
    </li>
    <li>Access to the sources and destinations your configuration will use.</li>
  </ul>
  <aside class="callout">
    <p>
      <strong>Start small.</strong> One leader and one worker group suit an evaluation or small deployment.
      Add groups to organize workloads and deployment policies.
    </p>
  </aside>

  <p>
    The leader trusts every enrolled worker with the whole cluster's configuration and leader-stored
    credentials. Groups do not restrict that access. Use separate clusters when workloads require
    separate trust boundaries.
  </p>
  <h2 id="connections">Connect your data systems</h2>
  <p>
    Choose a source to read, a pipeline to shape the records, and a destination to receive them.
    Workers make these connections, so check network access and credentials from the worker hosts.
    Use the connector options and release notes supplied with your version.
  </p>
  <ul>
    <li>
      <a href="/docs/sources">Sources</a> — input types, read positions, authentication, and setup.
    </li>
    <li>
      <a href="/docs/destinations">Destinations</a> — output types, delivery settings, and verification.
    </li>
    <li>
      <a href="/docs/secrets">Secrets &amp; credentials</a> — shared credentials and connection identities.
    </li>
  </ul>

  <h2 id="topology">Choose where to run Smeltr</h2>

  <h3>Standalone: one host</h3>
  <p>
    Run the leader and one worker as one Smeltr service, from one configuration file. Enable both
    <code>leader.enabled</code> and <code>worker.enabled</code>. For a local test, bind to
    <code>127.0.0.1</code> so only that machine can connect.
  </p>
  <TopologyDiagrams />

  <h3>Distributed: separate machines</h3>
  <p>
    Run the leader on its own machine and place workers near their sources and destinations. Workers
    connect to the leader; the leader does not need inbound access to worker hosts.
  </p>

  <p>
    The leader stores configuration history, accounts, and other installation state on its host.
    Preserve its data directory across restarts and upgrades, and back it up.
  </p>

  <h2 id="quick-start">Quick start</h2>
  <ol class="steps">
    <li>
      Choose your setup: <a href="/docs/install-leader/binary#first-start"
        >standalone with one built-in worker</a
      >, or <a href="/docs/https#direct">a leader with separate workers</a>. Prepare the account,
      files and directories in the installation guide before starting.
    </li>
    <li>
      Sign in with the initial credentials, choose a password, and confirm the default worker group
      exists.
    </li>
    <li>
      For standalone mode, the worker enrolls automatically. For separate workers, <a
        href="/docs/enroll-worker/host">create an enrollment code and enroll each worker</a
      >.
    </li>
    <li>
      Open Status. Wait for the worker to appear Online and compare its running commit with <code
        >main</code
      >. An online worker has contacted the leader; its running commit confirms which configuration
      it loaded.
    </li>
    <li>
      Use <a href="/docs/first-pipeline">Your first pipeline</a> to build, test, review and deploy a change,
      then verify a real record at the destination.
    </li>
  </ol>

  <h2 id="complete">Check the installation</h2>
  <p>
    Before handing a network deployment over, check each result. The local HTTP evaluation can defer
    HTTPS until you make it reachable over the network:
  </p>
  <ul class="checklist">
    <li>
      <strong>Trusted HTTPS.</strong> Open the leader at its configured address. The browser accepts
      its certificate. <a href="/docs/certificates#model">Set up trust</a>.
    </li>
    <li>
      <strong>Named administrator access.</strong> Sign in with your own account and complete any
      first-login password change. <a href="/docs/accounts#accounts">Create an account</a>.
    </li>
    <li>
      <strong>An online worker.</strong> Status shows the expected group and a recent heartbeat.
      <a href="/docs/enroll-worker/host#verify">Check worker status</a>.
    </li>
    <li>
      <strong>The intended configuration is running.</strong> Each worker in the test group reports
      the deployed commit. <a href="/docs/first-pipeline#deploy">Check deployment</a>.
    </li>
    <li>
      <strong>A record arrives at the destination.</strong> Send a fresh record and check its
      transformed fields where Smeltr delivers it.
      <a href="/docs/first-pipeline#verify">Verify the data path</a>.
    </li>
    <li>
      <strong>A recoverable backup.</strong> Take a consistent leader backup, keep the matching
      <a href="/docs/secrets#encryption-key">secrets encryption key</a>, and test a restore in
      isolation.
      <a href="/docs/backup-recovery#backup">Follow the backup procedure</a>.
    </li>
  </ul>

  <h2 id="next">Next steps</h2>
  <p>
    Use the <a href="/docs/operations">operations guide</a> to check workers, deploy changes, and
    recover from failures. If a check fails, use <a href="/docs/troubleshooting">Troubleshooting</a> to
    work through the connection, certificate, or deployment error.
  </p>
</GuideLayout>
