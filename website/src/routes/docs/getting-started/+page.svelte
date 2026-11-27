<script lang="ts">
  import { image } from '#lib/release.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  import TopologyDiagrams from '#lib/components/TopologyDiagrams.svelte';
  const sections = [
    { id: 'overview', title: 'How Smeltr fits together' },
    { id: 'topology', title: 'Choose an installation method' },
    { id: 'what-you-need', title: 'Prepare to install' },
    { id: 'quick-start', title: 'Start the leader and check your worker' },
    { id: 'connections', title: 'Connect your data systems' },
    { id: 'complete', title: 'Check the installation' },
    { id: 'next', title: 'Next steps' },
  ];
</script>

<GuideLayout
  title="Get started"
  description="Install a leader, connect a worker, and begin operating Smeltr."
  {sections}
>
  <h1>Set up Smeltr</h1>
  <p class="lede">
    Start a leader, sign in, and confirm that a worker is online and running the expected
    configuration. For a local evaluation, run both roles on one host.
  </p>

  <h2 id="overview">How Smeltr fits together</h2>
  <p>
    The leader hosts the browser interface and stores configuration history and accounts. Workers
    run the deployed configuration and move records from sources to destinations. A
    <strong>worker group</strong> is a set of workers that run the same group configuration.
  </p>
  <WorkflowDiagram kind="system" />

  <h2 id="topology">Choose an installation method</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Where you want to run Smeltr</th><th>Installation guide</th></tr></thead>
      <tbody>
        <tr>
          <td>One host for a local evaluation</td>
          <td>
            <a href="/docs/install-leader/binary#first-start">Standalone binary</a> or
            <a href="/docs/install-leader/container#compose">Docker Compose</a>
          </td>
        </tr>
        <tr>
          <td>A leader with workers on separate hosts</td>
          <td>
            <a href="/docs/install-leader/binary">Binary</a> or
            <a href="/docs/install-leader/container">Container</a>, with
            <a href="/docs/https">HTTPS for the leader</a>
          </td>
        </tr>
        <tr>
          <td>Kubernetes</td>
          <td>
            <a href="/docs/install-leader/kubernetes">Install the leader on Kubernetes</a>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
  <p>
    In standalone mode, one service runs both roles. In a distributed deployment, workers connect to
    the leader. Place them where they can receive or read from sources and reach destinations.
  </p>
  <TopologyDiagrams />

  <h2 id="what-you-need">Prepare to install</h2>
  <ul>
    <li>
      Choose a release and verify the binary checksum or
      <a href="/docs/install-leader/container#pull">container image</a> before installing. The
      published image is <code>{image}</code>.
    </li>
    <li>
      If you install the leader binary on a host, install Git there to serve the configuration
      repository. Workers use a built-in Git client.
    </li>
    <li>
      Keep the leader data directory on persistent storage. For a network deployment, prepare
      <a href="/docs/https">HTTPS</a> and check that browsers and separate workers can reach the leader.
    </li>
  </ul>
  <p>
    Every enrolled worker is trusted with the whole cluster's configuration and leader-stored
    credentials. Use separate clusters when workloads require separate trust boundaries.
  </p>

  <h2 id="quick-start">Start the leader and check your worker</h2>
  <ol class="steps">
    <li>Start the leader using the installation guide you chose above.</li>
    <li>
      <a href="/docs/first-login">Sign in with the initial credentials</a>, change your password,
      and check that the default worker group exists.
    </li>
    <li>
      The standalone worker enrolls automatically. For separate workers, create an enrollment code
      and follow the guide for
      <a href="/docs/enroll-worker/host">a host</a> or
      <a href="/docs/enroll-worker/kubernetes">Kubernetes</a>.
    </li>
    <li>
      Open <strong>Status</strong>. Check that the worker is <strong>Online</strong> in the expected
      group. Compare its <strong>Commit</strong> with the commit on <code>main</code>
      to confirm which configuration it loaded.
    </li>
  </ol>

  <h2 id="connections">Connect your data systems</h2>
  <p>
    If you have a test Kafka broker, follow <a href="/docs/first-pipeline">Your first pipeline</a>
    to deploy a route and verify delivery. To use your own systems, choose a connector from
    <a href="/docs/sources">Sources</a> and <a href="/docs/destinations">Destinations</a>. Check
    network access from the worker hosts and set up any
    <a href="/docs/secrets">credentials</a> the connectors require.
  </p>

  <h2 id="complete">Check the installation</h2>
  <p>
    Use this checklist before handing off a network deployment. For an evaluation over local HTTP,
    set up trusted HTTPS before making the leader reachable over the network.
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
      <strong>An online worker.</strong> Status shows the expected worker group and a recent
      heartbeat.
      <a href="/docs/enroll-worker/host#verify">Check worker status</a>.
    </li>
    <li>
      <strong>The intended configuration is running.</strong> Each worker in the intended group
      reports the deployed commit.
      <a href="/docs/git/configuration#verify">Check the running version</a>.
    </li>
    <li>
      <strong>A record arrives at the destination.</strong> Send a fresh record and check its
      transformed fields where Smeltr delivers it.
      <a href="/docs/destinations#verify">Verify delivery</a>.
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
