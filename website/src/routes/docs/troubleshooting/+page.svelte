<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';

  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'no-output', title: 'No records arriving' },
    { id: 'leader', title: 'Leader will not start' },
    { id: 'worker', title: 'Worker offline or unable to enroll' },
    { id: 'deploy', title: 'Unexpected deployment state' },
    { id: 'certificates', title: 'Certificate failures' },
  ];
</script>

<GuideLayout
  title="Troubleshooting"
  description="Diagnose common Smeltr setup and operations failures."
  {sections}
>
  <p class="eyebrow">Operations</p>
  <h1>Troubleshooting</h1>
  <p class="lede">Start with the error message, worker status and running commit.</p>

  <h2 id="no-output">No records arriving</h2>
  <p>
    Send one fresh record with a unique marker. Follow it through these checks in order; stop at the
    first result that does not match.
  </p>
  <ol>
    <li>
      <strong>The source has a record to read.</strong> Check the input topic, broker access, and
      consumer group's saved position. A metrics source needs a full bucket interval; a log source
      needs a new message at its selected severity.
      <a href="/docs/sources#verify">Check source input</a>.
    </li>
    <li>
      <strong>The worker runs the intended configuration.</strong> Compare its running commit with
      the commit you deployed, and inspect its latest errors. Online alone does not prove it loaded
      the change. <a href="/docs/git/configuration#verify">Check the running version</a>.
    </li>
    <li>
      <strong>The route matches.</strong> Preview a saved sample from that source. Check the route
      condition, pipeline, delivery condition, and destination connection.
      <a href="/docs/first-pipeline#check">Check the route</a>.
    </li>
    <li>
      <strong>The pipeline keeps the record.</strong> Inspect each step's result for a Drop
      condition or an <code>Unparsable</code> exception. Test a record that should pass as well as
      one the pipeline should filter out.
      <a href="/docs/steps/parser#unparsable">Check unparsable records</a>.
    </li>
    <li>
      <strong>The destination accepts it.</strong> Check Status → Issues and Logs for connection or
      refusal errors, then inspect the actual output topic or index for your marker. A preview does
      not send data. <a href="/docs/delivery#hold">Check delivery</a>.
    </li>
  </ol>
  <p>
    The <a href="/docs/first-pipeline#verify">worked Kafka delivery check</a> includes a producer command
    and a script that verifies the transformed record.
  </p>

  <h2 id="leader">Leader will not start</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Symptom</th><th>Check</th><th>Fix</th></tr></thead>
      <tbody
        ><tr
          ><td>Permission error</td><td>Service account and configured paths</td><td
            >Grant access to data, secrets encryption key and certificate files. The leader must own
            its data directory and the secrets encryption key's directory; on Kubernetes, point both
            inside their volumes.</td
          ></tr
        >
        <tr
          ><td>Address already in use</td><td>Bind address and port</td><td
            >Stop the conflicting listener or choose an available port.</td
          ></tr
        >
        <tr
          ><td>Configuration rejected</td><td>Startup error and TOML keys</td><td
            >Correct the named setting. Use HTTPS for non-loopback browser origins.</td
          ></tr
        >
        <tr
          ><td>A store is damaged</td><td>The file the startup error names</td><td
            >For <code>leader.db</code>, put the copy the error names, or your own backup, in its
            place, remove its <code>-wal</code> and <code>-shm</code> files, and start again.
            Changes since that copy are lost. For <code>repos/config.git</code>, restore a sound
            copy. With a Git host, you can move it aside instead: the leader takes the host's
            <code>main</code>, and saves you never pushed stay only in the moved copy. With neither,
            moving it aside starts an empty configuration, and workers run nothing until you write
            it again.</td
          ></tr
        ></tbody
      >
    </table>
  </div>
  <h2 id="worker">Worker offline or unable to enroll</h2>
  <p>
    If a worker appears offline, check its last heartbeat and whether the process is running on the
    host. Read its local logs for connection or certificate errors and restore its connection to the
    leader. Preserve its state directory if you restart it. A worker can keep processing cached
    configuration while the leader is unreachable; check destination output as well as its status.
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Symptom</th><th>Check</th><th>Fix</th></tr></thead>
      <tbody
        ><tr
          ><td>Cannot reach leader</td><td>DNS, routing and worker.leader.url</td><td
            >Use an address reachable from the worker.</td
          ></tr
        >
        <tr
          ><td>Certificate rejected</td><td>Server name and enrollment code trust mode</td><td
            >Fix the certificate or trust configuration. Check worker.leader.ca only if explicitly
            set.</td
          ></tr
        >
        <tr
          ><td>Enrollment refused</td><td
            >Enrollment code expiry, remaining uses, withdrawal, a replaced leader authority and
            deployed worker group</td
          ><td
            >Correct the worker group or create a new enrollment code; read the error before
            replacing credentials.</td
          ></tr
        >
        <tr
          ><td>Restarted with exit code 75</td><td><code>worker.enrolled_again</code> in its log</td
          ><td
            >None. The leader had forgotten it, so it enrolled again as a new worker and restarted
            as it.</td
          ></tr
        >
        <tr
          ><td>Refused and not enrolling again</td><td
            ><code>worker.not_enrolled_again</code> in its log</td
          ><td
            >Its enrollment code was withdrawn, used up or expired, or the leader's authority was
            replaced. Mint another, give it to the worker (on Kubernetes, update the Secret), and
            restart it.</td
          ></tr
        ></tbody
      >
    </table>
  </div>
  <ProductScreenshot
    source="fleet-attention"
    height={1800}
    crop={{ x: 680, y: 233, width: 1960, height: 1082 }}
    alt="Fleet status with one active issue: fw-collector-02 has stopped sending heartbeats. Its last-seen time is highlighted beside three online workers."
    caption="Example fleet: one worker needs attention. Check its last-seen time and the active issue before investigating its host."
  />
  <h2 id="deploy">Unexpected deployment state</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Symptom</th><th>Check</th><th>Fix</th></tr></thead>
      <tbody
        ><tr
          ><td>Worker Commit differs from deployed main</td><td
            >Worker active issues and last-seen time</td
          ><td>Fix the reported fetch or compile error; verify the running commit afterward.</td
          ></tr
        >
        <tr
          ><td>Worker kept its previous configuration</td><td
            >Leader reachability and unreachable timeout</td
          ><td
            >Restore connectivity and inspect Status. Do not edit worker configuration files
            directly.</td
          ></tr
        ></tbody
      >
    </table>
  </div>
  <h2 id="certificates">Certificate failures</h2>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Symptom</th><th>Check</th><th>Fix</th></tr></thead>
      <tbody
        ><tr
          ><td>Expired certificate</td><td>Certificate dates</td><td
            >Renew at the leader or proxy that serves it.</td
          ></tr
        >
        <tr
          ><td>Name mismatch</td><td>Public origin, worker URL and certificate names</td><td
            >Correct the URL or issue a certificate covering the required names.</td
          ></tr
        >
        <tr
          ><td>Untrusted authority</td><td>Browser and worker trust stores</td><td
            >Install the verified public CA where needed; never disable verification.</td
          ></tr
        ></tbody
      >
    </table>
  </div>
</GuideLayout>
