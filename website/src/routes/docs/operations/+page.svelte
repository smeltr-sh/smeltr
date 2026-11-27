<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'daily', title: 'Check workers and data flow' },
    { id: 'source-warnings', title: 'Investigate source warnings' },
    { id: 'alerts', title: 'Page on warnings' },
    { id: 'delivery', title: 'Delivery and recovery' },
    { id: 'logs', title: 'Change log verbosity' },
    { id: 'changes', title: 'Deploy and roll back configuration' },
    { id: 'outages', title: 'Leader outages and revoked workers' },
  ];
</script>

<GuideLayout
  title="Operate and recover the fleet"
  description="Operate and recover the fleet for Smeltr administrators."
  {sections}
>
  <h1>Operate and recover the fleet</h1>
  <p>
    Use Status to check worker health and the running configuration. Use the Map to trace records
    and Logs to investigate errors. For a specific symptom, start with
    <a href="/docs/troubleshooting">Troubleshooting</a>.
  </p>
  <h2 id="daily">Check workers and data flow</h2>
  <ol>
    <li>
      Open Status and check Online, Silent, Never heartbeated and Stopped workers, plus active
      warnings. A Stopped worker said it was stopping and raises no warning. An ephemeral worker
      leaves the list once it is gone.
    </li>
    <li>
      Select an affected worker and compare its reported Commit with the deployed main commit. Check
      its active issues and last-seen time. A heartbeat shows the worker recently contacted the
      leader. Check the running commit to see whether deployment succeeded.
    </li>
    <li>
      On the Map, trace the affected source, pipeline, and destination. Check throughput and owned
      partitions before changing thread limits.
    </li>
    <li>
      Use Logs to narrow by level, worker, worker group, message and time. Preserve timestamps,
      commit IDs and error text for an incident report; omit secrets and sensitive sample records.
    </li>
  </ol>
  <p>
    <a href="/docs/git/configuration#verify">How to compare the deployed and running commits →</a>
  </p>
  <h2 id="source-warnings">Investigate source warnings</h2>
  <p>
    On the Map, open the source's <strong>History</strong> tab to see warnings and its longest quiet
    spells. Open <strong>Senders</strong> to check whether the change is isolated to a sender. Smeltr
    learns each sender's usual rate separately.
  </p>
  <ul>
    <li>
      <strong>Lower volume.</strong> Smeltr warns when row counts stay below the usual range for that
      time of day. It checks the last 5 minutes, 30 minutes, 6 hours, and 24 hours against patterns from
      past weeks.
    </li>
    <li>
      <strong>Higher volume.</strong> Smeltr reports when the last 30 minutes are above the usual range.
    </li>
    <li>
      <strong>Sparse input.</strong> Smeltr warns when a source that is too sparse for rate checks goes
      1.5 times as long without a row as its longest quiet spell in the last year.
    </li>
  </ul>
  <p>
    In History, mark a quiet spell <strong>Normal</strong> to include it in the learned pattern or
    <strong>Outage</strong>
    to exclude it. Choose <strong>Fast</strong> to hear about a stop in about 5 minutes, or
    <strong>Quiet</strong> to report fewer brief dips and hear about a stop in about 30 minutes.
  </p>
  <h2 id="alerts">Page on warnings</h2>
  <p>
    The leader serves its active warnings at <code>/api/metrics</code>, in the format Prometheus
    scrapes. Each warning is a <code>smeltr_warning</code> series while it holds, labeled with its
    condition, severity, subject kind, subject, worker group, and, for a warning a worker logged,
    the class and type of that log line, so Alertmanager pages on it and resolves it when it clears.
    <code>smeltr_warning_start_time_seconds</code> says when each began.
  </p>
  <ol>
    <li>
      In Admin → Accounts, create a key account for the scraper. Its roles determine which worker
      groups' warnings it sees. Leader warnings require administrator access.
    </li>
    <li>
      Scrape <code>/api/metrics</code> with the key as a bearer token. A failed scrape is how Prometheus
      notices the leader itself is down.
    </li>
  </ol>
  <p>
    Without Prometheus, the leader sends warnings itself. In Admin → System → Alerts, add a webhook
    (Generic JSON, Slack, Microsoft Teams or PagerDuty) or, with the mail server on, an email
    channel. Tick the issues each channel gets. A channel is told when an issue is raised and again
    when it clears, and is sent the issue's message only if you include it. Mute a channel during
    planned work.
  </p>
  <pre><code
      >{`scrape_configs:
  - job_name: smeltr-leader
    scheme: https
    metrics_path: /api/metrics
    authorization:
      credentials_file: /etc/prometheus/smeltr-key
    static_configs:
      - targets: ['smeltr.example.com']`}</code
    ></pre>
  <h2 id="delivery">Delivery and recovery</h2>
  <p>
    For a stalled destination, distinguish connection failures from refused records. Check the
    destination's errors, its action for refused records, and any destinations chosen to keep those
    records. Check how many records are waiting and how long the source will keep them. Fix the
    cause before replaying or changing policy.
  </p>
  <p>
    Delivery covers <a href="/docs/delivery#hold">holding and retrying delivery</a>,
    <a href="/docs/delivery#dlq">failures while saving refused records</a>, and
    <a href="/docs/delivery#recovery">recovering and replaying records</a>.
  </p>
  <h2 id="logs">Change log verbosity</h2>
  <p>
    Set log levels in Admin › System › Logging. The default level applies to every log line unless
    an override names it. An override gives its own level to a class, a class and one type, or a
    service, such as kafka. A line takes the first override that names it: its class and type, then
    its service, then its class. The leader keeps a change at once and each worker from its next
    heartbeat, with no restart.
  </p>
  <p>
    A process starts at its startup log level and keeps it until the leader's levels reach it. To
    see more from a worker that cannot reach the leader, set <code>SMELTR_LOG_LEVEL</code> or the
    top-level
    <code>log_level</code> and restart it.
  </p>
  <p>
    Levels: error, warn, info (default), debug and trace. Use debug or trace briefly, then restore
    info. Do not share logs containing credentials or sensitive records.
  </p>
  <h2 id="changes">Deploy and roll back configuration</h2>
  <p>
    Save, test, review, then deploy. Deployment publishes configuration to <code>main</code>.
  </p>
  <ol>
    <li>For rollback, open Branches → History and identify the earlier known-good commit.</li>
    <li>
      Review its affected files and secret-version requirements. Rollback can affect the whole
      fleet, not only the worker group selected in the UI.
    </li>
    <li>
      Select <strong>Roll back to here</strong> and confirm <strong>Deploy</strong>. The fleet stays
      on that commit until <strong>Release</strong> on the same page.
    </li>
    <li>Check data flow in Status.</li>
  </ol>
  <p>
    Do not rewrite the worker's local configuration manually. A configuration rollback is different
    from <a href="/docs/upgrade#downgrade">downgrading the executable</a>.
  </p>
  <h2 id="outages">Leader outages and revoked workers</h2>
  <p>
    These outage rules apply to workers running separately from the leader. In standalone mode,
    stopping or restarting the process affects both roles. New workers need a successful initial
    check-in before they start following main. Once initialized, workers keep running while the
    leader is unavailable. A worker that restarts runs nothing until the leader answers its first
    check-in. The default unreachable policy has no automatic stop; an explicit
    worker.heartbeat.stop_after_unreachable_secs changes this behavior.
  </p>
  <p>
    Ephemeral workers run on through a leader outage and stay enrolled: the leader counts a worker's
    silence only while it is up. One that stops during the outage keeps trying to say so while it
    stops. If it never gets through, the leader reports it silent, then forgets it once the leader
    has been back for longer than the Ephemeral workers setting.
  </p>
  <p>
    Revoking a worker blocks leader access but does not stop its pipeline. To stop it immediately,
    stop or isolate the host and revoke its external credentials.
  </p>
</GuideLayout>
