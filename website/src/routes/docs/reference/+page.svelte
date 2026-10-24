<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import data from '#lib/startup-settings.json';
  import example from '#lib/startup-example.toml?raw';
  const sections = [
    { id: 'network', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'precedence', title: 'How it works' },
    { id: 'startup', title: 'Other settings' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Startup configuration reference"
  description="Startup configuration reference for Smeltr administrators."
  {sections}
>
  <h1>Startup configuration reference</h1>
  <p class="lede">
    Startup settings choose what a Smeltr process runs, where it listens and where it keeps its
    state. They come from <code>smeltr.toml</code> or the environment and apply on restart.
  </p>
  <h2 id="network">Before you start</h2>
  <p>
    Browsers and workers reach the configured leader listener (443 by default), or an HTTPS proxy
    (usually 443). Workers also need access to each source and destination's configured endpoints.
    Workers initiate heartbeats and Git configuration fetches; they need no inbound worker
    management connection from the leader. Ports and protocols for data systems come from their
    connector configuration, not a universal Smeltr port list.
  </p>
  <p>
    Protect leader state and secrets encryption keys, plus worker identity, credentials and trust
    files. See <a href="/docs/backup-recovery">Backup and recovery</a>. Use the startup reference
    for the version you received: settings can change between deliveries, and documentation for
    another version may not match your artifact.
  </p>
  <h2 id="settings">Settings</h2>
  <SettingsTable
    settings={data.settings.map((s) => ({
      name: s.key,
      env: s.env,
      absent: s.default,
      examples: s.examples,
      what: s.what,
      values: s.values,
    }))}
  />

  <h2 id="precedence">How it works</h2>
  <p>
    Set <code>SMELTR_CONFIG_FILE</code> to an absolute file path to choose the startup file.
    Otherwise Smeltr looks for <code>./smeltr.toml</code> in its working directory. Relative state paths
    are also relative to that working directory. A non-empty environment variable overrides the file,
    which overrides the default. Smeltr treats empty environment values as unset. Invalid values and unknown
    TOML keys fail startup.
  </p>
  <p>
    In <code>smeltr.toml</code>, each setting sits under the table its key names. The examples above
    are environment values: in the file a string is quoted and a list is an array.
  </p>
  <pre><code>{example}</code></pre>
  <p>
    Every startup setting applies when the process restarts. An existing CA keeps its stored
    validity period. A worker uses enrollment codes only while it holds no credential its leader
    knows.
  </p>

  <p>At least one of <code>leader.enabled</code> and <code>worker.enabled</code> must be true.</p>
  <h2 id="startup">Other settings</h2>
  <p>Settings outside the startup file change elsewhere:</p>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Change</th><th>Where</th><th>Takes effect</th></tr></thead>
      <tbody>
        <tr><td>Accounts</td><td>Admin → Accounts</td><td>Immediately</td></tr>
        <tr
          ><td>Session limits</td><td>Admin → System → Sessions</td><td
            >On the next session request</td
          ></tr
        >
        <tr
          ><td>Pipelines and group configuration</td><td
            >A branch → Open for review → Approve &amp; deploy</td
          ><td>When workers adopt the deployment</td></tr
        >
        <tr
          ><td>Certificates</td><td>Admin → System → Certificates</td><td
            >On reload; restart if the UI reports failure</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/backup-recovery">Backup and recovery</a></li>
    <li><a href="/docs/operations#logs">Change log verbosity</a></li>
  </ul>
</GuideLayout>
