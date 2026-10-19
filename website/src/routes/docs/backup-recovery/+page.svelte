<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'backup', title: 'Take a consistent offline backup' },
    { id: 'recover', title: 'Restore without creating two leaders' },
    { id: 'test', title: 'Test a restore safely' },
    { id: 'limits', title: 'Availability and worker state' },
  ];
</script>

<GuideLayout
  title="Backup and recovery"
  description="Backup and recovery for Smeltr administrators."
  {sections}
>
  <h1>Backup and recovery</h1>
  <p>
    A complete recovery needs both the leader data and its matching <strong>sealing key</strong>,
    which decrypts stored secrets. Keep separate, protected copies of both.
  </p>
  <h2 id="backup">Take a consistent offline backup</h2>
  <p>
    Stop the leader before copying its files so the backup is consistent. Schedule a maintenance
    window. Workers running in separate processes normally keep running their current configuration,
    but workers configured to stop when the leader is unreachable may stop. Browser sessions end. In
    standalone mode, stopping Smeltr also stops its worker and data processing.
  </p>
  <p>Complete this checklist in order:</p>
  <ul class="checklist">
    <li>
      Record the installed version, checksum or container-image ID, configuration file, environment
      settings, and service account. Keep the matching executable or container image for recovery.
    </li>
    <li>
      Stop the leader through its service manager or gracefully terminate the foreground process.
      Confirm it has exited before copying files.
    </li>
    <li>
      Copy the entire leader.storage.data_dir: <code>leader.db</code>, any accompanying SQLite
      files,
      <code>repos/</code>
      and <code>tls/</code>. Do not select just the database or just Git. Each start that finds
      <code>leader.db</code> sound also keeps a copy beside it as <code>leader.db.previous</code>.
    </li>
    <li>
      Back up the exact leader.secrets.key separately under restricted access. Label it so you can
      match it to the data backup. Also keep the service configuration and any private certificate
      authority or proxy configuration needed to restore access.
    </li>
    <li>
      Encrypt and restrict both backups, preserve ownership and permissions, and verify checksums.
      Restart the original leader and verify login and heartbeats.
    </li>
  </ul>
  <p>
    The key lives outside the data directory by default. Keep it separate so a stolen data backup
    does not also provide the key to decrypt stored secrets. TLS private keys inside the data
    directory also make that backup sensitive.
  </p>
  <h2 id="recover">Restore without creating two leaders</h2>
  <ol>
    <li>Stop or isolate the original leader and prevent its automatic restart.</li>
    <li>
      On the replacement host, install the same binary version or container-image ID first. Restore
      the complete data directory and matching sealing key to the configured paths, preserving
      restrictive permissions.
    </li>
    <li>
      Restore the configuration and verify DNS, public origin, ports and certificate names. Do not
      initialize an empty leader over the recovery target.
    </li>
    <li>
      Start it as the original service account. Sign in again; a backup holds no sessions. If you
      lose account access, use <a href="/docs/accounts#recovery">administrator recovery</a>.
    </li>
    <li>
      Check Git history, worker groups, certificate status, and any errors on the Secrets page.
      Confirm workers reconnect and compare each worker's reported Commit with the restored main
      commit.
    </li>
  </ol>
  <p>
    A restored Git history may be older than the fleet's cached configuration. Review that
    difference before allowing production workers to fetch from it.
  </p>
  <h2 id="test">Test a restore safely</h2>
  <p>
    Restore into a network-isolated environment with no production workers and no access to
    production sources or destinations. Use disposable worker state and non-production credentials.
    Check login, history, secret usability, trust and a sample deployment, then record the measured
    recovery time. A restored leader is not passive: do not expose a test restore to the production
    fleet.
  </p>
  <h2 id="limits">Availability and worker state</h2>
  <p>
    A standalone worker keeps its state in <code>worker/</code> beneath the leader data directory.
    Include that directory in the stopped-process backup. An isolated restore also starts that
    worker when <code>worker.enabled</code> is true, so block access to production data systems before
    starting it.
  </p>
  <p>
    Run one leader. If it is lost, restore a backup using the procedure above; Smeltr does not
    currently switch automatically to another leader. Preserve worker state across upgrades,
    including its identity, credential, trust files and local configuration repository. Never start
    two workers from the same copied identity. A rebuilt host should enroll afresh.
  </p>
</GuideLayout>
