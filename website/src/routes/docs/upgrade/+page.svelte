<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'new-release', title: 'Know when a release is out' },
    { id: 'executable', title: 'Upgrade the executable' },
    { id: 'kubernetes', title: 'Operate and upgrade on Kubernetes' },
  ];
</script>

<GuideLayout
  title="Upgrade Smeltr"
  description="Upgrade the Smeltr executable or container image, on a host or on Kubernetes."
  {sections}
>
  <h1>Upgrade Smeltr</h1>
  <h2 id="new-release">Know when a release is out</h2>
  <p>
    Once a day, the leader asks <code>api.github.com</code> whether a newer release is out. A newer
    release shows next to Version on
    <strong>Admin → System → General</strong>, and on the Issues page. To stop these checks, turn
    off
    <strong>Check for new releases</strong> there.
  </p>
  <h2 id="executable">Upgrade the executable</h2>
  <p>Complete this checklist in order:</p>
  <ul class="checklist">
    <li>
      Record the installed version, checksum or image digest, and configuration. Read the new
      version’s release notes before upgrading.
    </li>
    <li>
      Test the upgrade on an isolated copy of your backup. Check worker enrollment, certificates,
      secrets, and a pipeline that uses your usual sources and destinations.
    </li>
    <li>
      Take a stopped-leader backup and retain the previous binary or image with the matching
      pre-upgrade state: an image archive or a registry copy you control, not only its digest.
      Preserve worker directories.
    </li>
    <li>
      Within a maintenance window, stop each process before replacing its executable. The leader
      upgrades data an earlier release wrote when it starts, with no manual step. A worker on
      another major release keeps running what it has, takes no new deployment, and shows on Status
      as <strong>On another build</strong>. A worker on an older release of the leader’s major keeps
      taking deployments, but refuses one that uses something newer, so upgrade workers before using
      what a release adds.
    </li>
    <li>
      Verify login, certificate status, heartbeats, reported commits and destination output before
      closing the window.
    </li>
  </ul>
  <p>
    An older build starts on the leader’s upgraded data unless the newer build changed it in a way
    the older one cannot read. Then the older leader refuses to start and says why: restore the
    pre-upgrade artifact and its matching backup together. Restoring older configuration can also
    change what workers deploy.
  </p>
  <h2 id="kubernetes">Operate and upgrade on Kubernetes</h2>
  <ul>
    <li>
      Keep the leader at one replica. Additional leader replicas would compete for the same
      installation state; they do not provide automatic recovery.
    </li>
    <li>
      Before a leader upgrade, stop the leader StatefulSet and back up both leader claims,
      <code>leader-data</code> and <code>leader-secrets</code>, as a matched set. Retain the old
      image digest and manifests with that backup.
    </li>
    <li>
      Update leader and worker image digests only after testing the new release in an isolated
      namespace. Keep the leader and workers on one major release: a worker on another major takes
      no new deployment.
    </li>
    <li>
      When scaling a StatefulSet up, mint a permanent enrollment code for the new pods and
      temporarily restore the enrollment code Secret and environment entry. Each new worker pod
      receives its own identity and storage. To scale with load, <a
        href="/docs/enroll-worker/kubernetes#autoscale">autoscale</a
      > the group instead.
    </li>
    <li>
      Revoking a worker in Smeltr does not terminate its pod or external data access. Scale it down
      or delete the workload, revoke it, and rotate external credentials when required.
    </li>
  </ul>
  <p>
    Follow <a href="/docs/backup-recovery">Backup and recovery</a> and
    <a href="/docs/operations">Operate the fleet</a>
    for the application-level checks around these Kubernetes actions.
  </p>
</GuideLayout>
