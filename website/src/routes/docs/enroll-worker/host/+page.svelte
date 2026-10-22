<script lang="ts">
  import { image } from '#lib/release.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  const sections = [
    { id: 'before', title: 'Prepare the group and host' },
    { id: 'create', title: 'Create an enrollment code' },
    { id: 'configure', title: 'Configure and start' },
    { id: 'verify', title: 'Verify and handle failures' },
    { id: 'revoke', title: 'Revoke or replace a worker' },
  ];
</script>

<GuideLayout
  title="Enroll a worker on a host"
  description="Enroll and manage a Smeltr worker on a host or in a container."
  {sections}
>
  <h1>Enroll a worker on a host</h1>
  <p>
    This guide is for a worker running separately from the leader. A <a
      href="/docs/install-leader/binary#first-start">standalone deployment</a
    >
    enrolls its own worker automatically. To run workers in a cluster, see
    <a href="/docs/enroll-worker/kubernetes">Enroll workers on Kubernetes</a>.
  </p>
  <p>
    Run the leader and its workers from one major release: verified binaries, or image digests from
    <a href="/docs/install-leader/container#pull"><code>{image}</code></a>. A worker on another
    major keeps running what it has and takes no new deployment, and Status shows it as
    <strong>On another build</strong> with both releases. A build that is not a release matches only its
    own commit.
  </p>
  <p>
    The leader trusts an enrolled worker with every group's configuration and every credential it
    stores. Worker groups organize processing; use separate Smeltr clusters when you must isolate
    hosts from one another's configuration or credentials.
  </p>
  <h2 id="before">Prepare the group and host</h2>
  <p>
    Install that Smeltr executable or image on the worker host. Its built-in Git client fetches
    configuration from the leader. For a container, mount the configuration read-only and the worker
    state directory read-write outside the container. Give each worker its own state directory and
    preserve it across restarts. Never copy an enrolled worker’s state to another machine. The host
    must be able to connect to the leader, sources, and destinations.
  </p>
  <p>
    Use the default group for a first installation. For another group, open Admin → Worker groups,
    choose <strong>+ Add group</strong>, enter Name and Id, then choose <strong>Add</strong>. Review
    and deploy that change: a Pending group is not live until you deploy it.
  </p>
  <p>
    Group name changes also need deployment; group colors apply immediately. Deleting a group also
    needs deployment. Once it deploys, workers still in that group stop all their pipelines until
    you move them to another group or shut them down. Set
    <code>worker.group</code> to the group ID. Shared configuration can affect more than one group, so
    inspect the full change before deploying.
  </p>
  <h2 id="create">Create an enrollment code</h2>
  <p>
    An enrollment code is the value that lets a new worker connect to the leader and obtain its own
    credential.
  </p>
  <ol>
    <li>
      Open Admin → Workers → Enrollment codes and choose <strong>+ Add enrollment code</strong>.
    </li>
    <li>
      Set Group and Workers it brings in. A Permanent enrollment code also takes Workers and Valid
      for (hours); defaults are one worker and 24 hours. An Ephemeral one takes neither. No
      enrollment code outlasts the leader's certificate authority. Then select <strong>Mint</strong
      >.
    </li>
    <li>
      Correct “The address this worker will reach the leader at” to a URL reachable from the worker.
      A browser's localhost URL is not suitable for a remote worker.
    </li>
    <li>
      Copy the enrollment code before dismissing it. Smeltr shows the secret once and cannot
      retrieve it.
    </li>
  </ol>
  <p>
    The enrollment code includes a secret and tells the worker how to verify the leader's
    certificate. A permanent enrollment code works until it expires, you withdraw it, or it reaches
    its allowed number of workers. An ephemeral one works until you withdraw it, for workers an
    autoscaler starts and stops; see <a href="/docs/enroll-worker/kubernetes#autoscale"
      >Autoscale workers</a
    >. Either stops working when the leader's certificate authority is replaced.
  </p>
  <h2 id="configure">Configure and start</h2>
  <p>
    Save this as the worker’s configuration. Replace the placeholder with the complete enrollment
    code you copied. This example uses the direct HTTPS leader from the installation guide:
  </p>
  <pre><code
      >[worker]
enabled = true
group = "default"
enrollment_code = "PASTE_THE_COMPLETE_ENROLLMENT_CODE_HERE"

[worker.leader]
url = "https://smeltr.example.com"

[worker.storage]
state_dir = "/var/lib/smeltr-worker"</code
    ></pre>
  <pre><code>SMELTR_CONFIG_FILE=/absolute/path/to/worker.toml /absolute/path/to/smeltr</code></pre>
  <p>
    A worker whose group runs a
    <a href="/docs/sources#push-all-workers">receiver</a> listens on the receiver's port, so the host's
    firewall must let senders reach that port.
  </p>
  <p>
    For a container worker, set <code>SMELTR_CONFIG_FILE</code> to its mounted configuration path
    and mount <code>/var/lib/smeltr-worker</code> persistently. The container needs no inbound
    published port unless its group runs a receiver; then publish the receiver's port, for example
    <code>-p 8088:8088</code>. It must be able to reach the leader and every configured source and
    destination.
  </p>
  <p>
    Leave <code>worker.leader.ca</code> unset for normal enrollment. An enrollment code can verify
    the leader’s CA fingerprint and save the matching certificate. The worker also trusts the public
    roots, Mozilla’s and the host’s own, unless <code>worker.leader.trust_public</code> is false. If you
    set a CA file yourself, you must maintain it when the authority changes; automatic updates no longer
    apply. Keep certificate verification enabled.
  </p>
  <p>
    After enrollment, the state directory contains worker_id, credential.token, the configuration
    clone and trust material. Protect it. After enrollment, the worker uses its saved credential.
    Remove a permanent enrollment code from configuration, restart the worker, and confirm it
    reconnects. A fresh host or lost state needs a new enrollment.
  </p>
  <h2 id="verify">Verify and handle failures</h2>
  <p>
    On Status, confirm the worker is Online, reports the expected group, and shows its deployment
    state. Enrollment alone does not prove that a pipeline is running. A new worker must complete
    its first check-in and fetch configuration. A running worker keeps running through a leader
    outage; by default it does not stop solely because the leader is unreachable. A worker that
    restarts runs nothing until the leader answers its first check-in. See <a href="/docs/reference"
      >stop_after_unreachable_secs</a
    >.
  </p>
  <p>
    If enrollment fails, check the URL, the complete enrollment code, lifetime, remaining uses,
    group and certificate name. Withdraw an exposed enrollment code in the Enrollment codes table
    and mint another. Withdrawal prevents further enrollment but does not revoke workers already
    enrolled.
  </p>
  <h2 id="revoke">Revoke or replace a worker</h2>
  <p>
    Stop or isolate a compromised worker through your host-management controls, then select <strong
      >Revoke</strong
    > for that worker in Admin → Workers. Revocation refuses subsequent leader requests; it does not stop
    its running pipeline or its access to external data systems. Rotate external credentials where necessary.
    Enroll a rebuilt host using a new state directory and enrollment code, rather than reusing the revoked
    identity.
  </p>
</GuideLayout>
