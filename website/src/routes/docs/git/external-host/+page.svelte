<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  const sections = [
    { id: 'hosting', title: 'Repository hosting' },
    { id: 'remote', title: 'Follow a Git host' },
  ];
</script>

<GuideLayout
  title="External Git host"
  description="Where the leader hosts the configuration repository, and how it follows a Git host."
  {sections}
>
  <h1>External Git host</h1>
  <h2 id="hosting">Repository hosting</h2>
  <p>
    The leader manages one Git repository for the fleet at
    <code>&lt;data-dir&gt;/repos/config.git</code>. Workers fetch it over the leader's HTTPS
    endpoint using their enrolled credentials and configured TLS trust. The worker endpoint serves
    reads; configuration changes go through the Smeltr UI.
  </p>
  <p>
    Install Git on the leader host: it uses <code>git http-backend</code> to serve the repository.
    Workers use a built-in Git client and do not need a separate Git installation. Preserve each
    worker's state directory so its local repository and cached configuration survive restarts. See
    <a href="/docs/install-leader/binary">leader installation</a>
    and
    <a href="/docs/enroll-worker/host">worker enrollment</a>.
  </p>

  <h2 id="remote">Follow a Git host</h2>
  <p>
    The leader can follow a repository on a Git host. The host's <code>main</code> is then what the fleet
    runs, and review and merge happen on the host. The leader keeps its own repository and goes on serving
    workers from it; workers never reach the host and never hold its token.
  </p>
  <WorkflowDiagram kind="remote" />
  <ol>
    <li>
      <strong>Saves go out.</strong> A save in the Smeltr UI is a commit on the leader at once, which
      the leader then pushes to the host's branch of the same name. Branches shows each branch's push
      state.
    </li>
    <li>
      <strong>Main comes in.</strong> The leader fetches the host on a timer and on
      <strong>Sync now</strong>. When the host's <code>main</code> moves, the leader loads every
      worker group at the new commit and deploys it only if all of them load. A commit that does not
      load leaves the fleet on the last one that did, and Status reports
      <strong>Remote not followed</strong> with the reason until a later sync succeeds.
    </li>
  </ol>
  <h3 id="remote-settings">Connect a host</h3>
  <p>
    Set the remote under Admin → System → <strong>Git remote</strong>:
  </p>
  <ProductScreenshot
    source="git-remote-settings"
    height={1800}
    crop={{ x: 731, y: 479, width: 1666, height: 914 }}
    alt="The Git remote tab under Admin, System: URL, User, Token, Fetch every minutes, and Approve: pushes to main."
    caption="The Git remote settings, with a GitHub repository's clone URL."
  />
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Setting</th><th scope="col">What it holds</th></tr></thead>
      <tbody>
        <tr
          ><th scope="row">URL</th><td
            >The repository's HTTP or HTTPS clone URL. The leader does not support SSH.</td
          ></tr
        >
        <tr
          ><th scope="row">User</th><td
            >The name the leader sends the token with. Use the name your host takes with a token.</td
          ></tr
        >
        <tr
          ><th scope="row">Token</th><td
            >An access token that can read the repository and push branches to it, Git LFS included.
            The leader stores it encrypted, never shows it again, and forgets it when the URL
            changes.</td
          ></tr
        >
        <tr
          ><th scope="row">Fetch</th><td
            >How often the leader fetches the host, every 1 to 1440 minutes. Off, it fetches only on
            Sync now.</td
          ></tr
        >
        <tr
          ><th scope="row">Approve</th><td
            >Its <em>pushes to main</em> box, off by default. See
            <a href="#remote-approve">Approve with a remote</a>.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    Connected to an empty repository, the leader pushes its own <code>main</code> there first. The
    leader follows a repository that already has a <code>main</code> from then on.
  </p>
  <h3 id="remote-branches">Branches and refused pushes</h3>
  <p>
    Branches go from the leader to the host, never back: edit a Smeltr branch in the UI, not on the
    host. A save waits for nothing on the host; while the host cannot be reached, its branch shows
    <strong>not pushed</strong> and the leader pushes it when the host answers. If someone else
    pushed to that branch on the host, the push is refused rather than dropping their commits.
    Branches then shows both tips, and <strong>Overwrite remote</strong> puts the leader's branch there
    anyway.
  </p>
  <h3 id="remote-approve">Approve with a remote</h3>
  <p>
    With <strong>pushes to main</strong> clear, Smeltr does not offer Approve: merge the branch on
    the host, and the leader deploys the result once it follows it. Checked, Approve squashes the
    change, pushes it to the host's <code>main</code>, and deploys it once the host takes it. A host
    that protects <code>main</code>, or whose <code>main</code> moved since the last sync, refuses the
    push; nothing moves, and Approve shows the host's reason.
  </p>
  <h3 id="remote-lookups">Lookup files</h3>
  <p>
    Lookup files travel by Git LFS. Pushing a branch uploads its lookup files to the host's LFS
    server first, so a clone with git-lfs has every file. When a commit from the host names a lookup
    the leader lacks, the leader downloads it from that server and checks it against its pointer
    before it loads it. Commit lookup files on the host with git-lfs, or upload them through Smeltr:
    the leader refuses a lookup committed as a plain file.
  </p>
</GuideLayout>
