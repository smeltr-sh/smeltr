<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  const sections = [
    { id: 'configuration', title: 'What Git holds' },
    { id: 'publish', title: 'From a branch to main' },
    { id: 'workers', title: 'How workers follow main' },
    { id: 'verify', title: 'Check the running version' },
  ];
</script>

<GuideLayout
  title="How Git is used"
  description="How Git versions Smeltr configuration and how workers follow main."
  {sections}
>
  <h1>How Git is used</h1>
  <p class="lede">
    Git is Smeltr's configuration control plane: it stores the versions that workers run. Your edits
    live on a draft branch until you deploy them to <code>main</code>. Workers follow
    <code>main</code> and report the commit they are running. A commit identifies a saved version of the
    configuration.
  </p>

  <p>
    <a href="/docs/getting-started#overview"
      >See the leader, workers, and data connections in one diagram →</a
    >
  </p>

  <h2 id="configuration">What Git holds</h2>
  <p>
    Sources, destinations, routes, pipelines, tests, and supporting resource files are versioned
    together. Each worker group has its own configuration under
    <code>groups/&lt;group-id&gt;/</code>. Reusable configuration lives under <code>shared/</code>.
    A worker loads its group's configuration together with the shared content at the same commit. A
    saved sample is kept on your branch and deploys only when a test uses it. Deploying the change
    leaves out the others, and they are deleted with the branch.
  </p>
  <p>
    Configuration refers to credentials by name and version. You manage secret values through
    <a href="/docs/secrets">Secrets</a> or the worker environment. Host startup settings, accounts,
    and worker state have their own storage. To recover an installation, follow the
    <a href="/docs/backup-recovery">leader backup procedure</a>; a copy of the Git repository alone
    does not contain all of that state.
  </p>

  <p>
    Workers clone the whole configuration repository, including other groups. The leader also trusts
    an enrolled worker with every credential it stores. A worker group selects what a worker runs;
    it does not restrict what that worker can access. Use separate clusters for workloads that
    require this isolation.
  </p>
  <h2 id="publish">From a branch to main</h2>
  <WorkflowDiagram kind="review" />
  <p>
    Work on a branch in the Smeltr UI. Save your changes, run the tests, and open the change for
    review. Review the diff and the pipelines and routes it reaches. Bring the branch current if
    <code>main</code> has moved, then review the result again.
  </p>
  <p>
    Approval and deployment publish the change as a commit on <code>main</code>. That commit becomes
    the configuration workers follow. Saving a draft branch leaves the deployed configuration in
    place. See <a href="/docs/review">Review and deploy</a> for the procedure.
  </p>
  <p>
    To return to an earlier configuration, use Branches → History → <strong
      >Roll back to here</strong
    >. Review its effect across worker groups and check that its secret versions are still
    available. Then check the workers’ running commits and destination output, just as you would for
    any other deployment.
  </p>
  <p>
    A rollback writes no commit. It deploys the earlier commit and holds the fleet there: the leader
    does not deploy commits that reach <code>main</code> afterwards. History shows the hold, who
    made it, and how many commits wait, and Status reports <strong>Deploy held</strong> for as long
    as it lasts. <strong>Release</strong> deploys <code>main</code>'s head and ends the hold. A
    change approved during the hold merges to <code>main</code> and waits there for
    <strong>Release</strong>.
  </p>

  <h2 id="workers">How workers follow main</h2>
  <ol>
    <li>An enrolled worker completes its initial check-in with the leader.</li>
    <li>
      It periodically fetches the deployed commit into its local Git repository: <code>main</code>'s
      head, or the commit a rollback holds. The worker discovers it itself; its heartbeat reports
      deployment state back to the leader.
    </li>
    <li>
      It prepares the configuration for its worker group and the shared resources at that commit. If
      preparation fails, it can keep running its previous configuration and report the failure.
    </li>
    <li>
      It reports the commit it is running. When a change leaves its group and shared content
      unchanged, the worker can adopt the new commit without restarting its pipeline runtime.
    </li>
  </ol>
  <p>
    Each worker finds and loads changes independently. Publishing a commit does not switch the whole
    fleet at once. If a fetch fails, a worker keeps its current configuration and retries. A running
    worker keeps running through a leader outage; a worker that restarts runs nothing until the
    leader answers its first check-in. Check its <a href="/docs/operations#outages"
      >setting for stopping when the leader cannot be reached</a
    >.
  </p>

  <h2 id="verify">Check the running version</h2>
  <p>Record the deployed commit on <code>main</code>, then open Status for each intended worker.</p>
  <p>
    In the selected worker's details, compare <strong>Commit</strong> with the deployed main commit. This
    is the version the worker reports running. Check its last-seen time and active issues if it differs
    or the worker is offline.
  </p>
  <p>
    Once the intended workers report the deployed commit, send a fresh record and verify its output
    at the destination.
  </p>
</GuideLayout>
