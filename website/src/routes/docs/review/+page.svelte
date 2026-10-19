<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
  const sections = [
    { id: 'open', title: 'Open for review' },
    { id: 'check', title: 'Review the evidence' },
    { id: 'deploy', title: 'Deploy and verify' },
    { id: 'rollback', title: 'Roll back' },
  ];
</script>

<GuideLayout
  title="Review and deploy"
  description="Review configuration changes and verify worker adoption after deployment."
  {sections}
>
  <h1>Review and deploy</h1>
  <p>
    Deployment publishes configuration to <code>main</code>. It is complete only when the intended
    workers are running it.
  </p>
  <p>
    <a href="/docs/git/configuration">How Git versions configuration and workers follow main →</a>
  </p>
  <WorkflowDiagram kind="review" />
  <h2 id="open">Open for review</h2>
  <ol>
    <li>Save the branch and <a href="/build#test-heading">run its tests</a>.</li>
    <li>
      Select <strong>Open for review</strong> in the branch bar, check the title, description and test
      results it fills in, and open the change.
    </li>
    <li>
      If main has moved, bring the branch current and recheck the diff and tests before deployment.
    </li>
  </ol>
  <h2 id="check">Review the evidence</h2>
  <ul class="checklist">
    <li>The diff (the comparison with the deployed version) contains only intended edits.</li>
    <li>Preview output has the intended fields and types, and the tests pass.</li>
    <li>You have checked affected routes and destinations.</li>
    <li>Samples and changes contain no exposed credentials.</li>
    <li>
      Each affected pipeline and route has tests for the records it should keep, change, or drop.
      Add missing tests before deployment.
    </li>
    <li>The review includes shared configuration and other worker groups.</li>
    <li>Required secret versions and external credentials are available.</li>
    <li>You have identified a known-good configuration and recovery plan.</li>
  </ul>
  <h2 id="deploy">Deploy and verify</h2>
  <ol>
    <li>Deploy the reviewed change to main.</li>
    <li>
      Record the deployed main commit, then open Status and compare it with each intended worker's
      Commit.
    </li>
    <li>
      Investigate deployment failures and confirm destination output before calling the change
      complete.
    </li>
  </ol>
  <div class="table-scroll">
    <table>
      <thead><tr><th>Evidence</th><th>What it proves</th></tr></thead>
      <tbody>
        <tr><td>Commit on main</td><td>Configuration was published</td></tr>
        <tr
          ><td>Recent heartbeat</td><td
            >Worker recently contacted the leader; check the running commit separately</td
          ></tr
        >
        <tr
          ><td>Worker Commit matches deployed main</td><td
            >Worker reports running that configuration; also compare it with the commit you deployed</td
          ></tr
        >
        <tr
          ><td>Expected destination output</td><td>The checked data path behaves as intended</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <h2 id="rollback">Roll back</h2>
  <p>
    Use Branches → History → <strong>Roll back to here</strong>. Check secret versions and
    fleet-wide impact before confirming; verify worker adoption afterward.
  </p>
  <p>
    See <a href="/docs/operations#changes">the rollback procedure</a>. A configuration rollback is
    not a binary downgrade.
  </p>
</GuideLayout>
