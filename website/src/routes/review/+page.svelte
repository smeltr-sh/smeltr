<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';

  import WorkflowDiagram from '#lib/components/WorkflowDiagram.svelte';
</script>

<svelte:head
  ><title>Review &amp; deploy — smeltr</title><meta
    name="description"
    content="Review pipeline diffs and tests, merge to main, and roll back through deployment history."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">Review &amp; deploy</p>
    <h1>Review each change before deployment.</h1>
    <p class="lede">
      Edit on a branch and inspect the diff and tests. Choose <strong>Approve &amp; deploy</strong>
      to merge into <code>main</code>, then confirm each intended worker runs the new commit.
    </p>
  </section>
  <section class="demo-section">
    <div class="section-intro">
      <p class="eyebrow">Prepare for review</p>
      <h2>See which changes are ready for review.</h2>
      <p>
        See the changed files, the pipelines and routes affected, and their test results. The review
        also highlights affected pipelines and routes without tests. Smeltr allows approval even
        when tests fail; resolve unexpected failures before deploying.
      </p>
    </div>
    <ProductScreenshot
      source="build-changes"
      height={1800}
      crop={{ x: 452, y: 288, width: 2400, height: 520 }}
      alt="Branches page with a pending firewall change, one passing test, and one untested item."
      caption="The Branches page flags untested work on each open change. Open a change to inspect its files, diff, and test results."
    />
  </section>
  <section aria-labelledby="deployment-path">
    <h2 id="deployment-path">A branch becomes a deployed version.</h2>
    <WorkflowDiagram kind="review" />
    <p>
      Drafts stay on a Git branch until merged into <code>main</code>. Workers fetch
      <code>main</code> and report the commit they run. Check Status to confirm the intended workers
      have loaded the deployed commit.
      <a href="/docs/git/configuration">See how Git controls configuration →</a>
    </p>
  </section>
  <section>
    <div><h2>A previous version is still within reach.</h2></div>
    <div>
      <p>
        Deployment history lets you deploy an earlier main commit again. The configuration and its
        versioned resources travel together.
      </p>
      <p>
        A worker can reject a configuration it cannot compile and keep its previous deployment.
        Check worker status to confirm what is actually running.
      </p>
    </div>
  </section>
  <section aria-labelledby="history-heading">
    <h2 id="history-heading">Know who wrote and deployed each change.</h2>
    <ProductScreenshot
      source="deployment-history"
      width={3200}
      height={1800}
      crop={{ x: 450, y: 286, width: 2730, height: 1000 }}
      alt="Deployment history listing commits, change descriptions, authors, deployers, and Roll back to here controls for previous versions."
      caption="Example history: each change records its author and deployer, with earlier versions available for rollback."
    />
  </section>
  <section class="next-step">
    <p><a href="/docs/review">Read the step-by-step review guide →</a></p>
    <h2>Follow the change into the fleet.</h2>
    <div class="actions">
      <a class="btn large primary" href="/operate">Operate the fleet →</a><a
        class="btn large"
        href="/docs">Read the documentation</a
      >
    </div>
  </section>
</div>
