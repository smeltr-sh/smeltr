<script lang="ts">
  import EditorScreenshot from '#lib/components/EditorScreenshot.svelte';
  import Logo from '#lib/components/Logo.svelte';
  import HomeDataFlow from '#lib/components/HomeDataFlow.svelte';
  import { source } from '#lib/release.ts';
</script>

<svelte:head>
  <title>smeltr — fast, self-hosted telemetry pipelines</title>
  <meta
    name="description"
    content="Fast, self-hosted telemetry pipelines. Built visually. Managed with Git. Filter, parse, enrich, and route logs and telemetry; capture samples and test changes before deployment."
  />
</svelte:head>

<div class="marketing">
  <section class="page-hero home-hero" aria-labelledby="smeltr-heading">
    <div class="wordmark">
      <span class="hero-mark"><Logo size={112} /></span>
      <h1 id="smeltr-heading">smeltr</h1>
    </div>
    <p class="tagline">
      Fast, self-hosted telemetry pipelines.<br /><span>Built visually. Managed with Git.</span>
    </p>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a>
      <a class="btn large secondary" href="/demo/try/" rel="external">Try Demo</a>
    </div>
    <p class="free">
      Free for individuals and for organizations with fewer than 250 employees.
      <a href={source}>Source code on GitHub.</a>
    </p>
  </section>

  <section class="flow-section" id="where-it-fits" aria-labelledby="fit-heading">
    <div class="section-intro centered">
      <h2 id="fit-heading">Keep the data that matters.</h2>
      <p>Filter, shape, and route logs and telemetry between your sources and destinations.</p>
    </div>
    <HomeDataFlow />
    <p class="connection-links">
      <a href="/docs/sources">Explore sources</a>
      <a href="/docs/destinations">Explore destinations</a>
    </p>
  </section>

  <section class="demo-section" id="pipeline" aria-labelledby="editor-heading">
    <div class="section-intro centered">
      <p class="eyebrow">Build with real data</p>
      <h2 id="editor-heading">See what each change does.</h2>
      <p>
        Capture a sample, edit your pipeline, and inspect the output. Save the input and expected
        result as a regression test.
      </p>
    </div>
    <EditorScreenshot focused />
    <p class="connection-links">
      <a href="/demo/try/" rel="external">Try smeltr in your browser →</a>
    </p>
    <ol class="workflow" aria-label="From a sample to deployment">
      <li><span>01</span><a href="/docs/capture-sample">Capture real data</a></li>
      <li><span>02</span><a href="/build">Preview changes</a></li>
      <li><span>03</span><a href="/build#test-heading">Save tests</a></li>
      <li><span>04</span><a href="/docs/review">Review and merge</a></li>
    </ol>
  </section>

  <section class="lifecycle-links" aria-label="Why Smeltr">
    <article>
      <p class="eyebrow">Performance</p>
      <h2>Make compute go further.</h2>
      <p>
        Self-hosted processing designed for efficiency. Filter unwanted data before it reaches
        downstream systems, and run workers near your sources.
      </p>
      <a href="/docs/getting-started">Choose your deployment →</a>
    </article>
    <article>
      <p class="eyebrow">Change management</p>
      <h2>Git is the workflow.</h2>
      <p>
        Develop on a branch. Commit your changes, review the diff and test results, then merge to
        main to deploy. Workers follow main.
      </p>
      <a href="/docs/git/configuration">Explore the Git workflow →</a>
    </article>
    <article>
      <p class="eyebrow">Operations</p>
      <h2>Know what is running.</h2>
      <p>
        Trace data flow, investigate warnings, and see which commit each worker runs. Verify the
        result at the destination.
      </p>
      <a href="/operate">Operate the fleet →</a>
    </article>
  </section>
</div>

<style>
  .marketing .home-hero {
    text-align: center;
    padding-top: clamp(2rem, 5vw, 4rem);
    padding-bottom: clamp(2.5rem, 5vw, 4rem);
  }
  .wordmark {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 0.5rem;
  }
  .hero-mark {
    display: flex;
  }
  .home-hero h1 {
    margin: 0;
    max-width: none;
    font-size: clamp(4.5rem, 10vw, 8rem);
    line-height: 1.05;
    letter-spacing: -0.04em;
  }
  .home-hero .tagline {
    margin: 1.25rem auto 0;
    max-width: 36rem;
    font-size: clamp(1.15rem, 2.3vw, 1.65rem);
    line-height: 1.55;
    color: var(--ink);
    text-wrap: balance;
  }
  .tagline span {
    color: var(--dim);
  }
  .home-hero .actions {
    justify-content: center;
    margin-top: 1.6rem;
  }
  .home-hero .free {
    margin: 1rem auto 0;
    text-wrap: balance;
    color: var(--dim);
    font-size: 0.95rem;
  }
  .marketing .flow-section {
    border-top: 1px solid var(--border);
    padding-top: 2.75rem;
  }
  .centered {
    text-align: center;
  }
  .centered h2,
  .centered p {
    margin-inline: auto;
  }
  .centered h2 {
    max-width: 32ch;
  }
  .centered p {
    max-width: 42rem;
  }
  .marketing .connection-links {
    display: flex;
    flex-wrap: wrap;
    justify-content: center;
    align-items: center;
    gap: 0.5rem 1.5rem;
    margin: 1.25rem auto 0;
    text-align: center;
    font-size: 0.88rem;
  }
  .connection-links a {
    display: inline-flex;
    align-items: center;
    min-height: 44px;
    white-space: nowrap;
    text-underline-offset: 0.2em;
  }
  .workflow {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    list-style: none;
    padding: 0;
    margin: 2.5rem 0 0;
    border-block: 1px solid var(--border);
  }
  .workflow li {
    padding: 1.2rem 0.6rem;
    display: flex;
    gap: 0.65rem;
    align-items: baseline;
    justify-content: center;
  }
  .workflow span {
    font: 0.75rem var(--mono);
    color: var(--dim);
  }
  .workflow a {
    color: var(--ink);
    font-size: 0.85rem;
    text-underline-offset: 0.2em;
  }
  @media (max-width: 48rem) {
    .workflow {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .workflow li {
      justify-content: flex-start;
    }
  }
  @media (max-width: 30rem) {
    .hero-mark :global(svg) {
      width: 64px;
      height: 80px;
    }
    .wordmark {
      gap: 0;
    }
    .home-hero .actions {
      flex-direction: column;
      align-items: stretch;
    }
    .workflow {
      grid-template-columns: 1fr;
    }
  }
</style>
