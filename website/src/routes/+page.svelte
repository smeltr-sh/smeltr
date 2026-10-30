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
    <p class="what">
      Smeltr runs on your own servers, between the systems that send logs and the places that store
      them, such as your SIEM. You build pipelines that read each source in its own format, drop
      what nobody searches, and map the rest to one schema.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a>
      <a class="btn large secondary" href="/demo/try/" rel="external">Try the demo</a>
    </div>
    <p class="free">
      Free for individuals and for organizations with fewer than 250 employees.
      <a href={source}>Source code on GitHub.</a>
    </p>
  </section>

  <section class="flow-section" id="where-it-fits" aria-labelledby="fit-heading">
    <div class="section-intro centered">
      <h2 id="fit-heading">Less noise. One schema.</h2>
    </div>
    <HomeDataFlow />
    <p class="connection-links">
      <a href="/siem-costs">Cut SIEM costs →</a>
      <a href="/one-schema">Put every source in one schema →</a>
    </p>
  </section>

  <section class="demo-section" id="pipeline" aria-labelledby="editor-heading">
    <div class="section-intro centered">
      <p class="eyebrow">How it works</p>
      <h2 id="editor-heading">See what each change does.</h2>
      <p>
        Capture a sample, edit your pipeline, and inspect the output. Save the input and expected
        result as a regression test.
      </p>
    </div>
    <EditorScreenshot focused />
    <p class="connection-links">
      <a href="/how-it-works">How it works →</a>
      <a href="/demo/try/" rel="external">Try the demo →</a>
    </p>
  </section>

  <section class="lifecycle-links detail-grid" aria-label="Why trust it">
    <article>
      <p class="eyebrow">Your senders</p>
      <h2>Fits in front of the SIEM you have.</h2>
      <p>Splunk forwarders, HEC senders, Filebeat and Fluent Bit change only where they send.</p>
      <a href="/siem-costs#fit-heading">Cut SIEM costs →</a>
    </article>
    <article>
      <p class="eyebrow">Supply chain</p>
      <h2>Check what you install.</h2>
      <p>
        Every release comes with its SBOM, a CVE report and a STIG report. The source code is
        public.
      </p>
      <a href="/docs/release-extras">Release extras →</a>
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
  .home-hero .what {
    margin: 1rem auto 0;
    max-width: 40rem;
    font-size: 1.1rem;
    line-height: 1.6;
    color: var(--dim);
    text-wrap: balance;
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
  }
</style>
