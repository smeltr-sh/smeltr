<script lang="ts">
  import { image, latestFile, license, source } from '#lib/release.ts';
  import { latest } from '#lib/releases.ts';

  const pull = `docker pull ${image}:latest`;
  let copied = $state(false);
  async function copy() {
    await navigator.clipboard.writeText(pull);
    copied = true;
    setTimeout(() => (copied = false), 1500);
  }
</script>

<svelte:head
  ><title>Get smeltr</title><meta
    name="description"
    content="Smeltr is free for individuals and for organizations with fewer than 250 employees. Above that, it is $10,000 a year for each production leader, the server that manages your workers."
  /></svelte:head
>

<div class="marketing">
  <section class="page-hero">
    <h1>Get smeltr</h1>
  </section>
  {#if latest}
    <section class="release" id="download" aria-labelledby="release-heading">
      <div class="files">
        <p class="eyebrow">Latest release</p>
        <h2 id="release-heading">Smeltr {latest.version}</h2>
        <p class="date">{latest.date}</p>
        <p>{latest.summary}</p>
        <ul class="downloads">
          <li>
            <span class="command-row">
              <code class="command">{pull}</code>
              <button class="compact" onclick={copy}>{copied ? 'Copied' : 'Copy'}</button>
            </span>
            <a href="/docs/install-leader/container">Run the container image</a>
          </li>
          <li>
            <a href={latestFile('smeltr-linux.tar.gz')}>Linux x86-64</a>
            <a href="/docs/install-leader/binary">Install the binary</a>
          </li>
          <li>
            <a href={latestFile('smeltr-windows.zip')}>Windows x86-64</a>
            <a href="/docs/install-leader/binary">Install the binary</a>
          </li>
          <li>
            <span
              ><a href={latestFile('smeltr-extras.zip')}>Extras</a>: image archive, SBOM, CVE report
              and STIG report</span
            >
            <a href="/docs/release-extras">Release extras</a>
          </li>
          <li><a href={source}>Source code</a></li>
        </ul>
      </div>
      <aside>
        <h3>Highlights</h3>
        <ul>
          {#each latest.highlights ?? [] as h (h.title)}
            <li><a href={h.href}>{h.title}</a></li>
          {/each}
        </ul>
        <p class="more">
          <a href="/releases/{latest.version}">What's in {latest.version} →</a><br />
          <a href="/releases">What changed in each release →</a>
        </p>
      </aside>
    </section>
  {/if}
  <article>
    <h2 id="pricing">Pricing</h2>
    <p>
      Smeltr is free for production use by individuals, and by organizations with fewer than 250
      employees. Organizations with 250 or more employees pay $10,000 per year for each server they
      run as a leader in production, however many people use it. Evaluation, development, and
      testing are free for everyone.
    </p>
    <p>
      A leader is the Smeltr server you sign in to, where you build and deploy your configuration.
      Workers run it and move your data. One leader manages any number of workers, and only leaders
      are counted. <a href="/docs/getting-started#overview">How Smeltr fits together →</a>
    </p>
    <p>
      Every plan includes the same features. Email support is included with the paid license; the
      free plan has no support.
    </p>
    <p>
      To purchase a production license, email
      <a href="mailto:support@smeltr.sh">support@smeltr.sh</a>.
    </p>
    <h2 id="license">License details</h2>
    <p>
      Employees are counted as your organization already reports them, together with those of any
      parent company and of everything it controls. Contractors are not counted.
    </p>
    <p>
      An organization that grows to 250 employees or more keeps the free license for 90 days from
      then.
    </p>
    <p>
      There is no license key. Your paid invoice is your proof of license for one year from the date
      of purchase, and nothing in Smeltr needs activating.
    </p>
    <p>
      The free license does not cover offering Smeltr to others as a hosted or managed service. For
      that, email <a href="mailto:support@smeltr.sh">support@smeltr.sh</a>.
    </p>
    <p>
      Smeltr is licensed under the Business Source License 1.1. Each release becomes Apache 2.0 four
      years after it ships. The <a href={source}>source code</a> is public, and
      <a href={license}>the license</a> has the full terms.
    </p>
  </article>
</div>

<style>
  .marketing .page-hero {
    padding-bottom: 1.5rem;
  }
  .marketing .release {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 20rem;
    gap: 1.25rem;
    align-items: start;
    padding-block: 0 1rem;
  }
  .files,
  aside {
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    padding: 1.6rem;
  }
  .files h2 {
    margin: 0;
  }
  .date {
    margin: 0.2rem 0 1rem;
    color: var(--dim);
    font-size: 0.92rem;
  }
  .command-row {
    display: flex;
    gap: 0.5rem;
    align-items: center;
    flex: 1;
    max-width: 34rem;
  }
  .command-row code {
    flex: 1;
    min-width: 0;
    overflow-x: auto;
    white-space: nowrap;
    background: var(--field);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    padding: 0.55rem 0.75rem;
  }
  .downloads {
    list-style: none;
    padding: 0;
    margin: 0;
  }
  .downloads li {
    display: flex;
    justify-content: space-between;
    gap: 1rem;
    align-items: center;
    padding: 0.8rem 0;
    border-bottom: 1px solid var(--border);
  }
  .downloads li:last-child {
    border-bottom: 0;
  }
  aside h3 {
    margin: 0 0 0.75rem;
    font-size: 1.05rem;
  }
  aside ul {
    padding-left: 1.1rem;
    margin: 0;
  }
  aside li {
    margin: 0.45rem 0;
    font-size: 0.92rem;
  }
  aside .more {
    margin: 1.2rem 0 0;
    font-size: 0.92rem;
    line-height: 1.9;
  }
  @media (max-width: 46rem) {
    .marketing .release {
      grid-template-columns: 1fr;
    }
  }
  h2 {
    margin-top: 3rem;
  }
  article p {
    max-width: 43rem;
  }
</style>
