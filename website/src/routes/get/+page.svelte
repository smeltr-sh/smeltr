<script lang="ts">
  import { image, latestFile, license, source } from '#lib/release.ts';
</script>

<svelte:head
  ><title>Get smeltr</title><meta
    name="description"
    content="Smeltr is free under 250 employees. Above that, it is $10,000 a year per production leader."
  /></svelte:head
>

<div class="marketing">
  <section class="page-hero">
    <h1>Get smeltr</h1>
  </section>
  <article>
    <p>
      Smeltr is free for production use by organizations with fewer than 250 employees.
      Organizations with 250 or more employees pay $10,000 per year for each leader they run in
      production; workers are not counted. Evaluation, development, and testing are free for
      everyone.
    </p>
    <p>
      Every plan includes the same features. Email support is included with the paid license; the
      free plan has no support.
    </p>
    <h2 id="download">Download</h2>
    <div class="table-scroll">
      <table>
        <thead>
          <tr><th>Download</th><th>Install guide</th></tr>
        </thead>
        <tbody>
          <tr>
            <td><code class="command">docker pull {image}:latest</code></td>
            <td><a href="/docs/install-leader/container">Run the container image</a></td>
          </tr>
          <tr>
            <td><a href={latestFile('smeltr-linux.tar.gz')}>Linux x86-64</a></td>
            <td><a href="/docs/install-leader/binary">Install the binary</a></td>
          </tr>
          <tr>
            <td><a href={latestFile('smeltr-windows.zip')}>Windows x86-64</a></td>
            <td><a href="/docs/install-leader/binary">Install the binary</a></td>
          </tr>
          <tr>
            <td>
              <a href={latestFile('smeltr-extras.zip')}>Extras</a>: image archive, SBOM, STIG and
              CVE report
            </td>
            <td><a href="/docs/install-leader/container#archive">Run the container image</a></td>
          </tr>
          <tr>
            <td><a href={source}>Source code</a></td>
            <td></td>
          </tr>
        </tbody>
      </table>
    </div>
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
  /* A command is copied whole and a name is short, so a narrow screen scrolls the table
     rather than breaking its cells. */
  th,
  td {
    white-space: nowrap;
  }
  h2 {
    margin-top: 3rem;
  }
  article p {
    max-width: 43rem;
  }
</style>
