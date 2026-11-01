<script lang="ts">
  import example from '#lib/one-schema-example.json';
</script>

<svelte:head
  ><title>One schema — smeltr</title><meta
    name="description"
    content="Map each vendor's logs to ECS, OCSF or your own schema, so one search covers every source."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">One schema</p>
    <h1>Every source in the same fields.</h1>
    <p class="lede">
      Each vendor writes its logs its own way. A pipeline you write for each source maps them to
      ECS, OCSF or your own schema on the way in, so one search covers every source.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/demo/try/" rel="external">Try the demo</a>
      <a class="btn large" href="/get">Get smeltr</a>
    </div>
  </section>
  <section aria-labelledby="two-heading">
    <h2 id="two-heading">Two firewalls, one shape.</h2>
    <p>A Palo Alto Networks firewall and a FortiGate each log a denied connection their own way.</p>
    <div class="reading raw">
      <p class="label">Palo Alto Networks, as sent</p>
      <pre><code>{example.panos}</code></pre>
      <p class="label">FortiGate, as sent</p>
      <pre><code>{example.fortigate}</code></pre>
    </div>
    <p>After Smeltr, both are ECS, and the same field means the same thing:</p>
    <article>
      <div class="table-scroll">
        <table>
          <thead><tr><th>ECS field</th><th>Palo Alto Networks</th><th>FortiGate</th></tr></thead>
          <tbody>
            {#each example.rows as row (row.field)}
              <tr><td><code>{row.field}</code></td><td>{row.panos}</td><td>{row.fortigate}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
    <p><a href="/how-it-works">How it works →</a></p>
  </section>
  <section class="next-step">
    <h2>Start with two sources.</h2>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a><a
        class="btn large"
        href="/docs/getting-started">Read the getting-started guide</a
      >
    </div>
  </section>
</div>

<style>
  .raw {
    max-width: none;
  }
  .label {
    margin: 1rem 0 0.4rem;
    font-weight: 600;
    color: var(--ink);
  }
  /* A log line is one long line; wrapping shows it whole without a scrollbar. */
  pre {
    margin: 0;
    white-space: pre-wrap;
    overflow-wrap: anywhere;
  }
</style>
