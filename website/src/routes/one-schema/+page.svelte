<script lang="ts">
  import example from '#lib/one-schema-example.json';
  // The demo's two firewall pipelines, by what each step does.
  const steps: [string, string, string, string][] = [
    [
      'Parser',
      "Reads the vendor's format into fields",
      'Syslog header, then 54 CSV columns',
      'key=value',
    ],
    ['Rename', 'Moves each vendor field to its ECS field', '17 fields', '20 fields'],
    [
      'Eval',
      'Works out the ECS fields the vendor does not write, such as <code>event.outcome</code>',
      '16 fields',
      '12 fields',
    ],
    ['Drop', 'Removes records you do not keep', 'None', 'NTP traffic'],
    ['Lookup', 'Adds context from your own tables', 'Site, user, app risk', 'Site'],
    ['Eval', 'Removes the vendor fields left over', '15 fields', '21 fields'],
    [
      'Call',
      'Runs a pipeline both share, for the fields every source gets',
      '<code>ecs.version</code>, <code>event.ingested</code>',
      '<code>ecs.version</code>, <code>event.ingested</code>',
    ],
  ];
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
  </section>
  <section aria-labelledby="how-heading">
    <h2 id="how-heading">How it gets there.</h2>
    <p>
      Each format gets its own pipeline, and you write it. These are the demo's two pipelines, step
      by step:
    </p>
    <article>
      <div class="table-scroll">
        <table>
          <thead>
            <tr><th>Step</th><th>What it does</th><th>Palo Alto Networks</th><th>FortiGate</th></tr>
          </thead>
          <tbody>
            {#each steps as [step, does, panos, fortigate], i (i)}
              <tr>
                <td>{step}</td><td>{@html does}</td>
                <td>{@html panos}</td><td>{@html fortigate}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </article>
    <p>
      You build each one against records captured from that source, see every step's output, and
      save the result as a test. Import ECS, an OCSF class or a CSV of fields, or give your own as
      JSON Schema.
    </p>
  </section>
  <section aria-labelledby="cost-heading">
    <h2 id="cost-heading">What it costs.</h2>
    <ul>
      <li>
        Smeltr comes with no ready-made mappings. Someone who knows the vendor's fields and the
        schema writes each pipeline, and changes it when the vendor changes its format.
      </li>
      <li>
        Records can grow. In the demo, the ECS records, as compact JSON, are 1.2 times the size of
        the FortiGate lines they come from, and 1.9 times the Palo Alto Networks lines, which also
        gain looked-up fields. Dropping records and fields, as on
        <a href="/siem-costs">Cut SIEM costs</a>, is what makes the volume smaller.
      </li>
      <li>
        Smeltr shows where a pipeline's output does not match your schema, in the editor and in
        review, but it does not stop those records.
      </li>
    </ul>
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
