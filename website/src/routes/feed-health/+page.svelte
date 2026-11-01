<script lang="ts">
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
</script>

<svelte:head
  ><title>Feed health — smeltr</title><meta
    name="description"
    content="Smeltr learns what each source normally sends and the fields each pipeline declares, and warns when either changes."
  /></svelte:head
>
<div class="marketing">
  <section class="page-hero">
    <p class="eyebrow">Feed health</p>
    <h1>Know when a feed changes.</h1>
    <p class="lede">
      A syslog relay stops. A scan floods a firewall. A vendor upgrade changes a field. Smeltr
      learns what each source normally sends and the fields each pipeline declares, and warns when
      either changes.
    </p>
    <div class="actions">
      <a class="btn large primary" href="/demo/try/" rel="external">Try the demo</a>
      <a class="btn large" href="/get">Get smeltr</a>
    </div>
  </section>
  <section aria-labelledby="volume-heading">
    <h2 id="volume-heading">Too quiet, or too loud.</h2>
    <p>
      Smeltr learns how many rows each source reads at each time of day, from its last seven days.
      In the demo, FortiGate's logs reach Smeltr through two syslog relays. One goes down, the
      source reads half of what it should, and within ten minutes Smeltr warns:
    </p>
    <ProductScreenshot
      source="feed-health-issues"
      height={1800}
      crop={{ x: 472, y: 758, width: 2360, height: 176 }}
      alt="The Issues page: Rows below normal, for the fortigate-firewalls source, which read 421,715 rows in 30 minutes where 736,846 to 968,182 is normal at this time of day."
      caption="The Issues page names the source, what it read and what is normal."
    />
    <ProductScreenshot
      source="feed-health-volume"
      height={1800}
      crop={{ x: 2150, y: 470, width: 690, height: 950 }}
      alt="The Map's panel for the fortigate-firewalls source: Rows below normal, input 246 rows a second, expected 409 to 538. Its 24-hour chart follows the expected band through the day, then falls to half."
      caption="Over the last 24 hours the source reads within its band, until it falls to half."
    />
    <p>
      Smeltr starts warning once a source has three days of history. You set how wide the range is,
      for every source, under Admin → System.
    </p>
  </section>
  <section aria-labelledby="drift-heading">
    <h2 id="drift-heading">A field stops matching.</h2>
    <p>
      Each pipeline declares the fields it reads and their types. Smeltr warns when a declared
      field's values stop reading as its type, or when no row has had it for an hour. In the demo,
      one FortiGate sends its protocol, <code>proto</code>, as a name where the pipeline declares a
      number:
    </p>
    <ProductScreenshot
      source="feed-health-issues"
      height={1800}
      crop={{ x: 472, y: 558, width: 2360, height: 176 }}
      alt="The Issues page: Field not its declared type, for the fortigate-traffic pipeline, whose field proto has failed to read as its declared type in the last hour."
      caption="The Issues page names the pipeline, the field and the step."
    />
    <p>It also warns when a source sends rows that no route takes.</p>
  </section>
  <section aria-labelledby="where-heading">
    <h2 id="where-heading">Sent where your team looks.</h2>
    <p>
      Smeltr shows each issue on the Status and Issues pages and on the Map. Under Admin → System →
      Alerts, send issues to a webhook in Generic JSON, Slack, Microsoft Teams or PagerDuty format,
      or by email, and choose which kinds of issue each channel gets. Prometheus can also scrape
      them from <code>/api/metrics</code>.
    </p>
    <ProductScreenshot
      source="feed-health-alerts"
      height={4486}
      crop={{ x: 472, y: 1152, width: 1664, height: 594 }}
      alt="Admin, System, Alerts: three channels. Network on-call is a PagerDuty webhook for the network group, #siem-pipelines a Slack webhook for every group, and SOC engineering an email to soc-eng@example.com."
      caption="PagerDuty pages the network on-call, Slack tells the pipeline team, and email reaches SOC engineering."
    />
    <p><a href="/docs/operations">Operate a fleet →</a></p>
  </section>
  <section class="next-step">
    <h2>Start with one source.</h2>
    <div class="actions">
      <a class="btn large primary" href="/get">Get smeltr</a><a
        class="btn large"
        href="/docs/getting-started">Set up a deployment</a
      >
    </div>
  </section>
</div>

<style>
</style>
