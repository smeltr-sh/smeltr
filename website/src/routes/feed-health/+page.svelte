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
      phone={{
        source: 'feed-health-issues-phone',
        height: 2200,
        width: 1440,
        crop: { x: 472, y: 922, width: 920, height: 299 },
      }}
      alt="The Issues page: Rows below normal, for the fortigate-firewalls source, which read 564,427 rows in 30 minutes where 686,163 to 840,639 is normal at this time of day."
      caption="The Issues page names the source, what it read and what is normal."
    />
    <ProductScreenshot
      source="feed-health-volume"
      height={1800}
      crop={{ x: 2150, y: 470, width: 690, height: 950 }}
      alt="The Map's panel for the fortigate-firewalls source: Rows below normal, input 171 rows a second, expected 378 to 465. Its 24-hour chart follows the expected band through the day, then falls to half."
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
      phone={{
        source: 'feed-health-issues-phone',
        height: 2200,
        width: 1440,
        crop: { x: 472, y: 572, width: 920, height: 318 },
      }}
      alt="The Issues page: Field not its declared type, for the fortigate-traffic pipeline, whose field proto has failed to read as its declared type in the last hour."
      caption="The Issues page names the pipeline, the field and the step."
    />
    <p>It also warns when a source sends rows that no route takes.</p>
  </section>
  <section aria-labelledby="where-heading">
    <h2 id="where-heading">Sent where your team looks.</h2>
    <p>
      Every issue shows on the Status and Issues pages and on the Map. For the hours nobody is
      looking, Admin → System → Alerts sends issues to the teams you choose.
    </p>
    <ProductScreenshot
      source="feed-health-alerts"
      height={4506}
      crop={{ x: 472, y: 1152, width: 1664, height: 594 }}
      alt="Admin, System, Alerts: three channels. Network on-call is a PagerDuty webhook for the network worker group, #siem-pipelines a Slack webhook for every worker group, and SOC engineering an email to soc-eng@example.com."
      caption="PagerDuty pages the network on-call, Slack tells the pipeline team, and email reaches SOC engineering."
    />
    <p>
      Each channel takes only the issues ticked for it. Here the network on-call is paged when a
      firewall feed drops. The pipeline team hears about it in Slack too.
    </p>
    <ProductScreenshot
      source="feed-health-alerts"
      height={4506}
      crop={{ x: 472, y: 1766, width: 1664, height: 914 }}
      alt="What each channel gets: a row for each kind of issue and a column for each channel. Network on-call takes Source unreachable, Destination unreachable and Rows below normal; #siem-pipelines takes Rows below normal and Rows above normal."
      caption="A row for each kind of issue, a column for each channel."
    />
    <p>
      A channel is a webhook, in PagerDuty, Slack, Microsoft Teams or generic JSON format, or an
      email through your mail server. Prometheus can also scrape the same issues from <code
        >/api/metrics</code
      >.
    </p>
    <ProductScreenshot
      source="feed-health-alert-channel"
      height={1800}
      crop={{ x: 992, y: 390, width: 896, height: 1020 }}
      alt="Editing the Network on-call channel: format PagerDuty, URL https://events.pagerduty.com/v2/enqueue, routing key stored, worker groups only network, message included."
      caption="The network on-call channel pages through PagerDuty, for the network worker group only."
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
