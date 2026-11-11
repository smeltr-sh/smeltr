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
      Smeltr learns how many rows each source reads at each time of day, from its past weeks. In the
      demo, FortiGate's logs reach Smeltr through a syslog relay. When the relay goes down, the
      source stops:
    </p>
    <ProductScreenshot
      source="feed-health-volume"
      height={1800}
      crop={{ x: 2150, y: 470, width: 690, height: 950 }}
      alt="The Map's panel for the fortigate-firewalls source: Rows below normal, input 0 rows a second, expected 228 to 746. Its 24-hour chart runs inside the expected band through the day, then twice drops to nothing."
      caption="Over the last 24 hours the source reads within its band, until the relay stops, twice."
    />
    <p>
      Within twenty minutes, Smeltr raises an issue. It shows on the Issues page and the Map, and
      goes to every alert channel that takes it.
    </p>
    <ProductScreenshot
      source="feed-health-issues"
      height={1800}
      crop={{ x: 472, y: 756, width: 2360, height: 178 }}
      phone={{
        source: 'feed-health-issues-phone',
        height: 2200,
        width: 1440,
        crop: { x: 472, y: 916, width: 920, height: 382 },
      }}
      alt="The Issues page: Rows below normal, for the fortigate-firewalls source, which read 0 rows in 5 minutes where 71,695 to 226,858 is normal at this time of day."
      caption="The Issues page names the source, what it read and what is normal."
    />
    <p>
      Smeltr starts warning once a source has three days of history. A source too sparse for its
      rows to be counted, such as an audit log that writes a few rows a day, is watched for four
      weeks instead, then warns once it goes half again as long without a row as its longest quiet
      spell of the past year.
    </p>
    <p>
      Each source's History tab lists its warnings and longest quiet spells of the past year. A
      warning, and a quiet spell during one, is not learned unless an editor marks it Normal. Any
      other quiet spell is learned unless an editor marks it Outage:
    </p>
    <ProductScreenshot
      source="feed-health-history"
      height={1800}
      crop={{ x: 2150, y: 470, width: 690, height: 1000 }}
      alt="The Map's panel for the fortigate-firewalls source, History tab. Warnings: Nov 11, 18:00, for 20 minutes, and Nov 11, 17:15, for 44 minutes. Longest quiet spells: Nov 11, 17:20, for 15 minutes, inside the older warning. Each row has a Normal and Outage switch, set to Outage."
      caption="A warning lasts until no width reads the source below normal, so it outlasts the quiet spell inside it. None of the three is learned."
    />
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
      crop={{ x: 472, y: 464, width: 2360, height: 176 }}
      phone={{
        source: 'feed-health-issues-phone',
        height: 2200,
        width: 1440,
        crop: { x: 472, y: 464, width: 920, height: 336 },
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
      height={5080}
      crop={{ x: 472, y: 1152, width: 1664, height: 578 }}
      alt="Admin, System, Alerts: three channels. Network on-call is a PagerDuty webhook for the network worker group, #siem-pipelines a Slack webhook for every worker group, and SOC engineering an email to soc-eng@example.com."
      caption="PagerDuty pages the network on-call, Slack tells the pipeline team, and email reaches SOC engineering."
    />
    <p>
      Each channel takes only the issues ticked for it. Here the network on-call is paged when a
      firewall feed drops. The pipeline team hears about it in Slack too.
    </p>
    <ProductScreenshot
      source="feed-health-alerts"
      height={5080}
      crop={{ x: 472, y: 1731, width: 1664, height: 1357 }}
      alt="What each channel gets: a row for each kind of issue, in sections, and a column for each channel. Network on-call takes Source unreachable, Destination unreachable and Rows below normal; #siem-pipelines takes Rows below normal, Rows above normal and Stream unrouted."
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
      alt="Editing the Network on-call channel: format PagerDuty, URL http://127.0.0.1:9099/pagerduty, the demo's receiver, routing key stored, worker groups only network, message included."
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
