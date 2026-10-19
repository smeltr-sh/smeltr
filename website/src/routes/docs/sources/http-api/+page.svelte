<script lang="ts">
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import ProductScreenshot from '#lib/components/ProductScreenshot.svelte';
  import KindSettings from '#lib/components/KindSettings.svelte';
  import FieldsTable from '#lib/components/FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';
  import examples from '#lib/connector-examples.json';
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'start-and-stop', title: 'Where it starts' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout
  title="HTTP API source"
  description="Poll an address that answers a GET, or a POST of a fixed JSON body, with JSON or lines of text: an example, every setting, the fields it gives, and the errors it reports."
  {sections}
>
  <h1>HTTP API</h1>
  <p class="lede">
    Asks one address with a GET, or a POST of a fixed JSON body, every poll interval and reads what
    it answers as records.
  </p>
  <p>
    Many services list their events or alerts at an address that answers a GET with JSON or lines of
    text, and some, such as Starlink's telemetry stream, answer a POST of what to send. This source
    polls such an address. A service that needs more than one request and its pages, such as Okta's
    System Log, has a source of its own.
  </p>

  <h2 id="example">Example</h2>
  <p>
    Asks <code>api.example.com</code> for its events every minute. The source sends the
    <code>Authorization</code> header from the worker-host variable <code>API_TOKEN</code>, reads
    each element of the array at <code>/data</code> as one record, and follows the API's next links.
    It asks only for what happened since its last poll, through the <code>since</code> query parameter.
  </p>
  <pre><code>{@html highlight(json(examples.httpApiSource))}</code></pre>
  <ProductScreenshot
    source="http-api-source-form"
    height={2300}
    crop={{ x: 608, y: 419, width: 1664, height: 1462 }}
    alt="The HTTP API form on the Sources page: id events, the URL, records array at /data, pages follow next links, since parameter since."
    caption="The same source in the Sources page's add dialog, before you add its credential."
  />

  <h2 id="before-you-start">Before you start</h2>
  <ul>
    <li>
      Find the address that lists what you want, and check that a GET to it, or a POST of the body
      it asks for, answers with JSON or lines of text. If the answer is a JSON object, note the
      <a href="https://www.rfc-editor.org/rfc/rfc6901">JSON pointer</a> to the array that holds the
      records, such as <code>/data</code>.
    </li>
    <li>
      Find out how the API pages. This source follows a
      <code>Link: rel="next"</code> header. An API that pages another way gives one page per poll.
    </li>
    <li>
      Find the query parameter that asks for what happened since a time, if the API has one. Without
      it, every poll reads everything again.
    </li>
    <li>
      Put each secret header's whole value, such as <code>Bearer &lt;token&gt;</code>, in a
      <a href="/docs/secrets">credential</a>.
    </li>
    <li>Allow workers to reach the address.</li>
  </ul>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/sources#settings">every source</a> takes.</p>
  <KindSettings group={SOURCES} type="httpApi" />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each record holds its text exactly as the API wrote it. <code>__source.url</code> holds the address
    of the page the source read it from.
  </p>
  <FieldsTable names={kind(SOURCES, 'httpApi').fields ?? []} />

  <h2 id="start-and-stop">Where it starts</h2>
  <p>
    Without <code>sinceParam</code>, every poll asks for everything the API lists, so the source
    delivers the same records again on every poll. Leave the <code>sinceParam</code> parameter out
    of <code>url</code>; the source adds it whenever a poll asks from a time. A <code>start</code>
    of
    <code>new</code>
    needs <code>sinceParam</code>.
  </p>
  <p>
    With <code>sinceParam</code>, the first poll asks for everything, or with <code>start</code> set
    to <code>new</code>, for what happens from the first poll onward. Every later poll asks from the
    start of the last poll every destination took, less <code>sinceOverlapSecs</code>. A restarted
    worker resumes there, so it skips nothing. An API that records events late needs an overlap, and
    the events in the overlap arrive twice.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    One worker of the group holds the source, and the leader keeps how far it got. Each poll sends a
    GET, or a POST of <code>body</code>, to <code>url</code> with the committed headers and the
    credential headers. With
    <code>followLinks</code>, the source follows each answer's next link within the same poll, but
    only to the same scheme, host and port, because the credentials go with every request. A page
    that holds no records, or a link back to a page the poll already asked, ends the poll.
  </p>
  <p>
    The source holds each answer whole, up to 256 MiB, and asks for the next page only once every
    record of the last one is handed on. It follows no redirects. Each request has 120 seconds to
    connect and read its whole answer. A request that runs longer, or an answer larger than 256 MiB,
    fails.
  </p>
  <p>
    When the API answers 429 or 503, the source waits as long as its
    <code>Retry-After</code> asks, up to an hour. After any other failure it asks the same page again,
    waiting one second at first and up to five minutes.
  </p>

  <h2 id="verify">Verify</h2>
  <p>
    Find a record you know the API lists at the destination within a poll interval. On the Sources
    page, the <strong>worker</strong> column names the worker that holds the source.
  </p>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <p>The worker log shows what the API answered.</p>
  <dl>
    <dt><strong>Refused the credentials (401 or 403)</strong></dt>
    <dd>
      Check that the credential holds the header's whole value, such as
      <code>Bearer &lt;token&gt;</code>, and that the token may read the address.
    </dd>
    <dt><strong>Answered 301 or 302, give the address it leads to</strong></dt>
    <dd>Set <code>url</code> to the address the log names.</dd>
    <dt><strong>The same records arrive on every poll</strong></dt>
    <dd>
      Set <code>sinceParam</code> to the API's query parameter for a start time, or accept that each poll
      reads everything.
    </dd>
    <dt><strong>Nothing arrives</strong></dt>
    <dd>
      Check that <code>records</code> matches the answer, and with <code>array</code> that
      <code>recordsAt</code> points at the array. An answer with nothing at the pointer holds no records.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/sources/okta-logs">Okta System Log source</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
