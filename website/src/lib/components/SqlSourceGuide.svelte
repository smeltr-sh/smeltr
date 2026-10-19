<!--
  An SQL source's page. The PostgreSQL and MySQL pages share the engine, so they share every section but what one
  database does its own way: what to set up, its column types, its own problems and links.
-->
<script lang="ts">
  import type { Snippet } from 'svelte';
  import { highlight } from '#lib/highlight/highlight.ts';
  import GuideLayout from './GuideLayout.svelte';
  import KindSettings from './KindSettings.svelte';
  import FieldsTable from './FieldsTable.svelte';
  import { SOURCES, kind } from '#lib/connector-reference.ts';

  let {
    type,
    name,
    title,
    description,
    example,
    statement,
    clock,
    lede,
    exampleNote,
    screenshot,
    beforeYouStart,
    types,
    troubleshooting,
    seeAlso,
  }: {
    /** The bundle type. */
    type: string;
    /** The database, as a reader names it. */
    name: string;
    title: string;
    description: string;
    example: unknown;
    /** What the example runs on a poll after its first row is sent. */
    statement: string;
    /** The database's clock, as the statement spells it. */
    clock: string;
    lede: Snippet;
    exampleNote: Snippet;
    screenshot?: Snippet;
    beforeYouStart: Snippet;
    types: Snippet;
    troubleshooting: Snippet;
    seeAlso: Snippet;
  } = $props();

  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'before-you-start', title: 'Before you start' },
    { id: 'settings', title: 'Settings' },
    { id: 'fields-it-gives', title: 'Fields it gives' },
    { id: 'modes', title: 'How it finds new rows' },
    { id: 'types', title: 'Column types' },
    { id: 'start-and-stop', title: 'Where it starts' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'verify', title: 'Verify' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
  const json = (value: unknown) => JSON.stringify(value, null, 2);
</script>

<GuideLayout {title} {description} {sections}>
  {@render lede()}

  <h2 id="example">Example</h2>
  {@render exampleNote()}
  <pre><code>{@html highlight(json(example))}</code></pre>
  {@render screenshot?.()}

  <h2 id="before-you-start">Before you start</h2>
  {@render beforeYouStart()}

  <h2 id="settings">Settings</h2>
  <p>
    The form on the Sources page asks for the connection first, then the query. Its
    <strong>Run</strong> button runs the query on a worker of the group and shows the rows it read,
    so you choose the mode and the fields from the real result. The source also takes the settings
    <a href="/docs/sources#settings">every source</a> takes.
  </p>
  <KindSettings group={SOURCES} {type} />

  <h2 id="fields-it-gives">Fields it gives</h2>
  <p>
    Each record is one row. It holds a field for each entry of <code>fields</code>, read from the
    result column of the same name and held as the type the entry declares. A column no entry names
    is not read.
  </p>
  <FieldsTable names={kind(SOURCES, type).fields ?? []} />

  <h2 id="modes">How it finds new rows</h2>
  <p>
    <code>mode</code> says which rows a poll sends. The form lists the modes cheapest first, and
    suggests one from the result of <strong>Run</strong>.
  </p>
  <dl>
    <dt><strong>Incrementing</strong></dt>
    <dd>
      Sends each row once. <code>position</code> names a field whose value only grows as rows are
      added, such as an id or the time a row was inserted. Each poll reads the rows past the last
      row sent, in the order of <code>position</code>. A row added later with a smaller value than
      one already sent is never read.
    </dd>
    <dt><strong>Updated at</strong></dt>
    <dd>
      Sends each row when it is added and again each time it changes. <code>position</code> starts with
      a datetime that every insert and every update sets. A deleted row sends nothing.
    </dd>
    <dt><strong>Comparing</strong></dt>
    <dd>
      Sends each row whose <code>key</code> is new, or whose fields changed since the last poll. Each
      poll reads the whole result, up to 1,000,000 rows, and the leader keeps a hash of each row. Use
      it for a table with no column that grows, such as a list of accounts. A deleted row sends nothing;
      a row whose key comes back is sent again.
    </dd>
    <dt><strong>Every poll</strong></dt>
    <dd>
      Sends the whole result on every poll, and remembers nothing. Use it for a small table read as
      a whole, such as current settings.
    </dd>
  </dl>
  <p>
    In <code>position</code>, the fields after the first tell apart rows that share a value in it,
    such as an id after a time. When many rows share one time, add such a field. Without one, a poll
    that fills <code>batch</code> leaves its last rows sharing one value to the next poll, which
    reads up to twice <code>batch</code>. Past that, the rest of the rows sharing the value are
    skipped, and the worker log says so.
  </p>
  <p>
    A position that starts with a datetime reads only rows at least <code>settleSecs</code> behind
    the database's clock. A transaction that commits after a later one is then still read, as long
    as it commits within <code>settleSecs</code>. Rows arrive that much later.
  </p>
  <p>
    <code>:last.&lt;field&gt;</code> in the query is the value that field held in the last row sent,
    and <code>NULL</code> before the first. Use it to filter where the statement's own test cannot
    reach, such as inside a <code>GROUP BY</code>. Keep rows with the last value (<code>&gt;=</code
    >, not <code>&gt;</code>), because rows sharing it may not all have been sent.
  </p>

  <h2 id="types">Column types</h2>
  {@render types()}

  <h2 id="start-and-stop">Where it starts</h2>
  <p>
    A source in Incrementing or Updated at with no progress begins where <code>start</code> says:
  </p>
  <ul>
    <li>
      <code>now</code>, the default: at the largest position the query returns on its first poll. It
      sends the rows that hold that value, then the rows added after them.
    </li>
    <li><code>beginning</code>: every row the query returns.</li>
    <li><code>from</code>: the rows whose position is at or past <code>startFrom</code>.</li>
  </ul>
  <p>
    Comparing sends every row on its first poll. After that, the leader keeps how far every
    destination has taken, and a restarted worker, or another worker that takes the source over,
    resumes there. It may send a row twice and never skips one. Changing the connection's host, port
    or database, the query, the mode, or the fields of <code>position</code> or <code>key</code>
    starts the source afresh from <code>start</code>. The other settings keep its progress. The
    source keeps reading until you remove it.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    One worker of the group holds the source. Each poll runs your query inside a statement of
    Smeltr's own, which adds the position test, the order and the batch limit. After the example's
    first row, each of its polls runs:
  </p>
  <pre><code>{statement}</code></pre>
  <p>
    The form shows the exact statement for your settings under <strong
      >what smeltr runs instead</strong
    >. The query is one <code>SELECT</code> or <code>WITH</code>, with no <code>ORDER BY</code>,
    <code>LIMIT</code> or placeholders of its own. The source runs the statement in a read-only
    transaction, with the session's time zone set to UTC, and reads a time without a zone as UTC.
    The settle test compares with <code>{clock}</code>, the database's own clock.
  </p>
  <p>
    A poll that fills <code>batch</code> reads the next at once. Otherwise the next poll starts
    <code>pollIntervalSecs</code> after this one. When <strong>Run</strong> checks a poll's plan and
    finds that each poll would read a whole table of more than about 100,000 rows, the form warns.
    Index the first field of <code>position</code>, or filter inside the query with
    <code>:last.&lt;field&gt;</code>.
  </p>
  <p>
    A poll's result may hold up to 256 MiB. The source has the database stop a statement after 290
    seconds, or sooner if the database's own limit is shorter, and drops a connection that has not
    answered in 300. After a failure, the source asks again after one second, then waits twice as
    long each time, up to five minutes. It opens a new connection after one breaks.
  </p>

  <h2 id="verify">Verify</h2>
  <ol>
    <li>
      In the form, choose <strong>Run</strong>. It shows the first rows the query returns, the
      record the pipeline sees for the first, and, once you choose a position, whether a poll reads
      through an index.
    </li>
    <li>
      Once the source is deployed, add a row the query returns. It reaches the destination within
      <code>pollIntervalSecs</code>, plus <code>settleSecs</code> for a datetime position. On the
      Sources page, the <strong>worker</strong> column names the worker that holds the source.
    </li>
  </ol>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <p>
    The form shows what the {name} server answered to <strong>Run</strong>, and the worker log shows
    it for a poll.
  </p>
  <dl>
    {@render troubleshooting()}
    <dt><strong>The result has no column <code>x</code>, which the record declares</strong></dt>
    <dd>
      An entry of <code>fields</code> names a column the query does not return. Rename the entry, or
      give the column that name with <code>AS</code> in the query.
    </dd>
    <dt><strong>The result has two columns named <code>x</code></strong></dt>
    <dd>Give one of them another name with <code>AS</code> in the query.</dd>
    <dt><strong>Each poll scans all of a table</strong></dt>
    <dd>
      The database reads the whole table on each poll to find the new rows. Index the first field of
      <code>position</code>, or filter inside the query with <code>:last.&lt;field&gt;</code>.
    </dd>
    <dt><strong>More rows share one position value than twice the batch</strong></dt>
    <dd>
      Add a field to <code>position</code> that tells those rows apart, such as an id, or raise
      <code>batch</code>.
    </dd>
    <dt><strong>Two rows have the key …</strong></dt>
    <dd>
      In Comparing, <code>key</code> must be unique in the result. Add fields to <code>key</code> until
      it is.
    </dd>
    <dt><strong>Every poll reads at most … rows</strong></dt>
    <dd>
      In Every poll, the result holds more rows than <code>batch</code>. Raise <code>batch</code>,
      up to 100,000, or choose another mode.
    </dd>
    <dt><strong>This poll's result holds more than 256 MiB</strong></dt>
    <dd>Lower <code>batch</code>, or read fewer or narrower fields.</dd>
    <dt><strong>The position's first column is of a type smeltr reads only as text</strong></dt>
    <dd>
      Cast the column in the query to a type in the table under
      <a href="#types">Column types</a>.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/sources">Sources</a></li>
    <li><a href="/docs/secrets">Credentials</a></li>
    {@render seeAlso()}
  </ul>
</GuideLayout>

<style>
  dd {
    margin: 0.35rem 0 1.25rem;
  }
</style>
