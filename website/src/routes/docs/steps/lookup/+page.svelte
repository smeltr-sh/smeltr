<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/lookup.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('lookup', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'troubleshooting', title: 'Troubleshooting' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Lookup step"
  description="Enrich records from a lookup table with a Lookup step: exact, CIDR and regex matches, multiplicity, outputs with types and miss defaults."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Lookup step</h1>
  <p class="lede">
    A Lookup step matches one field of each record against a lookup table and writes values of the
    matching entry into the record as new fields.
  </p>
  <p>
    A <a href="#lookup-tables">lookup table</a> is a table of entries shared by id: a CSV file, an inline
    table typed into the editor, or an MMDB file of IP networks. A record whose input matches no entry
    is a miss, and each output takes its default on miss instead.
  </p>

  <h2 id="example">Example</h2>
  <StepExample example={examples.exact} />
  <p>
    The step looks up each <code>user</code> in the inline table <code>users</code> and adds the
    user's <code>team</code> and <code>user_level</code>.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'lookup').settings} />
  <p>
    An output's <code>type</code> is <code>str</code>, <code>i64</code>, <code>f64</code>,
    <code>bool</code> or <code>datetime</code>. A <code>datetime</code> reads an epoch number or a
    common date text by itself. Choose <strong>auto</strong> in the editor to give a
    <code>parseTime</code> format instead. A blank <code>default</code> means null.
  </p>
  <p>
    In the editor, <code>multiplicity</code> is <strong>match effort</strong> and
    <code>keyColumn</code> is <strong>key / match column</strong>. For a CSV or inline table,
    <code>outputs</code> shows as <strong>enrichment columns</strong>: one row per table column,
    with the output field it writes, its type, and its default on miss. The step leaves out a column
    typed <em>skip</em>, and every column starts as <em>skip</em> until you choose otherwise. The
    <code>keyColumn</code>
    comes first, marked <em>key</em>, and takes no default on miss. When the editor does not know
    the table's columns, an <strong>outputs</strong> list stands in for the enrichment columns: each row
    maps a column name to an output field, with a type and a default on miss.
  </p>

  <h2 id="how-it-works">How it works</h2>

  <h3 id="lookup-tables">Lookup tables</h3>
  <p>
    A lookup table is shared by id: any Lookup step in any pipeline can name it. Manage tables under
    <strong>Build</strong>, <strong>Resources</strong>, <strong>Lookup tables</strong>. A worker
    group also sees the tables of <code>shared/</code>. A table is one of three types:
  </p>
  <div class="table-scroll">
    <table>
      <thead><tr><th scope="col">Type</th><th scope="col">Holds</th></tr></thead>
      <tbody>
        <tr
          ><td><strong>CSV file</strong></td><td
            >A CSV file whose first row names the columns, with its own delimiter.</td
          ></tr
        >
        <tr
          ><td><strong>Built by hand</strong></td><td
            >An inline table: columns and rows you type into the page.</td
          ></tr
        >
        <tr
          ><td><strong>MaxMind DB (MMDB) file</strong></td><td
            >A MaxMind database of IP networks, such as a GeoIP file.</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    An upload, of up to 2 GiB, goes to the leader's lookup store, and the commit holds a pointer to
    it. Each worker fetches the files its deployment names and checks them against the pointer. A
    worker maps a file rather than copying it, and builds a CSV table's index once. Every table
    loads once, when the pipeline is compiled.
  </p>

  <h3 id="columns">CSV and inline tables</h3>
  <p>
    The step matches a CSV or inline table on its <code>keyColumn</code>. The step reads the
    <code>input</code> field, compares it with that column in every row, and writes each output's
    <code>source</code> column of the matching rows to its <code>field</code>. The record's other
    fields remain.
  </p>

  <h3 id="exact">Match a value exactly</h3>
  <p>
    An <code>exact</code> match finds the rows whose <code>keyColumn</code> equals the input. In the
    <a href="#example">example</a>, the JSON parser before the lookup declares <code>user</code> as
    a
    <code>str</code>. <code>carol</code> is not in the table, so <code>team</code> takes its default
    on miss, <code>unknown</code>. <code>user_level</code> has a blank default, so it is null and
    absent from JSON output. <code>user_level</code> is typed <code>i64</code>, so <code>3</code> is a
    number.
  </p>

  <h3 id="cidr">Match an IP address to a network</h3>
  <p>
    A <code>cidr</code> match finds the rows whose <code>keyColumn</code> holds a network that contains
    the input address. IPv4 and IPv6 both work. An input that is not an IP address is a miss.
  </p>
  <StepExample example={examples.cidr} />
  <p>
    <code>10.1.2.3</code> is in both networks. <code>best</code> keeps the longest prefix,
    <code>10.1.0.0/16</code>.
  </p>

  <h3 id="regex">Match a pattern</h3>
  <p>
    A <code>regex</code> match finds the rows whose <code>keyColumn</code> is a regular expression
    that matches somewhere in the input. Anchor it with <code>^</code> and <code>$</code> to match the
    whole value.
  </p>
  <StepExample example={examples.regex} />
  <p>
    <code>db-03-replica</code> matches both <code>^db-</code> and <code>^db-.*-replica$</code>.
    <code>best</code> keeps the pattern with the longest text.
  </p>

  <h3 id="multiplicity">Several matching rows</h3>
  <p>
    <code>multiplicity</code> chooses which of the matching rows the step writes the outputs from, when
    more than one row matches.
  </p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col"><code>multiplicity</code></th><th scope="col">Uses</th><th scope="col"
            >Output</th
          ></tr
        ></thead
      >
      <tbody>
        <tr
          ><td><code>best</code></td><td
            ><code>cidr</code>: the longest prefix. <code>regex</code>: the longest pattern text.
            <code>exact</code>: the first matching row. A tie keeps table order.</td
          ><td>one value</td></tr
        >
        <tr
          ><td><code>first</code></td><td>The first matching row in table order.</td><td
            >one value</td
          ></tr
        >
        <tr
          ><td><code>all</code></td><td>Every matching row, in table order.</td><td>an array</td
          ></tr
        >
      </tbody>
    </table>
  </div>
  <p>
    With <code>all</code>, each output field is an array of its type. A miss gives an empty array.
    With a default on miss, it gives an array holding that one default.
  </p>
  <StepExample example={examples.cidrAll} />

  <h3 id="mmdb">MMDB tables</h3>
  <p>
    An MMDB table is a MaxMind database: a file of IP networks, each carrying a record of values,
    such as a GeoIP file. The step matches the input address against the networks in the file and
    reads its outputs from the record of the most specific network that contains it. Its
    <code>matchKind</code> is always <code>cidr</code>, and it finds at most one record, so it has
    no
    <code>keyColumn</code> and no <code>multiplicity</code>. An input that is not an IP address is a
    miss.
  </p>
  <p>
    Each output's <code>source</code> is a dotted record path, such as
    <code>country.iso_code</code>. A value that does not read as its output's type is null on its
    record, with a warning; Smeltr still delivers the record.
  </p>

  <h3 id="filter">Process selected records</h3>
  <p>
    When the filter is false or null, the record continues with its existing values, and the output
    fields keep what they held.
  </p>

  <h3 id="edges">Misses and types</h3>
  <ul>
    <li>
      A miss adds no errors or warnings: each output takes its default on miss, or null when the
      default is blank.
    </li>
    <li>
      The step compares an input of type <code>str</code>, <code>i64</code>,
      <code>f64</code> or <code>bool</code> as its text, and <code>bytes</code> as its base64, so a
      status code <code>404</code> matches the key
      <code>404</code>. A null or missing input is a miss.
    </li>
  </ul>

  <h2 id="troubleshooting">Troubleshooting</h2>
  <dl>
    <dt><strong>Saving fails because the input field has no type</strong></dt>
    <dd>
      The <code>input</code> field needs a type. A field a JSON or Key/value parser did not declare
      has none; declare it in the parser, or <a href="/docs/code-reference#types">convert</a> it in an
      earlier step.
    </dd>
    <dt><strong>The pipeline fails to compile before any worker runs</strong></dt>
    <dd>
      Check the table: a missing file, a malformed CIDR block or regex in the <code>keyColumn</code
      >, or a row with the wrong number of cells. Check that each cell of a CSV or inline table, and
      each default on miss, reads as its output's type. Check that the <code>keyColumn</code> and
      every output's <code>source</code> are columns of the table.
    </dd>
  </dl>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/code-reference#types">Language reference: types and conversion</a></li>
  </ul>
</GuideLayout>
