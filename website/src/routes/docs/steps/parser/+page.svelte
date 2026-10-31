<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import examplesText from '#lib/steps/parser.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  import grokPatterns from '#lib/grok-patterns.json';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS, PARSER_FORMATS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('parser', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Parser step"
  description="Turn text into fields with a Parser step: JSON, CSV, Key/value and Regex/Grok formats, declared fields and types, and unparsable records."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Parser step</h1>
  <p class="lede">
    A Parser step reads one text field of each record and writes the fields it finds there into the
    record.
  </p>

  <p>
    The input is usually <code>_raw</code>, the text a source received. The declared fields are the
    ones later steps calculate with, each with its type. <strong>Suggest fields</strong> proposes
    declared fields from the sample and production. In each example, the input is the raw text of
    each record and the output is the parsed record, with <code>_raw</code> left out.
  </p>

  <h2 id="example">Example</h2>
  <p>
    This step reads JSON and declares <code>service</code> as <code>str</code> and
    <code>status</code> as <code>i64</code> (an integer).
  </p>
  <StepExample example={examples.json} />
  <p>Both declared fields arrive typed, and the undeclared <code>region</code> passes through.</p>
  <p>
    With <code>into</code>, the same fields arrive under one object instead of at the top level.
    This step declares <code>status</code>, which lands typed at <code>app.status</code>. In the
    GUI, <strong>write into</strong> is under <strong>Advanced</strong>.
  </p>
  <StepExample example={examples.into} />

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'parser').settings} />

  <h3 id="formats">Formats</h3>
  <p>
    <code>kind</code> names the format as <code>{'{ "type": "json" }'}</code>, with the format's own
    settings beside <code>type</code>. Every format also takes the
    <a href="#format-advanced">Advanced</a> setting <code>splitDots</code>.
  </p>

  <h4 id="json">JSON</h4>
  <p>
    <code>{'{ "type": "json" }'}</code> reads one <a href="#json-objects">JSON object</a> with named fields.
    It has no settings of its own.
  </p>

  <h4 id="csv">CSV</h4>
  <p>
    <code>{'{ "type": "csv" }'}</code> reads
    <a href="#csv-columns">values in a fixed column order</a>.
  </p>
  <SettingsTable settings={kind(PARSER_FORMATS, 'csv').settings} />

  <h4 id="key-value">Key/value</h4>
  <p>
    <code>{'{ "type": "keyValue" }'}</code> reads <a href="#key-value-pairs">named pairs</a> such as
    <code>status=200</code>.
  </p>
  <SettingsTable settings={kind(PARSER_FORMATS, 'keyValue').settings} />

  <h4 id="grok">Regex/Grok</h4>
  <p>
    <code>{'{ "type": "grok" }'}</code> reads text that a
    <a href="#grok-matching">regular expression or Grok pattern</a> matches, such as a syslog line.
  </p>
  <SettingsTable settings={kind(PARSER_FORMATS, 'grok').settings} />

  <h4 id="format-advanced">Advanced</h4>
  <p>
    Every format takes this setting. <a href="#advanced">Split keys at dots</a> shows what it does.
  </p>
  <SettingsTable settings={PARSER_FORMATS.common} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Text the format cannot read raises an <a href="#unparsable"><code>Unparsable</code> exception</a
    >, and a value that does not convert to its type raises <code>UnparsableField</code>.
  </p>
  <p>
    A Parser step can have a filter or sit inside an If step's branches. Records it skips keep their
    existing values and receive no errors or warnings from it.
  </p>

  <h3 id="json-objects">JSON objects</h3>
  <p>
    A record must be one JSON object. Top-level arrays, strings, numbers and literals do not parse.
    The parser accepts a trailing comma in an object or array, such as
    <code>{'{"status":200,}'}</code>; it does not support other JSON5 extensions.
  </p>
  <p>
    Fields you do not declare pass through to the output as they arrived. Nested fields use dots:
    <code>event.dataset</code> is the field <code>dataset</code> inside the object
    <code>event</code>. To declare an object, choose the type <code>object</code> and list its
    fields. A closed object keeps only the fields you list and drops its other fields; turn on
    <strong>open</strong> to keep them.
  </p>

  <h3 id="csv-columns">CSV columns and quoting</h3>
  <p>
    Each record is one data row, and the columns name its values by position. The parser does not
    read a header row. The parser drops a column typed <code>skip</code>, while the columns after it
    keep their positions. It ignores extra values beyond the columns; a missing value is null, and
    the output leaves out a null field.
  </p>
  <p>
    With <strong>rebuild the line</strong> on, under Advanced, each column of type <code>skip</code> is
    left empty in the field the step reads, so every column after it keeps its place. Every other character
    stays as it was written, quotes included. A SIEM that reads the vendor's format by position still
    reads the shorter line.
  </p>
  <p>
    Double quotes can enclose delimiters, and two quotes inside a quoted field represent one literal
    quote. An unclosed quote raises an <code>Unparsable</code> exception.
  </p>
  <p>This step reads comma-separated values with these columns:</p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr><th scope="col">Position</th><th scope="col">Name</th><th scope="col">Type</th></tr
        ></thead
      >
      <tbody>
        <tr><td>1</td><td><code>service</code></td><td><code>str</code></td></tr>
        <tr><td>2</td><td><code>debug</code></td><td><code>skip</code></td></tr>
        <tr><td>3</td><td><code>status</code></td><td><code>i64</code></td></tr>
      </tbody>
    </table>
  </div>
  <StepExample example={examples.csv} />

  <h3 id="key-value-pairs">Key/value pairs</h3>
  <p>
    The text is a list of pairs. <code>pairSep</code> divides one pair from the next, and
    <code>kvSep</code> divides a pair's key from its value. With <code>nextKey</code> on, the
    default, a key is the text after the last
    <code>pairSep</code> before a <code>kvSep</code>, and its value runs to the <code>pairSep</code>
    before the next key: in <code>msg=user logged in id=5</code>, <code>msg</code> is
    <code>user logged in</code>. A <code>kvSep</code> inside a value, with no <code>pairSep</code>
    before it, belongs to the value, so <code>url=http://x/?a=1</code> keeps
    <code>http://x/?a=1</code> whole. The parser trims whitespace around a key or an unquoted value.
    A word before the first key is a key whose value is the text <code>"true"</code>.
  </p>
  <p>
    With <code>nextKey</code> off, a value ends at the first <code>pairSep</code>, any word without
    <code>kvSep</code> is a key whose value is the text <code>"true"</code>, and the parser keeps
    whitespace. In
    <code>level=info debug x=1</code>, <code>level</code> is <code>info</code> and
    <code>debug</code> is <code>"true"</code>.
  </p>
  <p>
    <code>quote</code> lets a value hold <code>pairSep</code>: the value starts and ends with it.
    Inside it, a backslash before <code>quote</code> or before another backslash stands for that
    character. Only <code>pairSep</code> may follow the closing quote, and with
    <code>nextKey</code> on, spaces before it. Anything else there, or an unclosed quoted value,
    raises an
    <code>Unparsable</code> exception.
  </p>
  <p>
    This step separates pairs with one space and keys from values with <code>=</code>, and declares
    <code>service</code> as <code>str</code> and <code>status</code> as <code>i64</code>:
  </p>
  <StepExample example={examples.keyValue} />
  <p>With <code>quote</code> set, a quoted value can contain spaces:</p>
  <StepExample example={examples.keyValueQuoted} />

  <p>
    A key declared with type <code>skip</code> makes no field. With
    <strong>rebuild the line</strong> on, the step writes the field it reads back with each skipped pair
    left out, and every other pair as it was written.
  </p>

  <h3 id="grok-matching">Regex/Grok patterns</h3>
  <p>
    The Regex/Grok format reads regular expressions and Grok patterns. A pattern is a regular
    expression, and each named group, <code>{'(?<name>...)'}</code>, captures its match in a field.
    Grok adds named patterns on top: <code>{'%{NAME}'}</code> inserts a built-in or custom named
    pattern, and
    <code>{'%{NAME:field}'}</code> also captures its match in a field. The two forms mix freely in one
    pattern.
  </p>
  <p>
    The captures are the step's fields, each with a type. With several patterns, the first one that
    matches a record parses it. If none matches, the record has an <code>Unparsable</code> exception.
    A matched pattern with an absent optional capture leaves that field null.
  </p>
  <p>
    The parser groups each named pattern it inserts, so a choice or a case-insensitive flag applies
    within that pattern:
  </p>
  <ul>
    <li>
      The built-in <code>LOGLEVEL</code> ignores case. In <code>{'%{LOGLEVEL} ERROR'}</code>, the
      literal <code>ERROR</code> still requires uppercase. <code>WARN ERROR</code> matches;
      <code>warn error</code> does not.
    </li>
    <li>
      A custom pattern <code>AB</code> defined as <code>a|b</code> makes <code>{'^%{AB}c$'}</code>
      match <code>ac</code> and <code>bc</code>, not <code>a</code> alone.
    </li>
  </ul>
  <p>This step is a plain regular expression, with <code>status</code> as <code>i64</code>:</p>
  <StepExample example={examples.regex} />
  <p>
    The same parse with Grok's named patterns is <code>{'^%{WORD:service} %{INT:status}$'}</code>.
    In both, the anchors <code>^</code> and <code>$</code> require the whole input to match.
  </p>
  <StepExample example={examples.grok} />

  <h3 id="custom-grok-patterns">Custom Grok patterns</h3>
  <p>
    A custom pattern is a named pattern you define. A Grok pattern inserts it as
    <code>{'%{NAME}'}</code>, the same way it inserts a built-in. A custom pattern has one of two
    scopes:
  </p>
  <ul>
    <li>
      <strong>One step</strong>: defined under the step's <strong>pattern definitions</strong>. Only
      that step can name it.
    </li>
    <li>
      <strong>A group</strong>: defined under <strong>Build</strong>, <strong>Resources</strong>,
      <strong>Grok patterns</strong>. Any Grok step in the group can name it. A group also sees the
      patterns of <code>shared/</code>.
    </li>
  </ul>
  <p>A name is looked up in the step first, then the group, then the built-ins.</p>
  <p>This step uses the custom pattern <code>AB</code> and the built-in <code>LOGLEVEL</code>:</p>
  <StepExample example={examples.grokCustom} />

  <h3 id="grok-patterns">Built-in Grok patterns</h3>
  <p>
    A pattern is a regular expression in the syntax of Rust's <a
      href="https://docs.rs/regex/latest/regex/#syntax"><code>regex</code> crate</a
    >. A pattern takes no lookarounds and no backreferences. A field name in
    <code>{'%{NAME:field}'}</code>
    is one or more dot-separated parts, each of ASCII letters, digits and <code>_</code>, not
    starting with a digit. The field's type is the step's declaration, so there is no
    <code>{'%{NAME:field:type}'}</code>
    form. The parser refuses patterns nested more than 32 deep.
  </p>
  <div class="table-scroll">
    <table>
      <thead
        ><tr
          ><th scope="col">Pattern</th><th scope="col">Matches</th><th scope="col">Example</th></tr
        ></thead
      >
      <tbody>
        {#each grokPatterns as row (row.name)}
          <tr
            ><td><code class="name">{row.name}</code></td><td
              >{#each row.matches.split('`') as piece, i (i)}{#if i % 2 === 1}<code>{piece}</code
                  >{:else}{piece}{/if}{/each}</td
            ><td><code class="example">{row.example}</code></td></tr
          >
        {/each}
      </tbody>
    </table>
  </div>

  <h3 id="advanced">Split keys at dots</h3>
  <p>
    In the GUI, <strong>Split keys at dots</strong> is under <strong>Advanced</strong>.
  </p>
  <p>
    On, a dot in a field name nests the field: <code>geo.city</code> is the field <code>city</code>
    inside the object <code>geo</code>. Off, <code>geo.city</code> is one field name with a dot in
    it, which a later step reads as <code>$["geo.city"]</code>. The default follows the format. JSON
    and CSV have their own way to nest or none at all, so it is off. Key/value and Regex/Grok
    captures can nest only through a dot, so it is on.
  </p>
  <p>Turned on for JSON, a dotted field name becomes a nested object:</p>
  <StepExample example={examples.splitDotsJson} />
  <p>Turned off for Key/value, a dotted key stays one field name:</p>
  <StepExample example={examples.splitDotsKeyValue} />

  <h3 id="unparsable">Unparsable records</h3>
  <p>
    Text the format cannot read marks the record with an <code>Unparsable</code> exception. Its
    <a href="/docs/routing/exceptions#answers">exception answer</a> decides what happens to it:
  </p>
  <ul>
    <li>
      <strong>tag</strong> (default): the record continues with its exceptions in
      <code>smeltr.exception</code>. The fields parsed before the exception stay. Route delivery
      conditions decide where it goes.
    </li>
    <li>
      <strong>block</strong>: the source stops and processing waits for a new deployment. Correct
      the cause, then review and deploy the change.
    </li>
    <li><strong>drop</strong>: Smeltr counts the record and discards it.</li>
    <li>
      <strong>deliver</strong>: the record goes to the destinations you pick, with its exceptions in
      the <a href="/docs/routing/exceptions#tag">tag field</a>.
    </li>
    <li>
      <strong>ignore</strong>: the record goes on unmarked, and Smeltr still counts the exception.
    </li>
  </ul>
  <p>
    A value the parser cannot read as its declared type marks the record with an
    <code>UnparsableField</code> exception, and its field becomes null. Status → Issues warns when production
    stops matching a declared field.
  </p>
  <p>
    In these examples the second record is cut off after <code>"status":</code>. Tagged, it goes on
    with the field read before the exception:
  </p>
  <StepExample example={examples.failTag} />
  <p>Dropped, only the record that parsed goes on:</p>
  <StepExample example={examples.failDrop} />
  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/routing/exceptions#answers">Exception answers</a></li>
  </ul>
</GuideLayout>

<style>
  /* An example's spaces are part of what the pattern matched. */
  .example {
    white-space: pre;
  }
  .name {
    white-space: nowrap;
  }
</style>
