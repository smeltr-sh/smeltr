<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/rename.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('rename', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Rename step"
  description="Move fields to new names with a Rename step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Rename step</h1>
  <p class="lede">
    A Rename step moves each field it names to a new name, and the value keeps its type.
  </p>
  <p>
    Each pair names a field to move from, <code>from</code>, and the name to move it to,
    <code>to</code>. Each pair is the
    <a href="/docs/code-reference#statements">Code statement</a> <code>move(from, to);</code>.
  </p>

  <h2 id="example">Example</h2>
  <StepExample example={examples.basic} />
  <p>No pair names <code>status</code>, so it passes through unchanged.</p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'rename').settings} />
  <p>
    In the editor, a new Rename step starts with one empty pair, and <strong>+ pair</strong> adds a row.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    Later steps and destinations read the value under its new name only. A dot in the new name nests
    it, so <code>source.ip</code> puts the value in an object <code>source</code>.
  </p>

  <h3 id="objects">Objects and existing names</h3>
  <p>Renaming an object moves it whole, with every field inside it:</p>
  <StepExample example={examples.object} />
  <p>A rename onto a name that already holds a value replaces that value:</p>
  <StepExample example={examples.replace} />

  <h3 id="filter">Rename selected records</h3>
  <p>
    With a filter, the step renames only records where it is true. A record where it is false or
    null passes the step unchanged. A Rename step inside an <a href="/docs/steps/if">If step</a>
    branch works the same way: it renames only the records on that branch.
  </p>
  <p>The parser declares <code>kind</code> and <code>addr</code> as <code>str</code>.</p>
  <StepExample example={examples.filtered} />

  <h3 id="edges">Missing fields</h3>
  <p>A record without the <code>from</code> field passes the step unchanged:</p>
  <StepExample example={examples.missing} />

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/if">If step</a></li>
  </ul>
</GuideLayout>
