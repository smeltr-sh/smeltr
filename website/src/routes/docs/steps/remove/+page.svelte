<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { STEPS } from '#lib/step-reference.ts';
  import { kind } from '#lib/connector-reference.ts';
  import examplesText from '#lib/steps/remove.json?raw';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('remove', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Remove step"
  description="Take fields out of each record with a Remove step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Remove step</h1>
  <p class="lede">
    A Remove step takes the fields it names out of each record. With no filter, no later step or
    destination sees them.
  </p>
  <p>
    It names each field to remove. With no filter, the step is the
    <a href="/docs/code-reference#statements">Code statement</a> <code>delete(a, b);</code>.
  </p>

  <h2 id="example">Example</h2>
  <p>
    The parser declares the record's fields, so the step's list shows them, beside
    <code>_raw</code>, the text the parser read.
  </p>
  <StepExample example={examples.basic} />
  <p>The step does not name <code>user</code> or <code>status</code>, so they pass through.</p>

  <h2 id="settings">Settings</h2>
  <p>It also takes the settings <a href="/docs/steps#settings">every step</a> takes.</p>
  <SettingsTable settings={kind(STEPS, 'remove').settings} />
  <p>
    In the editor, the step lists the fields the pipeline declares where the step starts, ticked
    while it is kept. Untick a field to remove it. <strong>Find a field</strong> narrows the list,
    and
    <strong>Remove all</strong> and <strong>Keep all</strong> act on the fields it shows. To remove
    a field that matches nothing in the list, type its name and choose
    <strong>Remove &lt;name&gt;</strong> under the search box. A field a JSON or key-value parser read
    without declaring it is not in the list, and is removed this way. A name the record cannot hold there
    is an error at the step.
  </p>

  <h2 id="how-it-works">How it works</h2>
  <p>
    With no filter, later steps cannot read a removed field, and a step that does fails to compile.
    A later step can write the name again.
  </p>

  <h3 id="objects">Objects</h3>
  <p>A dot names a field inside an object, and removing an object removes every field inside it:</p>
  <StepExample example={examples.object} />

  <h3 id="filter">With a filter</h3>
  <p>
    With a filter, the step sets the fields to null on the records where it is true, and the fields
    stay for later steps. A record where it is false or null passes the step unchanged. A Remove
    step inside an <a href="/docs/steps/if">If step</a> branch works the same way.
  </p>
  <p>The parser declares <code>level</code> and <code>trace</code> as <code>str</code>.</p>
  <StepExample example={examples.filtered} />
  <p>A null field is left out of the JSON shown.</p>

  <h3 id="edges">Missing fields</h3>
  <p>A record without the field passes the step unchanged:</p>
  <StepExample example={examples.missing} />

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/rename">Rename step</a></li>
    <li><a href="/docs/steps/eval">Eval step</a></li>
  </ul>
</GuideLayout>
