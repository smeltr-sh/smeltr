<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import StepExample from '#lib/components/StepExample.svelte';
  import examplesText from '#lib/steps/if.json?raw';
  import { kind } from '#lib/connector-reference.ts';
  import { STEPS } from '#lib/step-reference.ts';
  import { stepExamples } from '#lib/step-pages.ts';
  // Each example is run against the engine by `tests/website-reference-tests.rs`.
  const examples = stepExamples('if', examplesText);
  const sections = [
    { id: 'example', title: 'Example' },
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="If step"
  description="Run one list of steps or another on each record with an If step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>If step</h1>
  <p class="lede">
    An If step reads a condition on each record and runs its <strong>then</strong> steps where the
    condition is true and its <strong>else</strong> steps everywhere else.
  </p>
  <p>
    Each branch, <strong>then</strong> or <strong>else</strong>, is a step list of its own, which
    you edit inside the If step's card.
  </p>

  <h2 id="example">Example</h2>
  <StepExample example={examples.basic} />
  <p>
    The parser declares <code>status</code> as <code>i64</code> and <code>path</code> as
    <code>str</code>. The 503 record takes the then branch; the two 200 records take the else
    branch, where the Drop step removes the <code>/health</code> record.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes an <code>id</code>, as <a href="/docs/steps#settings">every step</a> does.</p>
  <SettingsTable settings={kind(STEPS, 'if').settings} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    A record runs the steps of one branch, then continues with the step after the If step. An empty
    branch leaves its records unchanged.
  </p>
  <p>
    A branch can hold any step but <a href="/docs/steps/unroll">Unroll</a>, including another If
    step; an Unroll step in a branch is an error when you save. It changes how many records there
    are, and a record that takes the other branch has no item of the array. Unroll before the If
    step, then select the new records with the If step's condition or a Drop step. A
    <a href="/docs/steps/drop">Drop step</a> in a branch removes only records on that branch.
  </p>

  <h3 id="filter">If step or step filter</h3>
  <p>
    Every step but If and <a href="/docs/steps/comment">Comment</a> has a
    <strong>filter</strong>. A filter selects the records that one step processes; the other records
    pass that step unchanged. An If step selects records for a whole list of steps, and gives the
    other records a list of their own.
  </p>
  <p>
    A step inside a branch can also have a filter. It then processes the records where both the
    branch's condition and its filter hold.
  </p>

  <h3 id="edges">Null and missing fields</h3>
  <p>
    A condition that is null is not true, so the record takes the else branch. A record without
    <code>status</code> does:
  </p>
  <StepExample example={examples.missing} />

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/unroll">Unroll step</a></li>
    <li><a href="/docs/steps/drop">Drop step</a></li>
    <li><a href="/docs/steps/comment">Comment step</a></li>
    <li><a href="/docs/code-reference">Expression reference</a></li>
  </ul>
</GuideLayout>
