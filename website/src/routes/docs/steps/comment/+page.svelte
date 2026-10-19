<script lang="ts">
  import GuideLayout from '#lib/components/GuideLayout.svelte';
  import SettingsTable from '#lib/components/SettingsTable.svelte';
  import { kind } from '#lib/connector-reference.ts';
  import { STEPS } from '#lib/step-reference.ts';
  const sections = [
    { id: 'settings', title: 'Settings' },
    { id: 'how-it-works', title: 'How it works' },
    { id: 'see-also', title: 'See also' },
  ];
</script>

<GuideLayout
  title="Comment step"
  description="Leave a note between the steps of a pipeline with a Comment step."
  {sections}
>
  <p class="eyebrow">Pipeline reference</p>
  <h1>Comment step</h1>
  <p class="lede">
    A Comment step is a note in the step list. It does nothing when the pipeline runs.
  </p>

  <h2 id="settings">Settings</h2>
  <p>It also takes an <code>id</code>, as <a href="/docs/steps#settings">every step</a> does.</p>
  <SettingsTable settings={kind(STEPS, 'comment').settings} />

  <h2 id="how-it-works">How it works</h2>
  <p>
    Use a Comment step to say why the steps around it are there, for the next person who edits the
    pipeline. The step list shows its text on the card, so it reads without opening the step. Every
    record passes it unchanged.
  </p>
  <p>
    A Comment step can sit anywhere a step can, including in an <a href="/docs/steps/if">If step</a
    >'s branches. To say why one line of a calculation is the way it is, write a comment inside a
    <a href="/docs/steps/code">Code step</a> instead.
  </p>

  <h2 id="see-also">See also</h2>
  <ul>
    <li><a href="/docs/steps">Pipeline steps</a></li>
    <li><a href="/docs/steps/if">If step</a></li>
    <li><a href="/docs/steps/code">Code step</a></li>
  </ul>
</GuideLayout>
