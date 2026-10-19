<script lang="ts">
  import WorkflowDiagram from './WorkflowDiagram.svelte';
</script>

<WorkflowDiagram kind="dataPath" />
<p>
  What happens to a record decides when its batch's position commits. Until it commits, a restart
  reads the batch again.
</p>
<div class="table-scroll">
  <table>
    <thead><tr><th scope="col">The record</th><th scope="col">The position commits</th></tr></thead>
    <tbody>
      <tr
        ><td>Is acknowledged by every destination it was sent to</td><td
          >Once every older batch has committed too.</td
        ></tr
      >
      <tr><td>Is dropped by a step, or picked by no deliver rule</td><td>With its batch.</td></tr>
      <tr
        ><td>Reaches a destination that does not answer</td><td
          >Only once it lands. Egress retries, and a destination that stays down pauses the sources
          feeding it.</td
        ></tr
      >
      <tr
        ><td>Carries an exception</td><td
          >By the answer to it, as below. A pipeline's exceptions are answered by its route's
          <code>exceptions</code> over the group's; Unmatched and Unprocessable by the group's; Refused
          by the destination's over the group's.</td
        ></tr
      >
    </tbody>
  </table>
</div>
<div class="table-scroll">
  <table>
    <thead><tr><th scope="col">Answer</th><th scope="col">The position commits</th></tr></thead>
    <tbody>
      <tr
        ><td><code>ignore</code>, <code>tag</code></td><td
          >Once the destinations the record goes on to acknowledge it.</td
        ></tr
      >
      <tr><td><code>drop</code></td><td>With its batch.</td></tr>
      <tr
        ><td><code>deliver</code></td><td>Once every destination named acknowledges the record.</td
        ></tr
      >
      <tr
        ><td><code>block</code></td><td
          >Not until the record goes through. A refused record is sent again until the destination
          takes it; any other stops its source until a new config is deployed.</td
        ></tr
      >
    </tbody>
  </table>
</div>
<p>
  Unmatched, Unprocessable and Refused block unless an answer says otherwise, and take no
  <code>ignore</code> or <code>tag</code>.
  <a href="/docs/routing/exceptions#answers">Exceptions</a> covers each answer.
</p>
