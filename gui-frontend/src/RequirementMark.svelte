<!--
  A field's requirement, marked as every list of a schema's fields marks it: "* required", "° recommended",
  "optional". `text` says it in other words, such as an object's "24 recommended", in the requirement's color.
-->
<script lang="ts">
  import type { Requirement } from './api';
  import { REQUIREMENT_MARK } from './schemas';

  let { of, text }: { of: Requirement; text?: string } = $props();
</script>

<span class="requirement-mark {of}" class:dim={of === 'optional'}
  >{text ?? REQUIREMENT_MARK[of]}</span
>
