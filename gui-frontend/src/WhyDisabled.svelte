<!--
  Says why a control is disabled, beside it: on hover, and on a press, which a disabled control
  otherwise ignores. The reason is the `data-why` of the control or of what holds it -- a `Button`'s
  own, a `Locked`'s for all it holds. Mounted once, by `App`: the browser still hands a disabled
  control pointer events, which bubble here.
-->
<script lang="ts">
  import Popover from './Popover.svelte';

  const DISABLED = 'button:disabled, input:disabled, select:disabled, textarea:disabled';

  let pop: Popover;
  let at = $state<Element>();
  let why = $state('');

  /** The disabled control `e` landed on and why it is disabled, or nothing. */
  function disabledAt(e: PointerEvent): { control: Element; reason: string } | undefined {
    const control = e.target instanceof Element ? e.target.closest(DISABLED) : null;
    const reason = control?.closest<HTMLElement>('[data-why]')?.dataset.why;
    return control && reason ? { control, reason } : undefined;
  }

  /** Shows why the control `e` landed on is disabled; a press anywhere else closes it. */
  function show(e: PointerEvent) {
    const found = disabledAt(e);
    if (!found) {
      if (e.type === 'pointerdown') pop.toggle(false);
      return;
    }
    at = found.control;
    why = found.reason;
    pop.toggle(true);
  }
</script>

<svelte:document
  onkeydown={(e) => {
    if (e.key === 'Escape') pop.toggle(false);
  }}
  onpointerover={(e) => {
    if (e.pointerType === 'mouse') show(e);
  }}
  onpointerout={(e) => {
    if (e.pointerType === 'mouse' && disabledAt(e)?.control === at) pop.toggle(false);
  }}
  onpointerdown={show}
/>

<!-- Manual, since the platform's light dismiss would close it on the very press that opened it. -->
<Popover bind:this={pop} manual id="why-disabled" anchor={() => at!}>{why}</Popover>
