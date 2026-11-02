<!--
  The app's dialog chrome (STYLE_GUIDE.md, "Dialogs"): a native `<dialog>` opened with `showModal()`,
  a header rule, a body that scrolls on its own, and a footer of actions. A question (`Confirm`) and a
  new item's form (a page's add dialog) are both this, so they look and close alike.

  `open` is the caller's. Escape calls `oncancel`, which is how the caller learns the reader
  refused; the caller closes it by setting `open` false.
-->
<script lang="ts" module>
  import { getContext, setContext, type Snippet } from 'svelte';

  const INSIDE = Symbol('dialog');

  /** Whether the caller renders inside a dialog. A link away from the page would drop the dialog's
   * unadded draft, so a field that offers one leaves it out here. */
  export function insideDialog(): boolean {
    return getContext<boolean | undefined>(INSIDE) ?? false;
  }
</script>

<script lang="ts">
  import ErrorBar from './ErrorBar.svelte';
  import { errors } from './errors.svelte';

  let {
    open,
    title,
    wide = false,
    oncancel,
    onback,
    onsubmit,
    children,
    actions,
  }: {
    open: boolean;
    /** The header rule: what this is, in a word or two. */
    title: string;
    /** Room for a form or a catalog rather than a sentence. */
    wide?: boolean;
    oncancel: () => void;
    /** A step back to what came before, in the header: a form goes back to the choice it was opened
     * from. */
    onback?: () => void;
    /** Enter in a field, as the footer's last action would. */
    onsubmit?: () => void;
    children: Snippet;
    actions: Snippet;
  } = $props();

  setContext(INSIDE, true);

  let dialog: HTMLDialogElement | undefined = $state();
  const titleId = $props.id();

  /** What was reported before this opened: the page's bar holds that, behind the backdrop. */
  let since = $state(0);

  $effect(() => {
    if (!dialog) return;
    if (open && !dialog.open) {
      since = errors.seq;
      dialog.showModal();
    } else if (!open && dialog.open) dialog.close();
  });
</script>

<dialog
  bind:this={dialog}
  class:wide
  aria-labelledby={titleId}
  oncancel={(e) => {
    e.preventDefault();
    oncancel();
  }}
  onclose={() => {
    // A second Escape closes a modal whatever the first one's cancel said. Read it as a refusal,
    // and show the dialog again if the caller still holds it open.
    if (!open) return;
    oncancel();
    if (open) dialog?.showModal();
  }}
>
  {#if open}
    <form
      onsubmit={(e) => {
        e.preventDefault();
        onsubmit?.();
      }}
    >
      <h2 id={titleId}>
        {#if onback}
          <button type="button" class="icon back" aria-label="Back" onclick={onback}
            >&lsaquo;</button
          >
        {/if}
        {title}
      </h2>
      <div class="body"><ErrorBar {since} />{@render children()}</div>
      <!-- The refusal comes first in the source as well as on screen, so the platform's own focus
           lands on it. -->
      <div class="actions">{@render actions()}</div>
    </form>
  {/if}
</dialog>

<style>
  dialog {
    width: min(28rem, calc(100vw - 2rem));
    max-height: calc(100vh - 4rem);
    padding: 0;
    overflow: hidden;
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    background: var(--panel);
    color: var(--ink);
  }

  dialog.wide {
    width: min(52rem, calc(100vw - 2rem));
  }

  dialog::backdrop {
    background: oklch(0% 0 0 / 0.45);
  }

  form {
    display: flex;
    flex-direction: column;
    max-height: calc(100vh - 4rem);
  }

  /* The header rule: what is being asked, in a word or two. */
  h2 {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    margin: 0;
    padding: 0.5rem 0.85rem;
    border-bottom: 1px solid var(--border);
    font-size: var(--text-micro);
    font-weight: 700;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--dim);
  }

  .back {
    font-size: var(--text-body);
  }

  /* Its own scroll region, rather than the dialog's: the header rule and the footer stay reachable
     however much the body holds. It grows to the screen, and where it must still scroll, a shadow at
     the cut says more lies past it: an overlay scrollbar shows nothing until it is used. The two
     covers scroll with the content and hide each shadow at the matching end. */
  .body {
    padding: 0.85rem;
    min-height: 0;
    overflow-y: auto;
    background:
      linear-gradient(var(--panel) 30%, transparent) top / 100% 2rem no-repeat local,
      linear-gradient(transparent, var(--panel) 70%) bottom / 100% 2rem no-repeat local,
      radial-gradient(farthest-side at 50% 0, var(--shadow-edge), transparent) top / 100% 0.6rem
        no-repeat scroll,
      radial-gradient(farthest-side at 50% 100%, var(--shadow-edge), transparent) bottom / 100%
        0.6rem no-repeat scroll,
      var(--panel);
    --shadow-edge: color-mix(in oklch, var(--ink) 22%, transparent);
  }

  /* The footer rule: the refusal first and plain, the answer last and accented -- or in `--error`
     where it destroys something. */
  .actions {
    display: flex;
    justify-content: flex-end;
    gap: 0.6rem;
    padding: 0.6rem 0.85rem;
    border-top: 1px solid var(--border);
  }
</style>
