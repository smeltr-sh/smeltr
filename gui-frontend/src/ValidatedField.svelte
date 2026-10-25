<!--
  A text input that marks a compile error inline. `errors` is every message
  attributed to the enclosing step; a message is "about" this field when
  one of its backtick-quoted identifiers appears in the field's text,
  which compiler messages name consistently. The field only marks it: the
  message itself is written once, by whoever owns the list of errors (a
  step's own list), so it is never shown twice.

  `autocomplete` suggests identifiers as they're typed. `'condition'`,
  `'assignment'` and `'code'` suggest the functions that kind of box can call
  (`stdlib.ts`) anywhere, with a "ƒ" button to browse the whole list, and
  insert `name()`. They rank by the match, then a little by how often the
  bundle calls each (`usage.svelte.ts`).
  `'grok'` suggests built-in grok macros (`grokPatterns.ts`) and the bundle's
  own (`grokNamed`), only while
  the caret sits inside an unclosed `%{`, and completes to `NAME}`
  in place. Set only where an expression or pattern is typed, never on a
  plain field-name input.

  A field that suggests functions is code, so it is laid out in the style when it is left. It is colored as it is
  typed, and so is any field given its own `paint`: the text is drawn by a highlighted copy
  behind the input, whose own text is transparent. The copy matches the input's box and follows its scroll.

  A `wrap` field is a condition shown whole in a narrow box. Left, it is drawn broken before its operators
  (`conditionWrap.ts`) to the width it has, three lines at most, and a click or focus opens it to edit. Inside a
  `[data-wrap-room]`, the room is that element's width, and the box (`[data-wrap-box]`) shrinks to the longest line.
-->
<script lang="ts">
  import { tick } from 'svelte';
  import { float, type Anchor } from './float';
  import {
    ASSIGNMENT_FUNCTIONS,
    CODE_FUNCTIONS,
    CONDITION_FUNCTIONS,
    type StdFuncEntry,
  } from './stdlib';
  import { functionUsage } from './usage.svelte';
  import { GROK_BUILTIN_PATTERNS } from './grokPatterns';
  import {
    currentWord as wordAt,
    describedRuns,
    rankedSuggestions,
    memberSuggestions,
    type Member,
    inGrokMacroName,
  } from './autocomplete';
  import {
    highlight,
    KEYWORDS,
    LITERALS,
    tokenize,
  } from '../../website/src/lib/highlight/highlight';
  import { wrapCondition } from './conditionWrap';
  import type { Attachment } from 'svelte/attachments';
  import { fieldError, holdsName, markRuns, quotedTokens } from './errorMarks';
  import { formatCode, type DotAnswer } from './api';
  import { beginReconcile, endReconcile } from './grokFields';

  let {
    value = $bindable(''),
    errors = [],
    warnings = [],
    forced = null,
    placeholder = '',
    ariaLabel = '',
    autocomplete = null,
    grokNamed = [],
    multiline = false,
    rows = 6,
    paint,
    wrap = false,
    ask,
  }: {
    value: string;
    errors?: string[];
    /** Every warning attributed to the enclosing step (`Plan::warnings`), marked in the warning color where no error
     * is: a colored field marks the names one quotes, any other field all of its text when it holds one. A keyword or
     * literal it quotes (`break`, `true`) is no name. */
    warnings?: string[];
    /** An error already known to belong to this exact field -- an eval
     * assignment's own error, say (`CompileError::assignment_index`) --
     * marked regardless of whether its text happens to quote an identifier
     * this field's value contains. Takes precedence over `errors`, which
     * stays as the fallback for everything that arrives as a step-level
     * message with no more specific place to go. */
    forced?: string | null;
    placeholder?: string;
    ariaLabel?: string;
    autocomplete?: 'condition' | 'assignment' | 'code' | 'grok' | null;
    /** The bundle's own grok patterns, suggested after the built-ins (`sharedGrokSuggestions`). */
    grokNamed?: StdFuncEntry[];
    /** Renders a `<textarea>` instead of an `<input>` -- for multi-statement
     * fields like the Code step's body. Autocomplete works the same way,
     * just against whichever identifier run surrounds the caret across the
     * whole multi-line text. */
    multiline?: boolean;
    rows?: number;
    /** Colors text that is not an expression: the field's text as HTML, escaped. */
    paint?: (text: string) => string;
    /** Left, the text is drawn broken before its operators, to the room it has: see the comment at the top. */
    wrap?: boolean;
    /** Asks the engine what may follow the `.` at `dot` in `text`, where this expression sits (`afterDot`). Absent,
     * every function written on a value may. */
    ask?: (text: string, dot: number) => Promise<DotAnswer | null>;
  } = $props();

  /** A `wrap` field being written in: left, it shows its text laid out instead. */
  let editing = $state(false);
  let resting = $derived(wrap && !editing && value.trim() !== '');
  function stopEdit() {
    // Focus leaving the window is not leaving the field: it comes back to the text being written.
    if (document.hasFocus()) editing = false;
  }

  let measurer: CanvasRenderingContext2D | null = null;
  /** The width of one character of `cs`'s font, which is monospace. */
  function charWidth(cs: CSSStyleDeclaration): number {
    measurer ??= document.createElement('canvas').getContext('2d');
    if (!measurer) return parseFloat(cs.fontSize) * 0.6;
    measurer.font = `${cs.fontSize} ${cs.fontFamily}`;
    return measurer.measureText('0').width;
  }

  /** The most lines a left `wrap` field shows. */
  const WRAP_LINES = 3;
  /** Draws `src` laid out into the rest view, again whenever its room changes size. */
  function wrapTo(src: string): Attachment<HTMLElement> {
    return (el) => {
      const room = el.closest<HTMLElement>('[data-wrap-room]');
      const box = el.closest<HTMLElement>('[data-wrap-box]');
      const draw = () => {
        const cs = getComputedStyle(el);
        const edges =
          parseFloat(cs.paddingLeft) +
          parseFloat(cs.paddingRight) +
          parseFloat(cs.borderLeftWidth) +
          parseFloat(cs.borderRightWidth);
        const ch = charWidth(cs);
        // What else the box holds beside the text stays beside it, wherever the text's own width goes.
        const beside = room && box ? box.offsetWidth - el.offsetWidth : 0;
        const width = room ? room.clientWidth - beside : el.offsetWidth;
        const cols = Math.max(8, Math.floor((width - edges) / ch));
        let lines = wrapCondition(src, cols);
        if (lines.length > WRAP_LINES) {
          lines = lines.slice(0, WRAP_LINES);
          lines[WRAP_LINES - 1] += ' …';
        }
        if (room) {
          const longest = Math.max(...lines.map((l) => l.length));
          el.style.width = `${Math.ceil(longest * ch + edges)}px`;
        }
        el.innerHTML = lines.map(highlight).join('\n');
      };
      draw();
      const ro = new ResizeObserver(draw);
      ro.observe(room ?? el);
      return () => ro.disconnect();
    };
  }

  let matched = $derived(fieldError(value, errors, forced));

  // -- coloring ---------------------------------------------------------------

  let paintEl: HTMLPreElement | undefined = $state();
  let marksEl: HTMLPreElement | undefined = $state();
  /** The functions this field suggests: each kind of box can call its own. */
  let functions = $derived(
    autocomplete === 'condition'
      ? CONDITION_FUNCTIONS
      : autocomplete === 'assignment'
        ? ASSIGNMENT_FUNCTIONS
        : autocomplete === 'code'
          ? CODE_FUNCTIONS
          : null,
  );
  let colored = $derived(functions !== null || paint !== undefined);
  // The trailing newline gives a last empty line its height, as the textarea does.
  let painted = $derived(colored ? (paint ?? highlight)(value) + '\n' : '');

  /** The names the field's errors quote that the text holds. A colored
   * field marks just those, as a code editor does; anything else marks the
   * whole field. */
  let named = $derived(
    colored && matched
      ? (forced !== null ? [forced] : errors)
          .flatMap(quotedTokens)
          .filter((t) => holdsName(value, t))
      : [],
  );
  let warned = $derived(
    matched
      ? []
      : warnings
          .flatMap(quotedTokens)
          .filter((t) => !KEYWORDS.has(t) && !LITERALS.has(t) && holdsName(value, t)),
  );
  let runs = $derived(
    named.length > 0
      ? markRuns(value + '\n', named)
      : colored && warned.length > 0
        ? markRuns(value + '\n', warned)
        : [],
  );
  let whole = $derived(!!matched && runs.length === 0);
  // A name box has no painted copy to mark a run in, so a warning that names it marks all of it, as an error does.
  let wholeWarn = $derived(!colored && warned.length > 0);

  function syncScroll() {
    if (!inputEl) return;
    for (const el of [paintEl, marksEl]) {
      if (!el) continue;
      el.scrollTop = inputEl.scrollTop;
      el.scrollLeft = inputEl.scrollLeft;
    }
    placeList?.();
  }

  // An edit, a caret move or a drag can scroll the input without a scroll
  // event reaching us first.
  $effect(() => {
    void painted;
    void runs;
    tick().then(syncScroll);
  });

  // -- autocomplete -----------------------------------------------------------

  let inputEl: HTMLInputElement | HTMLTextAreaElement | undefined = $state();
  let open = $state(false);
  let highlighted = $state(0);

  function currentWord() {
    return wordAt(value, inputEl?.selectionStart ?? value.length);
  }

  let currentWordText = $derived(currentWord().word);

  let grokActive = $derived(autocomplete === 'grok' && inGrokMacroName(value, currentWord().pos));

  /** The `.` the word follows, or null where it follows none: a dot in a string, a comment or a number is not one. */
  let dot = $derived.by(() => {
    const { start } = currentWord();
    if (!functions || value[start - 1] !== '.') return null;
    const last = tokenize(value.slice(0, start)).at(-1);
    return last?.kind === 'plain' && last.text === '.' ? start - 1 : null;
  });

  /** The engine's answers, by the text up to the dot they are about, kept while the field is being written in. */
  const answers = new Map<string, DotAnswer | null>();
  const asking = new Set<string>();
  /** Bumped as an answer arrives, so what reads `answers` reads it again. */
  let answered = $state(0);

  /** What the dot follows: unknown until the engine answers, null where it follows no value. */
  let member = $derived.by((): Member | null => {
    if (dot === null) return null;
    void answered;
    const answer = answers.get(value.slice(0, dot + 1));
    if (answer === undefined) return { kind: 'unknown' };
    return answer && { kind: 'typed', ...answer };
  });

  // Asks once per text up to a dot: the letters after it filter the answer here.
  $effect(() => {
    if (dot === null || !ask) return;
    const head = value.slice(0, dot + 1);
    if (answers.has(head) || asking.has(head)) return;
    asking.add(head);
    ask(value, dot).then(
      (answer) => {
        asking.delete(head);
        // The highlight stays on the item it was moved to, so the reorder never changes what Enter picks.
        const moved = highlighted > 0 ? suggestions[highlighted]?.name : undefined;
        answers.set(head, answer);
        answered++;
        if (moved !== undefined)
          highlighted = Math.max(
            0,
            suggestions.findIndex((f) => f.name === moved),
          );
        if (answer === null && dot !== null && value.slice(0, dot + 1) === head) open = false;
      },
      () => asking.delete(head),
    );
  });

  let suggestions = $derived.by((): StdFuncEntry[] => {
    if (functions && member) {
      return memberSuggestions(member, functions, currentWordText, functionUsage);
    }
    if (functions) return rankedSuggestions(functions, currentWordText, { usage: functionUsage });
    if (grokActive) {
      return rankedSuggestions([...GROK_BUILTIN_PATTERNS, ...grokNamed], currentWordText, {
        matchDescription: false,
      });
    }
    return [];
  });
  // Opens on a non-matching word too: the "no matches" row is what points
  // a wrong guess (`toInt`) at the browse button.
  let showDropdown = $derived(open && (suggestions.length > 0 || currentWordText.length > 0));

  function onInput() {
    if (functions) {
      // A dot is where a list helps most: what can follow it depends on what is before it.
      open = currentWordText.length > 0 || member !== null;
    } else if (autocomplete === 'grok') {
      open = grokActive && currentWordText.length > 0;
    } else {
      return;
    }
    highlighted = 0;
  }

  /** Lays code out in the style when the field is left (`expr::pretty`). Text that does not parse yet stays as typed,
   * and so does text changed while the leader answered. A one-line field takes only a one-line layout. */
  async function onBlur() {
    open = false;
    // The record may change before the field is written in again.
    answers.clear();
    // Focus leaving the window is not leaving the field.
    if (!functions || !document.hasFocus()) return;
    const sent = value;
    // A Save pressed straight from the field waits for this answer, or it would send the text before its layout.
    beginReconcile();
    try {
      const answer = await formatCode(sent, autocomplete === 'code' ? 'block' : 'expr').catch(
        () => null,
      );
      const code = answer?.code;
      if (code == null || value !== sent || code === sent) return;
      if (!multiline && code.includes('\n')) return;
      value = code;
    } finally {
      endReconcile();
    }
  }

  function toggleBrowse() {
    open = !open;
    highlighted = 0;
    inputEl?.focus();
  }

  async function pick(fn: StdFuncEntry) {
    let { start, pos } = currentWord();
    // A function call is inserted whole. A grok macro's `%{` is already
    // typed (suggestions only show inside one); its own `}` is added only
    // when nothing already closes the macro past the caret -- editing
    // `%{WOR|:field}` or `%{WOR|}` (caret before an existing suffix) must
    // complete the name in place rather than appending a second `}`.
    let insertion: string;
    let caretOffset: number;
    if (autocomplete === 'grok') {
      const next = value[pos];
      insertion = next === ':' || next === '}' ? fn.name : `${fn.name}}`;
      caretOffset = fn.name.length;
    } else if (fn.field) {
      // A key that is not a name is written in brackets, in place of the dot.
      const bare = /^[A-Za-z_$][\w$]*$/.test(fn.name);
      if (!bare) start--;
      insertion = bare ? fn.name : `[${JSON.stringify(fn.name)}]`;
      caretOffset = insertion.length;
    } else {
      insertion = `${fn.name}()`;
      caretOffset = fn.name.length + 1;
    }
    value = value.slice(0, start) + insertion + value.slice(pos);
    open = false;
    // The caret offset is against the new text, so the DOM must hold it.
    await tick();
    const caretPos = start + caretOffset;
    inputEl?.setSelectionRange(caretPos, caretPos);
    inputEl?.focus();
  }

  /** The least room under the word the list takes before it goes over it: about three items. */
  const LIST_FLOOR = 160;

  /** The line box of the word being typed, on screen, held inside the field where the word is scrolled out of it.
   * The painted copy is laid out and scrolled as the field is, so a range over it finds the word. A field with no
   * painted copy gives its own box. */
  function wordBox(): Anchor | undefined {
    if (!inputEl) return undefined;
    const field = inputEl.getBoundingClientRect();
    if (!paintEl) return field;
    let rest = currentWord().start;
    const walk = document.createTreeWalker(paintEl, NodeFilter.SHOW_TEXT);
    for (let n = walk.nextNode(); n; n = walk.nextNode()) {
      const length = n.textContent?.length ?? 0;
      if (rest > length) {
        rest -= length;
        continue;
      }
      const range = document.createRange();
      range.setStart(n, rest);
      range.setEnd(n, Math.min(rest + 1, length));
      const word = range.getClientRects()[0] ?? range.getBoundingClientRect();
      const top = Math.min(Math.max(word.top, field.top), field.bottom);
      const bottom = Math.max(Math.min(word.bottom, field.bottom), field.top);
      const left = Math.min(Math.max(word.left, field.left), field.right);
      return { top, bottom, left, right: left };
    }
    return field;
  }

  /** Places the open list at the word being typed again. Set while the list is open. */
  let placeList: (() => void) | undefined;

  function onKeydown(e: KeyboardEvent) {
    // A `wrap` field is one line of text in a textarea: the box wraps it, a key does not.
    if (wrap && e.key === 'Enter' && !showDropdown) e.preventDefault();
    if (!showDropdown) return;
    if (e.key === 'Escape') {
      open = false;
      return;
    }
    if (suggestions.length === 0) return;
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      highlighted = (highlighted + 1) % suggestions.length;
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      highlighted = (highlighted - 1 + suggestions.length) % suggestions.length;
    } else if (e.key === 'Enter' || e.key === 'Tab') {
      e.preventDefault();
      pick(suggestions[highlighted]);
    }
  }
</script>

<span
  class="field-wrap"
  class:has-error={!!matched}
  class:whole
  class:multiline={multiline || wrap}
  class:colored
  class:wrapped={wrap}
>
  {#if resting}
    <!-- The field is still the textarea, unseen under this: a click or a focus reaches it. -->
    <span
      class="wrapview"
      class:field-error={!!matched}
      class:whole={!!matched}
      class:whole-warn={!matched && warned.length > 0}
      aria-hidden="true"
      {@attach wrapTo(value)}
    ></span>
  {/if}
  {#if colored && !resting}
    <pre
      bind:this={paintEl}
      class="paint"
      class:has-fn-toggle={functions !== null}
      aria-hidden="true">{@html painted}</pre>
  {/if}
  {#if runs.length > 0 && !resting}
    <!-- The same text again over the painted copy, transparent, so only the
       marks under the quoted names show. -->
    <pre
      bind:this={marksEl}
      class="paint marks"
      class:has-fn-toggle={functions !== null}
      aria-hidden="true">{#each runs as r}{#if r.mark}<span class="squiggle" class:warn={!matched}
            >{r.text}</span
          >{:else}{r.text}{/if}{/each}</pre>
  {/if}
  {#if multiline || wrap}
    <textarea
      bind:this={inputEl}
      rows={wrap ? 1 : rows}
      {placeholder}
      aria-label={ariaLabel}
      bind:value
      class:field-error={!!matched}
      aria-invalid={!!matched}
      class:whole
      class:whole-warn={wholeWarn}
      class:has-fn-toggle={functions !== null}
      class:under={resting}
      oninput={onInput}
      onkeydown={onKeydown}
      onscroll={syncScroll}
      onkeyup={syncScroll}
      onpointerup={syncScroll}
      onfocus={() => (editing = wrap)}
      onblur={() => {
        if (wrap) stopEdit();
        void onBlur();
      }}
      spellcheck="false"
      autocomplete="off"></textarea>
  {:else}
    <input
      bind:this={inputEl}
      type="text"
      {placeholder}
      aria-label={ariaLabel}
      bind:value
      class:field-error={!!matched}
      aria-invalid={!!matched}
      class:whole
      class:whole-warn={wholeWarn}
      class:has-fn-toggle={functions !== null}
      oninput={onInput}
      onkeydown={onKeydown}
      onscroll={syncScroll}
      onkeyup={syncScroll}
      onpointerup={syncScroll}
      onblur={onBlur}
      spellcheck="false"
      autocomplete="off"
    />
  {/if}
  {#if functions && !resting}
    <button
      type="button"
      class="fn-toggle"
      tabindex="-1"
      onmousedown={(e) => e.preventDefault()}
      onclick={toggleBrowse}
      aria-label="Browse functions"
    >
      ƒ
    </button>
  {/if}
  {#if showDropdown}
    <ul
      class="suggestions"
      role="listbox"
      popover="manual"
      {@attach float(wordBox, { floor: LIST_FLOOR }, (place) => (placeList = place))}
    >
      {#if suggestions.length === 0}
        <li class="no-match">
          <span class="fn-desc">
            {#if functions}
              No function starts with &ldquo;{currentWordText}&rdquo; -- click <strong>ƒ</strong> to browse
              everything.
            {:else}
              No pattern starts with &ldquo;{currentWordText}&rdquo;.
            {/if}
          </span>
        </li>
      {/if}
      {#each suggestions as fn, i}
        <li
          role="option"
          aria-selected={i === highlighted}
          class:highlighted={i === highlighted}
          class:misfit={!!fn.warning}
          class:after-fields={!fn.field && suggestions[i - 1]?.field}
          onmousedown={(e) => e.preventDefault()}
          onclick={() => pick(fn)}
          onkeydown={(e) => e.key === 'Enter' && pick(fn)}
          onmouseenter={() => (highlighted = i)}
        >
          <span class="fn-head">
            <span class="fn-sig">{fn.signature}</span>
            {#if fn.docs}
              <a
                class="fn-docs"
                href={fn.docs}
                target="_blank"
                rel="noopener"
                onclick={(e) => e.stopPropagation()}>Docs &nearr;</a
              >
            {/if}
          </span>
          {#if fn.warning}
            <span class="fn-warn">&#9888; {fn.warning}</span>
          {/if}
          <span class="fn-desc"
            >{#each describedRuns(fn.description) as p, j (j)}{#if p.code}<code>{p.text}</code
                >{:else}{p.text}{/if}{/each}</span
          >
        </li>
      {/each}
    </ul>
  {/if}
</span>

<style>
  .field-wrap {
    position: relative;
    display: inline-flex;
    flex: 1;
    min-width: 0;
  }

  /* The padding and font stay pinned here, not taken from the shared field:
     the painted copy below must match them glyph for glyph. */
  input,
  textarea {
    width: 100%;
    box-sizing: border-box;
    padding: 0.45rem 0.55rem;
    font-family: var(--mono);
    font-size: var(--text-body);
  }

  textarea {
    resize: vertical;
    line-height: 1.4;
  }

  input.has-fn-toggle {
    padding-right: 1.5rem;
  }

  textarea.has-fn-toggle {
    padding-right: 1.6rem;
  }

  /* A left `wrap` field: its lines laid out in the script, drawn as the field draws its text. It sizes the field,
     and the textarea under it fills the field unseen, so a click, a focus or a reader still reaches the field
     itself. */
  .wrapview {
    display: block;
    pointer-events: none;
    width: 100%;
    box-sizing: border-box;
    text-align: left;
    padding: 0.45rem 0.55rem;
    font-family: var(--mono);
    font-size: var(--text-body);
    line-height: 1.4;
    white-space: pre;
    overflow: hidden;
    color: var(--ink);
    background: var(--field);
    border: 1px solid var(--border);
    border-radius: var(--radius);
  }

  .wrapped textarea.under {
    position: absolute;
    inset: 0;
    height: 100%;
    max-height: none;
    opacity: 0;
  }

  .wrapped {
    width: 100%;
  }

  /* Written in, a `wrap` field grows with its text, to the three lines it shows when left, then scrolls. */
  .wrapped textarea {
    field-sizing: content;
    min-width: 100%;
    max-height: calc(3 * 1.4em + 0.9rem + 2px);
    resize: none;
  }

  /* The colored copy sits under the input in the same box: the same border
     width, padding, font and line height, so each glyph lands where the
     input's own (transparent) glyph is. */
  .paint {
    position: absolute;
    inset: 0;
    margin: 0;
    box-sizing: border-box;
    border: 1px solid transparent;
    border-radius: var(--radius);
    padding: 0.45rem 0.55rem;
    color: var(--ink);
    font-family: var(--mono);
    font-size: var(--text-body);
    line-height: normal;
    white-space: pre;
    overflow: hidden;
    pointer-events: none;
  }

  /* An input clips its text at the content box; the copy clips at its
     padding box, so it trims the padding back off. A textarea scrolls under
     its padding, as the copy does. */
  .paint:not(.multiline .paint) {
    clip-path: inset(0 calc(0.55rem + 1px));
  }

  .paint.has-fn-toggle:not(.multiline .paint) {
    clip-path: inset(0 calc(1.5rem + 1px) 0 calc(0.55rem + 1px));
  }

  .multiline .paint {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    line-height: 1.4;
  }

  /* The field's ground, under the copy: the copy itself is clipped to the
     input's content box, which would leave its padding bare. */
  .colored {
    background: var(--field);
    border-radius: var(--radius);
  }

  .paint.has-fn-toggle {
    padding-right: 1.5rem;
  }

  .multiline .paint.has-fn-toggle {
    padding-right: 1.6rem;
  }

  .colored input,
  .colored textarea {
    position: relative;
    background-color: transparent;
    color: transparent;
    caret-color: var(--ink);
  }

  .colored input::placeholder,
  .colored textarea::placeholder {
    color: var(--dim);
  }

  /* Both reserve the scrollbar's gutter, so a scrollbar appearing in the
     textarea cannot rewrap its text away from the copy's. */
  .colored textarea,
  .multiline .paint {
    overflow-wrap: anywhere;
    scrollbar-gutter: stable;
  }

  .paint :global(.tok-keyword),
  .wrapview :global(.tok-keyword) {
    color: var(--tok-keyword);
  }
  .paint :global(.tok-function),
  .wrapview :global(.tok-function) {
    color: var(--tok-function);
  }
  .paint :global(.tok-string),
  .wrapview :global(.tok-string) {
    color: var(--tok-string);
  }
  .paint :global(.tok-number),
  .wrapview :global(.tok-number) {
    color: var(--tok-number);
  }
  .paint :global(.tok-literal),
  .wrapview :global(.tok-literal) {
    color: var(--tok-literal);
  }
  .paint :global(.tok-type),
  .wrapview :global(.tok-type) {
    color: var(--tok-type);
  }
  .paint :global(.tok-regex),
  .wrapview :global(.tok-regex) {
    color: var(--tok-regex);
  }
  .paint :global(.tok-key),
  .wrapview :global(.tok-key) {
    color: var(--tok-key);
  }
  .paint :global(.tok-comment),
  .wrapview :global(.tok-comment) {
    color: var(--dim);
    font-style: italic;
  }

  /* A wavy underline under the text, the marker every editor uses for "this
     does not compile", as the website's code reference draws it. A colored
     field's text is the painted copy, so the copy carries it. */
  input.field-error,
  textarea.field-error,
  .wrapview.field-error {
    border-color: var(--error);
  }

  input.field-error.whole,
  textarea.field-error.whole,
  .wrapview.whole,
  .whole .paint,
  .squiggle {
    text-decoration: underline wavy var(--error);
    text-decoration-thickness: 1.5px;
    text-underline-offset: 2px;
  }

  input.whole-warn,
  .wrapview.whole-warn,
  .squiggle.warn {
    text-decoration: underline wavy var(--warn);
    text-decoration-thickness: 1.5px;
    text-underline-offset: 2px;
  }

  .marks {
    color: transparent;
  }

  .fn-toggle {
    position: absolute;
    right: 3px;
    top: 50%;
    transform: translateY(-50%);
    background: none;
    background-color: var(--field);
    line-height: 1;
    border: none;
    color: var(--dim);
    font-style: italic;
    font-size: var(--text-label);
    padding: 0.1rem 0.3rem;
    cursor: pointer;
    border-radius: var(--radius);
  }

  .fn-toggle:hover {
    color: var(--ink);
  }

  .multiline .fn-toggle {
    top: 5px;
    transform: none;
  }

  .suggestions {
    color: var(--ink);
    min-width: 260px;
    max-width: min(380px, calc(100vw - 2rem));
    max-height: 280px;
    margin: 0;
    padding: 0.25rem;
    list-style: none;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    box-shadow: 0 4px 14px rgba(0, 0, 0, 0.18);
  }

  .suggestions li {
    display: flex;
    flex-direction: column;
    gap: 0.1rem;
    padding: 0.3rem 0.5rem;
    border-radius: var(--radius-card);
    cursor: pointer;
  }

  .suggestions li.highlighted {
    background: color-mix(in srgb, var(--ink) 12%, var(--panel));
  }

  .suggestions li.no-match {
    cursor: default;
  }

  .fn-sig {
    font-family: var(--mono);
    font-size: var(--text-label);
    font-weight: 600;
    color: var(--ink);
  }

  .fn-head {
    display: flex;
    align-items: baseline;
    gap: 0.5rem;
  }

  .fn-docs {
    margin-left: auto;
    flex-shrink: 0;
    font-size: var(--text-micro);
  }

  .fn-desc {
    font-size: var(--text-micro);
    color: var(--dim);
    display: -webkit-box;
    -webkit-box-orient: vertical;
    -webkit-line-clamp: 3;
    line-clamp: 3;
    overflow: hidden;
  }

  /* Fields first, then functions: a rule between them, as the eye skims. */
  .suggestions li.after-fields {
    margin-top: 0.25rem;
    border-top: 1px solid var(--border);
    padding-top: 0.45rem;
  }

  /* A function that does not take the type before the dot: offered, below every one that does. */
  .misfit .fn-sig {
    color: var(--dim);
  }

  .fn-warn {
    font-size: var(--text-micro);
    color: var(--warn);
  }

  .fn-desc code {
    font-family: var(--mono);
    color: var(--ink);
  }
</style>
