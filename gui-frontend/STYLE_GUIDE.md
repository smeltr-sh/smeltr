# smeltr visual system — Foundry

A workshop tool: graphite and steel chrome, so the molten accent is the
only warm thing on the page; square, dense, a hairline grid. One shape and
one set of faces carry five color palettes, Foundry the default. This file
gives the palette, type, logo, shape and restraint rules the GUI runs on.
The code is the source of truth: Foundry and the shared controls in
`website/src/lib/foundry/`, which the website uses too, and the app's own
palettes and type scale in `App.svelte`'s `:root`. If this file and the
code disagree, the code wins.

## Logo

A pour of molten metal, cut with a second droplet in the surface color
so it reads as liquid:

```html
<svg viewBox="0 0 64 64" width="18" height="18" fill="none">
  <path d="M32 8 C42 24 48 34 48 42 A16 16 0 1 1 16 42 C16 34 22 24 32 8 Z" style="fill: var(--accent)" />
  <path d="M32 22 C37 30 40 35 40 40 A8 8 0 1 1 24 40 C24 35 27 30 32 22 Z" style="fill: var(--bg)" />
</svg>
```

Both fills are theme tokens. The pour takes the palette's `--accent`; the
cutout takes the surface it sits on, which in the nav is the band. It appears inline at the head of the band
(`Brand.svelte`) and as `public/favicon.svg`, which hardcodes the two colors
because a standalone SVG has no custom-property scope.

Minimum 20px mark height; below that the cutout stops reading. Keep the
mark's own width clear on every side.

The wordmark is set text: lowercase `smeltr`, the display face, weight 600.

## Glyphs

A glyph is the conventional sign for its operation -- braces for a parser, a
magnifier for a lookup, a diamond for a branch -- drawn on the logo's 64-unit
grid, one path, `currentColor`. Any horizontal bar in it is drafted, wider at
the base: the house's one touch.

The control that opens a menu carries three such bars:

```html
<svg viewBox="0 0 64 64" width="18" height="18" fill="none" aria-hidden="true">
  <path d="M14 14 H50 L54 22 H10 Z M14 28 H50 L54 36 H10 Z M14 42 H50 L54 50 H10 Z" fill="currentColor" />
</svg>
```

18px in a compact icon control, as the brand mark is, and never below 16px,
where the gaps close. It is `src/MenuIcon.svelte`, and it is also the control
that folds the section rail, the section's own menu.

A glyph is drawn once and used wherever its operation is offered, so the sign
and the operation stay one thing: `src/PencilIcon.svelte` is edit -- open this
object's editor -- and `src/ColumnsIcon.svelte` is stats, how much went each
way. `src/GearIcon.svelte` is view -- how the records are drawn,
the reader's own choice. `src/TrashIcon.svelte` is remove -- take this object out of the list
it is in. `src/DisableIcon.svelte` is disable -- keep a step in its pipeline but
out of play. Renaming is not its own sign: it is the editor a worker group happens to
have, having nothing else to change in place.

A state can have a sign too, when the page routes on it. `src/FailedIcon.svelte`
is failed -- a row the pipeline could not process -- a triangle with a bar
standing in it. It goes wherever those rows are named or wired: the
pipeline's failure setting, and the routes page's account of how a path
takes failed rows. The caller colors it:
ink in a heading, `--warn` where the row's state is the point.

`src/stepGlyphs.ts` holds the step badges' paths, one per `StepType`; the
badge sets the glyph before its label at the label's own text size.

On the map a node's card says its role -- a stadium for a source or a
destination, the flowchart's terminal; a block for a pipeline -- and its
glyph says the system behind it, from `src/systemGlyphs.ts`, at 16px.
smeltr's own sources are the droplet, cut for what they carry; the plain
droplet is smeltr itself.

## Color

Two layers. Every component references a token by name, never a literal
color.

The **palette** is the chrome, the accent, the second color and the
comment note. Foundry is the bare `:root`
(`website/src/lib/foundry/palette.css`); each other palette, in
`App.svelte`, is a `[data-palette]` block on the root, one light and
one dark (see "Theme"). A palette changes colors only.

A palette is one base hue and an accent well round the wheel from it,
as Foundry is steel against molten orange. The base colors the band, and
in dark it tints the surfaces faintly; light surfaces keep Foundry's
near-white, so the band is the only large field of color. The accent keeps
clear of the state hues -- green, yellow and red -- so a primary action
never reads as a status.

| Token | Role |
|---|---|
| `--bg` | Page background |
| `--panel` | Grouped surface: cards, editor panes, menus |
| `--field` | Where typed values sit: an input, a step card at rest. The lightest surface in light, the darkest in dark |
| `--ink` | Primary text, prose included |
| `--dim` | What a reader may skip: a date, a count |
| `--border` | Hairlines, card and input outlines |
| `--accent` | The one primary action per view, the active tab's underline, focus rings, the open step's rail, critical text |
| `--on-accent` | Text on an accent-filled surface |
| `--band-bg`, `--band-ink`, `--band-dim`, `--band-line` | The nav's own surface, ink, dim and hairline: a dark band |
| `--band-accent`, `--band-on-accent` | Header action and label colors, fixed across light and dark |
| `--scope-l`, `--scope-c` | A group hue's lightness and chroma: the accent's own |
| `--counter` | The second color where two peers must be told apart |
| `--comment-bg`, `--comment-border`, `--comment-ink` | A note: the Comment step's sticky note, and the notes that borrow it |

| Palette | Character |
|---|---|
| Foundry | Graphite and steel, a molten orange accent. The default |
| Ember | Oxblood chrome, steel-blue actions, a teal second color, sand notes |
| Frost | Ice-blue chrome, aurora-pink actions, a lavender second color, pale-blue notes |
| Verdigris | Patina-green chrome, violet actions, a blue second color, moss notes |
| Amethyst | Violet chrome, teal actions, a pink second color, lilac notes |

The **fixed layer** means the same thing whatever the chrome, so it keeps
its values in every palette:

| Token | Light | Dark | Role |
|---|---|---|---|
| `--error` | `oklch(48% 0.2 25)` | `oklch(70% 0.19 25)` | Validation errors, danger actions |
| `--warn` | `oklch(52% 0.13 55)` | `oklch(78% 0.16 85)` | Warnings |
| `--added` | `oklch(46% 0.13 145)` | `oklch(66% 0.15 145)` | Diff view, added |
| `--removed` | `oklch(54% 0.18 25)` | `oklch(68% 0.17 20)` | Diff view, removed |
| `--changed` | `oklch(56% 0.15 42)` | `oklch(66% 0.16 42)` | Diff view, rewritten by a step |
| `--tok-*` | | | Code coloring, one per token kind (`website/src/lib/highlight/highlight.ts`) |

The `--tok-*` tokens color code in an expression field and on the
website, which copies them. Each kind takes its own hue, so no two kinds
blur together; numbers and the literals `true`, `false`, `null` share the
orange family at two hues.

A warning bar is `.warn-bar` (`app.css`): a `--warn` stripe on a light
`--warn` tint, with the text in `--ink`. `--warn` text on its own tint
reads poorly, so in a bar only the stripe and the glyph carry the color.

A tint over a surface mixes in oklab:
`color-mix(in oklab, var(--error) 12%, var(--panel))`. A surface is a
near-gray whose hue is the palette's base, and oklch blends hue on its
own, so a mix in it carries the base's hue into the tint: an error band
over Foundry's steel panel reads blue.

`--accent` lightens in dark mode to stay vivid; `--on-accent` exists
because a fixed white label would then lose contrast.

`--counter` is for the case where a panel has to tell two peers apart and the accent
can only speak for one of them: the Status page draws rows in against
rows out with it. Foundry's is the molten accent turned to the far side of
the wheel at the accent's own lightness and chroma, so the two carry equal
weight: hue 250 and not 230 or 240, which at this chroma fall outside
sRGB and paint clipped. Each other palette picks a hue apart from both
its accent and its base. Two colors of one lightness are near-invisible
against each other in grayscale, so a place that uses it needs a second
channel -- position, a label, a shape -- and must not lean on hue alone.

`--changed` holds the molten hue in both themes. A third diff color has
to sit apart from added and removed, and a hue invented for it would land
among the group hues, where a color means which group you are editing.

The Comment step has its own sticky note, distinct from `--warn` because
a note is not an alert. Foundry's is below; each other palette gives it a
hue of its own:

| Token | Light | Dark |
|---|---|---|
| `--comment-bg` | `oklch(93% 0.03 95)` | `oklch(24% 0.04 90)` |
| `--comment-border` | `oklch(75% 0.08 90)` | `oklch(45% 0.07 85)` |
| `--comment-ink` | `oklch(45% 0.09 85)` | `oklch(78% 0.09 88)` |

## Typography

Fixed across palettes. Each face is bundled from fontsource (`main.ts`), so
a leader with no route to a font CDN still draws them, and each has a real
fallback.

- Display: `--font-display`, Geist 600, tracked -0.025em at title sizes.
  The wordmark, every page's `h1` (set globally in `App.svelte`), section
  and card titles.
- Body: `--font-body`, IBM Plex Sans 400/500/600/700, set on `body`.
  Chrome and prose.
- Mono: `--mono`, IBM Plex Mono 400/500. Values only (see "Restraint").
- An uppercase label -- a step's kind, a column head, a dialog's header
  rule -- is the body face at `--text-micro`, weight 700, tracked 0.08em,
  `--dim`.

`oklch()` assumes an evergreen browser, an accepted tradeoff for an
internal tool.

Six sizes, one per role, defined in `:root`. A component names a token,
never a number, so nothing can land between two roles.

| Token | Size | Role |
|---|---|---|
| `--text-title` | 1.25rem | A page's `h1` |
| `--text-section` | 0.95rem | A heading inside a page |
| `--text-body` | 0.85rem | `body`, and running prose |
| `--text-label` | 0.8rem | A field's name |
| `--text-note` | 0.75rem | A field note, an explainer's body |
| `--text-micro` | 0.7rem | Pills, chips, counts |

`body` sets `--text-body`, so text that names no size is body text rather
than the browser's 16px.

## Text

Five registers carry every word the app writes. Each has one size and one
place, and **size never inverts against importance**: text you must read
to configure correctly is never smaller than text that is optional.

A page carries no subtitles and no explanation paragraphs. Most labels need
no help at all.

- **Page title.** Names the task. The page opens with its title and the
  controls or tabs needed to begin. A panel heading is a title too: neither
  has a subtitle.
- **Field label.** The field's name. `--text-label`, weight 600. A
  parenthetical that is part of the name -- `fetch buffer (bytes)` --
  belongs here; a parenthetical that explains does not. A field the save
  refuses empty ends in a `*`, in `--error`; one that may be left empty
  carries no mark.
- **Field note.** One line under the input, only for what must be known
  about the value: a choice that moves a source, a list with nothing yet
  to pick, a port the authority must reach. `--text-note`, weight **400**,
  in `--accent`. A field shows its default itself, as the value it starts
  with, its placeholder or its first choice, so no note repeats it. A
  default the field cannot show, a format or a limit goes in the
  explainer.
- **Explainer.** Behind a `?` beside the title or label it belongs to, and
  only where that label leaves out something a first-time user needs to
  use the control correctly. A `?` on a label that says it all is noise.
  `Help.svelte`: a native `popover`, so it draws in the top layer where no
  ancestor can clip it, and Escape and click-away come from the platform.
  It explains only the thing it sits beside, usually in one sentence, and
  never repeats another `?` or explains something else on the page. It holds
  `<code>` and emphasis. A field's opens with **Required.** or **Optional.**,
  and an optional one says what empty does where nobody could guess it.
- **Critical.** An instruction that must be read before acting: data is
  lost, a step cannot be undone, a condition blocks the task.
  `.critical` (`app.css`): `--accent`, weight 600. The only prose a page
  shows unasked, so it stays rare.

Status is not prose: an error, a result, a loading line, an empty state
show where they happen.

`title` carries the full text of a value the layout truncates, and nothing
else. A control that shows no text -- Delete, Move up, the remove cross --
takes an `aria-label`. Neither carries an explanation: an explanation a
keyboard or touch user cannot reach is not written down.

## Controls

A button is always the same button, a field always the same field: each
control has one definition, so a fix lands everywhere. A page places a
control; it never restyles one. A new look is a variant of the shared
definition, not a page's own rule.

- **Button.** Every `<button>` (`website/src/lib/foundry/controls.css`):
  outlined, `--bg`, `--text-label`, a hover that inks the border. `.btn`
  draws a link as one. Variants:
  - `primary` -- the one primary action in a view, accent-filled.
  - `danger` -- the answer that destroys something, filled `--error`. A
    button that only opens the question is a plain button.
  - `compact` -- an action inside a row of a list.
  - `large` -- a call to action on a page of the website.
- **Text field.** Every text `input`, `select` and `textarea`: `--field`
  on a 1px `--border`, `--text-body`, 1rem on a phone so it does not zoom.
  A value field may set the mono face. One whose value does not hold
  (`aria-invalid`) takes an `--error` border.
- **Field.** `Field.svelte`: the label, a `?` when the label leaves
  something out, the control, then its note or its error. `required` or
  `optional` says whether it may be left empty; a toggle or a select that
  always holds a value is neither.
- **Filter.** `Chips.svelte`: All, then one chip per value, each with its count.
- **Fold all.** `FoldAllButton.svelte`: one button that folds every item of a list or unfolds them all.
- **Card.** `Panel.svelte`, below.
- **Error.** In `--error`, drawn by one of three components, never by
  hand. `ErrorBar.svelte` shows what a call refused or failed, under the nav
  and atop a dialog. `InlineError.svelte` shows an error beside the control it
  belongs to. `Field.svelte` shows a field's own. The flow is in
  `docs/gui-design.md`, "Errors".

A control that is its own thing -- a tab, a menu row, a chip, a step's
head controls -- sets everything it draws in its own scoped rule, which
wins because the shared definitions sit at zero specificity.

## Cards

The one grouped-surface shape in the app: `--panel` background,
`--radius-card`, 1px `--border`. `Panel.svelte` draws it, with its title
in a head ruled off from the body. A dialog is one, and a page
whose content is one long settled form -- Admin's Configuration, once it
exists -- is one too, wrapping the whole column below the page header. The card is the unit that groups; what is inside it (a dialog's
header/body/footer, a settings page's own sections) is the card's own
business, not the shape's.

A step card is the exception that proves the surface rule: at rest it is
`--field` on the panel with no border, the plane typed values sit on. The
pipeline's step list sits on the panel for that reason, and an if step is
the panel its branches' steps sit on. Open,
it takes its `--border` and a 3px accent rail down its leading edge
(`box-shadow: inset 3px 0 0 var(--accent)`). An if step takes `--border`
and reads as a branch by its glyph and its columns; a comment step keeps its
dashed note border. The step driving the diff view is a 6% ink tint. A
step's summary is code, so it is ink mono.

In the diff view, an open record the step changed takes a 3px `--added`
rail and a 7% `--added` tint, as a diff marks a change. An open record it
left alone takes a `--border` rail and an ink tint.

## Secondary tabs

Where a page's content splits into named sections that do not each need
their own hash -- Configuration's General/Authentication/Network &
TLS/Storage, Build's Resources rail entry opening onto Samples/Lookup
tables/Connection identities -- a row of tabs sits directly under the
page header, above the card. `--text-body`, `--dim`; the active tab
is `--ink`,
weight 600, with a 3px `--accent` underline. Unlike the section rail,
which is the router's own answer to "where am I" and always a link, a
secondary tab may or may not carry its own hash -- Configuration's do, so
a tab is a bookmarkable link like everything else, never only client
state.

## Table sorting

A column head with a label sorts its table (`SortButton.svelte`,
`sort.svelte.ts`) wherever the table has more than one row, all of them on
the page, in an order that is not itself the content. History and Logs
page from the leader in time order, and a lookup table's grid keeps its
rows where they were typed.

A click sorts ascending, the next click descending, and a click on another
column starts that one ascending. A table opens in its own order and keeps
it until a head is clicked. The sorted head turns `--ink` and carries an
arrow; the others stay uppercase labels. A value a row lacks sorts last
either way, since it is missing rather than small. A column with no label
-- a row's actions -- does not sort.

## Lists

A page whose content is a set of objects -- Sources, Destinations -- shows
them as one table (`ObjectList.svelte`), however many there are and of
however many types:

- **A toolbar.** The add button, a search across every column, and a chip
  per type with its count where the list mixes types.
- **One row per object**, its columns sortable ("Table sorting"). A row
  folds open in place to edit it, and its other actions sit in its ⋯ menu.
  A row also opens when the map lands on it, when it was just added, and
  when a save error names it.
- **What `shared/` declares** sits among the group's own, marked with the
  shared hue's dot, and does not fold: its menu leads to `shared/`.

A row keeps its place while it is open. Editing a field the list is sorted
by does not move the row out from under the cursor; the order is taken
again when the sort, the search, a chip or the number of rows changes.

Adding opens a dialog ("Dialogs"). Where the object has a type, the dialog
starts at a catalog (`TypeCatalog.svelte`): a search, a rail of categories
once there are two, and a tile per type. Picking one shows that type's
form. A type is fixed once the object is added; its row shows it as text.

Where the types fall into kinds that change what the object is -- a
source read by all workers, by one, or by each on its own host -- the
catalog asks for the kind first, on a card per kind with its `?`. The
catalog then opens with that kind's chip on (`Chips.svelte`), each chip
carrying its own `?`, and each tile carries its kind's sign. A search
reaches every kind.

## Toggle switch

An on/off setting drawn as a switch (`Toggle.svelte`): a persistent
yes/no a page's own Save commits, not one choice among several in a
list -- a step's `disabled`, a route's `final`, never a set of rows to
pick from, which stays a checkbox. `role="switch"`, a 1px `--border`
track that fills `--accent` when on, a `--panel` thumb that slides to the
far side. No motion beyond that: flat, like everything else in "Shape
and spacing".

## Bottom save bar

A page whose Save can be scrolled out of view under a long enough form
gets a persistent bar instead of relying on one at the top (`SaveBar.svelte`):
fixed to the viewport's bottom edge, `--panel` background, a 1px
`--border` top edge, no shadow. Shown only while there is something to
save -- a control that is always there and usually inert teaches people
to stop reading it, the same call "The branch" makes for the branch
chip's own waiting-work row. It carries the same dirty flag and Save
button a page's own header used to, and replaces them rather than
duplicating them.

It also names the file the Save writes, as `branch · group`. An id is
unique only within a group and a group's bundle lives only on a branch,
so the same page under a different pair is a different file; the chips
that say which pair is in hand scroll away, and this does not.

## Dialogs

A dialog asks one question and takes one answer. It is for a question that
has to be settled before a page's own Save can happen at all, or for a new
item: what kind it is, then the fields it needs, then Add. It is never a
second commit point beside the page's Save (see "Advanced settings"): an
added item joins the page as an unsaved edit, and the page's Save writes
it. Cancel leaves no trace. An item already on the page is edited where it
is, never in a dialog.

A native `<dialog>` opened with `showModal()`: the focus trap, Escape and
the top layer come from the platform rather than from an effect. Its
backdrop dims to `oklch(0% 0 0 / 0.45)`, so it plainly blocks; a question
that does not look like it blocks is a worse question.

A dialog is a card ("Cards"). Anatomy, top to bottom:

- **A header rule.** What is being asked, in a word or two -- an uppercase
  label ("Typography") -- closed by a hairline. It names the dialog for
  a screen reader too (`aria-labelledby`).
- **The body.** Why, at `--text-body` in `--ink`, and the field that answers
  it where the answer needs one. A yes-or-no question is answered by its
  buttons and takes no field. A new item's body is its type catalog, then
  its form; the header carries a ‹ back to the catalog once a type is
  picked. The body scrolls on its own, so the header and footer stay put.
- **A footer rule.** A hairline, then the actions at the right: the refusal
  first and plain, the answer last and accented -- filled `--error` instead
  where it destroys something.

`Dialog.svelte` draws all of this; `Confirm.svelte` is its question.

Escape and the refusing action mean the same thing, and neither is a
failure. Nothing broke -- what was being written is simply not written, and
what says so says that rather than showing an error.

A dialog that holds a running thing (`CaptureDialog.svelte`) has one button
while it runs, the one that ends it, and Escape does nothing: there is
nothing to refuse yet. Once it has ended the dialog asks its question, with
the refusal and the answer as above.

## Menus

A menu is the choices belonging to the control that opened it, drawn over
the page from that control's own corner. The chip stays where it was: what
was pressed has to remain visible, or the list belongs to nothing.

**Hover means the row responds.** It tints toward `--ink` over the panel
it sits on -- `color-mix(in oklab, var(--ink) 10%, var(--panel))`. Tinting
toward `--bg` takes the color of the page *behind* the menu, which reads as
a hole rather than as emphasis.

**The row already chosen is ink and bold**, so it survives beside a hovered
neighbor. It does not respond to
hover: its cursor says pressing it does nothing, and a background that
brightened would say the opposite.

**Focus shows on every row, chosen or not** -- the same tint, plus a 2px
`--accent` ring at `outline-offset: -2px`. The ring sits inside the row
because the panel clips at its own rounded edge, and one drawn outside the
border box is cut off on the first and last rows. Arriving somewhere has to
be visible whether or not pressing it does anything.

A menu's last row may be a note rather than a choice -- what could not be
read, or what the menu cannot offer yet. It takes none of a choice's
affordances, because pressing it does nothing.

## The branch

The branch chip names what you are editing on. Beside it, where a branch has
work, the same row says what is waiting and offers the one thing to do about
it -- "1 save, not open for review yet" and the button that opens it. The
chip says on what, so the words do not repeat it.

It says what is waiting before it says what to do, because the count is the
reason to act; a button alone sends the reader looking for it. It appears
only when there is something to act on -- never on `main`, never on a branch
with nothing saved -- since a control that is always there and usually dead
teaches people to stop reading it.

A branch is not a page, so this belongs to the shell rather than an editor:
work saved from Sources and work saved from a pipeline are one change. A
group is not a page either, but it governs only Build's pages, so it stays
on the trail row instead. Two levels, two rows: what you are editing on
above, where you are and in which group below.

## Group hues

The Build tab edits one worker group's bundle at a time, and editing the wrong
one is a mistake nothing on the page catches until after the save. So the
group is a color, not a word: a 4px rail down the column's own gutter and a
tint on the trail row, drawn from `--scope-h` on the root element.

- One hue per group, at the accent's lightness and chroma (`--scope-l`,
  `--scope-c`, which the theme sets). A group is that color turned, not a
  second palette.
- The color rides the chrome. Never content, never a control: the accent
  still means "action" and nothing else does.
- New groups take their hues from one fixed order, thirty degrees apart round
  the whole wheel: blue, magenta and teal first, the hues most unlike each
  other; then green, yellow and red-orange; then the six between them. A
  later hue can sit near `--added`, `--warn` or `--error`; the order keeps
  those for after the first three. An admin may still pick any hue; the
  wheel marks error, warn, and the viewer's own palette accent
  (`--accent-h`). A group's hue is shared and a palette is not, so in a
  palette whose accent is not molten a group can share the accent's hue.
- A group's hue is its own, stored by the leader, so it never moves under
  someone who has learned it. `shared` takes the next default after the
  seeded group's.
- `--scope-h` is set only where a scope applies. With it absent every rule
  reading it is invalid at computed-value time and paints nothing, which is
  what leaves the rest of the app uncolored.

## Shape and spacing

Fixed across palettes.

- Radius: `--radius` (3px) for every control and row, `--radius-card`
  (4px) for a card, 999px for a pill.
- Borders: `1px solid var(--border)` everywhere. A comment step overrides
  to a dashed `var(--comment-border)`.
- The nav is a band: its row sits on `--band-bg`, the page on `--bg`, so
  the chrome reads as one strip and the work sits below it. The nav's own
  pieces take the band's tokens under the page's names; the menus they
  open are page surfaces.
- Flat: no `box-shadow`, no gradients on surfaces. The exceptions are the
  open step's inset rail, the detached overlays -- `ValidatedField`'s
  floating popups and `Help`'s popover -- and the diagonal stripe marking
  an invalid input, which is a texture.
- Width is the content's to choose, with `2rem 1.5rem 4rem` page padding
  either way. A form reads badly past a column, so the ones that are forms
  sit in 800px. A table or a graph takes the frame. The pipeline editor
  takes the full width: its own draggable divider, not a reading measure,
  decides how its two panes split the space.
- A step answers to its own width, which the divider sets. An if step sets
  THEN beside ELSE from 36rem, where a step in each branch still reads,
  and stacks them below that. At 18.75rem and under a step's head folds:
  the badge shows its glyph, ◎ stays, and the other controls go behind
  the More menu. Both widths are in rem, so they move with the reader's text
  size, as the controls do.
- One gutter. A page starts where the wordmark and the breadcrumb start,
  whatever width it then takes, so the three read as one column.
- One measure. `App.svelte` sets `--measure` on the root element from the
  current route -- `100%` for the pipeline editor, unset (so `--width-page`
  applies) everywhere else -- and the nav's rows and `.column` both read
  it, so neither can disagree with the other. A narrow thing takes its reading
  measure inside the column rather than shrinking it.

## Sections

Five: Status, Map, Build, Branches, Admin.
`nav.ts` holds the shape — the tabs, what each hash means, which sub-pages
each tab has, and the trail under the nav that says where you are, so a
section nav can never disagree with the router.

The nav carries two things that are not navigation. The branch chip names
the branch every config call is made against; switching reloads, because a
page holds what it read and what it read was branch-scoped. Beside it is a
slot for who is signed in, empty while the operator surface is one shared
token -- a plain person glyph until there is a name to show.

The breadcrumb (`App.svelte`'s `.crumbs`, from `nav.ts`'s `trail`) is
shown on every page, section landing pages included: it is a second,
smaller answer to "where am I" beside the rail, useful on its own once a
rail has scrolled out of view or is narrower than the page. Its first
crumb links back to the section only when something follows it; the last
crumb is always the page itself and never a link.

A tab with sibling pages gets a rail (`SectionShell.svelte`) naming them,
current one marked, with the page itself beside it. The rail opens with
the section's own name as a heading, then the scope picker beneath it
where a page has one (`nav.ts`'s `showsScope`) -- both inside the same
bordered head block, since the scope only ever qualifies that section. Every
item carries an icon: Admin's are its pages' own components, the rest come
from `src/railGlyphs.ts`. The row already
on gets a left bar in `--accent` -- the rail is the section's tab strip,
and this is its active tab -- and an ink tint behind bold text, not a
filled button --
so an unhovered rail reads like the rest of the app's choice lists.
The rail folds to its icons and the menu control that unfolds it, giving
its width to the page; folded or not holds across sections and reloads.

## Theme

Two choices, both in the menu under the identity chip: light or dark
(System, Light, Dark), and the palette (Foundry, Ember, Frost, Verdigris,
Amethyst). A theme is chosen once and then forgotten, so it does not hold a
place on every screen; it belongs with the person using the app rather than
with the page they are on. Both persist in `localStorage`, as
`smeltr-theme` and `smeltr-palette`.

The root always carries a resolved `data-theme`, `light` or `dark`: System
follows `prefers-color-scheme` and keeps following it. So a palette is two
blocks, `[data-palette]` and `[data-palette][data-theme='dark']`, and no
media query. `index.html` sets both attributes before first paint;
`App.svelte` keeps them current. Foundry sets no `data-palette`. The
website sets `data-theme` only when its reader picks one, so
`palette.css` also follows the system under a media query.

## Restraint

Six rules that keep the accent meaning something.

1. **The accent is spent once per view**: the primary action. Links in prose
   are ink, underlined. Eyebrows, step numbers, arrows, glyphs and type
   badges are `--dim`. The active tab is ink and bold with a 3px accent
   underline: the one accent on a page with no primary action. Focus rings
   and the open step's rail are not spending. A selected or chosen thing is
   an ink tint, never an accent one.
2. **Prose is ink.** `--dim` is for a date, a count.
   A paragraph is never dim.
3. **A border is a boundary.** Two levels of container below the page.
   Lists are rows parted by hairlines or space, not stacks of cards.
4. **Mono is for values**: a name, an id, code, a number someone copies.
   Crumbs, placeholders, column heads and the branch chip are the body face.
5. **Controls arrive when wanted.** A step's head controls show on hover,
   on focus within the step, while it is open, and while one of them is on;
   where nothing hovers they always show. The step forms stay.
6. **One thing is big.** A page has one large element; the wordmark lives
   in the nav only.

Code colors color code only; nothing inside code is a control; inline code
is uncolored; state sits beside the line, not in it.

## Principles

- One accent, used once.
- Color is semantic: each token maps to one meaning.
- Monospace for values; sans for chrome and prose; the display face for
  titles.
- Square and dense, not cramped: `--text-body` throughout, and enough
  padding around a row or a field that pointing at it is comfortable rather
  than a precision exercise. A config tool, not a marketing surface -- a
  reader spends whole sessions here.

## Code formatting

Prettier, gated by `just check`. `singleQuote: true` and `printWidth: 100`,
chosen to match how the code was already written. `.prettierignore` exempts
Markdown: Prettier reflows fenced code, and this file quotes the logo's
SVG verbatim from `App.svelte` so the two can be read against each other.

## Advanced settings

A setting whose default is right for nearly every deployment goes in an
`<Advanced>` disclosure inside the section it qualifies, collapsed by
default. A disclosure, not a modal: a page has one commit point, its
Save button, and a modal's own OK/Cancel would be a second one.

1. Advanced means "has a default that is usually right", not "rarely
   used". A field with no default is an ordinary field.
2. It lives inside the thing it qualifies, never on a global settings
   page.
3. The collapsed header says when a setting is not at its default.
   `<Advanced>` takes a `modified` flag for this, or `values`, which
   names each such setting and its value. A disclosure that can hide a
   live value makes the visible config a lie. For the same reason it
   opens, and its header counts them, when a field inside has an error
   (`errors`).
4. Every field shows its default, in the words the code uses: as the
   value it starts with, its placeholder or its first choice, or, where
   it can show none of these, in its explainer.

