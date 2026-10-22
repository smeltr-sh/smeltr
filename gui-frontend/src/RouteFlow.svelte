<!--
  Every route from one source as one flow card: the source, each route's condition, its pipeline, then its
  delivery rules out to destination pills (`routeFlow.ts`).

  One route to one destination is drawn as a line, left to right. Anything more is drawn top to bottom, so the
  card does not read like the map's left-to-right columns; the boxes glide between the two shapes. A condition
  nobody has written is a [+] on its line: the line from the source to a pipeline holds the route's condition,
  and a line from a pipeline to a destination holds a delivery rule's `when`.

  Wiring is direct. Drag a socket onto a pill, or a pill onto a pipeline or a condition, to connect them; a click on
  a socket opens the same choice as a list, so the keyboard has the same reach. A line cuts from the cross that
  hovering it shows. `+` beside "destination" brings a configured destination onto the card: destinations are made
  on their own page, or, where the caller offers it, made there and then (`onnewdest`). A pipeline is chosen the
  same way, or, where the caller offers it, made in the pipeline editor (`onnewpipeline`).
-->
<script lang="ts">
  import { tick, untrack } from 'svelte';
  import { SvelteSet } from 'svelte/reactivity';
  import Help from './Help.svelte';
  import PickOrNew from './PickOrNew.svelte';
  import ValidatedField from './ValidatedField.svelte';
  import { pageValidation, type Placed } from './validation.svelte';
  import type { Attachment } from 'svelte/attachments';
  import { editable } from './access';
  import type { DeliverRuleDto, RouteDto } from './api';
  import { answerText, type Answer } from './exceptions';
  import {
    at,
    canMove,
    centerOver,
    corners,
    curve,
    gate,
    gated,
    halfway,
    isLine,
    moveDest,
    offCard,
    ruleName,
    path,
    pills,
    plain,
    pushApart,
    removeDest,
    setPlain,
    spots,
    tidy,
    type Spot,
    toggle,
    ungate,
    unsent,
    type Point,
  } from './routeFlow';

  let {
    routes,
    sourceIds,
    pipelineIds,
    destinationIds,
    label,
    pipelineLabel = label,
    unmatched = () => 'block',
    extras = $bindable([]),
    onaddroute,
    onremoveroute,
    onorderroute,
    onmoveroute,
    onsource,
    onnewdest,
    onnewpipeline,
    onnewsource,
  }: {
    /** The card's routes: every route of one source. */
    routes: RouteDto[];
    sourceIds: string[];
    pipelineIds: string[];
    destinationIds: string[];
    /** An option's text: the id, marked where `shared/` declares it. */
    label: (id: string) => string;
    /** An option's text for a pipeline: its name. */
    pipelineLabel?: (id: string) => string;
    /** How `route` answers `Unmatched`, which takes the rows its delivery rules decline. */
    unmatched?: (route: RouteDto) => Answer;
    /** Destinations on the card that no rule sends to (`routeFlow.ts`). The caller's, so they outlive the card
     * body a collapse unmounts. Only ever reassigned here, never mutated: the caller may hand in a fresh array. */
    extras?: string[];
    /** A new route from this card's source, its pipeline still to choose. */
    onaddroute?: () => void;
    onremoveroute?: (route: RouteDto) => void;
    /** Moves a route one place left (`by` -1) or right among this card's, with its destinations: the caller
     * holds the routes' order, which is the card's. */
    onorderroute?: (route: RouteDto, by: number) => void;
    /** One route left this card for `source`'s: the caller keeps it open there. */
    onmoveroute?: (source: string) => void;
    /** The card's routes moved to `source`, so the card is now that source's: the caller keeps it open there. */
    onsource?: (source: string) => void;
    /** Offered under the destinations to bring on, where the caller can make one there and then. The caller
     * hands the new destination's id to `place`, which puts it where it was asked for. */
    onnewdest?: (place: (dest: string) => void) => void;
    /** Offered last among a route's pipelines, where the caller can have one made and come back with it. */
    onnewpipeline?: (route: RouteDto) => void;
    /** Offered last among the sources, where the caller can make one there and then. The caller hands the new
     * source's id to `place`, which makes it the card's. */
    onnewsource?: (place: (source: string) => void) => void;
  } = $props();

  let source = $derived(routes[0]?.source ?? '');
  let onCard = $derived(pills(routes, extras));
  /** The routes and rules whose condition is being written: a blank one is no condition, and its field must not
   * vanish under the hand that is about to write one. */
  const writing = new SvelteSet<object>();
  const showsConditionText = (r: RouteDto) => (r.condition ?? '').trim() !== '';
  const showsCondition = (r: RouteDto) => showsConditionText(r) || writing.has(r);
  const boxed = (rule: DeliverRuleDto) => gated(rule) || writing.has(rule);
  let cardSpots = $derived(spots(routes, extras, boxed));
  const boxes = (r: RouteDto) => (r.deliver ?? []).filter(boxed);
  let line = $derived(isLine(routes, extras, routes.flatMap(boxes)));

  /** Moving one route splits it off the card: cards are its routes grouped by source. */
  function moveRoute(route: RouteDto, s: string) {
    if (s === route.source) return;
    route.source = s;
    onmoveroute?.(s);
  }
  function setSource(s: string) {
    if (s === source) return;
    for (const r of routes) r.source = s;
    onsource?.(s);
  }
  /** A pill whose last edge was cut stays on the card, warned, rather than vanishing under the hand. */
  function keep(dest: string) {
    if (unsent(routes, dest) && !extras.includes(dest)) extras = [...extras, dest];
  }
  function setEdge(rule: DeliverRuleDto, dest: string, on?: boolean) {
    toggle(rule, dest, on);
    keep(dest);
  }
  function setLine(route: RouteDto, dest: string, on: boolean) {
    setPlain(route, dest, on);
    keep(dest);
  }
  function bring(dest: string) {
    if (!onCard.includes(dest)) extras = [...extras, dest];
  }
  /** Takes a pill off: under a pipeline, that pipeline stops sending to it; brought on and unwired, it leaves the
   * card. */
  function takeOff(spot: Spot) {
    const route = routes[spot.at];
    const rule = typeof spot.via === 'number' ? route?.deliver?.[spot.via] : undefined;
    if (rule) toggle(rule, spot.dest, false);
    else if (route) setPlain(route, spot.dest, false);
    extras = extras.filter((d) => d !== spot.dest);
  }
  /** Moves a pill among its pipeline's until it has taken the place of the pill at `target`. */
  function moveTo(spot: Spot, target: number) {
    const route = routes[spot.at];
    if (!route) return;
    // The pill is followed by what feeds it, which a move does not change; its place does.
    const rule = typeof spot.via === 'number' ? route.deliver?.[spot.via] : undefined;
    const place = () =>
      spots([route], [], boxed).find(
        (x) =>
          x.dest === spot.dest &&
          (rule ? x.via !== 'p' && route.deliver?.[x.via] === rule : x.via === 'p'),
      )?.k ?? -1;
    const by = Math.sign(target - spot.k);
    for (let n = 0; n < spots([route], [], boxed).length; n++) {
      const k = place();
      if (k < 0 || (by < 0 ? k <= target : k >= target)) return;
      moveDest(route, k, by, boxed);
    }
  }
  /** `+ destination`: on a card of one route it is wired straight from its pipeline; with several, it waits on
   * the card for a wire. */
  function arrive(dest: string) {
    if (routes.length === 1) setLine(routes[0], dest, true);
    else bring(dest);
  }
  /** `+ New destination…`: what was made comes onto the card, and `then` wires it where it was asked for. */
  function newDest(then?: (dest: string) => void) {
    onnewdest?.((dest) => {
      bring(dest);
      then?.(dest);
    });
  }

  async function writeCondition(route: RouteDto) {
    writing.add(route);
    await tick();
    flowEl
      ?.querySelector<HTMLInputElement>(`[data-node="c:${routes.indexOf(route)}"] input`)
      ?.focus();
  }
  async function writeWhen(route: RouteDto, dest: string) {
    const rule = gate(route, dest, boxed);
    writing.add(rule);
    await tick();
    const i = routes.indexOf(route);
    const j = (route.deliver ?? []).indexOf(rule);
    flowEl?.querySelector<HTMLInputElement>(`[data-node="w:${i}:${j}"] input`)?.focus();
  }
  function setCondition(route: RouteDto, text: string) {
    route.condition = text.trim() === '' ? null : text;
    validation?.edited(route.id, 'condition');
  }
  function setWhen(route: RouteDto, rule: DeliverRuleDto, text: string) {
    if (text.trim() === '') delete rule.when;
    else rule.when = text;
    // A save still out is answered about the rules as they were sent: its errors are found by position.
    if (validation?.saving) {
      validation.edited(route.id, 'deliver.when', (route.deliver ?? []).indexOf(rule));
    } else validation?.drop(ruleErrors.get(rule) ?? []);
  }

  /** The routes page's save errors (`validation.svelte.ts`): a route's error is on its condition, or on a rule's
   * `when` by the rule's position. */
  const validation = pageValidation();
  /** Each rule's errors, found by position when the save is answered and kept by the rule from then on: a rule
   * added, moved or removed after leaves every mark on its own rule. */
  const ruleErrors = $derived.by(() => {
    void validation?.answered;
    return untrack(() => {
      const byRule = new Map<DeliverRuleDto, Placed[]>();
      for (const e of validation?.placed ?? []) {
        const rule =
          e.rule === null ? undefined : routes.find((r) => r.id === e.item.id)?.deliver?.[e.rule];
        if (rule && e.field === 'deliver.when') byRule.set(rule, [...(byRule.get(rule) ?? []), e]);
      }
      return byRule;
    });
  });
  /** What a rule's `when` was refused for, or null. */
  function whenError(rule: DeliverRuleDto): string | null {
    const errors = (ruleErrors.get(rule) ?? []).filter((e) => validation?.placed.includes(e));
    return errors.length > 0 ? errors.map((e) => e.message).join('\n') : null;
  }
  /** A condition box as the place its save errors are focused: a route's condition, or each error tied to `rule`. */
  function errorPlace(route: RouteDto, rule?: DeliverRuleDto): Attachment<HTMLElement> {
    return (box) => {
      const focus = () => box.querySelector('input')?.focus();
      if (!validation) return;
      if (!rule) return validation.mount(route.id, 'condition', focus);
      const off = (ruleErrors.get(rule) ?? []).map((e) =>
        validation.mount(route.id, 'deliver.when', focus, e.rule),
      );
      return () => off.forEach((f) => f());
    };
  }
  function dropCondition(route: RouteDto) {
    writing.delete(route);
    route.condition = null;
    validation?.edited(route.id, 'condition');
  }
  function dropWhen(route: RouteDto, rule: DeliverRuleDto) {
    writing.delete(rule);
    ungate(route, rule, boxed);
    validation?.drop(ruleErrors.get(rule) ?? []);
  }
  /** Focus leaving a condition left blank folds it back to its [+]. Read once focus has landed, so a press on the
   * box's own cross is the cross's. */
  function leave(box: Element | null, done: () => void) {
    requestAnimationFrame(() => {
      const at = document.activeElement;
      if (!(at && (box?.contains(at) || menuEl?.contains(at)))) done();
    });
  }
  function leaveCondition(e: FocusEvent, route: RouteDto) {
    const box = (e.currentTarget as HTMLElement).closest('.box');
    leave(box, () => {
      if (writing.has(route) && !showsConditionText(route)) writing.delete(route);
    });
  }
  function leaveWhen(e: FocusEvent, route: RouteDto, rule: DeliverRuleDto) {
    const box = (e.currentTarget as HTMLElement).closest('.box');
    leave(box, () => {
      if (writing.has(rule) && !gated(rule)) dropWhen(route, rule);
      else writing.delete(rule);
    });
  }

  // -- edges ------------------------------------------------------------------------------------------------

  let flowEl: HTMLDivElement | undefined = $state();

  /** How far from a box's socket a vertical card's line holds its controls: clear of the socket, in the gap
   * between the rows. */
  const GAP = 24;
  type Action = { at: Point; act: () => void };
  type Edge = { key: string; d: string; plus?: Action & { name: string }; cut?: Action };
  let edges: Edge[] = $state([]);
  /** The edge a tap picked, showing its cross. Hover shows it too. */
  let shown: string | null = $state(null);

  function node(key: string): HTMLElement | undefined {
    for (const el of flowEl?.querySelectorAll<HTMLElement>('[data-node]') ?? []) {
      if (el.dataset.node === key) return el;
    }
  }
  /** Where a line leaves (`out`) or enters a box: its right or left side on a line, its foot or head on a
   * vertical card. */
  function port(el: Element, out: boolean): Point {
    const r = el.getBoundingClientRect();
    const f = flowEl!.getBoundingClientRect();
    if (line) return { x: (out ? r.right : r.left) - f.left, y: r.top + r.height / 2 - f.top };
    return { x: r.left + r.width / 2 - f.left, y: (out ? r.bottom : r.top) - f.top };
  }

  /** Where each condition box sits in its row on a vertical card, by its `data-node`: its center's x within
   * the row. */
  let slots: Record<string, number> = $state({});
  let whensEl: HTMLDivElement | undefined = $state();
  /** How far apart two boxes' lines cross over below them. */
  const LANE = 8;
  /** The least room between two condition boxes side by side. */
  const BOX_GAP = 12;

  /** An element's x within the card as laid out, before any glide moves it: so a box placed from it does not
   * chase a pill mid-flight. */
  function layoutX(el: HTMLElement): number {
    let x = 0;
    for (
      let e: HTMLElement | null = el;
      e && e !== flowEl;
      e = e.offsetParent as HTMLElement | null
    )
      x += e.offsetLeft;
    return x;
  }
  const middle = (el: HTMLElement) => layoutX(el) + el.offsetWidth / 2;

  /** Each condition box sits over the middle of its destinations, or under its pipeline when it sends nowhere;
   * boxes that would overlap push each other aside. */
  function placeBoxes(): boolean {
    if (line || !whensEl) return false;
    const left = layoutX(whensEl);
    const keys: string[] = [];
    const want: number[] = [];
    routes.forEach((route, i) =>
      (route.deliver ?? []).forEach((rule, k) => {
        if (!boxed(rule)) return;
        const xs = rule.to.flatMap((d) => {
          const el = node(`d:${i}:${k}:${d}`);
          return el ? [middle(el)] : [];
        });
        const pipe = node(`p:${i}`);
        keys.push(`w:${i}:${k}`);
        want.push((xs.length ? centerOver(xs) : pipe ? middle(pipe) : left) - left);
      }),
    );
    const width = whensEl.querySelector<HTMLElement>('.slot')?.offsetWidth ?? 0;
    const xs = pushApart(want, width, BOX_GAP, 0, whensEl.offsetWidth);
    const next = Object.fromEntries(keys.map((key, j) => [key, Math.round(xs[j])]));
    const same =
      keys.length === Object.keys(slots).length && keys.every((key) => slots[key] === next[key]);
    if (!same) slots = next;
    return !same;
  }

  /** Edges are drawn from where the boxes landed, so they follow the layout and its glide. Lines turn in
   * elbows, as the map's do. */
  function measure() {
    if (!flowEl) return;
    if (placeBoxes()) void tick().then(measure);
    const down = !line;
    const out: Edge[] = [];
    const src = node('src');
    const f = flowEl.getBoundingClientRect();
    const rect = (el: Element) => {
      const r = el.getBoundingClientRect();
      return {
        left: r.left - f.left,
        right: r.right - f.left,
        top: r.top - f.top,
        bottom: r.bottom - f.top,
      };
    };
    // A vertical card's condition row: the lines into its boxes cross over above it, those out of them below it,
    // and a plain line runs down through it beside the boxes.
    const row = !line && whensEl ? rect(whensEl) : null;
    const boxes = row
      ? [...whensEl!.querySelectorAll<HTMLElement>('[data-node]')].map((el) => rect(el))
      : [];
    const order = new Map(
      Object.entries(slots)
        .sort((a, b) => a[1] - b[1])
        .map(([key], n) => [key, n]),
    );
    const draw = (key: string, pts: Point[]) => {
      const e: Edge = { key, d: path(pts) };
      out.push(e);
      return { e, pts };
    };
    const join = (
      key: string,
      a: HTMLElement | undefined,
      b: HTMLElement | undefined,
      mid?: number,
    ) => {
      if (!a || !b) return null;
      return draw(key, corners(port(a, true), port(b, false), down, mid));
    };
    /** The x a plain line runs down the condition row at: straight above its pill, or just past the box in the
     * way, on the side nearer the pill. */
    const lane = (x: number) => {
      for (let n = 0; n < boxes.length; n++) {
        const hit = boxes.find((b) => x > b.left - 6 && x < b.right + 6);
        if (!hit) return x;
        x = x < (hit.left + hit.right) / 2 ? hit.left - 8 : hit.right + 8;
      }
      return x;
    };
    routes.forEach((route, i) => {
      const pipe = node(`p:${i}`);
      const cond = node(`c:${i}`);
      if (cond) {
        join(`s${i}`, src, cond);
        join(`c${i}`, cond, pipe);
      } else {
        const j = join(`s${i}`, src, pipe);
        if (j && !readOnly)
          j.e.plus = {
            at: halfway(j.pts),
            name: 'add a condition',
            act: () => void writeCondition(route),
          };
      }
      const above = (p: Point) => (row ? (p.y + row.top) / 2 : undefined);
      for (const dest of plain(route, boxed)) {
        const pill = node(`d:${i}:p:${dest}`);
        if (!pipe || !pill) continue;
        const p = port(pipe, true);
        const q = port(pill, false);
        const pts = row
          ? (() => {
              const x = lane(q.x);
              const y1 = above(p)!;
              const y2 = (row.bottom + q.y) / 2 - ((order.size + 1) / 2) * LANE;
              return tidy([p, { x: p.x, y: y1 }, { x, y: y1 }, { x, y: y2 }, { x: q.x, y: y2 }, q]);
            })()
          : corners(p, q, down);
        const j = draw(`${i}>${dest}`, pts);
        if (readOnly) continue;
        // On a vertical card the row between holds condition boxes, so the line's controls sit in the gaps
        // around it: the [+] above the pill, the cross under the pipeline.
        j.e.plus = {
          at: down ? at(pts, -GAP) : halfway(pts),
          name: 'add a condition',
          act: () => void writeWhen(route, dest),
        };
        j.e.cut = {
          at: down ? at(pts, GAP) : halfway(pts, 0.82),
          act: () => setLine(route, dest, false),
        };
      }
      (route.deliver ?? []).forEach((rule, k) => {
        if (!boxed(rule)) return;
        const box = node(`w:${i}:${k}`);
        if (pipe && box) join(`${i}:${k}`, pipe, box, above(port(pipe, true)));
        for (const dest of rule.to) {
          const pill = node(`d:${i}:${k}:${dest}`);
          // Each box's lines cross over at a height of their own, so two boxes' lines never share a run.
          const below =
            row && pill
              ? (row.bottom + port(pill, false).y) / 2 +
                ((order.get(`w:${i}:${k}`) ?? 0) - (order.size - 1) / 2) * LANE
              : undefined;
          const j = join(`${i}:${k}>${dest}`, box, pill, below);
          if (j && !readOnly)
            j.e.cut = {
              at: down ? at(j.pts, -GAP) : halfway(j.pts),
              act: () => setEdge(rule, dest, false),
            };
        }
      });
    });
    edges = out;
  }

  $effect(() => {
    // Read so the effect reruns on any change the layout follows.
    JSON.stringify(routes.map((r) => [r.condition, r.pipeline, r.deliver]));
    void onCard.length;
    void writing.size;
    void line;
    void tick().then(measure);
  });
  $effect(() => {
    if (!flowEl) return;
    const ro = new ResizeObserver(() => measure());
    ro.observe(flowEl);
    return () => ro.disconnect();
  });

  // -- the glide between a line and a vertical card -----------------------------------------------------------

  // Each box flies from where it was to where it lands, so the turn between the two shapes reads as one motion.
  let wasLine: boolean | null = null;
  let from = new Map<string, DOMRect>();
  $effect.pre(() => {
    void line;
    from = new Map(
      [...(flowEl?.querySelectorAll<HTMLElement>('[data-node]') ?? [])].map((el) => [
        el.dataset.node ?? '',
        el.getBoundingClientRect(),
      ]),
    );
  });
  $effect(() => {
    const now = line;
    if (wasLine === null || wasLine === now) {
      wasLine = now;
      return;
    }
    wasLine = now;
    if (matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    void tick().then(() => {
      const ms = 450;
      for (const el of flowEl?.querySelectorAll<HTMLElement>('[data-node]') ?? []) {
        const a = from.get(el.dataset.node ?? '');
        const b = el.getBoundingClientRect();
        const frames = a
          ? [
              {
                transform: `translate(${a.left + a.width / 2 - (b.left + b.width / 2)}px, ${
                  a.top + a.height / 2 - (b.top + b.height / 2)
                }px) scale(${a.width / b.width}, 1)`,
              },
              { transform: 'none' },
            ]
          : [{ opacity: 0 }, { opacity: 1 }];
        el.animate(frames, { duration: ms, easing: 'cubic-bezier(.2,.8,.2,1)' });
      }
      // The lines follow the boxes through the glide.
      const end = performance.now() + ms;
      const step = () => {
        measure();
        if (performance.now() < end) requestAnimationFrame(step);
      };
      requestAnimationFrame(step);
    });
  });

  // -- menus ------------------------------------------------------------------------------------------------

  type Menu =
    | { kind: 'to'; route: RouteDto; rule?: DeliverRuleDto }
    | { kind: 'from'; spot: Spot }
    | { kind: 'add'; then?: (dest: string) => void }
    | { kind: 'pipe'; route: RouteDto }
    | { kind: 'move'; route: RouteDto }
    | { kind: 'dest'; spot: Spot };
  let menu: (Menu & { at: DOMRect }) | null = $state(null);
  let menuEl: HTMLDivElement | undefined = $state();

  /** What opened the menu, so a keyboard close hands focus back to it rather than to the page. */
  let trigger: HTMLElement | null = null;

  function openMenu(m: Menu, at: DOMRect, opener?: HTMLElement) {
    if (opener) trigger = opener;
    menu = { ...m, at };
    void tick().then(() => {
      if (!menuEl || !menu) return;
      place(menu.at);
      // A menu with nothing to choose takes focus itself, so focus can leave it and close it.
      (menuEl.querySelector<HTMLElement>('button') ?? menuEl).focus();
    });
  }
  /** The menu is fixed, so it is placed from the anchor's box and pulled back inside the window; it flips above
   * the anchor when below runs off the bottom. */
  function place(at: DOMRect) {
    if (!menuEl) return;
    const w = menuEl.offsetWidth;
    const h = menuEl.offsetHeight;
    menuEl.style.left = `${Math.max(8, Math.min(at.left, window.innerWidth - w - 8))}px`;
    const below = at.bottom + 4;
    const flip = below + h > window.innerHeight - 8 && at.top - h - 4 > 8;
    // Its height is capped to the window, so a menu that fits neither side still lands wholly inside it.
    const top = flip ? at.top - h - 4 : below;
    menuEl.style.top = `${Math.max(8, Math.min(top, window.innerHeight - h - 8))}px`;
  }
  function closeMenu(restoreFocus = false) {
    const opener = trigger;
    trigger = null;
    menu = null;
    // A choice that lit its box is gone with the menu, and a removed element sends no pointerleave.
    overKey = null;
    if (restoreFocus && opener?.isConnected) opener.focus();
  }
  /** Whether `route`, or its rule `rule`, sends to `dest`. */
  function sends(route: RouteDto, rule: DeliverRuleDto | undefined, dest: string): boolean {
    return rule ? rule.to.includes(dest) : plain(route, boxed).includes(dest);
  }
  function flip(route: RouteDto, rule: DeliverRuleDto | undefined, dest: string) {
    if (rule) setEdge(rule, dest);
    else setLine(route, dest, !plain(route, boxed).includes(dest));
  }

  // -- dragging ---------------------------------------------------------------------------------------------

  /** The dashed line under the pointer while a wire is being drawn. */
  let dragLine: { a: Point; b: Point } | null = $state(null);
  /** What the pointer is over that would take the wire, by its `data-drop`. */
  let overKey: string | null = $state(null);

  type Wire = {
    from: () => Point;
    targets: string;
    drop: (el: HTMLElement) => void;
    skip?: (e: PointerEvent) => boolean;
  };

  /** Drag from `el` onto an element matching `targets`. A press that does not move stays a click. */
  function wire(el: HTMLElement, opts: Wire) {
    let o = opts;
    let start: Point | null = null;
    let moved = false;
    let dragged = false;
    let target: HTMLElement | null = null;
    const down = (e: PointerEvent) => {
      if (e.button !== 0 || o.skip?.(e)) return;
      e.stopPropagation();
      el.setPointerCapture(e.pointerId);
      start = { x: e.clientX, y: e.clientY };
      moved = false;
    };
    const move = (e: PointerEvent) => {
      if (!start) return;
      if (!moved && Math.hypot(e.clientX - start.x, e.clientY - start.y) < 4) return;
      if (!moved) {
        moved = true;
        closeMenu();
      }
      const f = flowEl!.getBoundingClientRect();
      const a = o.from();
      const b = { x: e.clientX - f.left, y: e.clientY - f.top };
      const forward = line ? a.x <= b.x : a.y <= b.y;
      dragLine = forward ? { a, b } : { a: b, b: a };
      const hit = document.elementFromPoint(e.clientX, e.clientY);
      target = hit instanceof Element ? hit.closest<HTMLElement>(o.targets) : null;
      // Another open card has the same targets; its indexes would name this card's routes.
      if (target && !flowEl!.contains(target)) target = null;
      overKey = target?.dataset.drop ?? null;
    };
    const end = (e: PointerEvent) => {
      if (!start) return;
      start = null;
      el.releasePointerCapture?.(e.pointerId);
      if (!moved) return;
      const hit = target;
      dragLine = null;
      overKey = null;
      target = null;
      dragged = true;
      if (hit && e.type !== 'pointercancel') o.drop(hit);
    };
    // A drag ends with a click on the element; that one is the drag's, not a click.
    const click = (e: MouseEvent) => {
      if (!dragged) return;
      dragged = false;
      e.stopImmediatePropagation();
      e.preventDefault();
    };
    el.addEventListener('pointerdown', down);
    el.addEventListener('pointermove', move);
    el.addEventListener('pointerup', end);
    el.addEventListener('pointercancel', end);
    el.addEventListener('click', click, true);
    return {
      update(n: Wire) {
        o = n;
      },
      destroy() {
        el.removeEventListener('pointerdown', down);
        el.removeEventListener('pointermove', move);
        el.removeEventListener('pointerup', end);
        el.removeEventListener('pointercancel', end);
        el.removeEventListener('click', click, true);
      },
    };
  }

  /** A wire from a pipeline (`rule` undefined: its plain lines) or a condition box, onto a pill. */
  function fromBox(key: string, route: RouteDto, rule?: DeliverRuleDto): Wire {
    const wireTo = (d: string) => (rule ? setEdge(rule, d, true) : setLine(route, d, true));
    return {
      from: () => {
        const el = node(key);
        return el ? port(el, true) : { x: 0, y: 0 };
      },
      targets: '.pill, .newdest',
      drop: (el) => {
        // A destination id may be the empty string: only a missing one means the drop landed on `+ destination`.
        const dest = el.dataset.dest;
        if (dest !== undefined) wireTo(dest);
        else openMenu({ kind: 'add', then: wireTo }, el.getBoundingClientRect());
      },
    };
  }
  /** A wire from a pill onto a pipeline or a condition, or, dropped on another pill of its pipeline, a move to
   * that pill's place. */
  function fromPill(spot: Spot, skipButtons: boolean): Wire {
    const dest = spot.dest;
    return {
      from: () => {
        const el = node(`d:${spot.at}:${spot.via}:${dest}`);
        return el ? port(el, false) : { x: 0, y: 0 };
      },
      targets: spot.at >= 0 ? `.pipe, .box.when, .pill[data-at="${spot.at}"]` : '.pipe, .box.when',
      drop: (el) => {
        if (el.dataset.dest !== undefined) return moveTo(spot, Number(el.dataset.k));
        const [kind, i, k] = (el.dataset.drop ?? '').split(':');
        const route = routes[Number(i)];
        if (!route) return;
        if (kind === 'p') setLine(route, dest, true);
        else {
          const rule = route.deliver?.[Number(k)];
          if (rule) setEdge(rule, dest, true);
        }
      },
      skip: skipButtons
        ? (e) => e.target instanceof Element && !!e.target.closest('button, select, input')
        : undefined,
    };
  }

  /** A pipeline dropped on another of the card's takes its place, its destinations with it. */
  function fromPipe(route: RouteDto, i: number): Wire {
    return {
      from: () => {
        const el = node(`p:${i}`);
        return el ? port(el, true) : { x: 0, y: 0 };
      },
      targets: '.pipe',
      drop: (el) => {
        const j = Number((el.dataset.drop ?? '').split(':')[1]);
        for (let n = 0; n < Math.abs(j - i); n++) onorderroute?.(route, Math.sign(j - i));
      },
      skip: (e) =>
        readOnly ||
        !onorderroute ||
        routes.length < 2 ||
        (e.target instanceof Element && !!e.target.closest('button, select, input')),
    };
  }

  // Inert where the scope is not the person's to change.
  const readOnly = !editable();
</script>

<!-- Capture, so a press another card's socket stops from propagating still closes this card's menu. -->
<svelte:window
  onpointerdowncapture={(e) => {
    if (menu && menuEl && !(e.target instanceof Node && menuEl.contains(e.target))) closeMenu();
  }}
  onkeydown={(e) => {
    if (e.key === 'Escape' && menu) {
      e.preventDefault();
      closeMenu(true);
    }
  }}
  onscrollcapture={(e) => {
    // The menu is fixed and its anchor is not, so a scroll under it moves it with its anchor. Opening it can scroll
    // the card to bring the anchor into view. A menu with no anchor left points at nothing, and closes.
    if (!menu || (e.target instanceof Node && menuEl?.contains(e.target))) return;
    if (trigger?.isConnected) place(trigger.getBoundingClientRect());
    else closeMenu();
  }}
/>

{#snippet addButton(
  name: string,
  onclick: (e: MouseEvent & { currentTarget: HTMLButtonElement }) => void,
)}
  {#if !readOnly}
    <button type="button" class="add" aria-label={name} title={name} {onclick}>+</button>
  {/if}
{/snippet}

{#snippet sourceBox()}
  <div class="box node-src" data-node="src">
    <PickOrNew
      label="from source"
      value={source}
      choices={sourceIds}
      choiceLabel={label}
      newLabel={'+ New source…'}
      onnew={onnewsource && (() => onnewsource(setSource))}
      onpick={setSource}
    />
  </div>
{/snippet}

{#snippet conditionBox(route: RouteDto, i: number)}
  <div class="box cond" data-node="c:{i}" {@attach errorPlace(route)}>
    <div class="expr" onfocusout={(e) => leaveCondition(e, route)}>
      <ValidatedField
        ariaLabel="takes rows where"
        bind:value={() => route.condition ?? '', (v) => setCondition(route, v)}
        forced={validation?.message(route.id, 'condition') ?? null}
        autocomplete="condition"
        placeholder="true; every row"
      />
    </div>
    {#if !readOnly}
      <button
        type="button"
        class="ib x"
        aria-label="remove this condition"
        onclick={() => dropCondition(route)}>&#10005;</button
      >
    {/if}
  </div>
{/snippet}

{#snippet pipelineBox(route: RouteDto, i: number)}
  <div
    class="box pipe"
    data-guide="pipeline-pick"
    class:over={overKey === `p:${i}`}
    data-node="p:{i}"
    data-drop="p:{i}"
    use:wire={fromPipe(route, i)}
  >
    <!-- A new pipeline starts on a capture of the route's source, so it waits for one. -->
    <PickOrNew
      label="pipeline"
      value={route.pipeline}
      choices={pipelineIds}
      choiceLabel={pipelineLabel}
      newLabel={'+ New pipeline…'}
      onnew={route.source && onnewpipeline ? () => onnewpipeline(route) : undefined}
      onpick={(pipeline) => (route.pipeline = pipeline)}
    />
    {#if !readOnly && routes.length > 1}
      <button
        type="button"
        class="ib"
        aria-label="more"
        title="more"
        onclick={(e) =>
          openMenu(
            { kind: 'pipe', route },
            e.currentTarget.getBoundingClientRect(),
            e.currentTarget,
          )}>&#8943;</button
      >
    {/if}
    {#if !line}
      <button
        type="button"
        class="sock sock-out"
        aria-label="drag to a destination, or click to choose"
        use:wire={fromBox(`p:${i}`, route)}
        onclick={(e) =>
          openMenu({ kind: 'to', route }, e.currentTarget.getBoundingClientRect(), e.currentTarget)}
      ></button>
    {/if}
  </div>
{/snippet}

{#snippet whenBox(route: RouteDto, i: number, rule: DeliverRuleDto, k: number)}
  <div
    class="box cond when"
    class:over={overKey === `w:${i}:${k}`}
    data-node="w:{i}:{k}"
    data-drop="w:{i}:{k}"
    {@attach errorPlace(route, rule)}
  >
    <div class="expr" onfocusout={(e) => leaveWhen(e, route, rule)}>
      <ValidatedField
        ariaLabel="sends rows where"
        bind:value={() => rule.when ?? '', (v) => setWhen(route, rule, v)}
        forced={whenError(rule)}
        autocomplete="condition"
        placeholder="true; every row"
      />
    </div>
    {#if !readOnly}
      <button
        type="button"
        class="ib x"
        aria-label="remove this condition"
        onclick={() => dropWhen(route, rule)}>&#10005;</button
      >
    {/if}
    {#if !line}
      <button
        type="button"
        class="sock sock-out"
        aria-label="drag to a destination, or click to choose"
        use:wire={fromBox(`w:${i}:${k}`, route, rule)}
        onclick={(e) =>
          openMenu(
            { kind: 'to', route, rule },
            e.currentTarget.getBoundingClientRect(),
            e.currentTarget,
          )}
      ></button>
    {/if}
  </div>
  {#if rule.to.length === 0}
    {@render noDestination(route)}
  {/if}
{/snippet}

{#snippet noDestination(route: RouteDto)}
  <span class="warn">no destination: its rows go to Unmatched ({answerText(unmatched(route))})</span
  >
{/snippet}

{#snippet pill(spot: Spot)}
  {@const dest = spot.dest}
  {@const key = `d:${spot.at}:${spot.via}:${dest}`}
  <div class="pillcell">
    <div
      class="pill"
      class:over={overKey === key}
      data-node={key}
      data-drop={key}
      data-dest={dest}
      data-at={spot.at}
      data-k={spot.k}
      title={label(dest)}
      use:wire={fromPill(spot, true)}
    >
      {#if !line}
        <button
          type="button"
          class="sock sock-in"
          aria-label="choose what sends to {dest}"
          use:wire={fromPill(spot, false)}
          onclick={(e) =>
            openMenu(
              { kind: 'from', spot },
              e.currentTarget.getBoundingClientRect(),
              e.currentTarget,
            )}
        ></button>
      {/if}
      <span class="name">{dest}</span>
      {#if !readOnly && cardSpots.length > 1}
        <button
          type="button"
          class="ib"
          aria-label="more"
          title="more"
          onclick={(e) =>
            openMenu(
              { kind: 'dest', spot },
              e.currentTarget.getBoundingClientRect(),
              e.currentTarget,
            )}>&#8943;</button
        >
      {:else if !readOnly}
        <button
          type="button"
          class="ib x"
          aria-label="stop sending to {dest}"
          onclick={() => takeOff(spot)}>&#10005;</button
        >
      {/if}
    </div>
    {#if !destinationIds.includes(dest)}
      <span class="warn">no destination with this id exists</span>
    {:else if spot.at < 0}
      <span class="warn">nothing sends here</span>
    {/if}
  </div>
{/snippet}

{#snippet conditionHelp()}
  <Help label="takes rows where">
    Tested before the pipeline runs: it reads <code>_raw</code> and the source's declared fields, not
    fields the pipeline adds.
  </Help>
{/snippet}

{#snippet newDestButton()}
  <button
    type="button"
    class="ghost newdest"
    class:over={overKey === 'add'}
    data-drop="add"
    onclick={(e) =>
      openMenu(
        { kind: 'add', then: arrive },
        e.currentTarget.getBoundingClientRect(),
        e.currentTarget,
      )}>+ destination</button
  >
{/snippet}

<div class="flowwrap">
  <div
    class="flow"
    class:line
    class:tree={!line}
    class:dragging={dragLine !== null}
    style:--n={routes.length}
    style:--boxes={routes.flatMap((r) => (r.deliver ?? []).filter(boxed)).length}
    style:--pills={cardSpots.length}
    data-source={source}
    inert={readOnly}
    bind:this={flowEl}
  >
    <svg class="edges" aria-hidden="true">
      {#each edges as e (e.key)}
        {@const cut = e.cut}
        <g class:show={shown === e.key}>
          <path class="edge" d={e.d} />
          {#if cut}
            <!-- Pointer conveniences only: a socket's list, or a pill's cross on a line card, reaches every edge from the
                 keyboard. -->
            <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
            <path class="hit" d={e.d} onclick={() => (shown = shown === e.key ? null : e.key)} />
            <!-- svelte-ignore a11y_click_events_have_key_events, a11y_no_static_element_interactions -->
            <g
              class="cut"
              onclick={(ev) => {
                ev.stopPropagation();
                shown = null;
                cut.act();
              }}
            >
              <circle cx={cut.at.x} cy={cut.at.y} r="8" />
              <path
                d="M{cut.at.x - 3} {cut.at.y - 3} L{cut.at.x + 3} {cut.at.y + 3} M{cut.at.x +
                  3} {cut.at.y - 3} L{cut.at.x - 3} {cut.at.y + 3}"
              />
            </g>
            <title>remove this path</title>
          {/if}
        </g>
      {/each}
      {#if dragLine}
        <path class="drag" d={curve(dragLine.a, dragLine.b, !line)} />
      {/if}
    </svg>
    {#if line}
      {@const route = routes[0]}
      {@const box = boxes(route)[0]}
      {@const k = box ? (route.deliver ?? []).indexOf(box) : -1}
      <span class="lbl" style:grid-row="1" style:grid-column="1">from source</span>
      {#if showsCondition(route)}
        <span class="lbl" style:grid-row="1" style:grid-column="2"
          >takes rows where {@render conditionHelp()}</span
        >
      {/if}
      <span class="lbl" style:grid-row="1" style:grid-column="3"
        >pipeline {@render addButton('add a pipeline', () => onaddroute?.())}</span
      >
      {#if box}<span class="lbl" style:grid-row="1" style:grid-column="4">sends rows where</span
        >{/if}
      <span class="lbl" style:grid-row="1" style:grid-column="5"
        >destination {@render addButton('add a destination', (e) =>
          openMenu(
            { kind: 'add', then: arrive },
            e.currentTarget.getBoundingClientRect(),
            e.currentTarget,
          ),
        )}</span
      >
      <div class="cell" style:grid-row="2" style:grid-column="1">{@render sourceBox()}</div>
      <div class="cell" style:grid-row="2" style:grid-column="2">
        {#if showsCondition(route)}{@render conditionBox(route, 0)}{/if}
      </div>
      <div class="cell" style:grid-row="2" style:grid-column="3">
        {@render pipelineBox(route, 0)}
      </div>
      <div class="cell" style:grid-row="2" style:grid-column="4">
        {#if box}{@render whenBox(route, 0, box, k)}{/if}
      </div>
      <div class="cell" style:grid-row="2" style:grid-column="5">
        {#if cardSpots.length > 0}
          {@render pill(cardSpots[0])}
        {:else}
          {#if !readOnly}{@render newDestButton()}{/if}
          {@render noDestination(route)}
        {/if}
      </div>
    {:else}
      <span class="lbl" style:grid-row="1">from source</span>
      <div class="cell span" style:grid-row="1">{@render sourceBox()}</div>

      <span class="lbl" style:grid-row="2">takes rows where {@render conditionHelp()}</span>
      {#each routes as route, i (route)}
        <div class="cell" style:grid-row="2" style:grid-column={i + 2}>
          {#if showsCondition(route)}{@render conditionBox(route, i)}{/if}
        </div>
      {/each}

      <span class="lbl" style:grid-row="3"
        >pipeline {@render addButton('add a pipeline', () => onaddroute?.())}</span
      >
      {#each routes as route, i (route)}
        <div class="cell" style:grid-row="3" style:grid-column={i + 2}>
          {@render pipelineBox(route, i)}
          {#if (route.deliver ?? []).every((r) => r.to.length === 0)}
            {@render noDestination(route)}
          {/if}
        </div>
      {/each}

      <span class="lbl" style:grid-row="4">sends rows where</span>
      <div class="cell span whens" style:grid-row="4" bind:this={whensEl}>
        {#each routes as route, i (route)}
          {#each route.deliver ?? [] as rule, k (rule)}
            {#if boxed(rule)}
              {@const x = slots[`w:${i}:${k}`]}
              <div class="slot" style:left={x === undefined ? '50%' : `${x}px`}>
                {@render whenBox(route, i, rule, k)}
              </div>
            {/if}
          {/each}
        {/each}
      </div>

      <span class="lbl" style:grid-row="5"
        >destination {@render addButton('add a destination', (e) =>
          openMenu(
            { kind: 'add', then: arrive },
            e.currentTarget.getBoundingClientRect(),
            e.currentTarget,
          ),
        )}</span
      >
      <div class="cell span pills" style:grid-row="5">
        {#each cardSpots as spot (`${spot.at}:${spot.via}:${spot.dest}`)}{@render pill(spot)}{/each}
        {#if cardSpots.length === 0 && !readOnly}{@render newDestButton()}{/if}
      </div>
    {/if}

    <!-- After the boxes, so Tab reaches a box before the controls on its lines. -->
    {#each edges as e (e.key)}
      {#if e.plus}
        {@const plus = e.plus}
        <button
          type="button"
          class="plus"
          style:left="{plus.at.x}px"
          style:top="{plus.at.y}px"
          aria-label={plus.name}
          title={plus.name}
          onclick={plus.act}>+</button
        >
      {/if}
    {/each}
  </div>
</div>

{#if menu}
  <!-- Focus moving to anything outside the menu closes it: a keyboard press on another card's socket fires no
       pointerdown, so the window's capture above never sees it. A null target (a press on nothing focusable) is
       that capture's to answer. -->
  <div
    class="menu"
    tabindex="-1"
    bind:this={menuEl}
    onfocusout={(e) => {
      if (e.relatedTarget instanceof Node && !menuEl?.contains(e.relatedTarget)) closeMenu();
    }}
  >
    {#if menu.kind === 'to'}
      {@const route = menu.route}
      {@const rule = menu.rule}
      <span class="menu-note">goes to</span>
      {#each onCard as d (d)}
        <button type="button" onclick={() => flip(route, rule, d)}>
          <span class="k">{sends(route, rule, d) ? '✓' : ''}</span><span class="mono">{d}</span>
        </button>
      {/each}
      {#if onCard.length > 0}<hr />{/if}
      <button
        type="button"
        onclick={() => {
          if (!menu) return;
          // Read now: `route` and `rule` are this menu's, and the next menu takes its place.
          const [r, ru] = [route, rule];
          openMenu(
            { kind: 'add', then: (d) => (ru ? setEdge(ru, d, true) : setLine(r, d, true)) },
            menu.at,
          );
        }}
      >
        <span class="k">+</span>another destination&hellip;
      </button>
    {:else if menu.kind === 'pipe'}
      {@const route = menu.route}
      <span class="menu-note"
        >{route.pipeline ? pipelineLabel(route.pipeline) : '(no pipeline)'}</span
      >
      {#if onorderroute}
        {#each [-1, 1] as by (by)}
          {#if routes[routes.indexOf(route) + by]}
            <button
              type="button"
              onclick={() => {
                // Read before the menu closes: `route` is the open menu's, and is gone with it.
                const r = route;
                closeMenu(true);
                onorderroute(r, by);
              }}
            >
              {#if by < 0}<span class="k">&larr;</span>move left{:else}<span class="k">&rarr;</span
                >move right{/if}
            </button>
          {/if}
        {/each}
      {/if}
      <button
        type="button"
        onclick={() => {
          if (menu) openMenu({ kind: 'move', route }, menu.at);
        }}
      >
        <span class="k">&#8644;</span>move to another source&hellip;
      </button>
      <button
        type="button"
        onclick={() => {
          // Read before the menu closes: `route` is the open menu's, and is gone with it.
          const r = route;
          closeMenu(true);
          onremoveroute?.(r);
        }}
      >
        <span class="k">&#10005;</span>remove this pipeline
      </button>
    {:else if menu.kind === 'dest'}
      {@const spot = menu.spot}
      {@const route = routes[spot.at]}
      <span class="menu-note">{label(spot.dest)}</span>
      {#if route && canMove(route, spot.k, -1, boxed)}
        <button
          type="button"
          onclick={() => {
            // Read before the menu closes: `spot` is the open menu's, and is gone with it.
            const [r, k] = [route, spot.k];
            closeMenu(true);
            moveDest(r, k, -1, boxed);
          }}
        >
          <span class="k">&larr;</span>move left
        </button>
      {/if}
      {#if route && canMove(route, spot.k, 1, boxed)}
        <button
          type="button"
          onclick={() => {
            const [r, k] = [route, spot.k];
            closeMenu(true);
            moveDest(r, k, 1, boxed);
          }}
        >
          <span class="k">&rarr;</span>move right
        </button>
      {/if}
      <button
        type="button"
        onclick={() => {
          const sp = spot;
          closeMenu(true);
          takeOff(sp);
        }}
      >
        <span class="k">&#10005;</span>remove this destination
      </button>
    {:else if menu.kind === 'move'}
      {@const route = menu.route}
      <span class="menu-note">move to source</span>
      {#each sourceIds.filter((s) => s !== route.source) as s (s)}
        <button
          type="button"
          onclick={() => {
            const r = route;
            closeMenu();
            moveRoute(r, s);
          }}
        >
          <span class="k"></span><span class="mono">{label(s)}</span>
        </button>
      {/each}
      {#if onnewsource}
        <hr />
        <button
          type="button"
          onclick={() => {
            const r = route;
            closeMenu();
            onnewsource((s) => moveRoute(r, s));
          }}
        >
          <span class="k">+</span>New source&hellip;
        </button>
      {/if}
    {:else if menu.kind === 'from'}
      {@const dest = menu.spot.dest}
      {@const at = menu.spot.at}
      <span class="menu-note"><span class="mono">{dest}</span> takes rows from</span>
      <!-- Two choices can read alike, so each names its pipeline and lights its box. A pill under a pipeline takes
           rows from that pipeline only. -->
      {#each routes as route, i (route)}
        {#if at < 0 || at === i}
          {#each [undefined, ...(route.deliver ?? []).filter((r) => gated(r) || writing.has(r))] as rule (rule ?? route)}
            {@const key = rule ? `w:${i}:${(route.deliver ?? []).indexOf(rule)}` : `p:${i}`}
            <button
              type="button"
              onclick={() => flip(route, rule, dest)}
              onpointerenter={() => (overKey = key)}
              onpointerleave={() => (overKey = null)}
              onfocus={() => (overKey = key)}
              onblur={() => (overKey = null)}
            >
              <span class="k">{sends(route, rule, dest) ? '✓' : ''}</span><span class="n"
                >{route.pipeline ? pipelineLabel(route.pipeline) : '(no pipeline)'}</span
              ><span class="mono">{rule ? ruleName(rule) : 'every row'}</span>
            </button>
          {/each}
        {/if}
      {/each}
    {:else}
      {@const then = menu.then}
      {@const choices = offCard(destinationIds, onCard)}
      <span class="menu-note">destinations not on this card</span>
      {#each choices as d (d)}
        <button
          type="button"
          onclick={() => {
            bring(d);
            then?.(d);
            closeMenu(true);
          }}
        >
          <span class="k"></span><span class="mono">{label(d)}</span>
        </button>
      {/each}
      {#if choices.length === 0}
        <span class="menu-note">every destination is on the card</span>
      {/if}
      {#if onnewdest}
        <hr />
        <button
          type="button"
          onclick={() => {
            // Read before the menu closes: `then` is the open menu's, and is gone with it.
            const wire = then;
            closeMenu(true);
            newDest(wire);
          }}
        >
          <span class="k">+</span>New destination&hellip;
        </button>
      {/if}
    {/if}
  </div>
{/if}

<style>
  .flowwrap {
    overflow-x: auto;
  }
  /* A field's suggestions hang below the card, so the card stops clipping while one is being typed in. Only a
     field: a pressed button takes focus too, and the card scrolled back under the pointer moves the release off it. */
  .flowwrap:has(:is(input, textarea):focus) {
    overflow: visible;
  }
  .flow {
    position: relative;
    display: grid;
    align-items: center;
    padding: 6px 2px 10px;
  }
  /* A line: source, its condition, pipeline, a rule's condition, destination. A condition nobody wrote is a [+] on
     its line, so its column closes up to the line's width. */
  .flow.line {
    grid-template-columns:
      minmax(0, 1fr) minmax(2.5rem, auto) minmax(0, 1fr) minmax(2.5rem, auto)
      minmax(0, 1fr);
    column-gap: 1.25rem;
    row-gap: 0.4rem;
    min-width: 640px;
  }
  /* Top to bottom: a row per step, a column per route under the label column. */
  .flow.tree {
    grid-template-columns: 10rem repeat(var(--n), minmax(11rem, 1fr));
    column-gap: 1rem;
    row-gap: 2.6rem;
    min-width: calc(10rem + max(var(--n) * 12rem, var(--boxes) * 17rem, var(--pills) * 12rem));
  }
  .tree .lbl {
    grid-column: 1;
  }
  .tree .span {
    grid-column: 2 / -1;
    justify-self: center;
  }
  .tree .cell:not(.span) {
    justify-self: center;
    width: min(100%, 16rem);
  }
  /* One row: a line ends on a pill in its own place, so the card widens for its pills rather than wrapping them. */
  .tree .pills {
    justify-self: stretch;
    flex-direction: row;
    justify-content: space-around;
    align-items: flex-start;
    gap: 0.9rem;
  }
  .tree .pills .pillcell {
    flex: 0 1 14rem;
  }
  /* One box high, so a box written into it does not move the rows under it. Its boxes are placed over their
     destinations. */
  .tree .whens {
    justify-self: stretch;
    height: 40px;
  }
  .slot {
    position: absolute;
    top: 0;
    width: min(100%, 16rem);
    transform: translateX(-50%);
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
  }

  .lbl {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    font-size: var(--text-label);
    font-weight: 600;
    white-space: nowrap;
  }
  .cell {
    position: relative;
    z-index: 1;
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    min-width: 0;
  }
  /* A field's suggestions hang over the rows below it, so its cell stands above theirs while it is typed in. */
  .cell:focus-within {
    z-index: 2;
  }

  .edges {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    overflow: visible;
    pointer-events: none;
  }
  .edge {
    fill: none;
    stroke: var(--dim);
    stroke-width: 1.5;
  }
  .hit {
    fill: none;
    stroke: transparent;
    stroke-width: 14;
    pointer-events: stroke;
  }
  .cut {
    display: none;
    cursor: pointer;
    pointer-events: all;
  }
  .cut circle {
    fill: var(--panel);
    stroke: var(--error);
    stroke-width: 1.5;
  }
  .cut path {
    stroke: var(--error);
    stroke-width: 1.5;
    fill: none;
  }
  g:hover .cut,
  g.show .cut {
    display: block;
  }
  g:hover .edge,
  g.show .edge {
    stroke: var(--error);
    stroke-width: 2.5;
  }
  .drag {
    fill: none;
    stroke: var(--accent);
    stroke-width: 2;
    stroke-dasharray: 5 4;
  }

  /* A condition nobody has written: a small square on its line. */
  .plus {
    position: absolute;
    z-index: 2;
    width: 22px;
    height: 22px;
    margin: -11px 0 0 -11px;
    border: 1px solid var(--dim);
    border-radius: 4px;
    background: var(--bg);
    color: var(--dim);
    font: inherit;
    font-size: var(--text-label);
    line-height: 1;
    padding: 0;
    cursor: pointer;
  }
  .plus:hover,
  .plus:focus-visible {
    border-color: var(--ink);
    color: var(--ink);
  }
  .add {
    width: 20px;
    height: 20px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-label);
    line-height: 1;
    padding: 0;
    cursor: pointer;
  }
  .add:hover,
  .add:focus-visible {
    border-color: var(--ink);
    color: var(--ink);
  }

  /* One height for every box and pill, so a run of one of each is a straight line. */
  .box,
  .pill {
    box-sizing: border-box;
    height: 40px;
  }
  .box {
    position: relative;
    display: flex;
    align-items: center;
    gap: 6px;
    border: 1px solid var(--border);
    border-radius: var(--radius);
    background: var(--bg);
    padding: 0 6px;
  }
  .expr {
    flex: 1 1 0;
    min-width: 0;
  }
  .box :global(select) {
    flex: 1 1 0;
    min-width: 0;
    font-family: var(--mono);
    font-size: var(--text-label);
    padding: 4px 8px;
    border: none;
    background: transparent;
  }
  .box .x {
    opacity: 0.35;
  }
  .box:hover .x,
  .box:focus-within .x {
    opacity: 1;
  }
  .node-src {
    min-width: 14rem;
  }
  .pillcell {
    display: flex;
    flex-direction: column;
    gap: 0.3rem;
    min-width: 10rem;
  }
  .pill {
    position: relative;
    border: 1px solid var(--border);
    border-radius: 999px;
    background: var(--bg);
    font-family: var(--mono);
    font-size: var(--text-label);
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 0 6px 0 16px;
    touch-action: none;
    cursor: grab;
  }
  .pill .name {
    flex: 1 1 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .flow.dragging .pill,
  .flow.dragging .pipe,
  .flow.dragging .when {
    cursor: copy;
  }
  .over {
    border-color: var(--accent);
    border-style: solid;
    box-shadow: 0 0 0 1px var(--accent);
  }

  .ib {
    width: 24px;
    height: 24px;
    border-radius: 999px;
    border: 1px solid var(--border);
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-micro);
    line-height: 1;
    padding: 0;
    cursor: pointer;
    flex-shrink: 0;
  }
  .ib.x:hover {
    border-color: var(--error);
    color: var(--error);
  }

  /* The socket: where a wire leaves a box or enters a pill, drawn on its edge. */
  .sock {
    position: absolute;
    left: 50%;
    width: 18px;
    height: 18px;
    margin-left: -9px;
    border-radius: 999px;
    border: 2px solid var(--dim);
    background: var(--panel);
    padding: 0;
    cursor: grab;
    touch-action: none;
    z-index: 2;
  }
  .sock:hover,
  .sock:focus-visible {
    border-color: var(--accent);
  }
  .sock:active {
    cursor: grabbing;
  }
  .sock-out {
    bottom: -9px;
  }
  .sock-in {
    top: -9px;
  }

  .ghost {
    position: relative;
    z-index: 1;
    border: 1px dashed var(--border);
    border-radius: var(--radius);
    background: none;
    color: var(--dim);
    font: inherit;
    font-size: var(--text-label);
    padding: 8px;
    cursor: pointer;
    text-align: center;
    height: 40px;
  }
  .ghost:hover {
    border-color: var(--ink);
    color: var(--ink);
  }

  .warn {
    color: var(--warn);
    font-size: var(--text-note);
    padding-left: 4px;
  }

  /* Focused only when it has nothing to choose; the open menu already shows where focus is. */
  .menu:focus {
    outline: none;
  }
  .menu {
    position: fixed;
    z-index: 20;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: var(--radius-card);
    box-shadow: 0 4px 12px oklch(0% 0 0 / 0.12);
    padding: 4px;
    display: flex;
    flex-direction: column;
    gap: 2px;
    min-width: 190px;
    max-height: calc(100vh - 16px);
    overflow-y: auto;
    box-sizing: border-box;
  }
  .menu button {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    text-align: left;
    background: none;
    border: none;
    border-radius: var(--radius-card);
    padding: 6px 8px;
    font: inherit;
    font-size: var(--text-label);
    color: var(--ink);
    cursor: pointer;
  }
  .menu button:hover {
    background: var(--bg);
  }
  .menu hr {
    border: none;
    border-top: 1px solid var(--border);
    margin: 2px 6px;
    width: auto;
  }
  .menu-note {
    font-size: var(--text-note);
    color: var(--dim);
    padding: 4px 8px;
  }
  .k {
    width: 14px;
    color: var(--dim);
    font-size: var(--text-note);
  }
  .n {
    margin-right: 8px;
    color: var(--dim);
    font-size: var(--text-note);
    white-space: nowrap;
  }
  .mono {
    font-family: var(--mono);
  }
  code {
    font-family: var(--mono);
  }
</style>
