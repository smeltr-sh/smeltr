#!/usr/bin/env python3
"""Draws the benchmark chart from the rows of the benchmark log: one panel per
reference pipeline, its ratio to the reference at each log.

Every panel shares one y scale, so a slope reads the same in each, and each
names its least-squares slope over the last TREND logs: a loss too small to
pass for more than noise on one day still shows as a falling line. Under it,
the change the least-squares line makes over every log against the current
reference: whether the pipeline has been getting faster or slower since the
reference was pinned. A ratio against another reference measures something
else, so it is drawn but left out of that line.

Usage: bench-plot.py bench-log.md bench-log.svg
"""
import math
import sys
from html import escape

TREND = 7
COLS, PANEL_W, PANEL_H, GAP = 4, 220, 150, 16
PAD_L, PAD_R, PAD_T, PAD_B = 34, 10, 46, 22


def rows(path):
    """{pipeline: [(date, head, ref, ratio)]} in log order."""
    out = {}
    for line in open(path, encoding="utf-8"):
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        # n/a: the reference could not run that pipeline, so there is no ratio.
        if len(cells) < 5 or not cells[0][:4].isdigit() or cells[4] == "n/a":
            continue
        date, name, head, ref, ratio = cells[:5]
        out.setdefault(name, []).append((date, head, ref, float(ratio)))
    return out


def slope(ys):
    """Least-squares change per log, or None under three points."""
    n = len(ys)
    if n < 3:
        return None
    mx, my = (n - 1) / 2, sum(ys) / n
    return sum((x - mx) * (y - my) for x, y in enumerate(ys)) / sum((x - mx) ** 2 for x in range(n))


def panel(name, pts, lo, hi, x0, y0):
    w, h = PANEL_W - PAD_L - PAD_R, PANEL_H - PAD_T - PAD_B
    n = len(pts)
    px = lambda i: x0 + PAD_L + (w * i / (n - 1) if n > 1 else w / 2)
    py = lambda v: y0 + PAD_T + h * (hi - v) / (hi - lo)
    ys = [r for *_, r in pts]
    s = slope(ys[-TREND:])
    trend = "trend needs 3 logs" if s is None else f"trend {s:+.3f}/log"
    ref = pts[-1][2]
    since = [(d, r) for d, _, f, r in pts if f == ref]
    a = slope([r for _, r in since])
    overall = (
        f"vs {ref}: needs 3 logs" if a is None
        else f"since {since[0][0][5:]}: {a * (len(since) - 1):+.2f} over {len(since)} logs"
    )
    out = [
        f'<text class="title" x="{x0 + PAD_L}" y="{y0 + 14}">{escape(name)} {ys[-1]:.2f}</text>',
        f'<text class="note" x="{x0 + PAD_L}" y="{y0 + 27}">{trend}</text>',
        f'<text class="note" x="{x0 + PAD_L}" y="{y0 + 39}">{escape(overall)}</text>',
    ]
    for v in [lo + k / 2 for k in range(int((hi - lo) * 2) + 1)]:
        cls = "base" if v == 1.0 else "grid"
        out.append(f'<line class="{cls}" x1="{x0 + PAD_L}" x2="{x0 + PAD_L + w}" y1="{py(v):.1f}" y2="{py(v):.1f}"/>')
        out.append(f'<text class="tick" x="{x0 + PAD_L - 4}" y="{py(v) + 3:.1f}" text-anchor="end">{v:.1f}</text>')
    for i in sorted({0, n - 1}):
        anchor = "start" if i == 0 and n > 1 else "end" if i == n - 1 and n > 1 else "middle"
        out.append(f'<text class="tick" x="{px(i):.1f}" y="{y0 + PANEL_H - 6}" text-anchor="{anchor}">{pts[i][0][5:]}</text>')
    path = " ".join(f"{'M' if i == 0 else 'L'}{px(i):.1f},{py(v):.1f}" for i, v in enumerate(ys))
    out.append(f'<path class="line" d="{path}"/>')
    for i, (date, head, _, v) in enumerate(pts):
        out.append(
            f'<circle class="dot" cx="{px(i):.1f}" cy="{py(v):.1f}" r="4">'
            f"<title>{escape(name)} {date} {head}: {v:.2f}</title></circle>"
        )
    return out


def main(md, svg):
    data = rows(md)
    every = [r for pts in data.values() for *_, r in pts] + [1.0]
    lo, hi = math.floor(min(every) * 2) / 2, math.ceil(max(every) * 2) / 2
    # Every ratio on one half step, as a first log can be: the scale still needs a height.
    hi = max(hi, lo + 0.5)
    names = list(data)
    width = COLS * PANEL_W + (COLS - 1) * GAP
    height = -(-len(names) // COLS) * (PANEL_H + GAP) - GAP
    out = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" '
        'font-family="system-ui, sans-serif" role="img" aria-label="Ratio to the reference per pipeline, by log">',
        "<style>",
        ".bg{fill:#fcfcfb}.title{fill:#0b0b0b;font-size:13px;font-weight:600}.note,.tick{fill:#52514e;font-size:10px}"
        ".grid{stroke:#e4e3df;stroke-width:1}.base{stroke:#8d8c86;stroke-width:1;stroke-dasharray:3 3}"
        ".line{fill:none;stroke:#2a78d6;stroke-width:2}.dot{fill:#2a78d6;stroke:#fcfcfb;stroke-width:2}",
        "@media (prefers-color-scheme: dark){.bg{fill:#1a1a19}.title{fill:#ffffff}.note,.tick{fill:#c3c2b7}"
        ".grid{stroke:#33332f}.base{stroke:#8d8c86}.line{stroke:#3987e5}.dot{fill:#3987e5;stroke:#1a1a19}}",
        "</style>",
        f'<rect class="bg" width="{width}" height="{height}" rx="6"/>',
    ]
    for k, name in enumerate(names):
        out += panel(name, data[name], lo, hi, (k % COLS) * (PANEL_W + GAP), (k // COLS) * (PANEL_H + GAP))
    out.append("</svg>")
    open(svg, "w", encoding="utf-8").write("\n".join(out) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
