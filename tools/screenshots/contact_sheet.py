#!/usr/bin/env python3
"""One image of every screenshot, light beside dark, for a quick look.

    contact_sheet.py SHOTS_DIR OUT.png   # the sheet
    contact_sheet.py --clean SHOTS_DIR   # remove what a run writes there, and nothing else

A screenshot is one that `shots.json`, `incident.json` or `outage.json` names: SHOTS_DIR may hold other images.
"""

import json
import os
import sys

from PIL import Image, ImageDraw, ImageFont

DIR = os.path.dirname(os.path.abspath(__file__))
names = sorted(s["name"] for spec in ("shots.json", "incident.json", "outage.json")
               for s in json.load(open(os.path.join(DIR, spec))))

if sys.argv[1] == "--clean":
    src = sys.argv[2]
    for p in [os.path.join(src, f"{n}-{t}.png") for n in names for t in ("light", "dark")] + [
            os.path.join(src, "contact-sheet.png")]:
        if os.path.exists(p):
            os.remove(p)
    sys.exit(0)

src, out = sys.argv[1], sys.argv[2]
THUMB_W, GAP, LABEL = 560, 24, 34
font = ImageFont.load_default(size=20)

rows = []
for n in names:
    pair = []
    for theme in ("light", "dark"):
        p = os.path.join(src, f"{n}-{theme}.png")
        if os.path.exists(p):
            im = Image.open(p).convert("RGB")
            # Tall full-page shots are cut to a 16:10 top slice; the sheet is for finding, not reading.
            h = min(im.height, int(im.width * 0.625))
            im = im.crop((0, 0, im.width, h))
            pair.append(im.resize((THUMB_W, int(h * THUMB_W / im.width)), Image.LANCZOS))
    if pair:
        rows.append((n, pair))

cols = 2  # pairs per row
pair_w = THUMB_W * 2 + GAP
row_h = max(max(t.height for t in pair) for _, pair in rows) + LABEL
grid_rows = (len(rows) + cols - 1) // cols
sheet = Image.new("RGB", (cols * pair_w + (cols + 1) * GAP * 2, grid_rows * (row_h + GAP) + GAP), (236, 232, 228))
draw = ImageDraw.Draw(sheet)
for i, (n, pair) in enumerate(rows):
    x0 = GAP * 2 + (i % cols) * (pair_w + GAP * 2)
    y0 = GAP + (i // cols) * (row_h + GAP)
    draw.text((x0, y0 + 6), n, fill=(40, 30, 25), font=font)
    for j, t in enumerate(pair):
        sheet.paste(t, (x0 + j * (THUMB_W + GAP), y0 + LABEL))
sheet.save(out, optimize=True)
print(out, sheet.size)
