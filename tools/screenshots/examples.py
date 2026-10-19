"""The website's step reference examples, each shown as the GUI draws its step.

    python3 examples.py      # against the fleet `stage.py` staged

Every example in `website/src/lib/steps/<page>.json` becomes a pipeline on a branch of its own, never deployed, so
no other screenshot sees it. Each step the page shows (all but a leading parser) is opened and its card shot alone,
in light and dark, to `website/static/steps/<page>-<example>-<n>-<theme>.png`; `StepExample.svelte` draws those
images. The PNGs are reduced to a palette, which keeps a page of them light.
"""
import json
import pathlib
import subprocess

from PIL import Image

from stage import as_

HERE = pathlib.Path(__file__).parent
STEPS = HERE.parent.parent / "website" / "src" / "lib" / "steps"
SITE = HERE.parent.parent / "website" / "static" / "steps"
BRANCH = "website-examples"
SCOPE = "shared"


def cards(steps):
    """How many step cards `steps` draws, a branch's steps counted with the If step that holds them."""
    return sum(1 + cards(s.get("then", [])) + cards(s.get("else", [])) for s in steps)


def parsed(steps):
    """`steps` as `website-reference-tests` runs them: after a JSON parser of `_raw` unless they open with a parser."""
    if steps and steps[0]["type"] == "parser":
        return steps
    return [{"id": "parse", "type": "parser", "input": "_raw", "kind": {"type": "json"}}] + steps


def shown(steps):
    """The steps a page draws: a leading parser is named in the page's text instead."""
    return steps[1:] if len(steps) > 1 and steps[0]["type"] == "parser" else steps


def main():
    c = as_("admin")
    # From main each run, so an example renamed or removed since leaves nothing behind.
    c.call("DELETE", f"/api/branches/{BRANCH}")
    examples = [(page.stem, name, ex) for page in sorted(STEPS.glob("*.json"))
                for name, ex in json.loads(page.read_text()).items()]
    # What the examples name goes in first: a pipeline naming a missing table or pipeline is refused.
    tables = {t["id"]: t for _, _, ex in examples for t in ex.get("lookupTables", [])}
    c.put("lookup-tables", list(tables.values()), BRANCH, SCOPE)
    for p in {p["id"]: p for _, _, ex in examples for p in ex.get("pipelines", [])}.values():
        c.put_pipeline(p, BRANCH, SCOPE)
    shots = []
    for stem, name, ex in examples:
        pid = f"{stem}-{name}".lower()
        steps = parsed(ex["steps"])
        pipeline = {"id": pid, "name": pid, "steps": steps}
        c.put_pipeline(pipeline, BRANCH, SCOPE)
        for n in range(len(shown(steps))):
            k = len(steps) - len(shown(steps)) + cards(shown(steps)[:n])
            shots.append({
                "name": f"steps-{pid}-{n}", "site": f"steps/{stem}-{name}-{n}",
                "hash": f"#/build/pipelines/{pid}", "scope": SCOPE, "branch": BRANCH,
                "themes": ["light", "dark"], "height": 1600, "wait": "button.expander",
                "card": k, "steps": [{"delay": 1500}, {"drag": [".divider", 820]}, {"card": k, "expand": True}, {"delay": 800}],
            })
    spec = HERE / "state" / "examples-shots.json"
    spec.write_text(json.dumps(shots, indent=1))
    subprocess.run(["node", str(HERE / "shoot.mjs"), str(spec)], check=True)
    for png in SITE.glob("*.png"):
        Image.open(png).convert("RGB").quantize(256, method=Image.Quantize.MEDIANCUT).save(png, optimize=True)


if __name__ == "__main__":
    main()
