"""Cuts ocsf-schema-compiler's output to what `schema::ocsf` reads: each class's and object's name, caption and
attributes, a class's category for the dialog to group it by and the profiles it takes, each profile's caption, and of
each attribute its type, object type, array flag, requirement, caption, description, example and the profiles that
add it.

Usage: cut_ocsf.py <compiled.json> <out.json>
"""

import json
import sys

KEPT = ["type", "object_type", "is_array", "requirement", "caption", "description", "example", "profiles"]


def cut(of, takes_profiles=False):
    return {
        "name": of["name"],
        "caption": of.get("caption", ""),
        **({"category_name": of["category_name"]} if "category_name" in of else {}),
        **({"profiles": of["profiles"]} if takes_profiles and of.get("profiles") else {}),
        "attributes": {
            name: {k: a[k] for k in KEPT if k in a} for name, a in of["attributes"].items()
        },
    }


compiled = json.load(open(sys.argv[1], encoding="utf-8"))
out = {
    "version": compiled["version"],
    "classes": {name: cut(c, takes_profiles=True) for name, c in compiled["classes"].items()},
    "objects": {name: cut(o) for name, o in compiled["objects"].items()},
    "profiles": {name: {"caption": p.get("caption", name)} for name, p in compiled.get("profiles", {}).items()},
}
with open(sys.argv[2], "w", encoding="utf-8") as f:
    json.dump(out, f, indent=None, separators=(",", ":"), sort_keys=True)
    f.write("\n")
