"""The ECS fields Elastic Security requires and uses, read from its field reference's Markdown (argv[1]) and written
as `schema::ecs` reads them (argv[2]): one line per field, its requirement and its name. The fields under "Always
required fields" are required; every other field the page names is recommended. A value the page gives a field, as in
`event.category:authentication`, is the page's own and is dropped."""
import re
import sys

source, out = sys.argv[1], sys.argv[2]
required, recommended = [], []
section = ""
for line in open(source, encoding="utf-8"):
    if line.startswith("#"):
        section = line
        continue
    if not line.lstrip().startswith("* "):
        continue
    for name in re.findall(r"`([@a-z][a-z0-9_.@]*)(?::[^`]*)?`", line):
        into = required if "Always required fields" in section else recommended
        if name not in required and name not in recommended:
            into.append(name)
if not required:
    sys.exit("the page has no always required fields")
with open(out, "w", encoding="utf-8") as f:
    f.write("# Elastic Security's ECS field reference, written by `just schemas-refresh`.\n")
    for name in required:
        f.write(f"required {name}\n")
    for name in sorted(recommended):
        f.write(f"recommended {name}\n")
