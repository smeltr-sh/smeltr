"""How well each form's grouping predicts the weeks it was not fitted on: the share of held-out error it removes against
no grouping, averaged over the refreshes of each series (days split at UTC midnight). The test keeps the better of two
groups and seven at each refresh, so this is that candidate's score, kept or not."""
import json
import numpy as np

rows = {}
for f in ("extend_daygroups_forms.json", "extend_daygroups_forms_logs.json"):
    for x in json.load(open(f)):
        d = {c["form"]: c.get("silhouette") for c in x["configs"] if c["utc"]}
        rows.setdefault(x["series"], []).append([np.nan if d.get(k) is None else d[k] for k in "ABCH"])
out = {s: np.nanmean(np.array(v, float), 0).tolist() for s, v in rows.items() if not np.isnan(np.array(v, float)).all()}
json.dump(out, open("heldout_forms.json", "w"), indent=0)
m = np.array(list(out.values()))
print("| Series | A | B | C | H |\n|---|---|---|---|---|")
for s, v in out.items():
    print(f"| {(s.split('/')[-1] if s.startswith('real') else s).replace('Twitter_volume_', 'tweets ')} | " + " | ".join(f"{100 * x:.1f}" for x in v) + " |")
print(f"| **Mean of {len(m)} series** | " + " | ".join(f"**{100 * x:.1f}**" for x in m.mean(0)) + " |")
print("H below C on", int((m[:, 3] < m[:, 2]).sum()), "of", len(m))
