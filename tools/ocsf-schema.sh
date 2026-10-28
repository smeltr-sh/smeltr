#!/usr/bin/env bash
# Regenerates src/schema/ocsf_schema.rs from OCSF's schema at a release tag. Usage: tools/ocsf-schema.sh 1.9.0
set -euo pipefail
version="$1"
url="https://github.com/ocsf/ocsf-schema"
out="$(dirname "$0")/../src/schema/ocsf_schema.rs"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
git clone -q --depth 1 --branch "$version" "$url" "$dir" 2>/dev/null
[ "$(jq -r .version "$dir/version.json")" = "$version" ] || { echo "ocsf-schema.sh: $url at $version is not $version" >&2; exit 1; }

# Each class and object with every attribute it has: its own, those of what it extends, and those of the profiles it
# includes, each over the dictionary's. A profile's attributes are optional, since a producer chooses its profiles.
rows="$(jq -n -r \
  --slurpfile dictionary "$dir/dictionary.json" \
  --slurpfile categories "$dir/categories.json" \
  --slurpfile objects <(jq -s . "$dir"/objects/*.json) \
  --slurpfile events <(find "$dir/events" -name '*.json' -print0 | sort -z | xargs -0 jq -s .) \
  --slurpfile profiles <(for p in "$dir"/profiles/*.json; do jq --arg file "profiles/$(basename "$p")" '{($file): .}' "$p"; done | jq -s add) \
  -f /dev/stdin <<'JQ'
  ($dictionary[0].attributes) as $dict
  | ($dictionary[0].types.attributes) as $types
  | ($objects[0] | map({(.name): .}) | add) as $objs
  | ($events[0] | map({(.name): .}) | add) as $evts
  | ($profiles[0]) as $profs
  | ($categories[0].attributes) as $cats
  # A type's primitive: the base of a subtype such as `ip_t`.
  | def base($t): if $types[$t].type then base($types[$t].type) else $t end;
    def kind($t):
      if $t == "timestamp_t" then "EpochMillis" elif $t == "datetime_t" then "DateTime"
      elif ($t | endswith("_t")) | not then (if $t == "object" then "Json" else "Object(\"\($t)\")" end)
      else {"string_t": "Str", "integer_t": "Long", "long_t": "Long", "float_t": "Float", "boolean_t": "Bool",
            "datetime_t": "DateTime", "json_t": "Json"}[base($t)] // error("no kind for \($t)")
      end;
    # HTML as plain text: a list's items joined by commas, a nested list in parentheses after its item.
    def text: [scan("<[^>]*>|[^<]+")]
      | reduce .[] as $t ({out: "", firsts: []};
          if $t == "<ul>" or $t == "<ol>" then
            .out += (if (.firsts | length) > 0 then " (" else " " end) | .firsts += [true]
          elif $t == "</ul>" or $t == "</ol>" then
            .firsts |= .[:-1] | .out += (if (.firsts | length) > 0 then ")" else " " end)
          elif $t == "<li>" then
            .out += (if .firsts[-1] then "" else ", " end) | .firsts[-1] = false
          elif ($t | test("^<(br|/?p)\\b")) then .out += " "
          elif ($t | startswith("<")) then .
          else .out += $t end)
      | .out | gsub("&lt;"; "<") | gsub("&gt;"; ">") | gsub("&quot;"; "\"") | gsub("&amp;"; "&")
      | gsub("\\s+"; " ") | gsub(" ,"; ",") | gsub(" \\)"; ")") | sub("^ "; "") | sub(" $"; "");
    def own($e): ($e.attributes // {}) | del(."$include");
    def includes($e): [($e.attributes // {})."$include" // [] | if type == "array" then .[] else . end]
      | map($profs[.].attributes // {} | map_values(.requirement = "optional")) | add // {};
    # An entity's attributes, each the dictionary's entry with every override down the chain merged over it.
    def attributes($e; $all):
      (if $e.extends and $all[$e.extends] then attributes($all[$e.extends]; $all) else {} end) as $up
      | reduce ((includes($e) | to_entries[]), (own($e) | to_entries[])) as $a ($up;
          .[$a.key] = (($dict[$a.key] // {}) * (.[$a.key] // {}) * $a.value));
    # The datetime profile gives each timestamp a `_dt` twin, the same instant as RFC 3339 text.
    def dated: . + (to_entries | map(select(.value.type == "timestamp_t") | {key: "\(.key)_dt",
      value: (.value | .type = "datetime_t" | .requirement = "optional")}) | from_entries);
    def attr: "Attr { name: \(.key | @json), ty: \(kind(.value.type)), type_name: \(.value.type | sub("_t$"; "") | @json), array: \(.value.is_array == true), required: \(.value.requirement == "required"), description: \(.value.description // "" | text | @json) }";
    def attrs: to_entries | sort_by(.key) | map(attr);
    def category($e; $all): if $e.category then $e.category elif $e.extends then category($all[$e.extends]; $all) else null end;
    ($objs | to_entries | map(select(.key | startswith("_") | not)) | sort_by(.key) | map(.value as $o
      | {o: $o, attrs: (attributes($o; $objs) | dated | attrs)})) as $os
    | ([$evts[] | select(.uid != null)] | map(. as $c | category($c; $evts) as $cat
      | {c: $c, cat: $cats[$cat].uid, uid: ($cats[$cat].uid * 1000 + $c.uid), attrs: (attributes($c; $evts) | dated | attrs)})
      | sort_by(.uid)) as $cs
    # An attribute two entities share alike is written once, and each entity lists where.
    | ([$os[].attrs[], $cs[].attrs[]] | unique) as $unique
    | (reduce range($unique | length) as $i ({}; .[$unique[$i]] = $i)) as $at
    | def at: map($at[.] | tostring) | join(", ");
    "CATEGORIES",
    ($cats | to_entries | sort_by(.value.uid)[] | "    (\(.value.uid), \(.key | @json), \(.value.caption | @json)),"),
    "ATTRS",
    ($unique[] | "    \(.),"),
    "OBJECTS",
    ($os[] | "    Object {\n        name: \(.o.name | @json),\n        caption: \(.o.caption | @json),\n        description: \(.o.description // "" | text | @json),\n        attributes: &[\(.attrs | at)],\n    },"),
    "CLASSES",
    ($cs[] | "    Class {\n        name: \(.c.name | @json),\n        uid: \(.uid),\n        category: \(.cat),\n        caption: \(.c.caption | @json),\n        description: \(.c.description // "" | text | @json),\n        attributes: &[\(.attrs | at)],\n    },")
JQ
)"
# JSON's `\uXXXX` is Rust's `\u{XXXX}`.
rows="$(printf '%s\n' "$rows" | sed 's/\\u\([0-9a-fA-F]\{4\}\)/\\u{\1}/g')"
section() { printf '%s\n' "$rows" | awk -v from="$1" -v to="$2" '$0 == from { on = 1; next } $0 == to { on = 0 } on'; }

cat > "$out" <<RS
//! Every OCSF event class and object, each with every attribute it has, from OCSF ${version}.
//! Generated by \`tools/ocsf-schema.sh ${version}\` from ${url} at ${version}; do not edit by hand.

use super::ocsf::{Attr, Class, Object, Ty::*};

pub const VERSION: &str = "${version}";

/// Each category's uid, name and caption.
pub const CATEGORIES: &[(u32, &str, &str)] = &[
$(section CATEGORIES ATTRS)
];

/// Every attribute some class or object has, once however many have it alike.
pub const ATTRS: &[Attr] = &[
$(section ATTRS OBJECTS)
];

/// By name, in byte order.
pub const OBJECTS: &[Object] = &[
$(section OBJECTS CLASSES)
];

/// By class uid.
pub const CLASSES: &[Class] = &[
$(section CLASSES END)
];
RS
# Formatted as `cargo fmt` keeps it.
rustfmt --edition 2024 "$out"
