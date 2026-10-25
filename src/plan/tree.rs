//! The name tree: the record as the compiler holds it. A name is a leaf, one slot, or an object: its fields, and the
//! slot of its unclaimed fields where a spread gave it some. The record is the root object. A plan carries the finished
//! tree to the serializer, which writes at every object its fields, then its unclaimed fields a field does not name.

use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::core::id::SlotId;

#[derive(Clone, Debug, PartialEq)]
pub enum Node {
    Leaf(SlotId),
    Object(Object),
}

/// A slot's place in the tree: the leaf at a name, or the unclaimed fields of the object at a name. `""` is the root.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Place {
    Leaf(String),
    Unclaimed(String),
}

impl Place {
    /// The name the place sits at: the leaf's, or the object's.
    pub fn name(&self) -> &str {
        match self {
            Place::Leaf(n) | Place::Unclaimed(n) => n,
        }
    }

    /// The same place under `prefix`.
    pub fn under(&self, prefix: &str) -> Place {
        match self {
            Place::Leaf(n) => Place::Leaf(join(prefix, n)),
            Place::Unclaimed(n) => Place::Unclaimed(join(prefix, n)),
        }
    }
}

impl std::fmt::Display for Place {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Place::Leaf(n) => f.write_str(n),
            Place::Unclaimed(n) => f.write_str(&join(n, "*")),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Object {
    pub fields: BTreeMap<String, Node>,
    /// The keys a spread laid here that no field takes: a variant object per row. An object that has them is open
    /// (`DataType::Struct`'s second field): a read reaches them, each value a variant, and a write by key puts one
    /// there. One that has none is closed.
    pub unclaimed: Option<SlotId>,
    /// Keys of `unclaimed` that are gone, though its column still holds them: a step claimed each, and a rename then
    /// took the name away. Every reader of the column leaves them out, so the rename moves no data.
    pub removed: Vec<String>,
}

/// The root object's name in a pipeline: `$`, as in `$["@timestamp"]`. The tree's own name for it is `""`.
pub const ROOT: &str = "$";

/// What a record's field list names its root's unclaimed fields: a column holding one object per row, of every key
/// no field claims. No path spells it, so no field takes it.
pub const UNCLAIMED: &str = "...";

/// Whether `key`, a field of the root, is the engine's own: `__live` and a source's metadata. The serializer leaves
/// them out of the record, and `$` does too.
pub fn is_internal(key: &str) -> bool {
    key.starts_with("__")
}

/// Whether `key` is written bare in a name: it is not empty and holds none of `.` `[` `]` `"` `\`. Any other key is
/// written as a JSON string in brackets, `labels["app.kubernetes.io/name"]`, the way code reads it.
pub fn is_plain(key: &str) -> bool {
    !key.is_empty() && !key.bytes().any(|b| matches!(b, b'.' | b'[' | b']' | b'"' | b'\\'))
}

/// Append `key`, one key, to the name `path`.
pub fn push_key(path: &mut String, key: &str) {
    if is_plain(key) {
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(key);
    } else {
        path.push('[');
        path.push_str(&spell_quoted(key));
        path.push(']');
    }
}

/// `key`, a key a parser read, as a name: one key, or with `split_dots` a key at each of its dots.
pub fn name_of(key: &str, split_dots: bool) -> Cow<'_, str> {
    let plain = match split_dots {
        true => key.split('.').all(is_plain),
        false => is_plain(key),
    };
    if plain {
        return Cow::Borrowed(key);
    }
    let mut out = String::new();
    match split_dots {
        true => key.split('.').for_each(|k| push_key(&mut out, k)),
        false => push_key(&mut out, key),
    }
    Cow::Owned(out)
}

/// `key`, one key, as a name.
pub fn spell(key: &str) -> Cow<'_, str> {
    if is_plain(key) {
        return Cow::Borrowed(key);
    }
    let mut out = String::new();
    push_key(&mut out, key);
    Cow::Owned(out)
}

/// The first key of `name` and where it ends: the byte its spelling ends at, and where the rest of the name starts
/// (past a dot). A bracket that does not close is part of a bare key.
fn first(name: &str) -> (Cow<'_, str>, usize, usize) {
    let after = |end: usize| {
        if name.as_bytes().get(end) == Some(&b'.') {
            end + 1
        } else {
            end
        }
    };
    if let Some(quoted) = name.strip_prefix('[')
        && let Some((key, len)) = json_string(quoted)
        && quoted.as_bytes().get(len) == Some(&b']')
    {
        return (key, len + 2, after(len + 2));
    }
    let skip = usize::from(name.starts_with('['));
    let end = name[skip..]
        .bytes()
        .position(|b| b == b'.' || b == b'[')
        .map_or(name.len(), |p| p + skip);
    (Cow::Borrowed(&name[..end]), end, after(end))
}

/// The first key of `name`, and the rest of the name below it where there is one.
pub fn split_first(name: &str) -> (Cow<'_, str>, Option<&str>) {
    let (key, _, rest) = first(name);
    (key, (rest < name.len()).then(|| &name[rest..]))
}

/// The JSON string `s` starts with, decoded, and its length with its quotes.
fn json_string(s: &str) -> Option<(Cow<'_, str>, usize)> {
    let body = s.strip_prefix('"')?;
    let mut escaped = false;
    let close = body.bytes().position(|b| {
        let at = !escaped && b == b'"';
        escaped = !escaped && b == b'\\';
        at
    })?;
    let text = &body[..close];
    let key = if text.contains('\\') {
        Cow::Owned(serde_json::from_str(&s[..close + 2]).ok()?)
    } else {
        Cow::Borrowed(text)
    };
    Some((key, close + 2))
}

/// Each key of `name` with the byte its spelling ends at, `MAX_NESTING` keys at most: past the bound the rest of a
/// name is one key, as the serializer writes it.
fn keys(name: &str) -> impl Iterator<Item = (Cow<'_, str>, usize)> {
    let mut at = Some(0);
    let mut count = 0;
    std::iter::from_fn(move || {
        let start = at?;
        let rest = &name[start..];
        count += 1;
        if count == crate::format::json::MAX_NESTING {
            at = None;
            return Some((rest_key(rest), name.len()));
        }
        let (key, end, next) = first(rest);
        at = (start + next < name.len()).then_some(start + next);
        Some((key, start + end))
    })
}

/// `rest`, the rest of a name past the nesting bound, as the one key it is there: decoded where it spells one key.
pub fn rest_key(rest: &str) -> Cow<'_, str> {
    match first(rest) {
        (key, end, _) if end == rest.len() => key,
        _ => Cow::Borrowed(rest),
    }
}

/// The keys of `name`, decoded, `MAX_NESTING` at most: past the bound the rest of a name is one key, as the
/// serializer writes it.
pub fn segments(name: &str) -> impl Iterator<Item = Cow<'_, str>> {
    keys(name).map(|(key, _)| key)
}

/// The first key of `name`, as `name` spells it.
pub fn head(name: &str) -> &str {
    &name[..first(name).1]
}

/// `name` and `path`, a name under it, joined. Under `$`, the record, `path` is its own name.
pub fn join(name: &str, path: &str) -> String {
    match (name.is_empty() || name == ROOT && !path.is_empty(), path.is_empty()) {
        (true, _) => path.to_string(),
        (_, true) => name.to_string(),
        _ if path.starts_with('[') => format!("{name}{path}"),
        _ => format!("{name}.{path}"),
    }
}

/// The name of the key `key` of the object `name`.
pub fn child(name: &str, key: &str) -> String {
    let mut out = name.to_string();
    push_key(&mut out, key);
    out
}

/// `name`'s path from `prefix`, an object at or above it: what `join` joined to `prefix`.
pub fn key_under<'n>(prefix: &str, name: &'n str) -> &'n str {
    match name.strip_prefix(prefix) {
        Some(rest) if !prefix.is_empty() => rest.strip_prefix('.').unwrap_or(rest),
        _ => name,
    }
}

/// The object `name` sits in, and its key there. The root's own fields sit in `""`.
pub fn parent(name: &str) -> (&str, Cow<'_, str>) {
    let (mut above, mut end, mut key) = (0, 0, Cow::Borrowed(""));
    for (k, e) in keys(name) {
        (above, end, key) = (end, e, k);
    }
    (&name[..above], key)
}

/// The names of the objects above `name`, the outermost first.
pub fn ancestors(name: &str) -> impl Iterator<Item = &str> {
    let mut ends = keys(name).map(|(_, end)| end).peekable();
    std::iter::from_fn(move || {
        let end = ends.next()?;
        ends.peek().is_some().then(|| &name[..end])
    })
}

/// Read `text`, a name as a person writes one in a step's field, into its spelling here: keys joined by dots, a key
/// that is not plain as a JSON string in brackets. The error says what is wrong with it.
pub fn parse_name(text: &str) -> Result<String, String> {
    let mut out = String::new();
    // `$` is the record, so `$.a` is the name `a`, as the language reads it.
    let mut rest = match text.strip_prefix(ROOT) {
        Some("") => return Ok(ROOT.to_string()),
        Some(r) if r.starts_with('[') || r.len() > 1 && r.starts_with('.') => r.strip_prefix('.').unwrap_or(r),
        _ => text,
    };
    while !rest.is_empty() {
        if let Some(quoted) = rest.strip_prefix('[') {
            let Some((key, len)) = json_string(quoted).filter(|(_, len)| quoted.as_bytes().get(*len) == Some(&b']'))
            else {
                return Err(format!(
                    "`{text}`: a key in brackets is a string in quotes, as in `a[\"b.c\"]`"
                ));
            };
            push_key(&mut out, &key);
            rest = &quoted[len + 1..];
        } else {
            let end = rest.bytes().position(|b| b == b'.' || b == b'[').unwrap_or(rest.len());
            let key = &rest[..end];
            if !is_plain(key) {
                return Err(match key {
                    "" => format!("`{text}` has an empty key"),
                    _ => format!(
                        "`{text}`: write the key `{key}` in brackets, as `[{}]`",
                        spell_quoted(key)
                    ),
                });
            }
            push_key(&mut out, key);
            rest = &rest[end..];
        }
        if let Some(after) = rest.strip_prefix('.') {
            if after.is_empty() || after.starts_with('[') {
                return Err(format!("`{text}` has an empty key"));
            }
            rest = after;
        } else if !rest.is_empty() && !rest.starts_with('[') {
            return Err(format!("`{text}`: a key in brackets ends the name or meets `.` or `[`"));
        }
    }
    if out.is_empty() {
        return Err("a name is needed".into());
    }
    Ok(out)
}

/// `key` as a JSON string.
pub fn spell_quoted(key: &str) -> String {
    serde_json::to_string(key).expect("a str is JSON")
}

impl Object {
    /// The node at `name`.
    pub fn get(&self, name: &str) -> Option<&Node> {
        let mut segs = segments(name);
        let mut node = self.fields.get(segs.next()?.as_ref())?;
        for seg in segs {
            match node {
                Node::Object(o) => node = o.fields.get(seg.as_ref())?,
                Node::Leaf(_) => return None,
            }
        }
        Some(node)
    }

    /// The object at `name`. `""` is this one.
    pub fn object(&self, name: &str) -> Option<&Object> {
        if name.is_empty() {
            return Some(self);
        }
        match self.get(name)? {
            Node::Object(o) => Some(o),
            Node::Leaf(_) => None,
        }
    }

    /// `object`, to change it.
    pub fn object_mut(&mut self, name: &str) -> Option<&mut Object> {
        if name.is_empty() {
            return Some(self);
        }
        let mut at = self;
        for seg in segments(name) {
            match at.fields.get_mut(seg.as_ref())? {
                Node::Object(o) => at = o,
                Node::Leaf(_) => return None,
            }
        }
        Some(at)
    }

    /// The slot of the leaf at `name`.
    pub fn leaf(&self, name: &str) -> Option<SlotId> {
        match self.get(name)? {
            Node::Leaf(slot) => Some(*slot),
            Node::Object(_) => None,
        }
    }

    /// The leaf a path under it meets on the way to `name`: the longest name before one of `name`'s dots that is a
    /// leaf, with its slot.
    pub fn leaf_above<'n>(&self, name: &'n str) -> Option<(&'n str, SlotId)> {
        let mut at = self;
        let keys: Vec<(Cow<str>, usize)> = keys(name).collect();
        for (key, end) in &keys[..keys.len().saturating_sub(1)] {
            match at.fields.get(key.as_ref()) {
                Some(Node::Object(o)) => at = o,
                Some(Node::Leaf(slot)) => return Some((&name[..*end], *slot)),
                None => return None,
            }
        }
        None
    }

    /// The deepest object on the way to `name`, with its name, and the rest of `name` below it. `name` itself is
    /// not reached: a leaf or a missing name ends the walk at the object above it.
    pub fn deepest<'s, 'n>(&'s self, name: &'n str) -> (&'s Object, &'n str, &'n str) {
        let mut at = self;
        let mut end = 0;
        let keys: Vec<(Cow<str>, usize)> = keys(name).collect();
        for (key, e) in &keys[..keys.len().saturating_sub(1)] {
            let Some(Node::Object(o)) = at.fields.get(key.as_ref()) else {
                break;
            };
            at = o;
            end = *e;
        }
        (at, &name[..end], key_under(&name[..end], name))
    }

    /// Remove the node at `name`. The objects above it stay, empty or not: each still claims its key.
    pub fn take(&mut self, name: &str) -> Option<Node> {
        let (parent, key) = parent(name);
        self.object_mut(parent)?.fields.remove(key.as_ref())
    }

    /// Put `node` at `name`, making each object on the way, and return what was there. A leaf on the way becomes an
    /// object: a write under a leaf is refused before it gets here.
    pub fn put(&mut self, name: &str, node: Node) -> Option<Node> {
        let segs: Vec<Cow<str>> = segments(name).collect();
        let (last, path) = segs.split_last().expect("a name has a segment");
        let mut at = self;
        for seg in path {
            let entry = at
                .fields
                .entry(seg.to_string())
                .or_insert_with(|| Node::Object(Object::default()));
            if let Node::Leaf(_) = entry {
                *entry = Node::Object(Object::default());
            }
            let Node::Object(o) = entry else {
                unreachable!("made an object above")
            };
            at = o;
        }
        at.fields.insert(last.to_string(), node)
    }

    /// Every leaf under this object, with its name, sorted by name.
    pub fn leaves(&self) -> Vec<(String, SlotId)> {
        let mut out = Vec::new();
        self.walk("", &mut |name, node| {
            if let Node::Leaf(slot) = node {
                out.push((name.to_string(), *slot));
            }
        });
        out.sort();
        out
    }

    /// Every object under this one that holds unclaimed fields, this one included as `""`, with its slot.
    pub fn unclaimed_slots(&self) -> Vec<(String, SlotId)> {
        let mut out: Vec<(String, SlotId)> = self.unclaimed.map(|u| (String::new(), u)).into_iter().collect();
        self.walk("", &mut |name, node| {
            if let Node::Object(Object { unclaimed: Some(u), .. }) = node {
                out.push((name.to_string(), *u));
            }
        });
        out
    }

    /// Every slot the tree names, by its place: its leaves and its unclaimed fields.
    pub fn places(&self) -> Vec<(Place, SlotId)> {
        let mut out: Vec<(Place, SlotId)> = self.leaves().into_iter().map(|(n, s)| (Place::Leaf(n), s)).collect();
        out.extend(
            self.unclaimed_slots()
                .into_iter()
                .map(|(n, s)| (Place::Unclaimed(n), s)),
        );
        out
    }

    /// Every slot the tree names.
    pub fn slots(&self) -> Vec<SlotId> {
        self.places().into_iter().map(|(_, s)| s).collect()
    }

    /// The slot at `place`.
    pub fn slot(&self, place: &Place) -> Option<SlotId> {
        match place {
            Place::Leaf(n) => self.leaf(n),
            Place::Unclaimed(n) => self.object(n)?.unclaimed,
        }
    }

    /// Every object under this one, with its name, parents before children. This one is not among them.
    pub fn objects(&self) -> Vec<(String, &Object)> {
        let mut out = Vec::new();
        self.walk("", &mut |name, node| {
            if let Node::Object(o) = node {
                out.push((name.to_string(), o));
            }
        });
        out
    }

    /// Visit every node under this object, depth first, with its name.
    fn walk<'s>(&'s self, prefix: &str, f: &mut impl FnMut(&str, &'s Node)) {
        for (key, node) in &self.fields {
            let name = child(prefix, key);
            f(&name, node);
            if let Node::Object(o) = node {
                o.walk(&name, f);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every key a name can hold is spelled once and read back as itself.
    #[test]
    fn a_key_round_trips_through_its_spelling() {
        for key in [
            "app.kubernetes.io/name",
            "a[0]",
            r#"say "hi""#,
            r"back\slash",
            "",
            "]",
            "@timestamp",
            "x-y",
        ] {
            let name = child("labels", key);
            assert_eq!(segments(&name).collect::<Vec<_>>(), ["labels", key], "{name}");
            assert_eq!(parent(&name), ("labels", Cow::Borrowed(key)), "{name}");
            assert_eq!(parse_name(&name).as_deref(), Ok(name.as_str()), "{name}");
        }
    }

    /// A plain key is written bare; any other in brackets, with no dot before them.
    #[test]
    fn only_a_key_that_is_not_plain_takes_brackets() {
        assert_eq!(child("", "@timestamp"), "@timestamp");
        assert_eq!(child("a", "x-y"), "a.x-y");
        assert_eq!(child("", "a.b"), r#"["a.b"]"#);
        assert_eq!(
            child("k8s.labels", "app.kubernetes.io/name"),
            r#"k8s.labels["app.kubernetes.io/name"]"#
        );
        assert_eq!(join(r#"["a.b"]"#, r#"["c.d"].e"#), r#"["a.b"]["c.d"].e"#);
        assert_eq!(name_of("a.b", true), "a.b");
        assert_eq!(name_of("a.b", false), r#"["a.b"]"#);
    }

    #[test]
    fn ancestors_and_the_rest_of_a_name_follow_its_keys() {
        let name = r#"a["b.c"].d"#;
        assert_eq!(ancestors(name).collect::<Vec<_>>(), ["a", r#"a["b.c"]"#]);
        assert_eq!(key_under("a", name), r#"["b.c"].d"#);
        assert_eq!(head(name), "a");
        assert_eq!(split_first(r#"["b.c"].d"#), (Cow::Borrowed("b.c"), Some("d")));
    }

    /// Past the nesting bound the rest of a name is one key.
    #[test]
    fn a_name_has_at_most_the_bound_of_keys() {
        let deep = vec!["k"; 100].join(".");
        let keys: Vec<_> = segments(&deep).collect();
        assert_eq!(keys.len(), crate::format::json::MAX_NESTING);
        assert_eq!(
            keys.last().unwrap().split('.').count(),
            100 - crate::format::json::MAX_NESTING + 1
        );
    }

    /// Past the nesting bound the rest of a name is one key, decoded where it spells one.
    #[test]
    fn a_bracketed_key_past_the_bound_is_decoded() {
        let mut name = vec!["k"; crate::format::json::MAX_NESTING - 1].join(".");
        push_key(&mut name, "a.b");
        assert_eq!(segments(&name).last().as_deref(), Some("a.b"));
        name.push_str(".c");
        assert_eq!(segments(&name).last().as_deref(), Some(r#"["a.b"].c"#));
    }

    /// A field's text that is no name is refused, with what is wrong with it.
    #[test]
    fn parse_name_refuses_what_no_key_spells() {
        for bad in [r#"labels["app"#, r#"say "hi""#, "a..b", "a.", "a[0]", r#"a["b"]c"#, ""] {
            assert!(parse_name(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_name(r#"a["b"]"#).as_deref(), Ok("a.b"));
    }

    #[test]
    fn a_name_under_the_record_is_its_own() {
        for (text, want) in [("$", "$"), ("$.a", "a"), ("$.a.b", "a.b"), (r#"$["a.b"]"#, r#"["a.b"]"#), ("$a", "$a")] {
            assert_eq!(parse_name(text).as_deref(), Ok(want), "{text}");
        }
        assert!(parse_name("$.").is_err());
        assert_eq!(join(ROOT, "a.b"), "a.b");
        assert_eq!(join(ROOT, ""), ROOT);
    }
}
