//! A section file merged by object: two branches that each changed a different object of one section both keep
//! their change, and only an object both changed differently needs someone to choose.
//!
//! Objects are compared as JSON values and written as the text they were read as, so layout differences never make
//! a conflict and an object nobody changed keeps its bytes.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_json::value::RawValue;
use std::collections::{BTreeSet, HashMap};

use super::bundle::{GROUPS_DIR, LOOKUP_TABLES_FILE, SECTION_FILES, SHARED_DIR};

/// One side of a merge: `main`, or the branch being brought current with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Side {
    Main,
    Work,
}

/// An object both sides changed, each to something different. A version is `None` on a side that does not have it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectConflict {
    pub id: String,
    pub base: Option<String>,
    pub main: Option<String>,
    pub work: Option<String>,
    /// The bundle-relative file `main`'s version names: a lookup table's content. The chosen side's file comes with
    /// it, so a table never points at the other side's content.
    pub main_names: Option<String>,
    /// The same for the branch's version.
    pub work_names: Option<String>,
}

impl ObjectConflict {
    /// The file `side`'s version names, if any.
    pub fn names(&self, side: Side) -> Option<&str> {
        match side {
            Side::Main => self.main_names.as_deref(),
            Side::Work => self.work_names.as_deref(),
        }
    }
}

pub enum SectionMerge {
    /// Merged by object. `merged` is the file once every conflict has a choice, and `None` until then.
    Objects {
        conflicts: Vec<ObjectConflict>,
        merged: Option<Vec<u8>>,
    },
    /// Not mergeable by object: a side deleted the file, a version is not an array of objects with unique ids, or
    /// the two sides put the objects they share in different orders. The file is merged as a whole.
    Whole,
}

/// Whether `path`, relative to the repository, is a section file directly under a group's or `shared/`'s bundle.
pub fn is_section_file(path: &str) -> bool {
    let (bundle, file) = match path.rsplit_once('/') {
        Some(split) => split,
        None => return false,
    };
    let in_bundle = bundle == SHARED_DIR
        || bundle
            .strip_prefix(GROUPS_DIR)
            .and_then(|rest| rest.strip_prefix('/'))
            .is_some_and(|group| !group.is_empty() && !group.contains('/'));
    in_bundle && SECTION_FILES.contains(&file)
}

/// Three-way merge of one section file. `file` is its name in the bundle (`sources.json`); `choice` answers for an
/// object both sides changed, by id. A file both sides added merges against an empty base.
pub fn merge_section(
    file: &str,
    base: Option<&[u8]>,
    main: Option<&[u8]>,
    work: Option<&[u8]>,
    choice: impl Fn(&str) -> Option<Side>,
) -> SectionMerge {
    // A deleted file is a choice of the whole: merging it by object would bring back a section of a bundle the side
    // removed.
    if base.is_some() && (main.is_none() || work.is_none()) {
        return SectionMerge::Whole;
    }
    let (Some(base), Some(main), Some(work)) = (objects(base), objects(main), objects(work)) else {
        return SectionMerge::Whole;
    };
    let ids: BTreeSet<&str> = [&base, &main, &work]
        .into_iter()
        .flat_map(|side| side.iter().map(|o| o.id.as_str()))
        .collect();
    let find = |side: &'_ [Object<'_>], id: &str| side.iter().position(|o| o.id == id);

    // Which side's version each id takes, or `None` when it is gone from the result.
    let mut taken: HashMap<&str, (Side, usize)> = HashMap::new();
    let mut conflicts = Vec::new();
    let mut unresolved = false;
    for id in ids {
        let (b, m, w) = (find(&base, id), find(&main, id), find(&work, id));
        let (bv, mv, wv) = (value_at(&base, b), value_at(&main, m), value_at(&work, w));
        let side = if mv == wv || wv == bv {
            Some(Side::Main)
        } else if mv == bv {
            Some(Side::Work)
        } else {
            let text = |side: &[Object<'_>], at: Option<usize>| at.map(|i| side[i].raw.get().to_string());
            let names = |v: Option<&Value>| {
                v.filter(|_| file == LOOKUP_TABLES_FILE)
                    .and_then(|v| v["path"].as_str())
                    .map(str::to_string)
            };
            conflicts.push(ObjectConflict {
                id: id.to_string(),
                base: text(&base, b),
                main: text(&main, m),
                work: text(&work, w),
                main_names: names(mv),
                work_names: names(wv),
            });
            let chosen = choice(id);
            unresolved |= chosen.is_none();
            chosen
        };
        match side {
            Some(Side::Main) => m.map(|i| taken.insert(id, (Side::Main, i))),
            Some(Side::Work) => w.map(|i| taken.insert(id, (Side::Work, i))),
            None => None,
        };
    }

    let Some(order) = merged_order(&ids_of(&base), &ids_of(&main), &ids_of(&work), &taken) else {
        return SectionMerge::Whole;
    };
    let merged = (!unresolved).then(|| {
        let texts: Vec<&str> = order
            .iter()
            .map(|id| match taken[id] {
                (Side::Main, i) => main[i].raw.get(),
                (Side::Work, i) => work[i].raw.get(),
            })
            .collect();
        layout(&texts)
    });
    SectionMerge::Objects { conflicts, merged }
}

/// Whether `merged`, git's text merge of one section file, keeps what each side did to every object: an object one
/// side changed is that side's version, and an object both changed holds, in each top-level field, what the side that
/// changed the field made of it. A line merge can do otherwise and call it clean -- an edit lined up with a copy of
/// the lines it changed lands in another object -- so a text merge that does not keep this is not taken.
pub fn text_merge_keeps_each_side(base: &[u8], main: &[u8], work: &[u8], merged: &[u8]) -> bool {
    let (Some(base), Some(main), Some(work), Some(merged)) = (
        objects(Some(base)),
        objects(Some(main)),
        objects(Some(work)),
        objects(Some(merged)),
    ) else {
        return false;
    };
    let ids: BTreeSet<&str> = [&base, &main, &work]
        .into_iter()
        .flat_map(|side| side.iter().map(|o| o.id.as_str()))
        .collect();
    let find = |side: &'_ [Object<'_>], id: &str| side.iter().position(|o| o.id == id);
    let mut taken: HashMap<&str, (Side, usize)> = HashMap::new();
    for &id in &ids {
        let (b, m, w) = (find(&base, id), find(&main, id), find(&work, id));
        let (bv, mv, wv) = (value_at(&base, b), value_at(&main, m), value_at(&work, w));
        let at = find(&merged, id);
        let expected = if mv == wv || wv == bv {
            mv.cloned()
        } else if mv == bv {
            wv.cloned()
        } else {
            // Both changed it: neither may have removed it, and no field may be one both changed.
            let (Some(mv), Some(wv)) = (mv, wv) else {
                return false;
            };
            let Some(fields) = fields_merged(bv, mv, wv) else {
                return false;
            };
            Some(fields)
        };
        if value_at(&merged, at) != expected.as_ref() {
            return false;
        }
        // Which objects are kept, for the order they must be in.
        if let (Some(_), Some(i)) = (at, m.or(w)) {
            taken.insert(id, (if m.is_some() { Side::Main } else { Side::Work }, i));
        }
    }
    if merged.iter().any(|o| !ids.contains(o.id.as_str())) {
        return false;
    }
    merged_order(&ids_of(&base), &ids_of(&main), &ids_of(&work), &taken).as_deref() == Some(&ids_of(&merged)[..])
}

/// An object both sides changed, merged field by field: each top-level field takes the side that changed it. `None`
/// when a field was changed by both, differently.
fn fields_merged(base: Option<&Value>, main: &Value, work: &Value) -> Option<Value> {
    let (main, work) = (main.as_object()?, work.as_object()?);
    let empty = serde_json::Map::new();
    let base = base.and_then(Value::as_object).unwrap_or(&empty);
    let keys: BTreeSet<&String> = base.keys().chain(main.keys()).chain(work.keys()).collect();
    let mut merged = serde_json::Map::new();
    for key in keys {
        let (b, m, w) = (base.get(key), main.get(key), work.get(key));
        let kept = if m == w || w == b {
            m
        } else if m == b {
            w
        } else {
            return None;
        };
        if let Some(v) = kept {
            merged.insert(key.clone(), v.clone());
        }
    }
    Some(Value::Object(merged))
}

struct Object<'a> {
    id: String,
    raw: &'a RawValue,
    value: Value,
}

fn value_at<'s>(side: &'s [Object<'_>], at: Option<usize>) -> Option<&'s Value> {
    at.map(|i| &side[i].value)
}

fn ids_of<'s>(side: &'s [Object<'_>]) -> Vec<&'s str> {
    side.iter().map(|o| o.id.as_str()).collect()
}

/// A section's objects in file order. `None` when it is not an array of objects whose ids are unique strings.
fn objects(bytes: Option<&[u8]>) -> Option<Vec<Object<'_>>> {
    let Some(bytes) = bytes else {
        return Some(vec![]);
    };
    let raws: Vec<&RawValue> = serde_json::from_slice(bytes).ok()?;
    let mut seen = BTreeSet::new();
    raws.into_iter()
        .map(|raw| {
            let value: Value = serde_json::from_str(raw.get()).ok()?;
            let id = value.as_object()?.get("id")?.as_str()?.to_string();
            seen.insert(id.clone()).then_some(Object { id, raw, value })
        })
        .collect()
}

/// The order of the merged section. Order is meaningful -- routes match in it -- so one side's order of the objects
/// all three versions share is kept: the side that reordered them, or `main` when neither did. The other side's own
/// additions go in before the object that followed them there, or at the end. `None` when both sides reordered differently.
fn merged_order<'a>(
    base: &[&'a str],
    main: &[&'a str],
    work: &[&'a str],
    taken: &HashMap<&str, (Side, usize)>,
) -> Option<Vec<&'a str>> {
    let shared = |side: &[&'a str], with: &[&str]| -> Vec<&'a str> {
        side.iter().copied().filter(|id| with.contains(id)).collect()
    };
    let reordered = |side: &[&'a str]| shared(side, base) != shared(base, side);
    let (lead, other) = match (reordered(main), reordered(work)) {
        (true, true) if shared(&shared(main, base), work) != shared(&shared(work, base), main) => return None,
        (false, true) => (work, main),
        _ => (main, work),
    };
    let mut order: Vec<&'a str> = lead.iter().copied().filter(|id| taken.contains_key(id)).collect();
    for (i, id) in other.iter().enumerate() {
        if !taken.contains_key(id) || order.contains(id) {
            continue;
        }
        let at = other[i + 1..]
            .iter()
            .find_map(|after| order.iter().position(|o| o == after))
            .unwrap_or(order.len());
        order.insert(at, id);
    }
    Some(order)
}

/// Objects laid out as `serde_json::to_vec_pretty` lays out an array, so an object the editor wrote keeps its bytes.
fn layout(objects: &[&str]) -> Vec<u8> {
    if objects.is_empty() {
        return b"[]".to_vec();
    }
    format!("[\n  {}\n]", objects.join(",\n  ")).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(base: &str, main: &str, work: &str, choice: impl Fn(&str) -> Option<Side>) -> SectionMerge {
        merge_section(
            "sources.json",
            Some(base.as_bytes()),
            Some(main.as_bytes()),
            Some(work.as_bytes()),
            choice,
        )
    }

    fn keeps(base: &str, main: &str, work: &str, merged: &str) -> bool {
        text_merge_keeps_each_side(base.as_bytes(), main.as_bytes(), work.as_bytes(), merged.as_bytes())
    }

    #[test]
    fn a_text_merge_of_different_fields_of_one_object_keeps_each_side() {
        let base = r#"[{"id":"a","x":0,"y":0}]"#;
        assert!(keeps(
            base,
            r#"[{"id":"a","x":1,"y":0}]"#,
            r#"[{"id":"a","x":0,"y":2}]"#,
            r#"[{"id":"a","x":1,"y":2}]"#
        ));
    }

    #[test]
    fn a_text_merge_that_moves_an_edit_into_another_object_does_not() {
        let base = r#"[{"id":"a","v":0}]"#;
        let main = r#"[{"id":"a","v":1}]"#;
        let work = r#"[{"id":"a","v":0},{"id":"b","v":0}]"#;
        assert!(keeps(base, main, work, r#"[{"id":"a","v":1},{"id":"b","v":0}]"#));
        assert!(!keeps(base, main, work, r#"[{"id":"a","v":0},{"id":"b","v":1}]"#));
    }

    #[test]
    fn a_field_both_changed_or_an_object_dropped_is_not_kept() {
        let base = r#"[{"id":"a","v":0},{"id":"b"}]"#;
        let (main, work) = (r#"[{"id":"a","v":1},{"id":"b"}]"#, r#"[{"id":"a","v":2},{"id":"b"}]"#);
        assert!(!keeps(base, main, work, r#"[{"id":"a","v":2},{"id":"b"}]"#));
        assert!(!keeps(base, base, base, r#"[{"id":"a","v":0}]"#));
    }

    fn merged(base: &str, main: &str, work: &str) -> String {
        match merge(base, main, work, |_| None) {
            SectionMerge::Objects {
                conflicts,
                merged: Some(bytes),
            } if conflicts.is_empty() => String::from_utf8(bytes).unwrap(),
            SectionMerge::Objects { conflicts, .. } => panic!("conflicts: {conflicts:?}"),
            SectionMerge::Whole => panic!("merged as a whole"),
        }
    }

    #[test]
    fn each_side_keeps_its_own_addition_in_the_editors_layout() {
        let object = |id: &str| format!("{{\n    \"id\": \"{id}\",\n    \"type\": \"stdin\"\n  }}");
        let main = format!("[\n  {}\n]", object("a"));
        let work = format!("[\n  {}\n]", object("b"));
        assert_eq!(
            merged("[]", &main, &work),
            format!("[\n  {},\n  {}\n]", object("a"), object("b"))
        );
    }

    #[test]
    fn an_object_nobody_changed_keeps_its_bytes() {
        let base = r#"[{"type":"stdin","id":"a"}, {"id":"b"}]"#;
        let main = r#"[{"type":"stdin","id":"a"}, {"id":"b","v":1}]"#;
        let work = r#"[{"type":"stdin","id":"a"}, {"id":"b"}, {"id":"c"}]"#;
        assert_eq!(
            merged(base, main, work),
            "[\n  {\"type\":\"stdin\",\"id\":\"a\"},\n  {\"id\":\"b\",\"v\":1},\n  {\"id\":\"c\"}\n]"
        );
    }

    #[test]
    fn a_change_on_one_side_wins_deletion_included_and_layout_is_no_change() {
        let base = r#"[{"id":"a","v":0},{"id":"b","v":0}]"#;
        let main = r#"[{"id":"a","v":1},{"id":"b","v":0}]"#;
        let work = "[\n  {\"v\": 0, \"id\": \"a\"}\n]";
        assert_eq!(merged(base, main, work), "[\n  {\"id\":\"a\",\"v\":1}\n]");
    }

    #[test]
    fn the_same_change_on_both_sides_is_kept_once() {
        assert_eq!(
            merged("[]", r#"[{"id":"a","v":1}]"#, r#"[{"id":"a","v":1}]"#),
            "[\n  {\"id\":\"a\",\"v\":1}\n]"
        );
    }

    #[test]
    fn a_section_both_added_merges_against_an_empty_one() {
        let got = merge_section("sources.json", None, Some(br#"[{"id":"a"}]"#), Some(b"[]"), |_| None);
        assert!(matches!(got, SectionMerge::Objects { merged: Some(ref m), .. } if m == b"[\n  {\"id\":\"a\"}\n]"));
    }

    #[test]
    fn a_section_a_side_deleted_merges_as_a_whole() {
        let base: &[u8] = br#"[{"id":"a"}]"#;
        let more: &[u8] = br#"[{"id":"a"},{"id":"b"}]"#;
        assert!(matches!(
            merge_section("sources.json", Some(base), None, Some(more), |_| None),
            SectionMerge::Whole
        ));
        assert!(matches!(
            merge_section("sources.json", Some(base), Some(more), None, |_| None),
            SectionMerge::Whole
        ));
    }

    #[test]
    fn edit_and_edit_and_edit_and_delete_wait_for_a_choice_and_take_it() {
        let base = r#"[{"id":"a","v":0},{"id":"b","v":0}]"#;
        let main = r#"[{"id":"a","v":1},{"id":"b","v":1}]"#;
        let work = r#"[{"id":"a","v":2}]"#;
        let SectionMerge::Objects { conflicts, merged } = merge(base, main, work, |_| None) else {
            panic!("merged as a whole")
        };
        assert_eq!(merged, None);
        assert_eq!(conflicts.iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(conflicts[1].work, None, "b is gone on this branch");

        let choose = |id: &str| Some(if id == "a" { Side::Main } else { Side::Work });
        let SectionMerge::Objects {
            merged: Some(bytes), ..
        } = merge(base, main, work, choose)
        else {
            panic!("no merge")
        };
        assert_eq!(bytes, b"[\n  {\"id\":\"a\",\"v\":1}\n]");
    }

    #[test]
    fn additions_go_in_after_the_object_they_followed() {
        let base = r#"[{"id":"a"},{"id":"b"}]"#;
        let main = r#"[{"id":"x"},{"id":"a"},{"id":"b"}]"#;
        let work = r#"[{"id":"a"},{"id":"y"},{"id":"b"}]"#;
        let ids: Vec<String> = serde_json::from_str::<Vec<Value>>(&merged(base, main, work))
            .unwrap()
            .iter()
            .map(|v| v["id"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(ids, ["x", "a", "y", "b"]);
    }

    #[test]
    fn one_sides_reorder_is_kept() {
        let base = r#"[{"id":"a"},{"id":"b"}]"#;
        let main = r#"[{"id":"a"},{"id":"b"},{"id":"c"}]"#;
        let work = r#"[{"id":"b"},{"id":"a"}]"#;
        assert_eq!(
            merged(base, main, work),
            "[\n  {\"id\":\"b\"},\n  {\"id\":\"a\"},\n  {\"id\":\"c\"}\n]"
        );
    }

    #[test]
    fn different_reorders_or_objects_without_ids_merge_as_a_whole() {
        let base = r#"[{"id":"a"},{"id":"b"},{"id":"c"}]"#;
        let main = r#"[{"id":"b"},{"id":"a"},{"id":"c"}]"#;
        let work = r#"[{"id":"a"},{"id":"c"},{"id":"b"}]"#;
        assert!(matches!(merge(base, main, work, |_| None), SectionMerge::Whole));
        assert!(matches!(
            merge("[]", r#"[{"name":"a"}]"#, "[]", |_| None),
            SectionMerge::Whole
        ));
        assert!(matches!(
            merge("[]", r#"[{"id":"a"},{"id":"a"}]"#, "[]", |_| None),
            SectionMerge::Whole
        ));
        assert!(matches!(merge("[]", "not json", "[]", |_| None), SectionMerge::Whole));
    }

    #[test]
    fn a_lookup_table_conflict_names_its_files() {
        let got = merge_section(
            LOOKUP_TABLES_FILE,
            Some(br#"[{"id":"t","type":"csv","path":"lookups/t.csv","delimiter":","}]"#),
            Some(br#"[{"id":"t","type":"csv","path":"lookups/t.csv","delimiter":";"}]"#),
            Some(br#"[{"id":"t","type":"csv","path":"lookups/t2.csv","delimiter":","}]"#),
            |_| None,
        );
        let SectionMerge::Objects { conflicts, .. } = got else {
            panic!("merged as a whole")
        };
        assert_eq!(conflicts[0].names(Side::Main), Some("lookups/t.csv"));
        assert_eq!(conflicts[0].names(Side::Work), Some("lookups/t2.csv"));
    }

    #[test]
    fn section_files_are_the_ones_directly_in_a_bundle() {
        for (path, want) in [
            ("shared/sources.json", true),
            ("groups/west/routes.json", true),
            ("groups/west/pipelines/routes.json", false),
            ("groups/routes.json", false),
            ("sources.json", false),
            ("notes/sources.json", false),
            ("shared/system.json", false),
        ] {
            assert_eq!(is_section_file(path), want, "{path}");
        }
    }
}
