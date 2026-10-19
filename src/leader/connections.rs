//! What a change to one object of a branch reaches, in every group that runs it: the blast radius an editor
//! shows before anything changes (user story A5). Answered from the bundles alone, so an object mid-edit that does
//! not compile still answers.
//!
//! A group runs its own objects merged with `shared/`'s, so an object is linked within that merge
//! ([`super::map::merged_file`]). An object `shared/` declares runs in every group, and is answered across all of
//! them; each object it links to is named with the scope that declares it, since two groups may each
//! declare their own object under one id.

use std::collections::{BTreeSet, HashSet};

use serde::Serialize;

use crate::config_file::{self, FileConfig, Kind, Links, Object};

use super::map::{GroupBundle, merged_file};

/// One object linked to the one asked about.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct Connected {
    pub kind: Kind,
    pub id: String,
    /// The group that declares it, or `shared`.
    pub scope: String,
}

/// What a change to one object reaches, and the groups that run it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectConnections {
    /// What it holds a contract with on fields, though upstream ([`config_file::Connections::upstream`]).
    pub upstream: Vec<Connected>,
    pub downstream: Vec<Connected>,
    /// The groups that run it: its own, or every one for an object `shared/` declares, since each group runs all of
    /// `shared/` merged with its own.
    pub groups: Vec<String>,
    /// Of `groups`, those whose map draws it: every one for a source or destination, and for a pipeline those where
    /// a route runs it directly ([`super::map`]'s module doc).
    pub drawn_in: Vec<String>,
}

/// `object` as `groups` run it. `groups` holds the declaring group alone for a group's own object, and every group the
/// caller may see for one `shared/` declares; each group's bundle is its own, unmerged.
pub fn of(shared: &config_file::RawBundle, groups: &[GroupBundle], object: &Object) -> ObjectConnections {
    let (mut upstream, mut downstream) = (BTreeSet::new(), BTreeSet::new());
    let (mut running, mut drawn_in) = (Vec::new(), Vec::new());
    for group in groups {
        let file = merged_file(shared, &group.raw, &group.id);
        let links = Links::of(&file);
        let found = links.connections(object);
        let named = |o: Object| Connected {
            scope: if declares(&shared.file, o.kind).contains(o.id.as_str()) {
                config_file::SHARED_DIR.to_string()
            } else {
                group.id.clone()
            },
            kind: o.kind,
            id: o.id,
        };
        upstream.extend(found.upstream.into_iter().map(named));
        downstream.extend(found.downstream.into_iter().map(named));
        running.push(group.id.clone());
        let drawn = match object.kind {
            Kind::Source | Kind::Destination => true,
            Kind::Pipeline => links.routes.iter().any(|r| r.pipeline == object.id),
            _ => false,
        };
        if drawn {
            drawn_in.push(group.id.clone());
        }
    }
    ObjectConnections {
        upstream: upstream.into_iter().collect(),
        downstream: downstream.into_iter().collect(),
        groups: running,
        drawn_in,
    }
}

/// The ids `file` declares of `kind`.
fn declares(file: &FileConfig, kind: Kind) -> HashSet<&str> {
    match kind {
        Kind::Source => file.sources.iter().map(|o| o.id.as_str()).collect(),
        Kind::Route => file.routes.iter().map(|o| o.id.as_str()).collect(),
        Kind::Pipeline => file.pipelines.iter().map(|o| o.id.as_str()).collect(),
        Kind::Destination => file.destinations.iter().map(|o| o.id.as_str()).collect(),
        Kind::LookupTable => file.lookup_tables.iter().map(|o| o.id.as_str()).collect(),
        Kind::GrokPattern => file.grok_patterns.iter().map(|o| o.id.as_str()).collect(),
        Kind::Identity => file.identities.iter().map(|o| o.id.as_str()).collect(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::config_file::{BundleAt, RawBundle};

    fn raw(file: serde_json::Value) -> RawBundle {
        RawBundle {
            file: serde_json::from_value(file).unwrap(),
            unreadable: Vec::new(),
        }
    }

    fn group(id: &str, file: serde_json::Value) -> GroupBundle {
        GroupBundle {
            id: id.to_string(),
            raw: raw(file),
            at: BundleAt::Dir(std::env::temp_dir()),
        }
    }

    fn kafka_dest(id: &str) -> serde_json::Value {
        json!({"id": id, "type": "kafka", "brokers": "b:9092", "defaultTopic": id})
    }

    /// `shared/` declares one pipeline; `edge` runs it to its own destination, `core` runs nothing of it.
    fn fleet() -> (RawBundle, Vec<GroupBundle>) {
        let shared = raw(
            json!({"version": 1, "pipelines": [{"id": "normalize", "name": "n", "steps": []}],
                                "destinations": [kafka_dest("lake")]}),
        );
        let edge = group(
            "edge",
            json!({"version": 1,
            "sources": [{"id": "fw", "type": "kafka", "brokers": "b:9092", "topics": ["fw"], "groupId": "g"}],
            "routes": [{"id": "r", "source": "fw", "pipeline": "normalize", "deliver": [{"to": ["lake"]}]}]}),
        );
        let core = group("core", json!({"version": 1, "destinations": [kafka_dest("es")]}));
        (shared, vec![edge, core])
    }

    fn connected(kind: Kind, id: &str, scope: &str) -> Connected {
        Connected {
            kind,
            id: id.to_string(),
            scope: scope.to_string(),
        }
    }

    #[test]
    fn a_shared_object_runs_in_every_group_and_is_drawn_where_routed() {
        let (shared, groups) = fleet();
        let found = of(&shared, &groups, &Object::new(Kind::Pipeline, "normalize"));
        assert_eq!(found.groups, vec!["edge", "core"]);
        assert_eq!(found.drawn_in, vec!["edge"]);
        assert_eq!(found.upstream, vec![connected(Kind::Source, "fw", "edge")]);
        assert_eq!(
            found.downstream,
            vec![
                connected(Kind::Route, "r", "edge"),
                connected(Kind::Destination, "lake", "shared")
            ]
        );
    }

    #[test]
    fn a_groups_own_object_is_answered_in_its_group_even_unlinked() {
        let (shared, groups) = fleet();
        let core: Vec<_> = groups.into_iter().filter(|g| g.id == "core").collect();
        let found = of(&shared, &core, &Object::new(Kind::Destination, "es"));
        assert_eq!(
            found,
            ObjectConnections {
                groups: vec!["core".into()],
                drawn_in: vec!["core".into()],
                ..Default::default()
            }
        );
    }
}
