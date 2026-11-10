//! The releases of ECS and OCSF smeltr carries, so a schema can be read from a standard without first finding its
//! file. ECS is its `generated/csv/fields.csv`; OCSF is `ocsf-schema-compiler`'s output for each release it carries,
//! cut to what `ocsf` reads. `just schemas-refresh` replaces them.

use std::sync::LazyLock;

use serde_json::Value;

/// ECS's `fields.csv`.
pub const ECS: &str = include_str!("standards/ecs-fields.csv");

/// The ECS fields Elastic Security requires and uses: a line for each, its requirement and its name, beneath a
/// comment line. smeltr keeps the list by hand; `just schemas-refresh` does not touch it.
pub const ELASTIC_SECURITY: &str = include_str!("standards/elastic-security.txt");

/// The OCSF releases smeltr carries, each `ocsf-schema-compiler`'s output cut, newest first. An older one is kept
/// where a destination reads only up to it.
const OCSF: [&str; 2] = [
    include_str!("standards/ocsf-1.9.0.json"),
    include_str!("standards/ocsf-1.3.0.json"),
];

/// The compiled OCSF `version`, parsed afresh on each call: a schema is added seldom, and the parse is held only
/// while it reads. `None` where smeltr carries no such release.
pub fn ocsf(version: &str) -> Option<Value> {
    let at = OCSF_RELEASES.iter().position(|r| r.version == version)?;
    Some(parse(OCSF[at]))
}

fn parse(release: &str) -> Value {
    serde_json::from_str(release).expect("an OCSF release is the refresh's")
}

/// One OCSF release and its classes.
#[derive(Debug)]
pub struct OcsfRelease {
    pub version: String,
    pub classes: Vec<OcsfClass>,
}

/// Each OCSF release and its classes, kept from one parse because the dialog asks for them each time it opens.
static OCSF_RELEASES: LazyLock<Vec<OcsfRelease>> = LazyLock::new(|| {
    OCSF.iter()
        .map(|release| {
            let ocsf = parse(release);
            let version = ocsf["version"]
                .as_str()
                .expect("the compiled OCSF names its version")
                .to_string();
            let profile = |name: &str| OcsfProfile {
                name: name.to_string(),
                caption: ocsf["profiles"][name]["caption"].as_str().unwrap_or(name).to_string(),
            };
            let mut classes: Vec<OcsfClass> = (ocsf["classes"].as_object().into_iter().flatten())
                .map(|(name, c)| OcsfClass {
                    name: name.clone(),
                    caption: c["caption"].as_str().unwrap_or(name).to_string(),
                    category: c["category_name"].as_str().unwrap_or_default().to_string(),
                    profiles: (c["profiles"].as_array().into_iter().flatten())
                        .filter_map(Value::as_str)
                        .map(profile)
                        .collect(),
                })
                .collect();
            classes.sort_by(|a, b| a.name.cmp(&b.name));
            OcsfRelease { version, classes }
        })
        .collect()
});

/// The ECS release, as each row of its CSV names it.
pub fn ecs_version() -> &'static str {
    ECS.lines()
        .nth(1)
        .and_then(|row| row.split(',').next())
        .expect("fields.csv has a row")
}

/// The OCSF releases, newest first.
pub fn ocsf_releases() -> &'static [OcsfRelease] {
    &OCSF_RELEASES
}

/// One OCSF class, as the dialog offers it.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OcsfClass {
    /// What a schema is read from by.
    pub name: String,
    pub caption: String,
    /// OCSF's category, such as "Identity & Access Management"; empty for a class in none.
    pub category: String,
    /// The profiles the class takes, any of which a schema may be read with.
    pub profiles: Vec<OcsfProfile>,
}

/// One OCSF profile a class takes.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OcsfProfile {
    /// What `ocsf::Cut::profiles` names it by.
    pub name: String,
    pub caption: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_field_elastic_security_names_is_an_ecs_field() {
        let ecs = crate::schema::ecs::read(ECS).unwrap();
        for line in ELASTIC_SECURITY.lines().skip(1) {
            let (_, name) = line.split_once(' ').unwrap();
            assert!(
                ecs.iter().any(|f| f.field == name),
                "`{name}` is not in ECS {}",
                ecs_version()
            );
        }
    }

    #[test]
    fn each_release_reads() {
        assert!(ecs_version().split('.').count() == 3, "{}", ecs_version());
        let ecs = crate::schema::ecs::read(ECS).unwrap();
        assert!(ecs.iter().any(|f| f.field == "@timestamp"));

        for release in ocsf_releases() {
            let version = &release.version;
            assert!(version.split('.').count() == 3, "{version}");
            let ocsf = ocsf(version).unwrap();
            let auth = release.classes.iter().find(|c| c.name == "authentication").unwrap();
            assert_eq!(
                (auth.caption.as_str(), auth.category.as_str()),
                ("Authentication", "Identity & Access Management"),
                "{version}"
            );
            for class in &release.classes {
                let fields = crate::schema::ocsf::read(&ocsf, &class.name, &Default::default()).unwrap();
                assert!(!fields.is_empty(), "{version} {}", class.name);
            }
        }
        assert!(ocsf("0.1.0").is_none());
    }

    /// The profiles' attributes are left out unless taken, so `cloud` and `osint`, which their profiles require, are
    /// not required of every class.
    #[test]
    fn a_class_takes_its_profiles_only_when_asked() {
        for release in ocsf_releases() {
            let version = &release.version;
            let ocsf = ocsf(version).unwrap();
            let class = release.classes.iter().find(|c| c.name == "network_activity").unwrap();
            let host = class.profiles.iter().find(|p| p.name == "host").unwrap();
            assert_eq!(host.caption, "Host", "{version}");
            let read = |profiles: &[&str]| {
                let cut = crate::schema::ocsf::Cut {
                    profiles: profiles.iter().map(|p| p.to_string()).collect(),
                    ..Default::default()
                };
                let fields = crate::schema::ocsf::read(&ocsf, &class.name, &cut).unwrap();
                fields.into_iter().map(|f| f.field).collect::<Vec<_>>()
            };
            let bare = read(&[]);
            for left in ["cloud", "osint", "device", "metadata.profiles"] {
                assert!(!bare.iter().any(|f| f == left), "{version} {left}");
            }
            assert!(bare.iter().any(|f| f == "src_endpoint.ip"), "{version}");
            let with_host = read(&["host"]);
            assert!(with_host.iter().any(|f| f == "device"), "{version}");
            assert!(with_host.iter().any(|f| f == "metadata.profiles"), "{version}");
            assert!(!with_host.iter().any(|f| f == "cloud"), "{version}");
        }
    }

    /// The refresh writes a file for each release; each must be carried, newest first, since a read that names none takes the first.
    #[test]
    fn every_ocsf_file_is_carried_newest_first() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/schema/standards");
        let mut files: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.unwrap().file_name().into_string().ok())
            .filter_map(|n| Some(n.strip_prefix("ocsf-")?.strip_suffix(".json")?.to_string()))
            .collect();
        let semver = |v: &String| v.split('.').map(|p| p.parse::<u32>().unwrap()).collect::<Vec<_>>();
        files.sort_by_key(|v| std::cmp::Reverse(semver(v)));
        let carried: Vec<String> = ocsf_releases().iter().map(|r| r.version.clone()).collect();
        assert_eq!(carried, files);
    }
}
