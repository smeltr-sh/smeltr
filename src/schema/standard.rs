//! The releases of ECS and OCSF smeltr carries, so a schema can be read from a standard without first finding its
//! file. ECS is its `generated/csv/fields.csv`; OCSF is `ocsf-schema-compiler`'s output cut to what `ocsf` reads.
//! `just schemas-refresh` replaces both.

use std::sync::LazyLock;

use serde_json::Value;

/// ECS's `fields.csv`.
pub const ECS: &str = include_str!("standards/ecs-fields.csv");

/// The compiled OCSF, parsed once on first use.
pub static OCSF: LazyLock<Value> = LazyLock::new(|| {
    serde_json::from_str(include_str!("standards/ocsf.json")).expect("ocsf.json is the refresh's")
});

/// The ECS release, as each row of its CSV names it.
pub fn ecs_version() -> &'static str {
    ECS.lines()
        .nth(1)
        .and_then(|row| row.split(',').next())
        .expect("fields.csv has a row")
}

/// The OCSF release.
pub fn ocsf_version() -> &'static str {
    OCSF["version"].as_str().expect("the compiled OCSF names its version")
}

/// The OCSF classes, by name, in order.
pub fn ocsf_classes() -> Vec<String> {
    let mut names: Vec<String> = (OCSF["classes"].as_object())
        .map(|c| c.keys().cloned().collect())
        .unwrap_or_default();
    names.sort();
    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_release_reads() {
        assert!(ecs_version().split('.').count() == 3, "{}", ecs_version());
        let ecs = crate::schema::ecs::read(ECS).unwrap();
        assert!(ecs.iter().any(|f| f.field == "@timestamp"));

        assert!(ocsf_version().split('.').count() == 3, "{}", ocsf_version());
        let classes = ocsf_classes();
        assert!(classes.contains(&"authentication".to_string()));
        for class in &classes {
            let fields = crate::schema::ocsf::read(&OCSF, class, &Default::default()).unwrap();
            assert!(!fields.is_empty(), "{class}");
        }
    }
}
