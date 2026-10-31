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

/// One OCSF class, as the dialog offers it.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OcsfClass {
    /// What a schema is read from by.
    pub name: String,
    pub caption: String,
    /// OCSF's category, such as "Identity & Access Management"; empty for a class in none.
    pub category: String,
}

/// The OCSF classes, by name.
pub fn ocsf_classes() -> Vec<OcsfClass> {
    let mut classes: Vec<OcsfClass> = (OCSF["classes"].as_object().into_iter().flatten())
        .map(|(name, c)| OcsfClass {
            name: name.clone(),
            caption: c["caption"].as_str().unwrap_or(name).to_string(),
            category: c["category_name"].as_str().unwrap_or_default().to_string(),
        })
        .collect();
    classes.sort_by(|a, b| a.name.cmp(&b.name));
    classes
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
        let auth = classes.iter().find(|c| c.name == "authentication").unwrap();
        assert_eq!((auth.caption.as_str(), auth.category.as_str()), ("Authentication", "Identity & Access Management"));
        for class in &classes {
            let fields = crate::schema::ocsf::read(&OCSF, &class.name, &Default::default()).unwrap();
            assert!(!fields.is_empty(), "{}", class.name);
        }
    }
}
