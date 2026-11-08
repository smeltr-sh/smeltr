use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeltaDestinationDto {
    /// The table's root, `s3://bucket/path`. Exactly one of `location` and `unityCatalog`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// A Unity Catalog external table, whose root and storage credentials the catalog gives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unity_catalog: Option<UnityCatalogDto>,
    /// The columns a created table is partitioned by, each a top-level column of the record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,
    /// How the bucket is reached and signed for. None of its fields set takes the host's.
    #[serde(flatten)]
    pub access: S3AccessDto,
    #[serde(flatten)]
    pub tls: TlsDto,
    /// Absent is zstd.
    #[serde(default = "CompressionDto::zstd", skip_serializing_if = "CompressionDto::is_zstd")]
    pub compression: CompressionDto,
    /// Parts of one file uploading at once.
    #[serde(default, skip_serializing_if = "InFlight::is_default")]
    pub in_flight: InFlight,
    #[serde(default, skip_serializing_if = "ObjectBytes::is_default")]
    pub max_object_bytes: ObjectBytes,
    /// Memory for records not yet committed. Unset is `BufferBytes::OBJECT_DEFAULT`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_bytes: Option<BufferBytes>,
    /// Seconds one store call, or one commit, may take before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// Seconds files stay open before they are committed and their batches acknowledged.
    #[serde(default, skip_serializing_if = "FileInterval::is_default")]
    pub file_interval_secs: FileInterval,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for DeltaDestinationDto {
    /// Everything a Delta destination is checked for: its delivery limits, how it reaches its bucket, where its
    /// table is and how a created one is partitioned.
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            unknown: _,
            location,
            unity_catalog,
            partition_by,
            access,
            tls,
            compression: _,
            in_flight,
            max_object_bytes,
            buffer_bytes,
            stall_after_secs,
            file_interval_secs,
        } = self;
        let id = d.id.as_str();
        check_http_delivery(
            &HttpDelivery {
                id,
                url: access.endpoint.as_deref().unwrap_or(S3_ENDPOINT),
                url_field: "endpoint",
                zone_urls: None,
                request_field: "maxObjectBytes",
                tls,
                // A file's parts, not requests: checked below.
                in_flight: None,
                request_bytes: max_object_bytes.0,
                request_limits: (ObjectBytes::MIN, ObjectBytes::MAX),
                request_what: "a data file size",
                limit_why: "a file is finished early at this size",
                // Bounded below, against a file's own limits: files upload in parts, so it need not hold one.
                buffer_bytes: None,
                stall_after: *stall_after_secs,
            },
            errors,
        );
        check_file_limits(id, *in_flight, *buffer_bytes, errors);
        let what = format!("destination {id:?}");
        check_s3_access(access, &what, errors);
        match (location, unity_catalog) {
            (Some(_), Some(_)) => errors.at("location").push(format!(
                "{what} names both a location and a unityCatalog table. Name the table's root in S3 as location, or its \
                 Unity Catalog table as unityCatalog, not both"
            )),
            (None, None) => errors.at("location").push(format!(
                "{what} names no table. Name its root in S3 as location, as s3://bucket/path, or its Unity Catalog table \
                 as unityCatalog"
            )),
            (Some(location), None) => {
                let bucket = ["s3://", "s3a://"]
                    .iter()
                    .find_map(|scheme| location.trim().strip_prefix(scheme))
                    .map(|rest| rest.split('/').next().unwrap_or_default());
                if bucket.is_none_or(str::is_empty) {
                    errors.at("location").push(format!(
                        "{what} keeps its table at {location:?}; give the table's root in S3, as s3://bucket/path"
                    ));
                }
            }
            (None, Some(uc)) => check_unity_catalog(uc, access, &what, errors),
        }
        // Delta partitions by top-level columns, by their values, so a term is a column's name and nothing more.
        let mut seen = HashSet::new();
        for column in partition_by {
            if column.trim().is_empty() || column.contains(['.', '(', ')', ',']) {
                errors.at("partitionBy").push(format!(
                    "{what} partitions by {column:?}; a Delta table is partitioned by top-level columns, so name one \
                     column, as host"
                ));
            } else if !seen.insert(column) {
                errors
                    .at("partitionBy")
                    .push(format!("{what} partitions by {column:?} twice; name each column once"));
            }
        }
        check_file_interval(*file_interval_secs, &what, "committed", errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            location,
            unity_catalog,
            partition_by,
            access,
            tls,
            compression,
            in_flight,
            max_object_bytes,
            buffer_bytes,
            stall_after_secs,
            file_interval_secs,
        } = self;
        DestinationKind::Delta(DeltaDestinationConfig {
            // Naming both, or neither, is refused where the fields are checked.
            location: location.as_deref().map(str::trim).unwrap_or_default().to_string(),
            unity_catalog: unity_catalog.as_ref().map(|u| UnityCatalogTable {
                workspace: u.workspace.trim().to_string(),
                table: u.table.trim().to_string(),
                client_id: u.client_id.trim().to_string(),
                client_secret: to_secret_ref(&u.client_secret),
            }),
            partition_by: partition_by.clone(),
            storage: to_s3_access(access),
            compression: compression.to_plan(),
            tls: to_tls(tls, of, Side::Client),
            in_flight: in_flight.0,
            max_object_bytes: max_object_bytes.0,
            buffer_bytes: buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
            stall_after: std::time::Duration::from_secs(stall_after_secs.0),
            file_interval: std::time::Duration::from_secs(file_interval_secs.0),
        })
    }

    fn urls(&self) -> Vec<(&'static str, &str)> {
        // A Unity Catalog workspace is held to more than a URL, a root alone, by its own check.
        self.access.urls()
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            unity_catalog, access, ..
        } = self;
        (unity_catalog
            .iter()
            .map(|u| ("unityCatalog.clientSecret", &u.client_secret)))
        .chain(access.secrets())
        .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            unity_catalog, access, ..
        } = self;
        (unity_catalog.iter_mut().map(|u| &mut u.client_secret))
            .chain(access.secrets_mut())
            .collect()
    }

    fn tls(&self) -> Option<TlsDto> {
        Some(self.tls.clone())
    }

    fn states_buffer(&self) -> bool {
        self.buffer_bytes.is_some()
    }
}
