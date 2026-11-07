use super::super::*;
use super::DestinationSpec;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IcebergDestinationDto {
    /// The catalog's base address, before `/v1`.
    pub catalog: String,
    /// Absent is the catalog's default warehouse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// The table's namespace, its levels joined by `.`.
    pub namespace: String,
    pub table: String,
    /// The partition spec a created table takes: `host`, `day(ts)`, `bucket(16, id)`, `truncate(4, name)`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,
    /// A bearer token for the catalog. At most one of `token` and `clientId`; neither signs in as no one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<SecretRefDto>,
    /// OAuth client credentials: `clientId` and `clientSecret` together.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<SecretRefDto>,
    /// Where client credentials are exchanged for a token. Absent is the catalog's own `/v1/oauth/tokens`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// Signs every catalog call with AWS Signature Version 4 for this service, `glue` or `s3tables`, with the AWS
    /// credentials storage names, else the host's, for the region the catalog's AWS address names, else `region`.
    /// In place of `token` and `clientId`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signing_name: Option<String>,
    /// Storage credentials of its own. None of its fields set uses what the catalog vends.
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
    /// Seconds one store or catalog call, or one commit, may take before it is a stall.
    #[serde(default, skip_serializing_if = "StallAfter::is_default")]
    pub stall_after_secs: StallAfter,
    /// Seconds files stay open before they are committed and their batches acknowledged.
    #[serde(default, skip_serializing_if = "FileInterval::is_default")]
    pub file_interval_secs: FileInterval,
    #[serde(flatten, default)]
    pub unknown: NoUnknownKeys,
}

impl DestinationSpec for IcebergDestinationDto {
    /// Everything an Iceberg destination is checked for: its delivery limits, where its catalog is, how it signs in,
    /// which table it writes and how a created one is partitioned.
    fn check(&self, d: &DestinationDto, errors: &mut ItemErrors<'_>) {
        let Self {
            unknown: _,
            catalog,
            warehouse,
            namespace,
            table,
            partition_by,
            token,
            client_id,
            client_secret,
            token_url,
            scope,
            signing_name,
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
                url: catalog,
                url_field: "catalog",
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
        let blank = |v: &str| v.trim().is_empty();
        if token_url.as_deref().is_some_and(blank) {
            errors
                .at("tokenUrl")
                .push(format!("{what} has an empty tokenUrl; name one, or leave it out"));
        }
        if token.is_some() && client_id.is_some() {
            errors.at("token").push(format!(
                "{what} names both a token and a clientId. Name a token, or clientId and clientSecret, not both"
            ));
        }
        if let Some(name) = signing_name
            && !blank(name)
            && !SIGNING_NAMES.contains(&name.trim())
        {
            errors.at("signingName").push(format!(
                "{what} signs its catalog calls for {name:?}; the signing service is {}",
                SIGNING_NAMES.join(" or ")
            ));
        }
        if signing_name.is_some()
            && access.region.is_none()
            && crate::io::iceberg::table::signing_region(catalog).is_none()
        {
            errors.at("region").push(format!(
                "{what} signs its catalog calls with AWS signatures, and its catalog's address {catalog:?} names no AWS \
                 region. Name the region the catalog is in"
            ));
        }
        if signing_name.is_some() && (token.is_some() || client_id.is_some()) {
            errors.at("signingName").push(format!(
                "{what} names a signingName beside a token or clientId. A catalog signed in to with AWS signatures takes \
                 neither"
            ));
        }
        if client_id.is_some() != client_secret.is_some() {
            let missing = if client_id.is_none() {
                "clientId"
            } else {
                "clientSecret"
            };
            errors.at(missing).push(format!(
                "{what} names half of its client credentials. Name both clientId and clientSecret, or neither"
            ));
        }
        if (token_url.is_some() || scope.is_some()) && client_id.is_none() {
            let field = if token_url.is_some() { "tokenUrl" } else { "scope" };
            errors.at(field).push(format!(
                "{what} has a tokenUrl or scope and no clientId; both serve signing in with client credentials"
            ));
        }
        for (field, value) in [
            ("warehouse", warehouse),
            ("clientId", client_id),
            ("scope", scope),
            ("signingName", signing_name),
        ] {
            if value.as_deref().is_some_and(blank) {
                errors
                    .at(field)
                    .push(format!("{what} has an empty {field}; name one, or leave it out"));
            }
        }
        if blank(namespace) {
            errors.at("namespace").push(format!("{what} names no namespace"));
        } else if namespace.trim().split('.').any(blank) {
            errors.at("namespace").push(format!(
                "{what} names the namespace {namespace:?}, which has an empty level. Join its levels with single dots, \
                 as logs.web"
            ));
        }
        if blank(table) {
            errors.at("table").push(format!("{what} names no table"));
        }
        let mut names = std::collections::HashSet::new();
        for term in partition_by {
            match crate::io::iceberg::transform::partition_term(term) {
                Err(why) => errors
                    .at("partitionBy")
                    .push(format!("{what} has a partitionBy term it cannot read: {why}")),
                // Iceberg names each partition field once: a catalog refuses to create a table with a second of a name.
                Ok((transform, path)) => {
                    let name = crate::io::iceberg::transform::partition_name(&transform, &path);
                    if names.contains(&name) {
                        errors.at("partitionBy").push(format!(
                            "{what} has two partitionBy terms naming the partition field {name:?}; keep one"
                        ));
                    }
                    names.insert(name);
                }
            }
        }
        if !access.is_empty() {
            check_s3_access(access, &what, errors);
        }
        check_file_interval(*file_interval_secs, &what, "committed", errors);
    }

    fn to_kind(&self, of: &str) -> DestinationKind {
        let Self {
            unknown: _,
            catalog,
            warehouse,
            namespace,
            table,
            partition_by,
            token,
            client_id,
            client_secret,
            token_url,
            scope,
            signing_name,
            access,
            tls,
            compression,
            in_flight,
            max_object_bytes,
            buffer_bytes,
            stall_after_secs,
            file_interval_secs,
        } = self;
        DestinationKind::Iceberg(IcebergDestinationConfig {
            catalog: catalog.trim().to_string(),
            warehouse: warehouse.as_ref().map(|w| w.trim().to_string()),
            sign_in: match (token, client_id, client_secret) {
                // Naming it beside a token or a client's credentials is refused where the fields are checked.
                _ if signing_name.is_some() => CatalogSignIn::SigV4 {
                    signing_name: signing_name.as_deref().unwrap_or_default().trim().to_string(),
                },
                (Some(token), _, _) => CatalogSignIn::Bearer(to_secret_ref(token)),
                (None, Some(client_id), Some(client_secret)) => CatalogSignIn::ClientCredentials {
                    token_url: token_url.as_ref().map(|u| u.trim().to_string()),
                    client_id: client_id.trim().to_string(),
                    client_secret: to_secret_ref(client_secret),
                    scope: scope.as_ref().map(|s| s.trim().to_string()),
                },
                // Half of a client's credentials is refused where the fields are checked.
                _ => CatalogSignIn::None,
            },
            namespace: namespace
                .trim()
                .split('.')
                .map(|level| level.trim().to_string())
                .collect(),
            table: table.trim().to_string(),
            partition_by: partition_by.clone(),
            storage: (!access.is_empty()).then(|| to_s3_access(access)),
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
        let Self {
            catalog,
            token_url,
            access,
            ..
        } = self;
        let mut urls = vec![("catalog", catalog.as_str())];
        urls.extend(token_url.as_deref().map(|u| ("tokenUrl", u)));
        if !access.is_empty() {
            urls.extend(access.urls());
        }
        urls
    }

    fn secrets(&self) -> Vec<(&'static str, &SecretRefDto)> {
        let Self {
            token,
            client_secret,
            access,
            ..
        } = self;
        (token.iter().map(|r| ("token", r)))
            .chain(client_secret.iter().map(|r| ("clientSecret", r)))
            .chain(access.secrets())
            .collect()
    }

    fn secrets_mut(&mut self) -> Vec<&mut SecretRefDto> {
        let Self {
            token,
            client_secret,
            access,
            ..
        } = self;
        (token.iter_mut().chain(client_secret.iter_mut()))
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
