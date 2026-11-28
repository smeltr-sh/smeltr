//! What the loader of a source, destination or cloud kind checks and converts with: the `check_*` and `to_*`
//! functions the kinds share, and the types they report through (`ItemErrors`, `ValidationError`). A loader imports
//! these from here; `load` keeps what loads a bundle as a whole: pipelines, routes, steps, lookups and ids.

pub(super) use std::collections::{BTreeMap, HashMap, HashSet};
pub(super) use std::time::Duration;

pub(super) use crate::core::column::DataType;
pub(super) use crate::format::encoding::Encoding;
pub(super) use crate::io::entra::IMDS_API_VERSION;
pub(super) use crate::io::framing::{ByteOrder, Counts, LengthPrefix};
pub(super) use crate::io::line_breaker::{LineBreaker, PER_LINE};
pub(super) use crate::io::logs::LogsSourceConfig;
pub(super) use crate::io::object_source::MAX_RECORD_BYTES;
pub(super) use crate::io::trust::Side;
pub(super) use crate::metrics::MetricsSourceConfig;
pub(super) use crate::plan::config::Payload;
pub(super) use crate::plan::topology::{
    AccessKey, Acknowledgement, AfterCommit, ApiRecords, AssumedRole, AzureAccess, AzureCredential, BulkAction,
    CatalogSignIn, Cloud, ConnectionLimits, DeltaDestinationConfig, DestinationKind, Endpoint, EntraSignIn, Framing,
    GcsAccess, HttpDestinationConfig, IcebergDestinationConfig, KafkaDestinationConfig, KafkaProps, KafkaService,
    KafkaSourceConfig, ListPlace, ListStart, LogAnalyticsDestinationConfig, Newlines, ObjectFormat, ObjectQueue,
    ObjectSourceConfig, ObjectsDestinationConfig, OktaSignIn, OtlpDestinationConfig, PrometheusDestinationConfig,
    RemoteWriteVersion, RequestBody, S3Access, SftpAccess, SftpCredential, SftpStore, SourceKind,
    SplunkHecDestinationConfig, StartPosition, StopPosition, Tls, Transport, UnityCatalogTable,
};
pub(super) use crate::plan::topology::{Signal, SourceMeta};

pub(super) use super::super::bytes;
pub(super) use super::super::dto::*;

/// One problem `validate` found, attributed to where it came from when it is known. A compile error carries the
/// pipeline and function step whose expression failed. A check on one source or destination carries that
/// item and, where one setting is wrong, its field. A problem between items carries no attribution.
#[derive(Clone, Debug, PartialEq)]
pub struct ValidationError {
    pub pipeline: Option<String>,
    pub function: Option<crate::core::id::FunctionId>,
    /// `function`, resolved to the step's own string id through that pipeline's `step_ids`. Stable across a `call`
    /// step's inlining, where `function` alone is not.
    pub step: Option<String>,
    /// The eval assignment, by its position in the step's own list. See `CompileError::assignment_index`.
    pub assignment_index: Option<u32>,
    /// The source or destination this is about.
    pub item: Option<ItemRef>,
    /// The setting this is about, by its name in the file: a key of `item`, or of the assignment. A key inside a map
    /// follows the map's name after a dot, as in `properties.security.protocol`.
    pub field: Option<&'static str>,
    /// The route's delivery rule this is about, by its position in the route's `deliver`. See `CompileError::rule`.
    pub rule: Option<u32>,
    /// The part of `field`'s text at fault. See `CompileError::span`.
    pub span: Option<crate::expr::ast::Span>,
    pub message: String,
}

impl From<String> for ValidationError {
    fn from(message: String) -> Self {
        ValidationError {
            pipeline: None,
            function: None,
            step: None,
            assignment_index: None,
            item: None,
            field: None,
            rule: None,
            span: None,
            message,
        }
    }
}

/// One item of a bundle section that is a list: the section's name in the file, and the item's id.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct ItemRef {
    pub section: &'static str,
    pub id: String,
}

/// Where the checks on one item put what they find, so each error names the item and, through `at`, the field.
pub(super) struct ItemErrors<'a> {
    errors: &'a mut Vec<ValidationError>,
    item: ItemRef,
    field: Option<&'static str>,
}

impl<'a> ItemErrors<'a> {
    pub(super) fn new(errors: &'a mut Vec<ValidationError>, section: &'static str, id: &str) -> Self {
        ItemErrors {
            errors,
            item: ItemRef {
                section,
                id: id.to_string(),
            },
            field: None,
        }
    }

    /// A problem with the item as a whole, or after `at`, with one setting. `message` names the item, as the error
    /// bar shows it without its id.
    pub(super) fn push(&mut self, message: String) {
        debug_assert!(
            message.contains(&self.item.id),
            "{message:?} does not name {}",
            self.item.id
        );
        self.errors.push(ValidationError {
            item: Some(self.item.clone()),
            field: self.field,
            ..message.into()
        });
    }

    /// The same item's errors, about one of its settings.
    pub(super) fn at(&mut self, field: &'static str) -> ItemErrors<'_> {
        ItemErrors {
            errors: self.errors,
            item: self.item.clone(),
            field: Some(field),
        }
    }
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl ValidationError {
    /// Reads the message text alone, so callers that only ever cared what the problem said do not have to learn
    /// about attribution to keep asking.
    pub fn contains(&self, needle: &str) -> bool {
        self.message.contains(needle)
    }
}

pub(super) fn collect<T>(items: impl Iterator<Item = Result<T, String>>) -> (Vec<T>, Vec<String>) {
    let mut oks = Vec::new();
    let mut errs = Vec::new();
    for r in items {
        match r {
            Ok(v) => oks.push(v),
            Err(e) => errs.push(e),
        }
    }
    (oks, errs)
}

/// The TLS `of` speaks, or `None` for a connector that says nothing about it. A receiver trusts no public root:
/// whom it admits is its authorities' say alone.
pub(super) fn to_tls(dto: &TlsDto, of: &str, side: Side) -> Option<Tls> {
    dto.is_set().then(|| Tls {
        of: of.to_string(),
        certificate: dto.certificate.as_deref().map(|c| c.trim().to_string()),
        authorities: dto.trusted_authorities.clone(),
        trust_public: side == Side::Client && dto.trust_public != Some(false),
        side,
        senders: Default::default(),
    })
}

pub(super) fn to_start(start: StartPositionDto) -> StartPosition {
    match start {
        StartPositionDto::Earliest => StartPosition::Earliest,
        StartPositionDto::Latest => StartPosition::Latest,
        StartPositionDto::Timestamp { millis } => StartPosition::Timestamp(millis),
    }
}

pub(super) fn to_stop(stop: StopPositionDto) -> StopPosition {
    match stop {
        StopPositionDto::Timestamp { millis } => StopPosition::Timestamp(millis),
    }
}

/// The consumer group a Kafka source that names none reads as outside any worker group.
pub(super) const DEFAULT_GROUP_ID: &str = "smeltr";

/// The database an SQL source connects to, from its connection settings alone.
pub(super) fn sql_database(sql: &SqlSourceDto, of: &str) -> crate::plan::topology::SqlDatabase {
    let kind = sql.kind;
    let (host, port, database, user) = (
        sql.host.trim().to_string(),
        sql.port(kind),
        sql.database.trim().to_string(),
        sql.user.trim().to_string(),
    );
    let password = sql.password.as_ref().map(to_secret_ref);
    let encrypted = match sql.encrypted {
        SqlTlsDto::Require => crate::io::sql_source::Encrypted::Require,
        SqlTlsDto::Disable => crate::io::sql_source::Encrypted::Disable,
    };
    let tls = to_tls(&sql.tls, of, Side::Client);
    match kind {
        SqlKind::Postgres => crate::plan::topology::SqlDatabase::Postgres(crate::io::postgres::PostgresConnection {
            host,
            port,
            database,
            user,
            password,
            encrypted,
            tls,
        }),
        SqlKind::MySql => crate::plan::topology::SqlDatabase::MySql(crate::io::mysql::MySqlConnection {
            host,
            port,
            database,
            user,
            password,
            encrypted,
            tls,
        }),
    }
}

/// A flush that outlasts the consumer's `max.poll.interval.ms` gets the consumer evicted, which turns one stalled
/// destination into a group-wide rebalance.
pub(super) fn check_stall_after(t: StallAfter, id: &str, errors: &mut ItemErrors<'_>) {
    if t.0 < StallAfter::MIN || t.0 > StallAfter::MAX {
        errors.at("stallAfterSecs").push(format!(
            "destination {id:?} sets a stall threshold of {}s; it must be between {} and {}. \
             A flush that outlasts the consumer's poll interval gets the consumer evicted, \
             which turns one stalled destination into a group-wide rebalance",
            t.0,
            StallAfter::MIN,
            StallAfter::MAX
        ));
    }
}

/// The shortest stall threshold an SFTP destination takes. A write is cut off at the threshold, so a shorter one
/// would cut off a sign-in before it finishes, and a request on a dead connection before the SFTP client's own 10 s
/// timeout drops that connection.
pub(super) const SFTP_MIN_STALL_AFTER: u64 = 15;

/// Properties smeltr sets itself, with the reason it owns each, which `/api/system` serves to the GUI. Both spellings
/// of an aliased property appear: librdkafka resolves either to the same setting, and operator properties are applied
/// after smeltr's.
pub(crate) const RESERVED_KAFKA_PROPS: &[(&str, &str)] = &[
    ("bootstrap.servers", "set from the `brokers` field"),
    // librdkafka would run it through system() on every worker. A bundle may not carry a shell command, and smeltr gets
    // the ticket itself from the keytab and principal a bundle names.
    (
        "sasl.kerberos.kinit.cmd",
        "it runs a shell command on every worker, and a config may not carry one",
    ),
    // At 0 librdkafka runs no `kinit`; smeltr gets and renews the ticket from the keytab itself (`io::kafka::kerberos`).
    (
        "sasl.kerberos.min.time.before.relogin",
        "smeltr renews the Kerberos ticket itself, from the keytab",
    ),
    (
        "metadata.broker.list",
        "another name for bootstrap.servers, which is set from the `brokers` field",
    ),
    ("group.id", "set from this source's `groupId` field"),
    ("client.rack", "each worker sets it from its zone"),
    ("group.instance.id", "each worker sets it from its own id"),
    // smeltr commits explicitly once a batch's destinations have flushed; letting librdkafka auto-commit would
    // acknowledge offsets for rows that have not been delivered, silently breaking at-least-once.
    (
        "enable.auto.commit",
        "smeltr commits only once a batch is delivered, and auto-commit would mark rows done before they are",
    ),
    // A deprecated topic-level property for librdkafka's legacy simple consumer, which the high-level consumer smeltr
    // runs ignores; setting it reads as a way to control auto-commit and does nothing at all, where
    // `enable.auto.commit` is the global property smeltr pins off and reserves.
    (
        "auto.commit.enable",
        "it does nothing on the consumer smeltr runs, and smeltr keeps auto-commit off",
    ),
    ("auto.offset.reset", "set from this source's `start` field"),
    // `io::kafka::MAX_POLL_INTERVAL`. A flush must not outlast it (`StallAfter::MAX`), or the group evicts the consumer.
    (
        "max.poll.interval.ms",
        "fixed at 5 minutes, which caps each destination's `stallAfterSecs` field, so a slow destination cannot rebalance the group",
    ),
    // That field offers exactly the codecs this build can produce.
    ("compression.type", "set from this destination's `compression` field"),
    (
        "compression.codec",
        "another name for compression.type, which is set from this destination's `compression` field",
    ),
    ("acks", "set from this destination's `acks` field"),
    (
        "request.required.acks",
        "another name for acks, which is set from this destination's `acks` field",
    ),
    // That field makes a reader's memory footprint a number this deployment chose.
    (
        "queued.max.messages.kbytes",
        "set from this source's `fetchBytes` field",
    ),
    // The producer holds half of that field, and the egress queue in front of it the other half.
    (
        "queue.buffering.max.kbytes",
        "set from this destination's `bufferBytes` field",
    ),
    // smeltr reports librdkafka's own statistics as `smeltr_destination_throttled_seconds_total` and
    // `smeltr_destination_buffered_bytes`, which is the only way broker-enforced quota throttling is visible at all;
    // setting it to 0 would switch those series off with nothing to say they had stopped.
    (
        "statistics.interval.ms",
        "smeltr needs it on to report destination throttling and buffer size",
    ),
    (
        "queue.buffering.max.messages",
        "set to its maximum, so this destination's `bufferBytes` field bounds the queue",
    ),
    // One fetch may carry half the queue, so a fetch always lands with room behind it.
    ("fetch.max.bytes", "set from this source's `fetchBytes` field"),
    // librdkafka refuses a fetch larger than the message that carries it.
    (
        "receive.message.max.bytes",
        "raised with the fetch size, which is set from this source's `fetchBytes` field",
    ),
    // The same as `fetch.max.bytes`, so one hot partition may fill a fetch.
    ("fetch.message.max.bytes", "set from this source's `fetchBytes` field"),
    // smeltr runs the producer with it off: an idempotent producer keeps at most five requests in flight, too few for a
    // far link.
    (
        "enable.idempotence",
        "smeltr keeps it off, because an idempotent producer allows too few requests in flight for a distant broker",
    ),
    (
        "transactional.id",
        "smeltr never begins a transaction, and a transactional producer refuses records sent outside one",
    ),
    // Cooperative and eager members must not share a consumer group.
    (
        "partition.assignment.strategy",
        "smeltr uses cooperative rebalancing, and one member on another strategy breaks the group for every worker",
    ),
];

/// Whether a librdkafka property's value is a credential, and so may only be named through `credentials`.
/// Classified by shape rather than listed: librdkafka gains properties with each release, and an unrecognized key that
/// says `password` must still be refused. A path to key material is not a credential.
pub(super) fn is_credential_valued(key: &str) -> bool {
    const CREDENTIAL_WORDS: &[&str] = &["password", "passphrase", "secret"];
    /// Keys carrying a private key *inline*, as opposed to a path to one.
    const INLINE_PRIVATE_KEYS: &[&str] = &["ssl.key.pem", "sasl.oauthbearer.assertion.private.key.pem"];
    let lower = key.to_ascii_lowercase();
    CREDENTIAL_WORDS.iter().any(|w| lower.contains(w)) || INLINE_PRIVATE_KEYS.contains(&lower.as_str())
}

/// PEM key material, whatever the property is called.
pub(super) fn looks_like_key_material(value: &str) -> bool {
    value.contains("-----BEGIN")
}

/// Infallible; [`validate_kafka_props`] has already run.
pub(super) fn to_kafka_props(dto: &crate::config_file::dto::KafkaPropsDto) -> crate::plan::topology::KafkaProps {
    crate::plan::topology::KafkaProps {
        literal: dto.properties.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        credentials: dto
            .credentials
            .iter()
            .map(|(k, r)| (k.clone(), to_secret_ref(r)))
            .collect(),
    }
}

pub(super) fn to_framing(dto: &FramingSettingsDto) -> Framing {
    match dto.framing {
        // Validation refused a pattern that does not compile before any of this ran, so the fallback is unreachable.
        FramingDto::Lines => dto
            .line_breaker
            .as_deref()
            .and_then(to_line_breaker)
            .map_or(Framing::Lines, Framing::LineBreaker),
        FramingDto::Syslog => Framing::Syslog(dto.line_breaker.as_deref().and_then(to_line_breaker)),
        FramingDto::Object => Framing::Object,
        FramingDto::EscapedLines => Framing::EscapedLines,
        // Validation refused a length prefix or a fixed size that is absent or out of range.
        FramingDto::LengthPrefix => {
            let prefix = dto.length_prefix.expect("validation required a lengthPrefix");
            Framing::LengthPrefix(LengthPrefix {
                offset: prefix.offset as usize,
                width: prefix.bytes as usize,
                order: match prefix.order {
                    ByteOrderDto::Big => ByteOrder::Big,
                    ByteOrderDto::Little => ByteOrder::Little,
                },
                counts: match prefix.counts {
                    CountsDto::Rest => Counts::Rest,
                    CountsDto::Message => Counts::Message,
                },
            })
        }
        FramingDto::Fixed => Framing::Fixed(dto.fixed_bytes.expect("validation required fixedBytes") as usize),
    }
}

/// Each sourcetype's line breaker. One with none, or `PER_LINE`, is left out, as an event per line. Validation refused a
/// pattern that does not compile before any of this ran.
pub(super) fn to_line_breakers(sourcetypes: &BTreeMap<String, SourcetypeDto>) -> BTreeMap<String, LineBreaker> {
    sourcetypes
        .iter()
        .filter_map(|(name, t)| {
            let pattern = t.line_breaker.as_deref().filter(|p| *p != PER_LINE)?;
            Some((name.clone(), to_line_breaker(pattern)?))
        })
        .collect()
}

/// The line breaker of `pattern`, or `None` when it does not compile.
pub(super) fn to_line_breaker(pattern: &str) -> Option<LineBreaker> {
    regex::bytes::Regex::new(pattern).ok().map(LineBreaker)
}

/// A listing source of `queue`. Nothing hides a listed object, so its visibility is the queue kinds' default.
pub(super) fn listed(
    queue: ObjectQueue,
    tls: Option<Tls>,
    framing: &FramingSettingsDto,
    encoding: &EncodingLabel,
) -> SourceKind {
    SourceKind::Objects(ObjectSourceConfig {
        queue,
        tls,
        framing: to_framing(framing),
        // Validation refused an unknown label before any of this ran, so the fallback is unreachable.
        encoding: Encoding::for_label(&encoding.0).unwrap_or_default(),
        avro: None,
        visibility: Duration::from_secs(VisibilitySecs::DEFAULT),
    })
}

/// The listing of `prefix` in `place`.
pub(super) fn to_list(place: ListPlace, listing: &ListingDto) -> ObjectQueue {
    ObjectQueue::List {
        place,
        prefix: listing.prefix.to_string(),
        poll_interval: Duration::from_secs(listing.poll_interval_secs.0),
        start: match listing.start {
            ListStartDto::Everything => ListStart::Everything,
            ListStartDto::New => ListStart::New,
        },
        after_commit: match listing.after_commit {
            AfterCommitDto::Keep => AfterCommit::Keep,
            AfterCommitDto::Delete => AfterCommit::Delete,
            // Validation refused a move with no prefix before any of this ran.
            AfterCommitDto::Move => AfterCommit::Move {
                prefix: listing.move_prefix.clone().unwrap_or_default(),
            },
        },
    }
}

pub(super) fn to_gcs_access(dto: &GcsAccessDto) -> GcsAccess {
    GcsAccess {
        endpoint: dto.endpoint.clone(),
        service_account_key: dto.service_account_key.as_ref().map(to_secret_ref),
    }
}

/// Every address a client of this account connects out to: the account's own storage services, which a default
/// endpoint leaves unnamed, and where it signs in.
pub(super) fn azure_dials(dto: &AzureAccessDto) -> Vec<String> {
    crate::io::storage_queue::blob_services(&to_azure_access(dto))
        .into_iter()
        .chain(dto.auth_endpoint.clone())
        .collect()
}

pub(super) fn to_azure_access(dto: &AzureAccessDto) -> AzureAccess {
    AzureAccess {
        account: dto.account.clone(),
        endpoint: dto.endpoint.clone(),
        auth_endpoint: dto.auth_endpoint.clone(),
        // More than one, or part of a principal, is refused where the fields are checked.
        credential: match (
            &dto.access_key,
            &dto.sas_token,
            &dto.tenant_id,
            &dto.client_id,
            &dto.client_secret,
        ) {
            (Some(key), ..) => Some(AzureCredential::AccessKey(to_secret_ref(key))),
            (_, Some(token), ..) => Some(AzureCredential::SasToken(to_secret_ref(token))),
            (_, _, Some(tenant_id), Some(client_id), Some(secret)) => Some(AzureCredential::ClientSecret {
                tenant_id: tenant_id.clone(),
                client_id: client_id.clone(),
                secret: to_secret_ref(secret),
            }),
            _ => dto
                .managed_identity_client_id
                .as_ref()
                .map(|client_id| AzureCredential::ManagedIdentity {
                    client_id: client_id.trim().to_string(),
                }),
        },
    }
}

/// `directory` as one path: repeated slashes read as one, and no trailing slash but on the root.
pub(super) fn sftp_directory(directory: &str) -> String {
    let parts: Vec<&str> = directory.trim().split('/').filter(|p| !p.is_empty()).collect();
    match (directory.trim().starts_with('/'), parts.join("/")) {
        (true, d) => format!("/{d}"),
        (false, d) => d,
    }
}

pub(super) fn to_sftp_access(dto: &SftpAccessDto) -> SftpAccess {
    SftpAccess {
        host: dto.host.trim().to_string(),
        // `check_sftp_access` refused a port out of range.
        port: u16::try_from(dto.port).unwrap_or_default(),
        username: dto.username.trim().to_string(),
        // Validation refused naming both or neither before any of this ran, so the empty password is unreachable.
        credential: match (&dto.private_key, &dto.password) {
            (Some(key), _) => SftpCredential::PrivateKey(to_secret_ref(key)),
            (None, password) => SftpCredential::Password(to_secret_ref(
                password.as_ref().unwrap_or(&SecretRefDto::WorkerLocal(String::new())),
            )),
        },
        host_key: dto.host_key.trim().to_string(),
    }
}

pub(super) fn to_s3_access(dto: &S3AccessDto) -> S3Access {
    S3Access {
        region: dto.region.clone(),
        endpoint: dto.endpoint.clone(),
        auth_endpoint: dto.auth_endpoint.clone(),
        access_key: match (&dto.access_key_id, &dto.secret_access_key) {
            (Some(id), Some(secret)) => Some(AccessKey {
                id: to_secret_ref(id),
                secret: to_secret_ref(secret),
            }),
            // Half a key is refused where the fields are checked.
            _ => None,
        },
        role: dto.role_arn.as_ref().map(|arn| AssumedRole {
            arn: arn.clone(),
            external_id: dto.external_id.clone(),
        }),
    }
}

/// The longest name an Azure Storage queue takes.
pub(super) const QUEUE_NAME_MAX: usize = 63;

/// Refuses an object source's empty queue URL, and a visibility the queue would refuse.
pub(super) fn check_queue(queue_url: &str, visibility: VisibilitySecs, what: &str, errors: &mut ItemErrors<'_>) {
    if queue_url.trim().is_empty() {
        errors.at("queueUrl").push(format!("{what} names no queueUrl"));
    }
    check_visibility(visibility, VisibilitySecs::MAX, what, errors);
}

/// Refuses a visibility the queue would refuse: above `max`, its longest, or below the shortest any queue takes.
pub(super) fn check_visibility(visibility: VisibilitySecs, max: u64, what: &str, errors: &mut ItemErrors<'_>) {
    if !(VisibilitySecs::MIN..=max).contains(&visibility.0) {
        errors.at("visibilitySecs").push(format!(
            "{what} hides a received message for {}s; visibilitySecs must be between {} and {max}",
            visibility.0,
            VisibilitySecs::MIN,
        ));
    }
}

/// Refuses a listing source's poll and after-commit settings that would stall it or read an object twice.
pub(super) fn check_listing(listing: &ListingDto, what: &str, errors: &mut ItemErrors<'_>) {
    let ListingDto {
        prefix,
        poll_interval_secs: poll_interval,
        start,
        after_commit,
        move_prefix,
    } = listing;
    let (poll_interval, start, after_commit) = (*poll_interval, *start, *after_commit);
    if poll_interval.0 == 0 {
        errors.at("pollIntervalSecs").push(format!(
            "{what} lists its bucket every 0 seconds; pollIntervalSecs is at least 1"
        ));
    }
    if start == ListStartDto::New && after_commit != AfterCommitDto::Keep {
        errors.at("start").push(format!(
            "{what} starts at new objects and does not keep what it reads. Only the ledger that afterCommit \
             \"keep\" writes records the first listing as read; with no ledger every object it found would be read \
             again"
        ));
    }
    if prefix.starts_with('/') {
        errors.at("prefix").push(format!(
            "{what} has the prefix {prefix:?}, which starts with \"/\" and so matches no key; drop the leading \"/\""
        ));
    }
    match (after_commit, move_prefix) {
        (AfterCommitDto::Move, None) => {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads and names no movePrefix to move it under"
            ));
        }
        (AfterCommitDto::Move, Some(to)) if to.trim().is_empty() => {
            errors
                .at("movePrefix")
                .push(format!("{what} has an empty movePrefix; name where a read object goes"));
        }
        (AfterCommitDto::Move, Some(to)) if to.starts_with('/') => {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which starts with \"/\"; drop the leading \"/\""
            ));
        }
        // A moved key is `movePrefix` + what follows `prefix` in it, so a `/` on one and not the other doubles or
        // drops the `/` between them.
        (AfterCommitDto::Move, Some(to)) if (prefix.is_empty() || prefix.ends_with('/')) != to.ends_with('/') => {
            let listed = if prefix.is_empty() {
                "the whole bucket".to_string()
            } else {
                format!("the prefix {prefix:?}")
            };
            errors.at("movePrefix").push(format!(
                "{what} lists {listed} and moves what it reads under {to:?}, so a moved key gains or loses a \"/\". \
                 End both in \"/\" or neither; an empty prefix ends in \"/\""
            ));
        }
        // `Path::parse` takes `done//` by dropping both `/`s, yet refuses the key `done//x`, so a stem it rewrites
        // is refused.
        (AfterCommitDto::Move, Some(to))
            if {
                let stem = to.strip_suffix('/').unwrap_or(to);
                !object_store::path::Path::parse(stem).is_ok_and(|p| p.as_ref() == stem)
            } =>
        {
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which is not an object path. A path is segments \
                 joined by single \"/\", none empty, \".\" or \"..\", \
                 and no control character"
            ));
        }
        (AfterCommitDto::Move, Some(to)) if to.starts_with(prefix) || prefix.starts_with(to.as_str()) => {
            let listed = if prefix.is_empty() {
                "the whole bucket, which it lists".to_string()
            } else {
                format!("the prefix {prefix:?} it lists")
            };
            errors.at("movePrefix").push(format!(
                "{what} moves what it reads under {to:?}, which overlaps {listed}, so it would list every moved \
                 object again. Name a movePrefix outside it"
            ));
        }
        (AfterCommitDto::Keep | AfterCommitDto::Delete, Some(_)) => {
            errors.at("movePrefix").push(format!(
                "{what} names a movePrefix and does not move what it reads; drop it, or set afterCommit to \"move\""
            ));
        }
        _ => {}
    }
}

/// The longest ack deadline Pub/Sub takes, in seconds.
pub(super) const ACK_DEADLINE_MAX: u64 = 600;

/// Refuses a Pub/Sub subscription that is not named in full, as `projects/<project>/subscriptions/<subscription>`.
pub(super) fn check_subscription(subscription: &str, what: &str, errors: &mut ItemErrors<'_>) {
    let parts: Vec<&str> = subscription.trim().split('/').collect();
    let named = matches!(parts.as_slice(), ["projects", project, "subscriptions", name]
        if !project.is_empty() && !name.is_empty());
    if subscription.trim().is_empty() {
        errors.at("subscription").push(format!("{what} names no subscription"));
    } else if !named {
        errors.at("subscription").push(format!(
            "{what} has the subscription {subscription:?}. Name it in full, as \
             projects/<project>/subscriptions/<subscription>"
        ));
    }
}

/// `url` as a whole http:// or https:// URL, the one shape every URL field takes; `None` when it is not one.
pub(crate) fn web_url(url: &str) -> Option<reqwest::Url> {
    reqwest::Url::parse(url.trim())
        .ok()
        .filter(|u| matches!(u.scheme(), "http" | "https") && u.has_host())
}

/// Refuses `url` at `field` unless it is a whole http:// or https:// URL. A blank one is left to the check that
/// says what the field needs. `what` names the connector.
pub(super) fn check_web_url(url: &str, field: &'static str, what: &str, errors: &mut ItemErrors<'_>) {
    if !url.trim().is_empty() && web_url(url).is_none() {
        errors.at(field).push(format!(
            "{what} has {field} {url:?}; give a whole http:// or https:// URL"
        ));
    }
}

/// Refuses a GCS access block with an endpoint that is named but empty. `what` names the connector, and `field`
/// the endpoint's.
pub(super) fn check_endpoint(endpoint: Option<&str>, field: &'static str, what: &str, errors: &mut ItemErrors<'_>) {
    if endpoint.is_some_and(|e| e.trim().is_empty()) {
        errors
            .at(field)
            .push(format!("{what} has an empty {field}; name one, or leave it out"));
    }
}

/// Refuses an SFTP access block that cannot reach or sign in to its server as written. `what` names the connector.
pub(super) fn check_sftp_access(a: &SftpAccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    if a.host.trim().is_empty() {
        errors.at("host").push(format!("{what} names no host"));
    }
    if a.port == 0 || a.port > u32::from(u16::MAX) {
        errors.at("port").push(format!(
            "{what} has port {}; name the server's port, 1 to 65535, 22 by default",
            a.port
        ));
    }
    if a.username.trim().is_empty() {
        errors.at("username").push(format!("{what} names no username"));
    }
    if a.password.is_some() == a.private_key.is_some() {
        errors.at("password").push(format!(
            "{what} must name exactly one of password and privateKey to sign in with"
        ));
    }
    if let Err(e) = russh::keys::PublicKey::from_openssh(a.host_key.trim()) {
        errors.at("hostKey").push(format!(
            "{what} has a hostKey that is not a public key as OpenSSH writes it (\"ssh-ed25519 AAAA…\"): {e}. \
             Copy the server's line from its known_hosts entry or from ssh-keyscan, without the host name"
        ));
    }
}

/// The registry a Kafka or Event Hubs destination registers its Avro schemas in, where its format is Avro.
pub(super) fn avro_write(
    format: FormatDto,
    registry: &Option<crate::config_file::dto::SchemaRegistryDto>,
) -> Option<crate::plan::topology::SchemaRegistry> {
    registry.as_ref().filter(|_| format == FormatDto::Avro).map(to_registry)
}

pub(super) fn to_registry(dto: &crate::config_file::dto::SchemaRegistryDto) -> crate::plan::topology::SchemaRegistry {
    crate::plan::topology::SchemaRegistry {
        url: dto.url.clone(),
        // Validation refused a username without a password, and a password without a username.
        basic: dto.username.clone().zip(dto.password.as_ref().map(to_secret_ref)),
    }
}

pub(super) fn to_secret_ref(dto: &crate::config_file::dto::SecretRefDto) -> crate::plan::topology::SecretRef {
    use crate::config_file::dto::SecretRefDto;
    match dto {
        SecretRefDto::WorkerLocal(var) => crate::plan::topology::SecretRef::WorkerLocal(var.clone()),
        SecretRefDto::LeaderStored(l) => crate::plan::topology::SecretRef::LeaderStored {
            name: l.secret.clone(),
            version: l.version,
        },
    }
}

/// Every key a Kafka client sets, with the map that sets it.
pub(super) fn kafka_keys(props: &KafkaPropsDto) -> impl Iterator<Item = (&'static str, &String)> {
    (props.properties.keys().map(|k| ("properties", k))).chain(props.credentials.keys().map(|k| ("credentials", k)))
}

/// Checks one Kafka client's operator-supplied properties.
pub(super) fn validate_kafka_props(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    for (map, key) in kafka_keys(dto) {
        if let Some((_, why)) = RESERVED_KAFKA_PROPS.iter().find(|(k, _)| k == key) {
            errors.at(map).push(format!(
                "{what} sets the librdkafka property {key:?}, which smeltr owns: {why}"
            ));
        }
    }
    for (key, value) in dto.properties.iter() {
        if is_credential_valued(key) {
            errors.at("properties").push(format!(
                "{what} puts {key:?} in `properties`, whose values are committed to the \
                 config repository and read in review. Move it to `credentials` and name \
                 where the value lives instead"
            ));
        } else if looks_like_key_material(value) {
            errors.at("properties").push(format!(
                "{what} puts what looks like PEM key material in `properties` under {key:?}, \
                 and those values are committed to the config repository and read in review. \
                 Move it to `credentials` and name where the value lives, or point at it \
                 with a `*.location` path instead"
            ));
        }
    }
    // Which mechanism is selected decides which provider librdkafka must have linked, and `validate_literal_props` can
    // only ask about a value it can see. It is never a credential, so naming it here would hide the one question that
    // has to be answered before a deploy rather than after.
    for key in dto.credentials.keys() {
        if matches!(key.as_str(), "sasl.mechanism" | "sasl.mechanisms") {
            errors.at("credentials").push(format!(
                "{what} names {key:?} in `credentials`, whose value is not knowable at \
                 save time. It holds no secret, so put it in `properties` and let the build \
                 be asked whether it carries that mechanism"
            ));
        }
    }

    // A property in both maps has two answers and no rule for picking one.
    for key in dto.credentials.keys() {
        if dto.properties.contains_key(key) {
            errors.at("credentials").push(format!(
                "{what} sets {key:?} in both `properties` and `credentials`; \
                 give it one value or the other"
            ));
        }
    }

    // librdkafka checks the rest: a misspelled name or an out-of-range value is refused at save, not at deploy. Keys
    // already refused above are left out so one mistake gets one error.
    let remaining = crate::plan::topology::KafkaProps {
        literal: dto
            .properties
            .iter()
            .filter(|(k, _)| !RESERVED_KAFKA_PROPS.iter().any(|(r, _)| r == k) && !is_credential_valued(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
        credentials: Vec::new(),
    };
    let mut refused = Vec::new();
    crate::io::kafka::validate_literal_props(&remaining, what, &mut refused);
    for message in refused {
        errors.at("properties").push(message);
    }
    check_sasl_mechanism_case(dto, what, errors);
    check_sasl_login(dto, what, errors);
}

/// Refuses a mechanism librdkafka knows written in another case: it matches names case-sensitively, so it would refuse
/// the mechanism only once a worker builds the client.
pub(super) fn check_sasl_mechanism_case(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    const MECHANISMS: [&str; 5] = ["GSSAPI", "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];
    for key in ["sasl.mechanism", "sasl.mechanisms"] {
        let Some(mechanism) = dto.properties.get(key) else {
            continue;
        };
        if let Some(name) = MECHANISMS
            .iter()
            .find(|m| m.eq_ignore_ascii_case(mechanism) && *m != mechanism)
        {
            errors.at("properties").push(format!(
                "{what} sets {key:?} to {mechanism:?}, which librdkafka does not know: it names mechanisms \
                 case-sensitively. Write {name:?}"
            ));
        }
    }
}

/// Refuses a PLAIN or SCRAM login without both its username and its password, which librdkafka would refuse only once
/// a worker builds the client.
pub(super) fn check_sasl_login(dto: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    // A protocol named in `credentials` is not knowable at save time, so it may be SASL.
    let sasl = dto.credentials.contains_key("security.protocol")
        || (dto.properties.get("security.protocol")).is_some_and(|p| p.to_ascii_uppercase().starts_with("SASL_"));
    // Either spelling may be the one librdkafka takes, and it names mechanisms case-sensitively.
    let Some(mechanism) = ["sasl.mechanism", "sasl.mechanisms"]
        .into_iter()
        .filter_map(|k| dto.properties.get(k))
        .find(|m| matches!(m.as_str(), "PLAIN" | "SCRAM-SHA-256" | "SCRAM-SHA-512"))
    else {
        return;
    };
    if !sasl {
        return;
    }
    let keys = [
        ("sasl.username", "`properties`, or in `credentials` if it is secret"),
        ("sasl.password", "`credentials`"),
    ];
    for (key, where_) in keys {
        if !dto.properties.contains_key(key) && !dto.credentials.contains_key(key) {
            errors.at("properties").push(format!(
                "{what} signs in with SASL {mechanism} and does not set {key:?}, which it needs. Set it in {where_}"
            ));
        }
    }
}

// -- TLS --------------------------------------------------------------------

/// librdkafka property names that answer the same question a connector's TLS settings answer: what this client
/// presents, and what it trusts. A connector setting TLS and one of these has two answers to one question, and
/// librdkafka would silently take whichever was set last. `/api/system` serves them to the GUI.
pub(crate) const TLS_KAFKA_PROPS: &[&str] = &[
    "ssl.certificate.pem",
    "ssl.certificate.location",
    "ssl.key.pem",
    "ssl.key.location",
    "ssl.ca.pem",
    "ssl.ca.location",
    // PKCS#12: the same certificate and key, in one file, under other names.
    "ssl.keystore.location",
    "ssl.keystore.password",
    // Not material, but the one thing that would make a certificate's key mean different things to two
    // connectors: librdkafka decrypts a passphrase-protected key and ureq cannot, so a connector holding this
    // beside a certificate is a deployment that works on Kafka and fails on Elasticsearch. The value is a credential
    // and may be worker-local, so save time cannot look at the key and answer this any other way.
    "ssl.key.password",
    // Naming authorities is naming what a far end is verified against; a connector that also turns verification
    // off is not doing that at all. Elasticsearch has no equivalent knob, so the authorities would verify on one
    // connector and not the other -- the same asymmetry `ssl.key.password` is refused for.
    "enable.ssl.certificate.verification",
    // A different half of the same question: the chain can check out under the connector's own authorities while
    // this decides whether the certificate is also checked against the broker's own name. "none" is what turns
    // that off -- any certificate the authorities ever issued, for any host, then answers for this one.
    "ssl.endpoint.identification.algorithm",
];

/// The librdkafka security protocols that negotiate TLS. Under any other, nothing a connector's TLS settings say is
/// used, and librdkafka's own default is `plaintext`.
pub(super) const TLS_PROTOCOLS: &[&str] = &["SSL", "SASL_SSL"];

/// Refuses `pem` unless it holds certificates and nothing else. Read with the same library that will present
/// them, so a save cannot accept PEM the connector would go on to refuse.
///
/// Every PEM section, not `CertificateDer`'s own iterator: that one selects certificate sections rather than
/// proving there is nothing else, so a certificate beside a CSR or a public key would pass it silently.
///
/// Each section's DER is parsed as X.509, not just labeled `CERTIFICATE`: a `CERTIFICATE` section wrapping
/// malformed or truncated DER passes the label check and only surfaces at the TLS handshake, in a chain member
/// `io::trust::refuse_unless_usable` never reads (it looks at the leaf alone) or in an authorities bundle
/// nothing else here reads at all.
pub(super) fn refuse_unless_certificates(pem: &str) -> Result<(), String> {
    use rustls::pki_types::pem::{PemObject, SectionKind};

    if pem.contains("PRIVATE KEY") {
        return Err("it holds a private key".to_string());
    }
    let mut certificates = 0;
    for section in <(SectionKind, Vec<u8>)>::pem_slice_iter(pem.as_bytes()) {
        let (kind, der) = section.map_err(|e| format!("{e}"))?;
        if kind != SectionKind::Certificate {
            return Err(format!("it holds a {kind:?} section, not a certificate"));
        }
        let (remainder, _) = x509_parser::parse_x509_certificate(&der)
            .map_err(|e| format!("it holds a certificate that is not one this can read: {e}"))?;
        if !remainder.is_empty() {
            return Err("it holds a certificate followed by bytes that are not one".to_string());
        }
        certificates += 1;
    }
    if certificates == 0 {
        return Err("it holds no certificate".to_string());
    }
    Ok(())
}

/// A receiver speaking `receiver`, with what every receiver takes. It listens on `port` at every address of the host,
/// IPv4 and IPv6: `[::]`, which the receiver opens dual-stack. The worker that runs it narrows that to the address its
/// own startup settings name (`startup_config::worker_listen_address`), since an address is a fact about one host.
pub(super) fn receiver_config<'a>(
    receiver: crate::plan::topology::Receiver,
    answer_after_delivery: bool,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    tokens: impl Iterator<Item = (&'a String, &'a SecretRefDto)>,
    connections: crate::plan::topology::ConnectionLimits,
    of: &str,
) -> SourceKind {
    SourceKind::Receiver(crate::plan::topology::ReceiverConfig {
        receiver,
        listen: std::net::SocketAddr::new(std::net::Ipv6Addr::UNSPECIFIED.into(), listener.port),
        tokens: tokens.map(|(k, r)| (k.clone(), to_secret_ref(r))).collect(),
        tls: to_tls(&tls.tls(), of, Side::Server).map(|served| Tls {
            senders: to_senders(tls),
            ..served
        }),
        max_request_bytes: usize::try_from(listener.max_request_bytes.0).unwrap_or(usize::MAX),
        answer_after_delivery,
        connections,
    })
}

/// Whom a receiver admits by a sender's certificate besides its authorities. What does not read was refused at save.
pub(super) fn to_senders(tls: &ServedTlsDto) -> crate::plan::topology::Senders {
    crate::plan::topology::Senders {
        fingerprints: tls
            .sender_fingerprints
            .iter()
            .filter_map(|f| crate::io::senders::Fingerprint::parse(f).ok())
            .collect(),
        names: tls.sender_names.iter().map(|n| n.trim().to_string()).collect(),
    }
}

/// What `dto` says of a receiver's connections.
pub(super) fn to_connection_limits(dto: &ConnectionLimitsDto) -> crate::plan::topology::ConnectionLimits {
    let size = |n: u64| usize::try_from(n).unwrap_or(usize::MAX);
    crate::plan::topology::ConnectionLimits {
        max: dto.max_connections.map(size),
        per_sender: dto.max_connections_per_sender.map(size),
        idle: dto.idle_timeout_secs.map(std::time::Duration::from_secs),
    }
}

/// Checks what a connector's TLS settings say, before anything reads them: a certificate named by name, authorities
/// that are certificates, and a trust setting that leaves something to verify a far end against.
pub(super) fn check_tls(tls: &TlsDto, what: &str, errors: &mut ItemErrors<'_>) {
    if tls.certificate.as_deref().is_some_and(|c| c.trim().is_empty()) {
        errors.at("certificate").push(format!(
            "{what} names a certificate by a blank name. Choose one from the list"
        ));
    }
    if tls.certificate.is_none() && tls.trusted_authorities.is_none() && tls.trust_public == Some(true) {
        errors.at("trustPublic").push(format!(
            "{what} names neither a certificate nor trusted authorities, so trust public roots changes nothing. Choose a \
             certificate or paste trusted authorities, or leave trust public roots at its default"
        ));
    }
    if tls.trust_public == Some(false) && tls.trusted_authorities.is_none() {
        errors.at("trustPublic").push(format!(
            "{what} trusts no public root and names no trusted authority, so nothing it connects to could be verified. \
             Paste its trusted authorities, or trust public roots"
        ));
    }
    if let Some(authorities) = &tls.trusted_authorities
        && let Err(why) = refuse_unless_certificates(authorities)
    {
        errors.at("trustedAuthorities").push(format!(
            "{what} trusts what is not a certificate authority: {why}. Leave it out to verify a far end against \
             the public roots"
        ));
    }
}

/// Refuses TLS settings on a connector reached at `urls` of which none negotiates TLS: nothing they say would be
/// used. A blank address is refused on its own.
pub(super) fn check_tls_reaches(tls: &TlsDto, urls: &[(&'static str, &str)], what: &str, errors: &mut ItemErrors<'_>) {
    let https = |url: &str| url.trim().to_ascii_lowercase().starts_with("https://");
    if tls.is_set()
        && let Some((field, url)) = urls.first()
        && !url.trim().is_empty()
        && !urls.iter().any(|(_, url)| https(url))
    {
        errors.at(field).push(format!(
            "{what} sets TLS and is reached over {url:?}, which does not negotiate TLS, so nothing its certificate or \
             trusted authorities say would be used. Give it an https:// URL, or take them off it"
        ));
    }
}

/// Refuses a Kafka client whose own properties disagree with its TLS settings.
pub(super) fn check_kafka_tls(tls: &TlsDto, props: &KafkaPropsDto, what: &str, errors: &mut ItemErrors<'_>) {
    if !tls.is_set() {
        return;
    }

    for (map, key) in kafka_keys(props) {
        if TLS_KAFKA_PROPS.contains(&key.to_ascii_lowercase().as_str()) {
            errors.at(map).push(format!(
                "{what} sets TLS and also sets {key:?}, which names the same material. Take one of them out, so \
                 there is one place it can come from"
            ));
        }
    }

    // librdkafka defaults this to plaintext, so absent is as wrong as a plaintext value: the TLS settings would be
    // configuration nothing reads.
    match props.properties.get("security.protocol") {
        Some(protocol) if TLS_PROTOCOLS.contains(&protocol.to_ascii_uppercase().as_str()) => {}
        Some(protocol) => errors.at("properties.security.protocol").push(format!(
            "{what} sets TLS and sets \"security.protocol\" to {protocol:?}, which does not negotiate TLS, so \
             nothing its certificate or trusted authorities say would be used. Set {} or {}, or take them off it",
            TLS_PROTOCOLS[0], TLS_PROTOCOLS[1]
        )),
        None => errors.at("properties.security.protocol").push(format!(
            "{what} sets TLS and does not set \"security.protocol\", which librdkafka defaults to plaintext, so \
             nothing its certificate or trusted authorities say would be used. Set it to {} or {} in `properties`, or take \
             them off it",
            TLS_PROTOCOLS[0], TLS_PROTOCOLS[1]
        )),
    }
}

/// Two sources reading one topic under one consumer group on one cluster are one group member each, so the group
/// splits the topic's partitions between them and each pipeline sees part of it. Refused on the later source.
pub(super) fn check_group_topics(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    /// A source reading a topic: its brokers, and its id.
    type Reader<'a> = (HashSet<String>, &'a str);
    // Every source reading each (group, topic) so far.
    let mut readers: HashMap<(&str, &str), Vec<Reader<'_>>> = HashMap::new();
    for s in &file.sources {
        let (brokers, group, topics, field) = match &s.kind {
            SourceKindDto::Kafka(KafkaSourceDto {
                brokers,
                group_id,
                topics,
                ..
            }) => (brokers.clone(), group_id, topics, "topics"),
            SourceKindDto::EventHubs(EventHubsSourceDto {
                namespace,
                consumer_group,
                event_hubs,
                ..
            }) => (event_hubs_brokers(namespace), consumer_group, event_hubs, "eventHubs"),
            _ => continue,
        };
        let cluster = cluster_of(&brokers);
        // Blank is each worker group's own, filled at the merge, so `shared/` checked alone cannot say which.
        if group.trim().is_empty() {
            continue;
        }
        let errors = &mut ItemErrors::new(attributed, "sources", &s.id);
        for topic in topics.iter().filter(|t| !t.trim().is_empty()) {
            let seen = readers.entry((group.as_str(), topic.trim())).or_default();
            let first = seen
                .iter()
                .find(|(other, id)| *id != s.id && !other.is_disjoint(&cluster))
                .map(|(_, id)| *id);
            seen.push((cluster.clone(), &s.id));
            if let Some(first) = first {
                errors.at(field).push(format!(
                    "source {:?} reads {topic:?} in consumer group {group:?} on the same brokers as source {first:?}, \
                     so the group would split its partitions between them and each would see part of it. Read it \
                     from one source, or give one of them its own group",
                    s.id
                ));
            }
        }
    }
}

/// The brokers a list names. Two lists that share a broker name one cluster, however much else each lists: a broker
/// belongs to one.
pub(super) fn cluster_of(brokers: &str) -> HashSet<String> {
    brokers
        .split(',')
        .map(|b| b.trim().to_ascii_lowercase())
        .filter(|b| !b.is_empty())
        .collect()
}

/// Refuses a Basic sign-in where it does not hold: a username without a password or the other way round, and a blank
/// username. `fields` names the username's and the password's; `whom` says what signs in, as the message's subject.
pub(super) fn check_basic(
    username: &Option<String>,
    password: &Option<crate::config_file::dto::SecretRefDto>,
    fields: (&'static str, &'static str),
    whom: &str,
    errors: &mut ItemErrors<'_>,
) {
    match (username, password) {
        (Some(_), None) => errors
            .at(fields.1)
            .push(format!("{whom} with a username but no password")),
        (None, Some(_)) => errors
            .at(fields.0)
            .push(format!("{whom} with a password but no username")),
        (Some(u), Some(_)) if u.trim().is_empty() => errors.at(fields.0).push(format!("{whom} as a blank username")),
        _ => {}
    }
}

/// Refuses an Elasticsearch destination's sign-in where it does not hold: `check_basic`'s, and a password beside
/// `auth`.
pub(super) fn check_elastic_sign_in(
    id: &str,
    auth: &Option<crate::config_file::dto::SecretRefDto>,
    username: &Option<String>,
    password: &Option<crate::config_file::dto::SecretRefDto>,
    errors: &mut ItemErrors<'_>,
) {
    let whom = format!("destination {id:?} signs in");
    check_basic(username, password, ("username", "password"), &whom, errors);
    if auth.is_some() && password.is_some() {
        errors.at("auth").push(format!(
            "destination {id:?} signs in with both an API key and a password; keep one"
        ));
    }
}

/// Refuses a registry with no address, and a sign-in `check_basic` refuses.
pub(super) fn check_registry(
    dto: &crate::config_file::dto::SchemaRegistryDto,
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    if dto.url.trim().is_empty() {
        errors
            .at("schemaRegistry.url")
            .push(format!("{what} names a schema registry but not its address"));
    }
    check_basic(
        &dto.username,
        &dto.password,
        ("schemaRegistry.username", "schemaRegistry.password"),
        &format!("{what} signs in to its schema registry"),
        errors,
    );
}

/// Refuses how a source reads its payloads where it does not hold: a stream's Avro read without a registry, an
/// encoding or a framing beside Avro, fields that declare no record, and a registry or fields where the source reads
/// no Avro.
pub(super) fn check_read(kind: &SourceKindDto, id: &str, errors: &mut ItemErrors<'_>) {
    use crate::config_file::dto::SourceFormatDto;
    let Some(read) = kind.read() else { return };
    let what = format!("source {id:?}");
    let stream = matches!(kind, SourceKindDto::Kafka(_) | SourceKindDto::EventHubs(_));
    let framing = kind.framing();
    if let Some(registry) = kind.schema_registry() {
        check_registry(registry, &what, errors);
    }
    let Some(SourceFormatDto::Avro) = read.format else {
        if kind.schema_registry().is_some() {
            errors.at("schemaRegistry").push(format!(
                "{what} names a schema registry but reads no Avro; set format to \"avro\""
            ));
        }
        if !read.fields.is_empty() {
            errors.at("fields").push(format!(
                "{what} declares fields but reads no Avro; set format to \"avro\""
            ));
        }
        return;
    };
    if stream && kind.schema_registry().is_none() {
        errors.at("schemaRegistry.url").push(format!(
            "{what} reads Avro, which a stream frames with its schema's id in a schema registry, but names none"
        ));
    }
    if kind.encoding() != Encoding::default() {
        errors.at("encoding").push(format!(
            "{what} reads Avro, whose strings are UTF-8; leave encoding at {:?}",
            EncodingLabel::DEFAULT
        ));
    }
    if framing.is_some_and(|f| !f.framing.is_default() || f.line_breaker.is_some()) {
        errors.at("framing").push(format!(
            "{what} reads Avro container files, each its own records; remove framing and lineBreaker"
        ));
    }
    if let Err(why) = SqlFieldDto::open_meta(crate::plan::topology::SourceMeta::NONE, &read.fields) {
        errors.at("fields").push(format!("{what}: {why}"));
    }
}

/// Refuses a source's encoding label that names no encoding smeltr knows.
pub(super) fn check_encoding(encoding: &EncodingLabel, id: &str, errors: &mut ItemErrors<'_>) {
    if Encoding::for_label(&encoding.0).is_none() {
        errors.at("encoding").push(format!(
            "source {id:?} declares encoding {:?}, which is not a name smeltr knows. Use \
             {:?} to detect per record, \"none\" for bytes that are not text, or a text \
             encoding's name (\"utf-8\", \"utf-16le\", \"windows-1252\", \"shift_jis\")",
            encoding.0,
            EncodingLabel::DEFAULT
        ));
    }
}

/// Refuses a line breaker on any framing but lines, and one `check_line_breaker` refuses; a length prefix and a fixed
/// size on any framing but their own, absent from their own, or that no message or record of `max` bytes could hold.
/// `unit` is the reader's word for it: a receiver's "message", an object's "record".
pub(super) fn check_framing(dto: &FramingSettingsDto, id: &str, max: u64, unit: &str, errors: &mut ItemErrors<'_>) {
    let what = format!("source {id:?}");
    if let Some(line_breaker) = &dto.line_breaker {
        if let FramingDto::Lines | FramingDto::Syslog = dto.framing {
            check_line_breaker(&what, line_breaker, errors.at("lineBreaker"));
        } else {
            errors.at("lineBreaker").push(format!(
                "{what} sets lineBreaker, which only lines and syslog framing read"
            ));
        }
    }
    match (dto.framing, dto.length_prefix) {
        (FramingDto::LengthPrefix, None) => errors.at("lengthPrefix").push(format!(
            "{what} cuts {unit}s by a length prefix; give the length's size in bytes, and its offset if any, in lengthPrefix"
        )),
        (FramingDto::LengthPrefix, Some(prefix)) => {
            if !(1..=8).contains(&prefix.bytes) {
                errors.at("lengthPrefix").push(format!(
                    "{what} reads a length of {} bytes; it must be 1 to 8",
                    prefix.bytes
                ));
            } else if prefix.offset.saturating_add(prefix.bytes) > max {
                errors.at("lengthPrefix").push(format!(
                    "{what} reads a length that ends {} bytes into a {unit}, past the largest {unit} of {}",
                    prefix.offset.saturating_add(prefix.bytes),
                    bytes::exact(max as usize)
                ));
            }
        }
        (_, Some(_)) => errors.at("lengthPrefix").push(format!(
            "{what} sets lengthPrefix, which only lengthPrefix framing reads"
        )),
        (_, None) => {}
    }
    match (dto.framing, dto.fixed_bytes) {
        (FramingDto::Fixed, None) => errors.at("fixedBytes").push(format!(
            "{what} cuts {unit}s by a fixed size; give how many bytes in fixedBytes"
        )),
        (FramingDto::Fixed, Some(size)) if size == 0 || size > max => errors.at("fixedBytes").push(format!(
            "{what} cuts a {unit} every {size} bytes; it must be 1 to {}, the largest {unit}",
            bytes::exact(max as usize)
        )),
        (FramingDto::Fixed, Some(_)) | (_, None) => {}
        (_, Some(_)) => errors
            .at("fixedBytes")
            .push(format!("{what} sets fixedBytes, which only fixed framing reads")),
    }
}

/// Refuses a sourcetype named by a blank name, and a line breaker it gives that `check_line_breaker` refuses.
pub(super) fn check_sourcetypes(id: &str, sourcetypes: &BTreeMap<String, SourcetypeDto>, errors: &mut ItemErrors<'_>) {
    let what = format!("source {id:?}");
    for (name, t) in sourcetypes {
        // A field is named by a static path, so a refusal names the sourcetype in its message.
        if name.trim().is_empty() {
            errors
                .at("sourcetypes")
                .push(format!("{what} names a sourcetype by a blank name"));
        }
        let what = format!("source {id}'s sourcetype {name:?}");
        if let Some(line_breaker) = &t.line_breaker {
            check_line_breaker(&what, line_breaker, errors.at("sourcetypes"));
        }
    }
}

/// Refuses a line breaker that is blank, not a pattern, has no capture group to mark the break, or matches no bytes
/// at all.
pub(super) fn check_line_breaker(what: &str, pattern: &str, mut errors: ItemErrors<'_>) {
    if let Some(why) = line_breaker_refusal(what, pattern) {
        errors.push(why);
    }
}

/// Why a save refuses `pattern` as a line breaker, said of `what`, or `None` when it takes it. The editor asks this as
/// a line breaker is typed, so its verdict is the save's.
pub(crate) fn line_breaker_refusal(what: &str, pattern: &str) -> Option<String> {
    if pattern.trim().is_empty() {
        return Some(format!("{what} names a blank lineBreaker pattern"));
    }
    match regex::bytes::Regex::new(pattern) {
        Err(e) => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which is not a pattern: {e}"
        )),
        // `LineBreaker` drops the first group as the break.
        Ok(re) if re.captures_len() < 2 => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which has no capture group to mark the break"
        )),
        Ok(re) if re.is_match(b"") => Some(format!(
            "{what} ends an event where \"{pattern}\" matches, which can match no bytes at all, and so would break \
             between every byte"
        )),
        Ok(_) => None,
    }
}

/// Refuses a Kafka consumer's fetch size outside what one reader may hold.
pub(super) fn check_fetch_bytes(fetch_bytes: Option<FetchBytes>, id: &str, errors: &mut ItemErrors<'_>) {
    if let Some(fetch_bytes) = fetch_bytes.filter(|f| f.0 < FetchBytes::MIN || f.0 > FetchBytes::MAX) {
        errors.at("fetchBytes").push(format!(
            "source {id:?} lets its consumer hold {} of fetched records; it must \
             be between {} and {}. Below the floor a reader waits on fetches rather \
             than working; above the ceiling one reader's queue outweighs every other \
             buffer the worker holds put together",
            bytes::exact(fetch_bytes.0),
            bytes::exact(FetchBytes::MIN),
            bytes::exact(FetchBytes::MAX)
        ));
    }
}

/// The bootstrap address of an Event Hubs namespace: a bare name is in Azure's public cloud, and a host without a port
/// is reached on 9093, Event Hubs' Kafka port.
pub(crate) fn event_hubs_brokers(namespace: &str) -> String {
    let namespace = namespace.trim();
    if !namespace.contains('.') && !namespace.contains(':') {
        format!("{namespace}.servicebus.windows.net:9093")
    } else if namespace.contains(':') {
        namespace.to_string()
    } else {
        format!("{namespace}:9093")
    }
}

/// What signs a Kafka client in to an Event Hubs namespace at `brokers`, as `sign_in` says: its connection string as
/// the password, else an Entra token for the namespace, obtained with a service principal's secret or, naming
/// neither, from the host's managed identity: the one `managedIdentityClientId` names, else the host's own
/// (`io::kafka::apply_client_config` completes that one on the worker).
pub(super) fn event_hubs_props(
    brokers: &str,
    connection_string: Option<&SecretRefDto>,
    sign_in: &EntraSignInDto,
) -> KafkaProps {
    // Event Hubs always negotiates TLS; only the emulator, on this host, does not.
    let protocol = if brokers.split(',').all(on_loopback) {
        "SASL_PLAINTEXT"
    } else {
        "SASL_SSL"
    };
    // An Entra token is for the namespace, named by its host.
    let namespace = brokers.split(',').next().unwrap_or_default();
    let host = match crate::startup_config::split_authority(namespace) {
        // An IPv6 literal keeps its brackets in a URL.
        Some((h, _)) if h.contains(':') => format!("[{h}]"),
        Some((h, _)) => h,
        None => namespace.to_string(),
    };
    let literal = |pairs: &[(&str, String)]| -> Vec<(String, String)> {
        (pairs.iter().map(|(k, v)| (k.to_string(), v.clone()))).collect()
    };
    // More than one credential, or part of a principal, is refused where the fields are checked.
    if let Some(connection_string) = connection_string {
        return KafkaProps {
            literal: literal(&[
                ("sasl.mechanism", "PLAIN".into()),
                ("sasl.username", "$ConnectionString".into()),
                ("security.protocol", protocol.into()),
            ]),
            credentials: vec![("sasl.password".to_string(), to_secret_ref(connection_string))],
        };
    }
    match to_entra_sign_in(sign_in) {
        EntraSignIn::ServicePrincipal {
            authority,
            tenant_id,
            client_id,
            secret,
        } => {
            let authority = authority.as_deref().unwrap_or(crate::io::entra_authority(&host));
            KafkaProps {
                literal: literal(&[
                    ("sasl.mechanism", "OAUTHBEARER".into()),
                    ("sasl.oauthbearer.method", "oidc".into()),
                    ("sasl.oauthbearer.client.id", client_id),
                    (
                        "sasl.oauthbearer.token.endpoint.url",
                        format!("{authority}/{tenant_id}/oauth2/v2.0/token"),
                    ),
                    ("sasl.oauthbearer.scope", format!("https://{host}/.default")),
                    ("security.protocol", protocol.into()),
                ]),
                credentials: vec![("sasl.oauthbearer.client.secret".to_string(), secret)],
            }
        }
        EntraSignIn::ManagedIdentity { client_id } => KafkaProps {
            literal: literal(&[
                ("sasl.mechanism", "OAUTHBEARER".into()),
                ("sasl.oauthbearer.method", "oidc".into()),
                ("sasl.oauthbearer.metadata.authentication.type", "azure_imds".into()),
                (
                    "sasl.oauthbearer.config",
                    match client_id {
                        Some(client_id) => format!(
                            "query=api-version={IMDS_API_VERSION}&resource=https://{host}&client_id={client_id}"
                        ),
                        None => format!("query=api-version={IMDS_API_VERSION}&resource=https://{host}"),
                    },
                ),
                ("security.protocol", protocol.into()),
            ]),
            credentials: Vec::new(),
        },
    }
}

/// Whether `url` names anything after its host: a path, a query or a fragment.
pub(super) fn names_a_path(url: &str) -> bool {
    let url = url.trim();
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    rest.find(['/', '?', '#']).is_some_and(|i| rest[i..] != *"/")
}

/// Refuses an Event Hubs namespace that is blank or is not a name, a host name or `host:port`.
pub(super) fn check_event_hubs_namespace(namespace: &str, what: &str, errors: &mut ItemErrors<'_>) {
    if namespace.trim().is_empty() {
        errors.at("namespace").push(format!("{what} names no namespace"));
    } else if namespace.contains("://") || namespace.contains([',', '/']) {
        errors.at("namespace").push(format!(
            "{what} names the namespace {namespace:?}; give its name, such as \"acme\", or its host name, such as \
             \"acme.servicebus.usgovcloudapi.net\", without a scheme or a path"
        ));
    }
}

/// Refuses a managed identity's client id that is not one: Azure names an identity by a GUID, and the id is sent
/// in the metadata service's query.
pub(super) fn check_managed_identity_client_id(client_id: Option<&str>, what: &str, errors: &mut ItemErrors<'_>) {
    let Some(client_id) = client_id.map(str::trim).filter(|c| !c.is_empty()) else {
        return;
    };
    if !client_id.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
        errors.at("managedIdentityClientId").push(format!(
            "{what} has managedIdentityClientId {client_id:?}, which is not a client id. Give the managed identity's \
             client id, a GUID, not its name or resource id"
        ));
    }
}

/// The most a Log Analytics destination's request may carry, before compression: the most the Logs Ingestion API takes
/// in one call.
pub(super) const LOG_ANALYTICS_MAX_REQUEST_BYTES: usize = 1024 * 1024;

/// The Logs Ingestion API version a Log Analytics destination's requests name.
pub(super) const LOGS_INGESTION_API_VERSION: &str = "2023-01-01";

/// Where rows for `stream` of the data collection rule `rule_id` are POSTed, under the rule's `endpoint`.
pub(super) fn log_analytics_stream_url(endpoint: &str, rule_id: &str, stream: &str) -> String {
    format!(
        "{}/dataCollectionRules/{}/streams/{}?api-version={LOGS_INGESTION_API_VERSION}",
        endpoint.trim().trim_end_matches('/'),
        crate::io::http::percent_encode(rule_id.trim()),
        crate::io::http::percent_encode(stream.trim()),
    )
}

/// Refuses a Log Analytics destination's rule or stream that the Logs Ingestion API could not name, and an endpoint that
/// names a path: the rule and stream make the path.
pub(super) fn check_log_analytics_rule(
    id: &str,
    endpoint: &str,
    rule_id: &str,
    stream: &str,
    errors: &mut ItemErrors<'_>,
) {
    let rule = rule_id.trim();
    let hex = rule.strip_prefix("dcr-").unwrap_or_default();
    if hex.len() != 32 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        errors.at("ruleId").push(format!(
            "destination {id:?} has ruleId {rule_id:?}; give the data collection rule's immutable id, \"dcr-\" and \
             32 hex digits, from the rule's JSON view"
        ));
    }
    if stream.trim().is_empty() {
        errors.at("stream").push(format!(
            "destination {id:?} names no stream; give the rule's stream, such as \"Custom-Events_CL\""
        ));
    }
    if names_a_path(endpoint) {
        errors.at("endpoint").push(format!(
            "destination {id:?} has endpoint {endpoint:?}; give the endpoint alone, such as \
             \"https://dce-1a2b.eastus-1.ingest.monitor.azure.com\": the rule and the stream follow it"
        ));
    }
}

/// Refuses an Event Hubs sign-in that names more than one credential, part of a service principal, or an empty or
/// unused field.
pub(super) fn check_event_hubs_sign_in(
    connection_string: Option<&SecretRefDto>,
    a: &EntraSignInDto,
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    let named: &[&str] = if connection_string.is_some() {
        &["connectionString"]
    } else {
        &[]
    };
    check_entra_sign_in(a, named, &["connectionString"], what, errors);
}

/// Refuses an Entra sign-in that names more than one credential, part of a service principal, or an empty or unused
/// field. `takes` are the kind's own credentials it may name in place of Entra's, and `named` those of them it does.
pub(super) fn check_entra_sign_in(
    a: &EntraSignInDto,
    named: &[&'static str],
    takes: &[&'static str],
    what: &str,
    errors: &mut ItemErrors<'_>,
) {
    let principal = [a.tenant_id.is_some(), a.client_id.is_some(), a.client_secret.is_some()];
    if principal.contains(&true) && principal.contains(&false) {
        let missing = match (&a.tenant_id, &a.client_id) {
            (None, _) => "tenantId",
            (_, None) => "clientId",
            _ => "clientSecret",
        };
        errors.at(missing).push(format!(
            "{what} names part of a service principal. Name tenantId, clientId and clientSecret together, or none \
             of them"
        ));
    }
    let managed = a.managed_identity_client_id.is_some();
    if named.len() + usize::from(principal.contains(&true)) + usize::from(managed) > 1 {
        // Two or more named: the managed identity, or else one of the kind's own credentials, is one of them.
        let field = if managed {
            "managedIdentityClientId"
        } else {
            named.first().copied().unwrap_or("clientSecret")
        };
        let others: String = takes.iter().map(|o| format!("{o}, ")).collect();
        errors.at(field).push(format!(
            "{what} names more than one credential. Name one of {others}a service principal and \
             managedIdentityClientId, or none to use the host's managed identity"
        ));
    }
    if a.auth_endpoint.is_some() && a.client_secret.is_none() {
        errors.at("clientSecret").push(format!(
            "{what} has an authEndpoint and no clientSecret; only a service principal obtains a token there"
        ));
    }
    if let Some(endpoint) = a.auth_endpoint.as_deref().filter(|e| names_a_path(e)) {
        errors.at("authEndpoint").push(format!(
            "{what} has authEndpoint {endpoint:?}; give the sign-in host alone, such as \
             \"https://login.microsoftonline.us\": the tenant follows it from tenantId"
        ));
    }
    check_managed_identity_client_id(a.managed_identity_client_id.as_deref(), what, errors);
    for (field, value) in [
        ("authEndpoint", &a.auth_endpoint),
        ("tenantId", &a.tenant_id),
        ("clientId", &a.client_id),
        ("managedIdentityClientId", &a.managed_identity_client_id),
    ] {
        if value.as_deref().is_some_and(|v| v.trim().is_empty()) {
            errors
                .at(field)
                .push(format!("{what} has an empty {field}; name one, or leave it out"));
        }
    }
}

/// How `sign_in` signs in, as a worker does it. The fields are checked first, so a partial service principal is
/// never seen.
pub(super) fn to_entra_sign_in(sign_in: &EntraSignInDto) -> EntraSignIn {
    match (&sign_in.tenant_id, &sign_in.client_id, &sign_in.client_secret) {
        (Some(tenant_id), Some(client_id), Some(secret)) => EntraSignIn::ServicePrincipal {
            authority: (sign_in.auth_endpoint.as_deref()).map(|a| a.trim().trim_end_matches('/').to_string()),
            tenant_id: tenant_id.trim().to_string(),
            client_id: client_id.trim().to_string(),
            secret: to_secret_ref(secret),
        },
        _ => EntraSignIn::ManagedIdentity {
            client_id: (sign_in.managed_identity_client_id.as_deref()).map(|c| c.trim().to_string()),
        },
    }
}

/// Refuses a Kafka producer's `bufferBytes` outside the range a worker can hold.
pub(super) fn check_kafka_buffer(buffer_bytes: Option<BufferBytes>, id: &str, errors: &mut ItemErrors<'_>) {
    if let Some(buffer_bytes) = buffer_bytes.filter(|b| b.0 < BufferBytes::MIN || b.0 > BufferBytes::MAX) {
        errors.at("bufferBytes").push(format!(
            "destination {id:?} lets egress and its producer each hold {}; it must be between {} \
             and {}. Below the floor an ordinary batch fills the queue and reports a \
             stall it is not in; above the ceiling a destination that stops draining \
             costs more memory than the rest of the worker put together",
            bytes::exact(buffer_bytes.0),
            bytes::exact(BufferBytes::MIN),
            bytes::exact(BufferBytes::MAX)
        ));
    }
}

/// Whether `url` names this host: `localhost` or a loopback address. `url` may be a bare `host:port`.
pub(crate) fn on_loopback(url: &str) -> bool {
    let url = url.trim();
    let rest = url.split_once("://").map_or(url, |(_, r)| r);
    crate::startup_config::split_authority(rest.split(['/', '?', '#']).next().unwrap_or_default())
        .is_some_and(|(host, _)| crate::startup_config::is_loopback(&host))
}

/// Whether `url` is plain http:// to a host other than this one: what a credential or a payload may never cross.
pub(crate) fn plain_off_this_host(url: &str) -> bool {
    let url = url.trim();
    url.get(..7)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
        && !on_loopback(url)
}

/// Every field with no workable default, and every bounded value. About emptiness and range, not reachability: a broker
/// that is down is a runtime concern, or a restart would depend on everything else being up. Whitespace counts as
/// empty.
pub(super) fn blank(v: &str) -> bool {
    v.trim().is_empty()
}

pub(super) fn required_fields(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    check_receiver_ports(file, attributed);

    let with_tls = (file
        .sources
        .iter()
        .filter_map(|s| Some(("sources", "source", &s.id, s.tls()?))))
    .chain(
        file.destinations
            .iter()
            .filter_map(|d| Some(("destinations", "destination", &d.id, d.tls()?))),
    );
    for (section, what, id, tls) in with_tls {
        check_tls(
            &tls,
            &format!("{what} {id:?}"),
            &mut ItemErrors::new(attributed, section, id),
        );
    }

    // A credential that names nothing is refused here, or it surfaces only when a worker fails to look it up.
    let named = (file.sources.iter().map(|s| ("sources", "source", &s.id, s.secrets()))).chain(
        file.destinations
            .iter()
            .map(|d| ("destinations", "destination", &d.id, d.secrets())),
    );
    for (section, what, id, secrets) in named {
        let errors = &mut ItemErrors::new(attributed, section, id);
        let mut fields: Vec<_> = secrets
            .iter()
            .filter(|(_, r)| blank(r.name()))
            .map(|(f, _)| *f)
            .collect();
        fields.dedup();
        for field in fields {
            errors.at(field).push(format!(
                "{what} {id:?} names a credential by a blank name. Name the environment variable or leader secret \
                 holding it"
            ));
        }
    }

    check_group_topics(file, attributed);

    let addressed = (file.sources.iter().map(|s| ("sources", "source", &s.id, s.urls()))).chain(
        file.destinations
            .iter()
            .map(|d| ("destinations", "destination", &d.id, d.urls())),
    );
    for (section, what, id, urls) in addressed {
        let errors = &mut ItemErrors::new(attributed, section, id);
        for (field, url) in urls {
            check_web_url(url, field, &format!("{what} {id:?}"), errors);
        }
    }

    for s in &file.sources {
        let errors = &mut ItemErrors::new(attributed, "sources", &s.id);
        check_read(&s.kind, &s.id, errors);
        s.kind.spec().check(s, errors);
    }

    for d in &file.destinations {
        let errors = &mut ItemErrors::new(attributed, "destinations", &d.id);
        d.kind.spec().check(d, errors);
    }
    for p in &file.pipelines {
        eval_assignments(&p.steps, p, &blank, attributed);
    }
}

/// An eval assignment writes `target` from `expr`, so neither is optional: a blank target names a
/// column nothing can read back, and a blank expression has nothing to compute. Steps nest, so this
/// walks `if` branches too.
///
/// Attributed to the assignment's row and field, so the editor puts the message on the box that is blank.
pub(super) fn eval_assignments(
    steps: &[StepDto],
    pipeline: &PipelineDto,
    blank: &impl Fn(&str) -> bool,
    errors: &mut Vec<ValidationError>,
) {
    for step in steps {
        match step {
            StepDto::If { then, r#else, .. } => {
                eval_assignments(then, pipeline, blank, errors);
                eval_assignments(r#else, pipeline, blank, errors);
            }
            StepDto::Eval { id, assignments, .. } => {
                for (index, a) in assignments.iter().enumerate() {
                    let at = |field, message| ValidationError {
                        pipeline: Some(pipeline.id.clone()),
                        step: Some(id.clone()),
                        assignment_index: Some(index as u32),
                        field: Some(field),
                        ..ValidationError::from(message)
                    };
                    if blank(&a.target) {
                        errors.push(at(
                            "target",
                            format!(
                                "pipeline {:?}, step {id:?}: an assignment has no target field. \
                                 Name the field it writes, or remove the assignment",
                                pipeline.name
                            ),
                        ));
                    }
                    if blank(&a.expr) {
                        // Naming a target that is itself blank would read as `assignment to ""`.
                        let which = match blank(&a.target) {
                            true => "an assignment".to_string(),
                            false => format!("the assignment to {:?}", a.target),
                        };
                        errors.push(at(
                            "expr",
                            format!(
                                "pipeline {:?}, step {id:?}: {which} has no expression. Give it \
                                 one, or remove the assignment",
                                pipeline.name
                            ),
                        ));
                    }
                }
            }
            _ => {}
        }
    }
}

/// The checks every object store destination shares: its delivery limits, prefix and format.
pub(super) fn check_objects(id: &str, o: &ObjectSettingsDto, url: &str, tls: &TlsDto, errors: &mut ItemErrors<'_>) {
    check_http_delivery(
        &HttpDelivery {
            id,
            url,
            url_field: "endpoint",
            zone_urls: None,
            request_field: "maxObjectBytes",
            tls,
            // A file's parts, not requests: checked below.
            in_flight: None,
            request_bytes: o.max_object_bytes.0,
            request_limits: (ObjectBytes::MIN, ObjectBytes::MAX),
            request_what: "an object size",
            limit_why: "a file is finished early at this size",
            // Bounded below, against the object's own limits: files upload in parts, so it need not hold one.
            buffer_bytes: None,
            stall_after: o.stall_after_secs,
        },
        errors,
    );
    check_file_limits(id, o.in_flight, o.buffer_bytes, errors);
    if let Err(why) = crate::io::objects::check_prefix(&o.prefix.0) {
        errors
            .at("prefix")
            .push(format!("destination {id:?} has the prefix {:?}: {why}", o.prefix.0));
    }
    check_field(
        id,
        matches!(o.format, ObjectFormatDto::Text | ObjectFormatDto::Bytes),
        &o.field,
        errors,
    );
    if o.newlines.is_some() && o.format != ObjectFormatDto::Bytes {
        errors.at("newlines").push(format!(
            "destination {id:?} names newlines, which only the \"bytes\" format reads"
        ));
    }
    if o.variant.is_some() && o.format != ObjectFormatDto::Parquet {
        errors.at("variant").push(format!(
            "destination {id:?} names variant, which only the \"parquet\" format reads"
        ));
    }
    check_compression_level(id, o.compression, o.compression_level, false, errors);
    if o.format == ObjectFormatDto::Avro && o.compression == CompressionDto::Lz4 {
        errors.at("compression").push(format!(
            "destination {id:?} writes Avro compressed with lz4, which Avro has no codec for; use \"gzip\", \"snappy\", \"zstd\" or \"none\""
        ));
    }
    check_file_interval(o.file_interval_secs, &format!("destination {id:?}"), "written", errors);
}

/// The buffer and upload concurrency of a destination that writes files: one that uploads them in parts, so its
/// buffer need not hold a whole file.
pub(super) fn check_file_limits(
    id: &str,
    in_flight: InFlight,
    buffer_bytes: Option<BufferBytes>,
    errors: &mut ItemErrors<'_>,
) {
    if let Some(stated) = buffer_bytes
        && (stated.0 < BufferBytes::MIN || stated.0 > BufferBytes::OBJECT_MAX)
    {
        errors.at("bufferBytes").push(format!(
            "destination {id:?} lets egress hold {} for it; it must be between {} and {}. A file is finished once \
             the rows it holds reach 3/4 of this, so it sizes the files, and egress holds those rows until then",
            bytes::exact(stated.0),
            bytes::exact(BufferBytes::MIN),
            bytes::exact(BufferBytes::OBJECT_MAX)
        ));
    }
    if in_flight.0 == 0 || in_flight.0 > crate::plan::topology::MAX_SENDERS {
        errors.at("inFlight").push(format!(
            "destination {id:?} uploads {} parts of a file at once; it must be between 1 and {}",
            in_flight.0,
            crate::plan::topology::MAX_SENDERS
        ));
    }
}

/// A source waits up to the interval for its batches' acknowledgement, and after a crash reads that much again.
/// `finished` is what becomes of a file in its destination's words: an object store's is written, a table's committed.
pub(super) fn check_file_interval(
    file_interval: FileInterval,
    what: &str,
    finished: &str,
    errors: &mut ItemErrors<'_>,
) {
    if file_interval.0 < FileInterval::MIN || file_interval.0 > FileInterval::MAX {
        errors.at("fileIntervalSecs").push(format!(
            "{what} has fileIntervalSecs {}; it must be between {} and {} seconds. A file stays open that long, \
             and its batches are acknowledged only when it is {finished}, so a source waits up to that long for \
             them and, after a crash, reads that much again.",
            file_interval.0,
            FileInterval::MIN,
            FileInterval::MAX,
        ));
    }
}

/// Refuses a Unity Catalog table that cannot be found or signed in to as written.
pub(super) fn check_unity_catalog(uc: &UnityCatalogDto, access: &S3AccessDto, what: &str, errors: &mut ItemErrors<'_>) {
    let blank = |v: &str| v.trim().is_empty();
    let workspace = &uc.workspace;
    // Every API is asked under the workspace's root, so a URL copied from a browser, with a path or `?o=`, is no base.
    let root = |u: &reqwest::Url| u.path() == "/" && u.query().is_none() && u.fragment().is_none();
    if !reqwest::Url::parse(workspace.trim())
        .is_ok_and(|u| matches!(u.scheme(), "http" | "https") && u.has_host() && root(&u))
    {
        errors.at("unityCatalog.workspace").push(format!(
            "{what} reaches its Unity Catalog workspace at {workspace:?}; give the workspace's https:// URL and \
             nothing after its host, as https://dbc-1234.cloud.databricks.com"
        ));
    }
    if access.region.as_deref().is_none_or(blank) {
        errors.at("region").push(format!(
            "{what} names no region beside a unityCatalog table; name the bucket's region, as us-west-2"
        ));
    }
    let table = &uc.table;
    let parts: Vec<&str> = table.trim().split('.').collect();
    if parts.len() != 3 || parts.iter().copied().any(blank) {
        errors.at("unityCatalog.table").push(format!(
            "{what} names the Unity Catalog table {table:?}; name it by its three parts, as catalog.schema.table"
        ));
    }
    if blank(&uc.client_id) {
        errors.at("unityCatalog.clientId").push(format!(
            "{what} names no clientId; name the OAuth client id of the service principal it signs in as"
        ));
    }
    // Unity Catalog vends the credentials for the table's storage, so storage credentials of its own go unused.
    let own = [
        ("accessKeyId", access.access_key_id.is_some()),
        ("secretAccessKey", access.secret_access_key.is_some()),
        ("roleArn", access.role_arn.is_some()),
        ("authEndpoint", access.auth_endpoint.is_some()),
    ];
    for (field, _) in own.into_iter().filter(|(_, set)| *set) {
        errors.at(field).push(format!(
            "{what} names {field} beside a unityCatalog table. Unity Catalog gives the credentials for the table's \
             storage; take {field} out"
        ));
    }
}

/// The services an Iceberg catalog signed with AWS signatures answers as: Glue's Iceberg endpoint, and S3 Tables.
/// Another name signs for a service that is no catalog, and AWS signs `s3` unlike the rest.
pub(super) const SIGNING_NAMES: [&str; 2] = ["glue", "s3tables"];

/// A cloud destination's storage class is one of `classes`, the cloud's own.
pub(super) fn check_storage_class(id: &str, cloud: &CloudSettingsDto, classes: &[&str], errors: &mut ItemErrors<'_>) {
    match &cloud.storage_class {
        Some(class) if class.trim().is_empty() => errors.at("storageClass").push(format!(
            "destination {id:?} has an empty storageClass; name one, or leave it out"
        )),
        Some(class) if !classes.contains(&class.as_str()) => errors.at("storageClass").push(format!(
            "destination {id:?} has the storage class {class:?}, which is none of {}",
            classes.join(", ")
        )),
        _ => {}
    }
}

/// A destination's `field` is read only by formats `"text"` and `"bytes"`, and names a field when it is there.
pub(super) fn check_field(id: &str, reads_field: bool, field: &Option<String>, errors: &mut ItemErrors<'_>) {
    match field {
        Some(_) if !reads_field => errors.at("field").push(format!(
            "destination {id:?} names a field, which only the \"text\" and \"bytes\" formats read"
        )),
        Some(field) if field.trim().is_empty() => errors.at("field").push(format!(
            "destination {id:?} has an empty field; name one, or leave it out"
        )),
        _ => {}
    }
}

/// Refuses a stream destination's Avro without a registry, and a registry where it writes no Avro.
pub(super) fn check_stream_avro(
    format: FormatDto,
    registry: &Option<crate::config_file::dto::SchemaRegistryDto>,
    id: &str,
    errors: &mut ItemErrors<'_>,
) {
    let what = format!("destination {id:?}");
    match (format == FormatDto::Avro, registry) {
        (true, None) => errors.at("schemaRegistry.url").push(format!(
            "{what} writes Avro, which a stream frames with its schema's id in a schema registry, but names none"
        )),
        (false, Some(_)) => errors.at("schemaRegistry").push(format!(
            "{what} names a schema registry but writes no Avro; set format to \"avro\""
        )),
        (true, Some(registry)) => check_registry(registry, &what, errors),
        (false, None) => {}
    }
}

/// Refuses Avro from a destination whose requests carry text, not a stream's records or an object's file.
pub(super) fn refuse_avro(format: FormatDto, id: &str, errors: &mut ItemErrors<'_>) {
    if format == FormatDto::Avro {
        errors.at("format").push(format!(
            "destination {id:?} writes format \"avro\", which only a stream or an object destination writes"
        ));
    }
}

/// What a Kafka, Splunk or HTTP destination's `format` writes of each row.
pub(super) fn to_payload(format: FormatDto, field: &Option<String>) -> Payload {
    match format {
        // An Avro destination writes its record by its own encoder, which `KafkaDestinationConfig::avro` holds.
        FormatDto::Json | FormatDto::Avro => Payload::Record,
        FormatDto::Text => Payload::Text {
            field: text_field(field).into(),
        },
        FormatDto::Bytes => Payload::Bytes {
            field: bytes_field(field).into(),
        },
    }
}

/// The field format `"text"` writes: `_raw` unless one is named.
pub(super) fn text_field(field: &Option<String>) -> String {
    field.clone().unwrap_or_else(|| "_raw".to_string())
}

/// The field format `"bytes"` writes: `__source.bytes` unless one is named.
pub(super) fn bytes_field(field: &Option<String>) -> String {
    field
        .clone()
        .unwrap_or_else(|| crate::plan::topology::SOURCE_BYTES.to_string())
}

/// `url` as parsed, so what goes on the wire is percent-encoded as the save checked it.
pub(super) fn as_parsed(url: &str) -> String {
    reqwest::Url::parse(url.trim()).map_or_else(|_| url.trim().to_string(), String::from)
}

/// The delivery settings every destination over `io::http` shares, and the words its own limits are stated in.
pub(super) struct HttpDelivery<'a> {
    pub(super) id: &'a str,
    pub(super) url: &'a str,
    /// The name the file gives `url`.
    pub(super) url_field: &'static str,
    /// Zone -> the address a worker there sends to instead, checked as `url` is.
    pub(super) zone_urls: Option<&'a BTreeMap<String, String>>,
    /// The name the file gives `request_bytes`.
    pub(super) request_field: &'static str,
    pub(super) tls: &'a TlsDto,
    /// Requests the destination starts with outstanding; `None` checks none.
    pub(super) in_flight: Option<InFlight>,
    pub(super) request_bytes: usize,
    pub(super) request_limits: (usize, usize),
    pub(super) request_what: &'a str,
    pub(super) limit_why: &'a str,
    pub(super) buffer_bytes: Option<BufferBytes>,
    pub(super) stall_after: StallAfter,
}

pub(super) fn check_http_delivery(d: &HttpDelivery<'_>, errors: &mut ItemErrors<'_>) {
    let id = d.id;
    if d.url.trim().is_empty() {
        errors.at(d.url_field).push(format!("destination {id:?} has no URL"));
    }
    let zone_urls = d.zone_urls.into_iter().flatten();
    for (zone, url) in zone_urls.clone() {
        if zone.trim().is_empty() {
            errors
                .at("zoneUrls")
                .push(format!("destination {id:?} names a zone by a blank name"));
        } else if url.trim().is_empty() {
            errors
                .at("zoneUrls")
                .push(format!("destination {id:?} has no URL for the zone {zone:?}"));
        }
    }
    // http:// never negotiates TLS, so TLS settings on one are configuration nothing reads.
    let addresses = std::iter::once((d.url_field, d.url)).chain(zone_urls.map(|(_, url)| ("zoneUrls", url.as_str())));
    for (field, url) in addresses {
        check_tls_reaches(d.tls, &[(field, url)], &format!("destination {id:?}"), errors);
    }
    check_stall_after(d.stall_after, id, errors);
    let (min, max) = d.request_limits;
    if d.request_bytes < min || d.request_bytes > max {
        errors.at(d.request_field).push(format!(
            "destination {id:?} sets {} of {}; it must be between {} and {}. {}",
            d.request_what,
            bytes::exact(d.request_bytes),
            bytes::exact(min),
            bytes::exact(max),
            d.limit_why
        ));
    }
    if let Some(stated) = d.buffer_bytes {
        if stated.0 < BufferBytes::MIN || stated.0 > BufferBytes::MAX {
            errors.at("bufferBytes").push(format!(
                "destination {id:?} lets egress hold {} for it; it must be between \
                 {} and {}. Egress holds a batch from acceptance until the service \
                 answers for it, so this bounds both the queue and the requests \
                 outstanding against it",
                bytes::exact(stated.0),
                bytes::exact(BufferBytes::MIN),
                bytes::exact(BufferBytes::MAX)
            ));
        }
        if stated.0 < d.request_bytes {
            errors.at("bufferBytes").push(format!(
                "destination {id:?} lets egress hold {} but sends requests of up to \
                 {}. The buffer must fit a whole request, or there is never room to \
                 hand one over",
                bytes::exact(stated.0),
                bytes::exact(d.request_bytes)
            ));
        }
    }
    let Some(in_flight) = d.in_flight else {
        return;
    };
    let affords = affords(d.buffer_bytes, d.request_bytes);
    if in_flight.0 == 0 || in_flight.0 > crate::plan::topology::MAX_SENDERS {
        errors.at("inFlight").push(format!(
            "destination {id:?} starts at {} concurrent requests; it must be between 1 and \
             {}. Each one is a sender thread holding its own connection, and egress runs \
             one thread per destination, so this is what one worker shows the service",
            in_flight.0,
            crate::plan::topology::MAX_SENDERS
        ));
    } else if in_flight.start(affords) > affords {
        errors.at("inFlight").push(format!(
            "destination {id:?} starts at {} concurrent requests but its buffer affords {}. \
             The buffer holds a request from acceptance until the service answers for it, \
             so a start above what it affords can never be reached",
            in_flight.0, affords
        ));
    }
}

/// Refuses a `recordsAt` that is not a JSON pointer, or that names an array a source set to `body` or `lines` never
/// reads.
pub(super) fn check_records_at(
    what: &str,
    records: ApiRecordsDto,
    records_at: &Option<String>,
    errors: &mut ItemErrors<'_>,
) {
    match (records, records_at) {
        (ApiRecordsDto::Array, Some(at)) if !at.is_empty() && !at.starts_with('/') => errors.at("recordsAt").push(
            format!("{what} reads its records at {at:?}, which is not a JSON pointer; one starts with /, as /data"),
        ),
        (ApiRecordsDto::Body | ApiRecordsDto::Lines, Some(_)) => errors.at("recordsAt").push(format!(
            "{what} says where an array of records is, and does not read an array; set records to \"array\" or \
             leave recordsAt out"
        )),
        _ => {}
    }
}

/// What a receiver must be before it can listen: a port, an address that is one, a certificate to serve, and someone
/// it admits. With no token, authorities or sender fingerprints to admit a sender by, anyone who can reach the port
/// could send.
pub(super) fn check_receiver(
    what: &str,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    tokens: &[&String],
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, tls, errors);
    if tokens.is_empty() && !vouches {
        errors.at("tokens").push(format!(
            "{what} admits anyone who can reach its port; give it a token, trusted authorities a sender's certificate \
             must chain to, or fingerprints of the certificates it admits"
        ));
    }
    for name in tokens {
        if name.trim().is_empty() {
            errors
                .at("tokens")
                .push(format!("{what} names a token by a blank name"));
        }
    }
}

/// A receiver's connection limits. Only a receiver whose transport is `tcp` reads them.
pub(super) fn check_connection_limits(
    what: &str,
    connections: &ConnectionLimitsDto,
    tcp: bool,
    errors: &mut ItemErrors<'_>,
) {
    let ConnectionLimitsDto {
        max_connections,
        max_connections_per_sender,
        idle_timeout_secs,
    } = connections;
    let set = [
        ("maxConnections", max_connections.is_some()),
        ("maxConnectionsPerSender", max_connections_per_sender.is_some()),
        ("idleTimeoutSecs", idle_timeout_secs.is_some()),
    ];
    for (field, set) in set {
        if set && !tcp {
            errors.at(field).push(format!(
                "{what} sets {field}, which only TCP reads: over UDP no connection is held open"
            ));
        }
    }
    let most = ConnectionLimits::MOST as u64;
    if let Some(n) = max_connections
        && !(1..=most).contains(n)
    {
        errors.at("maxConnections").push(format!(
            "{what} holds up to {n} connections; maxConnections is between 1 and {most}"
        ));
    }
    if let Some(n) = max_connections_per_sender
        && !(1..=max_connections.unwrap_or(most)).contains(n)
    {
        errors.at("maxConnectionsPerSender").push(match max_connections {
            Some(max) => format!(
                "{what} holds up to {n} connections from one sender; maxConnectionsPerSender is between 1 and its \
                 maxConnections, {max}"
            ),
            None => format!(
                "{what} holds up to {n} connections from one sender; maxConnectionsPerSender is between 1 and {most}"
            ),
        });
    }
    if *idle_timeout_secs == Some(0) {
        errors.at("idleTimeoutSecs").push(format!(
            "{what} closes a connection idle for 0 seconds; idleTimeoutSecs is at least 1"
        ));
    }
}

/// Checks a socket receiver as [`check_receiver`] checks any receiver, except that the networks it admits senders
/// from stand in for tokens, since its senders present none. Over UDP it serves no TLS, so only its networks can admit
/// a sender.
pub(super) fn check_socket_receiver(
    what: &str,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    allow_from: &[String],
    transport: Transport,
    errors: &mut ItemErrors<'_>,
) {
    let vouches = check_listener(what, listener, tls, errors);
    if transport == Transport::Udp && tls.is_set() {
        errors.at("certificate").push(format!(
            "{what} takes UDP, which it serves without TLS; take its certificate and trusted authorities off, or switch it to \
             TCP to serve TLS"
        ));
    }
    let vouches = vouches && transport == Transport::Tcp;
    if allow_from.is_empty() && !vouches {
        errors.at("allowFrom").push(format!(
            "{what} admits anyone who can reach its port; give it the networks its senders connect from, trusted \
             authorities a sender's certificate must chain to, or fingerprints of the certificates it admits"
        ));
    }
    for block in allow_from {
        if block.trim().parse::<ipnetwork::IpNetwork>().is_err() {
            errors.at("allowFrom").push(format!(
                "{what} admits senders from {block:?}, which is not a network; give an address or a CIDR block, as \
                 10.0.0.0/8"
            ));
        }
    }
}

/// What every receiver's listener must be: a port, and a certificate to serve where it serves TLS. Whether its
/// authorities or sender fingerprints vouch for a sender, which then needs nothing else to be admitted.
pub(super) fn check_listener(
    what: &str,
    listener: &ReceiverSettingsDto,
    tls: &ServedTlsDto,
    errors: &mut ItemErrors<'_>,
) -> bool {
    let ReceiverSettingsDto {
        port,
        max_request_bytes,
    } = listener;
    let blank = |v: &str| v.trim().is_empty();
    if *port == 0 {
        errors
            .at("port")
            .push(format!("{what} listens on port 0; give the port senders connect to"));
    }
    if tls.is_set() && tls.certificate.is_none() {
        errors.at("certificate").push(format!(
            "{what} serves TLS and names no certificate; a receiver serves one to every sender. Choose one from \
             the list"
        ));
    }
    if !(MaxRequestBytes::MIN..=MaxRequestBytes::MAX).contains(&max_request_bytes.0) {
        errors.at("maxRequestBytes").push(format!(
            "{what} reads requests of up to {} bytes; maxRequestBytes is between {} and {}",
            max_request_bytes.0,
            MaxRequestBytes::MIN,
            MaxRequestBytes::MAX
        ));
    }
    for fingerprint in &tls.sender_fingerprints {
        if let Err(why) = crate::io::senders::Fingerprint::parse(fingerprint) {
            errors.at("senderFingerprints").push(format!(
                "{what} admits a sender by {fingerprint:?}, which is not a fingerprint: {why}. Write one as \
                 sha-256:AB:CD:…"
            ));
        }
    }
    for name in &tls.sender_names {
        if !crate::io::senders::is_host(name.trim()) {
            errors.at("senderNames").push(format!(
                "{what} admits senders named {name:?}, which is not a host name; give one as host.example.com, or \
                 *.example.com for any host in it"
            ));
        }
    }
    let authorities = tls.trusted_authorities.as_deref().is_some_and(|a| !blank(a));
    if !tls.sender_names.is_empty() && !authorities {
        errors.at("senderNames").push(format!(
            "{what} names its senders' hosts and trusts no authority, so no certificate can match those names; paste \
             the trusted authorities their certificates chain to"
        ));
    }
    authorities || !tls.sender_fingerprints.is_empty()
}

/// Refuses two receivers on one port: every worker of the group binds each, and the second bind would fail on every
/// one of them. OTLP receivers of different signals share one, as an exporter sends every signal to one port, so they
/// serve it alike: on one address, under one certificate. A TCP port and a UDP port are two ports, whatever their number.
pub(super) fn check_receiver_ports(file: &FileConfig, attributed: &mut Vec<ValidationError>) {
    let mut by_port: std::collections::BTreeMap<(Transport, u16), Vec<&SourceDto>> = std::collections::BTreeMap::new();
    for s in &file.sources {
        let Some(port) = s.kind.receiver_port().filter(|p| *p != 0) else {
            continue;
        };
        let on_port = by_port.entry((s.kind.receiver_transport(), port)).or_default();
        let why = match (on_port.first(), &s.kind) {
            (None, _) => None,
            (
                Some(
                    first @ SourceDto {
                        kind:
                            SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto {
                                tls: theirs,
                                connections: their_limits,
                                ..
                            }),
                        ..
                    },
                ),
                SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto {
                    tls,
                    kind: OtlpReceiverDto { signal, .. },
                    connections,
                    ..
                }),
            ) => {
                let same = |o: &SourceDto| matches!(&o.kind, SourceKindDto::OtlpReceiver(OtlpReceiverSourceDto { kind: OtlpReceiverDto { signal: other, .. }, .. }) if other == signal);
                if let Some(taken) = on_port.iter().find(|o| same(o)) {
                    Some(format!(
                        "source {:?} takes {} on port {port}, which source {:?} already does; an export of \
                         one signal reaches one receiver, so give it another port",
                        s.id,
                        signal.name(),
                        taken.id
                    ))
                } else if tls != theirs {
                    Some(format!(
                        "source {:?} shares port {port} with source {:?}, which serves it; give both the same \
                         certificate and trusted authorities, or this one another port",
                        s.id, first.id
                    ))
                } else if to_connection_limits(connections) != to_connection_limits(their_limits) {
                    Some(format!(
                        "source {:?} shares port {port} with source {:?}, which serves it; give both the same \
                         maxConnections, maxConnectionsPerSender and idleTimeoutSecs, or this one another port",
                        s.id, first.id
                    ))
                } else {
                    None
                }
            }
            (Some(first), _) => Some(format!(
                "source {:?} listens on port {port}, which source {:?} already does; every worker binds both, so \
                 give each its own port",
                s.id, first.id
            )),
        };
        if let Some(why) = why {
            ItemErrors::new(attributed, "sources", &s.id).at("port").push(why);
        }
        on_port.push(s);
    }
}

/// What an SQL source reads, whatever its database: the settings every SQL kind shares.
pub(super) struct SqlRead<'a> {
    pub(super) query: &'a str,
    pub(super) mode: &'a SqlModeDto,
    pub(super) fields: &'a [SqlFieldDto],
    pub(super) poll_interval_secs: PollIntervalSecs,
    pub(super) batch: SqlBatch,
}

/// The value `startFrom` names, in a position whose first column is `ty`: an integer for an i64, an ISO 8601 time
/// with its offset for a datetime.
pub(super) fn sql_start_value(value: &serde_json::Value, ty: &DataType) -> Result<crate::io::values::Value, String> {
    use crate::io::values::Value;
    match ty {
        DataType::I64 => value
            .as_i64()
            .map(Value::Int)
            .ok_or_else(|| format!("{value} is not an integer, and the position's first column is an i64")),
        DataType::DateTime => value
            .as_str()
            .and_then(|text| {
                ["%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z"]
                    .iter()
                    .find_map(|fmt| crate::ops::timeparse::parse_with_format(text, fmt))
            })
            .map(Value::Time)
            .ok_or_else(|| {
                format!(
                    "{value} is not a time like \"2026-01-31T09:00:00Z\", and the position's first column is a datetime"
                )
            }),
        other => Err(format!("the position's first column is {other:?}, which has no start")),
    }
}

/// How long a position whose first column is a time waits for late commits when its source names no wait.
pub(super) const SQL_SETTLE_SECS: u32 = 30;

impl SqlRead<'_> {
    /// Refuses a query `dialect` would not run as a source's, fields that declare no record, and a mode the fields
    /// cannot hold.
    pub(super) fn check(
        &self,
        id: &str,
        dialect: crate::io::sql::Dialect,
        kind: crate::plan::topology::SourceMeta,
        errors: &mut ItemErrors<'_>,
    ) {
        let what = format!("source {id:?}");
        // The possessive leaves the id unquoted: `source "pg"'s query` reads as broken punctuation.
        let whose = format!("source {id}'s");
        let query = match crate::io::sql::Query::parse(self.query, dialect) {
            Ok(query) => Some(query),
            Err(why) => {
                errors.at("query").push(format!("{whose} query: {why}"));
                None
            }
        };
        if let Err(why) = SqlFieldDto::meta(kind, self.fields) {
            errors.at("fields").push(format!("{what}: {why}"));
        }
        let types: HashMap<&str, Option<DataType>> = self
            .fields
            .iter()
            .map(|f| (f.name.as_str(), crate::expr::parse::parse_type(&f.ty).ok()))
            .collect();
        if self.poll_interval_secs.0 == 0 {
            errors
                .at("pollIntervalSecs")
                .push(format!("{what} polls every 0 seconds; pollIntervalSecs is at least 1"));
        }
        if !(1..=SqlBatch::MAX).contains(&self.batch.0) {
            errors.at("batch").push(format!(
                "{what} reads {} rows a poll; batch is between 1 and {}",
                self.batch.0,
                SqlBatch::MAX
            ));
        }
        // A mode reads fields, so it is checked once there are some. Run reads a query that declares none yet.
        if self.fields.is_empty() {
            return;
        }
        let named = |column: &str| types.contains_key(column);
        match self.mode {
            SqlModeDto::Incrementing {
                position,
                settle_secs,
                start,
                start_from,
            }
            | SqlModeDto::UpdatedAt {
                position,
                settle_secs,
                start,
                start_from,
            } => {
                let updated_at = matches!(self.mode, SqlModeDto::UpdatedAt { .. });
                let Some(first) = position.first() else {
                    errors.at("mode").push(format!(
                        "{what} names no position; give the column rows are read in the order of"
                    ));
                    return;
                };
                for (i, column) in position.iter().enumerate() {
                    if position[..i].contains(column) {
                        errors
                            .at("mode")
                            .push(format!("{what} names `{column}` twice in its position"));
                    } else if !named(column) {
                        errors.at("mode").push(format!(
                            "{whose} position names `{column}`, which is not one of its fields; a position is read \
                             from the record"
                        ));
                    }
                }
                let first_type = types.get(first.as_str()).cloned().flatten();
                match (&first_type, updated_at) {
                    (Some(DataType::DateTime), _) | (Some(DataType::I64), false) | (None, _) => {}
                    (Some(_), false) => errors.at("mode").push(format!(
                        "{what} reads by `{first}`, which is neither an i64 nor a datetime; a position is a number \
                         or a time that only grows"
                    )),
                    (Some(_), true) => errors.at("mode").push(format!(
                        "{what} reads by updated at `{first}`, which is not a datetime; give the time every insert \
                         and update sets"
                    )),
                }
                for column in &position[1..] {
                    if let Some(Some(ty)) = types.get(column.as_str())
                        && !matches!(ty, DataType::I64 | DataType::DateTime | DataType::Str)
                    {
                        errors.at("mode").push(format!(
                            "{what} tells rows apart by `{column}`, which is not an i64, a datetime or a str"
                        ));
                    }
                }
                if settle_secs.is_some() && first_type.as_ref().is_some_and(|t| *t != DataType::DateTime) {
                    errors.at("mode").push(format!(
                        "{what} waits for late commits by `{first}`, which is not a time; only a time can wait. \
                         Take settleSecs off"
                    ));
                }
                match (start, start_from) {
                    (SqlStartDto::From, None) => errors
                        .at("mode")
                        .push(format!("{what} starts from a value it does not give; give startFrom")),
                    (SqlStartDto::From, Some(value)) => {
                        if let Some(ty) = &first_type
                            && let Err(why) = sql_start_value(value, ty)
                        {
                            errors.at("mode").push(format!("{whose} startFrom: {why}"));
                        }
                    }
                    (SqlStartDto::Now | SqlStartDto::Beginning, Some(_)) => errors.at("mode").push(format!(
                        "{what} gives startFrom but does not start from it; take startFrom off, or set start to from"
                    )),
                    (SqlStartDto::Now | SqlStartDto::Beginning, None) => {}
                }
                if let Some(query) = &query {
                    for column in query.last_columns() {
                        if !named(column) {
                            errors.at("query").push(format!(
                                "{whose} query names :last.{column}, and `{column}` is not one of its fields; :last \
                                 is the value a field held in the last row sent"
                            ));
                        }
                    }
                }
            }
            SqlModeDto::Comparing { key } => {
                if key.is_empty() {
                    errors.at("mode").push(format!(
                        "{what} compares values by no key; give the columns that tell each row apart"
                    ));
                }
                for column in key.iter().filter(|c| !named(c)) {
                    errors
                        .at("mode")
                        .push(format!("{whose} key names `{column}`, which is not one of its fields"));
                }
            }
            SqlModeDto::EveryPoll => {}
        }
        if let (Some(query), SqlModeDto::Comparing { .. } | SqlModeDto::EveryPoll) = (&query, self.mode)
            && let Some(column) = query.last_columns().first()
        {
            errors.at("query").push(format!(
                "{whose} query names :last.{column}; only a mode that reads past the last row sent has one"
            ));
        }
    }

    /// The source `check` passed. `database` names where it connects, as its place begins; `topic` is what each
    /// record's `__source.database` reads.
    pub(super) fn config(
        &self,
        dialect: crate::io::sql::Dialect,
        kind: crate::plan::topology::SourceMeta,
        database: &str,
        topic: String,
        connection: crate::plan::topology::SqlDatabase,
    ) -> crate::plan::topology::SqlConfig {
        use crate::io::sql_source::{Mode, Position, Start};
        let query = crate::io::sql::Query::parse(self.query, dialect)
            .unwrap_or_else(|why| unreachable!("validation refuses a query that does not parse: {why}"));
        let fields: Vec<(String, DataType)> = self
            .fields
            .iter()
            .map(|f| {
                let ty = crate::expr::parse::parse_type(&f.ty)
                    .unwrap_or_else(|why| unreachable!("validation refuses a type that does not parse: {why:?}"));
                (f.name.clone(), ty)
            })
            .collect();
        let position =
            |columns: &[String], settle_secs: Option<u32>, start: SqlStartDto, from: Option<&serde_json::Value>| {
                let first = fields
                    .iter()
                    .find(|(name, _)| Some(name) == columns.first())
                    .map(|(_, ty)| ty);
                let time = first == Some(&DataType::DateTime);
                let start = match (start, from, first) {
                    (SqlStartDto::Now, ..) => Start::Now,
                    (SqlStartDto::Beginning, ..) => Start::Beginning,
                    (SqlStartDto::From, Some(value), Some(ty)) => {
                        Start::From(sql_start_value(value, ty).unwrap_or_else(|why| {
                            unreachable!("validation refuses a startFrom that does not read: {why}")
                        }))
                    }
                    (SqlStartDto::From, ..) => unreachable!("validation refuses a start from no value"),
                };
                Position {
                    start,
                    columns: columns.to_vec(),
                    settle_secs: if time {
                        Some(settle_secs.unwrap_or(SQL_SETTLE_SECS))
                    } else {
                        None
                    },
                }
            };
        let (mode, by) = match self.mode {
            SqlModeDto::Incrementing {
                position: p,
                settle_secs,
                start,
                start_from,
            } => (
                Mode::Incrementing(position(p, *settle_secs, *start, start_from.as_ref())),
                format!("incrementing:{}", p.join(",")),
            ),
            SqlModeDto::UpdatedAt {
                position: p,
                settle_secs,
                start,
                start_from,
            } => (
                Mode::UpdatedAt(position(p, *settle_secs, *start, start_from.as_ref())),
                format!("updatedAt:{}", p.join(",")),
            ),
            SqlModeDto::Comparing { key } => (
                Mode::Comparing { key: key.clone() },
                format!("comparing:{}", key.join(",")),
            ),
            SqlModeDto::EveryPoll => (Mode::EveryPoll, "everyPoll".to_string()),
        };
        // The query and the columns its mode remembers by decide what the stored progress means, so either changing
        // starts afresh. A digest keeps the place short whatever the query's length.
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, format!("{by}\n{}", self.query).as_bytes());
        let digest: String = digest.as_ref()[..8].iter().map(|b| format!("{b:02x}")).collect();
        let meta = SqlFieldDto::meta(kind, self.fields)
            .unwrap_or_else(|why| unreachable!("validation refuses fields that declare no record: {why}"));
        crate::plan::topology::SqlConfig {
            database: connection,
            source: crate::io::sql_source::SqlSourceConfig {
                query,
                mode,
                fields: fields.into(),
                poll_interval: Duration::from_secs(self.poll_interval_secs.0),
                batch: self.batch.0,
                place: topic.into(),
            },
            meta,
            place: format!("{database}#{digest}"),
        }
    }
}

/// What every API source's address and schedule must be: a URL at `field`, TLS settings only where TLS would use
/// them, and a poll interval of at least a second. The URL, when it is a whole http:// or https:// one.
pub(super) fn check_api_source(
    what: &str,
    (field, url): (&'static str, &str),
    tls: &TlsDto,
    poll_interval: PollIntervalSecs,
    errors: &mut ItemErrors<'_>,
) -> Option<reqwest::Url> {
    check_tls_reaches(tls, &[(field, url)], what, errors);
    if poll_interval.0 == 0 {
        errors
            .at("pollIntervalSecs")
            .push(format!("{what} asks every 0 seconds; pollIntervalSecs is at least 1"));
    }
    if url.trim().is_empty() {
        errors.at(field).push(format!("{what} names no {field}"));
        return None;
    }
    // The source's URLs are refused once, with every other URL field's.
    let parsed = web_url(url)?;
    Some(parsed)
}

/// An app's sign-in, once [`check_app_sign_in`] passed it. `None` from one naming nothing.
pub(super) fn to_app_sign_in(dto: &AppSignInDto) -> Option<crate::plan::topology::AppSignIn> {
    use crate::plan::topology::{AppProof, AppSignIn, TokenAt};
    let given = |v: &Option<String>| v.as_deref().map(str::trim).filter(|v| !v.is_empty()).map(String::from);
    let token_at = match (given(&dto.token_url), given(&dto.issuer)) {
        (Some(url), _) => TokenAt::Url(url),
        (None, Some(issuer)) => TokenAt::Issuer(issuer),
        (None, None) => return None,
    };
    let proof = match (&dto.client_secret, &dto.private_key) {
        (Some(secret), _) => AppProof::Secret(to_secret_ref(secret)),
        (None, Some(key)) => AppProof::Key {
            private_key: to_secret_ref(key),
            key_id: given(&dto.key_id),
        },
        (None, None) => return None,
    };
    Some(AppSignIn {
        client_id: given(&dto.client_id)?,
        token_at,
        scope: given(&dto.scope),
        proof,
    })
}

/// That an app's sign-in names an app, one proof and one place to sign in, and that nothing else sets the header
/// its token goes in.
pub(super) fn check_app_sign_in(
    what: &str,
    sign_in: &AppSignInDto,
    headers: &BTreeMap<String, String>,
    credentials: &BTreeMap<String, SecretRefDto>,
    errors: &mut ItemErrors<'_>,
) {
    if sign_in.is_empty() {
        return;
    }
    let given = |v: &Option<String>| v.as_deref().is_some_and(|v| !v.trim().is_empty());
    if !given(&sign_in.client_id) {
        errors.at("clientId").push(format!(
            "{what} names part of an app's sign-in and no clientId; give the app's clientId, or drop the rest"
        ));
    }
    match (&sign_in.client_secret, &sign_in.private_key) {
        (Some(_), Some(_)) => errors.at("clientSecret").push(format!(
            "{what} names both a clientSecret and a privateKey; give the one the app signs in with"
        )),
        (None, None) => errors.at("clientSecret").push(format!(
            "{what} signs in as an app and names no clientSecret or privateKey; give the one the app signs in with"
        )),
        _ => {}
    }
    if sign_in.key_id.is_some() && sign_in.private_key.is_none() {
        errors.at("keyId").push(format!(
            "{what} names a keyId with no privateKey; give it beside privateKey, or drop it"
        ));
    }
    match (given(&sign_in.token_url), given(&sign_in.issuer)) {
        (true, true) => errors.at("tokenUrl").push(format!(
            "{what} names both a tokenUrl and an issuer; give the token endpoint, or the issuer that names it"
        )),
        (false, false) => errors.at("tokenUrl").push(format!(
            "{what} signs in as an app and names no tokenUrl or issuer; give the token endpoint, or the issuer \
             that names it"
        )),
        _ => {}
    }
    // Plain HTTP to a token endpoint loads, as every address a credential goes to does. An issuer's discovery is
    // held to the leader's own login rule, which takes plain HTTP on this host alone.
    if let Some(url) = sign_in
        .token_url
        .as_deref()
        .map(str::trim)
        .filter(|u| !plain_off_this_host(u))
        && let Err(e) = crate::io::discovery::check_url(url, true)
    {
        errors.at("tokenUrl").push(format!("{what} signs in at {e}"));
    }
    if let Some(issuer) = sign_in.issuer.as_deref().map(str::trim)
        && let Err(e) = crate::io::discovery::check_issuer(issuer)
    {
        errors.at("issuer").push(format!("{what} names the issuer {e}"));
    }
    if let Some((map, name)) = (headers.keys().map(|k| ("headers", k)))
        .chain(credentials.keys().map(|k| ("credentials", k)))
        .find(|(_, name)| name.eq_ignore_ascii_case("authorization"))
    {
        errors.at(map).push(format!(
            "{what} sets the header {name:?} and signs in as an app, whose token goes there; give one"
        ));
    }
}

/// Headers smeltr sets on every request an HTTP destination sends, by their lowercase names: a second value would
/// contradict it.
pub(super) const DESTINATION_HEADERS: &[&str] = &[
    "content-type",
    "content-encoding",
    "content-length",
    "transfer-encoding",
    "host",
    "cookie",
];

/// What a Prometheus destination sets itself: an HTTP destination's, and the remote_write version.
pub(super) const PROMETHEUS_HEADERS: &[&str] = &{
    let mut owned = [""; DESTINATION_HEADERS.len() + 1];
    let mut at = 0;
    while at < DESTINATION_HEADERS.len() {
        owned[at] = DESTINATION_HEADERS[at];
        at += 1;
    }
    owned[at] = "x-prometheus-remote-write-version";
    owned
};

/// Headers smeltr sets on every request an API source sends, by their lowercase names.
pub(super) const SOURCE_HEADERS: &[&str] = &["content-length", "transfer-encoding", "host"];

/// An HTTP item's headers: each a name HTTP allows, set once across both maps, none of the `owned` ones smeltr sets
/// itself `owned_why`, a committed value holding no line break.
pub(super) fn check_http_headers(
    what: &str,
    headers: &BTreeMap<String, String>,
    credentials: &BTreeMap<String, SecretRefDto>,
    owned: &[&str],
    owned_why: &str,
    errors: &mut ItemErrors<'_>,
) {
    let names = (headers.keys().map(|k| ("headers", k))).chain(credentials.keys().map(|k| ("credentials", k)));
    let mut seen: HashSet<String> = HashSet::new();
    for (map, name) in names {
        let lower = name.to_ascii_lowercase();
        if name.is_empty() || !name.bytes().all(is_token_byte) {
            errors.at(map).push(format!(
                "{what} names the header {name:?}, which is not a header name: letters, digits and \
                 !#$%&'*+-.^_`|~ only"
            ));
        } else if owned.contains(&lower.as_str()) {
            errors.at(map).push(format!(
                "{what} sets the header {name:?}, which smeltr sets itself {owned_why}"
            ));
        } else if !seen.insert(lower) {
            // Header names are case-insensitive, so two spellings are one header with two values.
            errors
                .at(map)
                .push(format!("{what} sets the header {name:?} twice; give it one value"));
        }
    }
    for (name, value) in headers {
        if value.chars().any(|c| c.is_control() && c != '\t') {
            errors.at("headers").push(format!(
                "{what} gives the header {name:?} a value with a line break or control character"
            ));
        }
    }
}

/// A byte a query parameter's name may hold without being escaped.
pub(super) fn is_query_name_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"-._~[]".contains(&b)
}

/// A byte RFC 9110 allows in a header name.
pub(super) fn is_token_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// A destination over the HTTP core, sending records to `url` with no headers of its own, as its sizes and stall
/// threshold say. Each kind sets what it sends on top of it.
pub(super) fn to_http_core(
    url: String,
    tls: Option<Tls>,
    in_flight: &InFlight,
    max_request_bytes: usize,
    buffer_bytes: Option<BufferBytes>,
    stall_after_secs: &StallAfter,
) -> HttpDestinationConfig {
    HttpDestinationConfig {
        url,
        zone_urls: BTreeMap::new(),
        body: RequestBody::default(),
        payload: Payload::Record,
        compression: crate::plan::topology::Compression::None,
        headers: Vec::new(),
        credentials: Vec::new(),
        tls,
        in_flight: in_flight.start(affords(buffer_bytes, max_request_bytes)),
        max_request_bytes,
        buffer_bytes: effective_buffer_bytes(buffer_bytes, max_request_bytes),
        stall_after: std::time::Duration::from_secs(stall_after_secs.0),
    }
}

/// Checks that a destination's `compressionLevel` is one its codec takes (`CompressionDto::levels`); `kafka` for a destination
/// librdkafka compresses for.
pub(super) fn check_compression_level(
    id: &str,
    compression: CompressionDto,
    level: Option<u8>,
    kafka: bool,
    errors: &mut ItemErrors<'_>,
) {
    let Some(level) = level else { return };
    let codec = compression.to_plan(None).as_property();
    match compression.levels(kafka) {
        Some((levels, _)) if levels.contains(&level) => {}
        Some((levels, _)) => errors.at("compressionLevel").push(format!(
            "destination {id:?} compresses with {codec} at level {level}; {codec} takes levels {} to {}",
            levels.start(),
            levels.end()
        )),
        None => errors.at("compressionLevel").push(format!(
            "destination {id:?} sets a compression level, which {codec} does not take"
        )),
    }
}

/// An HTTP request is sent as none or gzip.
pub(super) fn check_http_compression(id: &str, compression: &CompressionDto, errors: &mut ItemErrors<'_>) {
    if !matches!(compression, CompressionDto::None | CompressionDto::Gzip) {
        errors.at("compression").push(format!(
            "destination {id:?} compresses with {compression:?}; set compression to none or gzip"
        ));
    }
}

/// How many requests of `request_bytes` the buffer holds at once.
pub(super) fn affords(buffer_bytes: Option<BufferBytes>, request_bytes: usize) -> usize {
    effective_buffer_bytes(buffer_bytes, request_bytes) / request_bytes.max(1)
}

/// What egress may hold for an HTTP destination. Unset takes the default, or one whole request if that is larger:
/// raising the request size alone must never leave a destination unable to hand a request over.
pub(super) fn effective_buffer_bytes(stated: Option<BufferBytes>, request_bytes: usize) -> usize {
    match stated {
        Some(b) => b.0,
        None => BufferBytes::DEFAULT.max(request_bytes),
    }
}

pub(super) fn to_objects(
    cloud: Cloud,
    o: &ObjectSettingsDto,
    settings: &CloudSettingsDto,
    tls: &TlsDto,
    of: &str,
) -> DestinationKind {
    DestinationKind::Objects(ObjectsDestinationConfig {
        cloud,
        prefix: o.prefix.0.clone(),
        format: match o.format {
            ObjectFormatDto::Ndjson => ObjectFormat::Ndjson,
            ObjectFormatDto::Json => ObjectFormat::Json,
            ObjectFormatDto::Parquet => ObjectFormat::Parquet {
                native_variants: o.variant == Some(VariantDto::Native),
            },
            ObjectFormatDto::Avro => ObjectFormat::Avro,
            ObjectFormatDto::Csv => ObjectFormat::Csv,
            ObjectFormatDto::Bytes => ObjectFormat::Bytes {
                field: bytes_field(&o.field).into(),
                newlines: match o.newlines {
                    None | Some(NewlinesDto::Escape) => Newlines::Escape,
                    Some(NewlinesDto::Refuse) => Newlines::Refuse,
                },
            },
            ObjectFormatDto::Text => ObjectFormat::Text {
                field: text_field(&o.field).into(),
            },
        },
        compression: o.compression.to_plan(o.compression_level),
        storage_class: settings.storage_class.clone(),
        tls: to_tls(tls, of, Side::Client),
        in_flight: o.in_flight.0,
        max_object_bytes: o.max_object_bytes.0,
        buffer_bytes: o.buffer_bytes.map_or(BufferBytes::OBJECT_DEFAULT, |b| b.0),
        stall_after: std::time::Duration::from_secs(o.stall_after_secs.0),
        file_interval: std::time::Duration::from_secs(o.file_interval_secs.0),
    })
}
