//! Kafka `Source` and `Destination` on `rdkafka`'s blocking clients (`BaseConsumer`, `BaseProducer`): no async runtime
//! in the data plane.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rdkafka::ClientContext;
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::bindings;
use rdkafka::client::{DefaultClientContext, OAuthToken};
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{IsError, KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message};
use rdkafka::producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext};
use rdkafka::statistics::Statistics;
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use crate::core::id::DestId;
use crate::io::{
    BatchCompletion, BatchId, CommitMap, Destination, Dispatched, IoError, OutgoingRecord, OwnedRecord, PartitionLag,
    Rejection, Source, SourceRecord, TopicPartition, Undelivered,
};
use crate::metrics::MetricsHandle;
use crate::plan::topology::{HeldSecrets, KafkaDestinationConfig, KafkaProps, KafkaSourceConfig, StopPosition, Tls};

mod client;
mod entra;

pub use client::Clients;

/// Budget for one metadata call to the broker.
const SETUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long one attempt to place newly assigned partitions waits on the broker. A partition not placed in time is
/// held paused and placed again on a later poll, at most every `PLACE_RETRY_EVERY`, so neither the group's hand-over
/// nor a poll waits long on a broker that does not answer.
const PLACE_TIMEOUT: Duration = Duration::from_secs(5);
const PLACE_RETRY_EVERY: Duration = Duration::from_secs(5);

/// How long the consumer may go between polls before the broker evicts it and rebalances its partitions away. A
/// destination's stall threshold must stay under it: a flush that outlasts this turns one unreachable destination into
/// group-wide rebalance churn.
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(300);

/// How often a source's lag sampler pays for a broker round trip. Committed offsets come back in one batched call,
/// but `fetch_watermarks` has no such form -- one call per assigned partition, every interval. E2's window is
/// minutes wide, so nothing needs this fresher than a chart does, and `leader::warnings` reads lag in ten-second
/// buckets: one bucket, one sample.
const LAG_SAMPLE_INTERVAL: Duration = Duration::from_secs(10);

/// A stale-commit burst this soon after the last one is a warning: one rebalance is a deploy, two this close are a
/// group that keeps moving.
const STALE_BURST_REPEAT: Duration = Duration::from_secs(10 * 60);

/// Budget for one lag query. The sampler runs on its own thread, so a slow broker costs one late sample and nothing
/// on the reader. Dropping a source waits for the query in flight, so this also bounds that.
const LAG_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

// `session.timeout.ms`, `heartbeat.interval.ms` and `queued.min.messages` stay at librdkafka's defaults. The fetch
// sizes follow `fetchBytes`. The producer's batch, linger and requests in flight stay at librdkafka's own; its record
// may be as large as the broker takes, and its batch is lowered to that where it is less (`message_ceiling`).
// Compression and security are the operator's, through `apply_props`.

/// What smeltr calls itself to a broker.
const CLIENT_ID: &str = "smeltr";

/// Budget for the question a destination asks its broker before its producer is built (`message_ceiling`). A broker
/// that cannot answer in this time gets librdkafka's defaults and a deployment that goes ahead. Destinations are built
/// one after another, so an unreachable broker costs a deployment this much per destination.
const MESSAGE_CEILING_TIMEOUT: Duration = Duration::from_secs(5);

/// librdkafka's own `batch.size` and `message.max.bytes`. A producer keeps this batch unless its broker takes less.
const DEFAULT_BATCH_BYTES: usize = 1_000_000;

/// What librdkafka and the delivery tag hold for each queued record beyond its payload and key. librdkafka's own
/// bounds count payloads alone, or records whatever their size, so the producer counts each record at this much more
/// against its share of `bufferBytes` (`Held`). `per_message_overhead` keeps it true across librdkafka upgrades.
const MESSAGE_OVERHEAD_BYTES: usize = 225;

/// What a record costs the producer's share of `bufferBytes` while librdkafka holds it.
fn held_bytes(value: usize, key: usize) -> usize {
    value + key + MESSAGE_OVERHEAD_BYTES
}

/// What one queued record carries back to its delivery report. Sixteen bytes, so each record's allocation is the
/// allocator's smallest (`per_message_overhead`): a record is under `message.max.bytes`'s 1 GB, and a batch has fewer
/// rows than `u32` counts.
struct Sent {
    /// The send-sequence its batch was tagged with, so an acknowledgment is attributed to the batch.
    seq: u64,
    /// Its `held_bytes`, given back when it is reported.
    held: u32,
    /// Its position in the rows its batch was handed over with, so a failure names it.
    row: u32,
}

/// The range librdkafka lets `message.max.bytes` take. A broker ceiling outside it is brought inside: a producer built
/// outside the range never comes up at all.
const MIN_MESSAGE_BYTES: usize = 1_000;
const MAX_MESSAGE_BYTES: usize = 1_000_000_000;

impl From<KafkaError> for IoError {
    fn from(e: KafkaError) -> IoError {
        if is_rejection(&e) {
            IoError::rejected(e.to_string())
        } else {
            IoError::transient(e.to_string())
        }
    }
}

/// Whether the broker looked at this record and refused it, as opposed to being unreachable or busy. An allowlist: an
/// unrecognized code is transient, which costs a stall but never discards a record.
fn is_rejection(e: &KafkaError) -> bool {
    let code = match e {
        KafkaError::MessageProduction(c) => *c,
        _ => return false,
    };
    matches!(
        code,
        RDKafkaErrorCode::MessageSizeTooLarge
            | RDKafkaErrorCode::InvalidTopic
            | RDKafkaErrorCode::UnknownTopic
            | RDKafkaErrorCode::UnknownTopicOrPartition
            | RDKafkaErrorCode::InvalidMessage
            | RDKafkaErrorCode::InvalidMessageSize
            | RDKafkaErrorCode::InvalidRecord
    )
}

/// Whether a commit failed because the group rebalanced or the topic was deleted, so this consumer no longer owns what
/// it is committing. The records were delivered and the new owner resumes from the last commit, so the commit is
/// dropped rather than retried.
/// An allowlist: an unrecognized code stays an error.
fn is_stale_commit(e: &KafkaError) -> bool {
    let code = match e {
        KafkaError::ConsumerCommit(c) => *c,
        _ => return false,
    };
    matches!(
        code,
        RDKafkaErrorCode::IllegalGeneration
            | RDKafkaErrorCode::UnknownMemberId
            | RDKafkaErrorCode::RebalanceInProgress
            // The topic was deleted: its partitions leave this consumer at the next listing.
            | RDKafkaErrorCode::UnknownTopicOrPartition
    )
}

/// The pattern `topic` names, when it names one: a topic that starts with `^` reads every topic of the cluster whose
/// whole name matches it, as the `regex` crate reads it.
pub fn topic_pattern(topic: &str) -> Option<Result<regex::Regex, regex::Error>> {
    let pattern = topic.strip_prefix('^')?;
    Some(regex::Regex::new(&format!("^(?:{pattern})$")))
}

/// How many partitions `config`'s topics have in total, or `None` if the broker could not be asked.
pub fn total_partitions(config: &KafkaSourceConfig, secrets: &HeldSecrets) -> Option<usize> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.brokers)
        .set("group.id", &config.group_id);
    // The same configuration the source itself gets: a secured broker answers a metadata request no sooner than any
    // other, so without it this asks anonymously, fails, and the source is sized as `Unknown`.
    apply_client_config(&mut client_config, &config.props, config.tls.as_ref(), secrets).ok()?;
    // A throwaway consumer for one metadata call, not a `KafkaSource`: nothing here reads its connectivity health
    // back.
    let consumer: BaseConsumer = client_config.create().ok()?;
    entra::sign_in_unpolled(consumer.client(), client_config.get("sasl.oauthbearer.config"));
    let mut total = 0;
    for topic in &config.topics {
        let md = consumer.fetch_metadata(Some(topic), SETUP_TIMEOUT).ok()?;
        total += md
            .topics()
            .iter()
            .find(|t| t.name() == topic)
            .map(|t| t.partitions().len())?;
    }
    Some(total)
}

/// Checks operator-supplied literal properties against the linked librdkafka, one message per bad pair.
///
/// librdkafka is asked rather than a list kept here, so the check matches what this build supports, covers values as
/// well as names, and reports librdkafka's own message. One config per pair, so every bad property is named. Nothing
/// connects and no thread starts.
///
/// `credentials` is not checked here: its values are not knowable at save time, and probing with a placeholder
/// would refuse numeric and enum properties whose real value is fine. A typo there surfaces when the client is built
/// (`apply_props`).
pub fn validate_literal_props(props: &KafkaProps, what: &str, errors: &mut Vec<String>) {
    for (key, value) in &props.literal {
        let mut probe = ClientConfig::new();
        probe.set(key, value);
        if let Err(e) = probe.create_native_config() {
            errors.push(format!("{what} sets {key:?}: {e}"));
        }
    }

    // The loop above cannot catch this one. librdkafka marks a property whose value needs a compiled-in feature --
    // `compression.type` names the codecs it has -- but `sasl.mechanisms` is a plain string it accepts whatever it
    // links, choosing a provider only when a client is built. Asking here keeps the promise the rest of this function
    // makes.
    let selects_gssapi = props
        .literal
        .iter()
        .any(|(k, v)| matches!(k.as_str(), "sasl.mechanism" | "sasl.mechanisms") && v == "GSSAPI");
    if selects_gssapi && !supports_gssapi() {
        errors.push(format!(
            "{what} sets SASL GSSAPI (Kerberos), which this build does not carry"
        ));
    }
}

/// The SASL mechanisms this build can select, in the order a form offers them. Every librdkafka build carries the
/// first four; GSSAPI needs a provider linked at build time (`supports_gssapi`).
pub fn available_sasl_mechanisms() -> Vec<&'static str> {
    let mut mechanisms = vec!["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"];
    if supports_gssapi() {
        mechanisms.push("GSSAPI");
    }
    mechanisms
}

/// Whether this build links a GSSAPI provider. `Cargo.toml` decides it per platform (its Kerberos section): Linux
/// links Cyrus SASL and krb5, Windows has SSPI with nothing linked, and no other target carries one. librdkafka's
/// `builtin.features` cannot say -- the cmake build defines `WITH_SASL_CYRUS` as 0 rather than leaving it undefined,
/// and that table tests `defined()`, so it lists `sasl_gssapi` on every build.
fn supports_gssapi() -> bool {
    cfg!(any(windows, target_os = "linux"))
}

/// Everything an operator configured about this client, over smeltr's own keys: the TLS it speaks,
/// then the properties.
///
/// One function, so a consumer and a producer cannot be set up differently. The order would matter if anything
/// could set a property its TLS also sets, since `ClientConfig::set` keeps the last value and says nothing;
/// load-time validation refuses that pair, which is what leaves the order free to be the readable one.
fn apply_client_config(
    config: &mut ClientConfig,
    props: &KafkaProps,
    tls: Option<&Tls>,
    secrets: &HeldSecrets,
) -> Result<(), IoError> {
    apply_client_config_on(config, props, tls, secrets, entra::in_workload())
}

/// [`apply_client_config`] on a host that is, or is not, a pod with a workload identity.
fn apply_client_config_on(
    config: &mut ClientConfig,
    props: &KafkaProps,
    tls: Option<&Tls>,
    secrets: &HeldSecrets,
    in_workload: bool,
) -> Result<(), IoError> {
    apply_tls(config, tls, secrets)?;
    // Speaking no TLS of its own, librdkafka would trust the host's store alone. A client given its own roots keeps them.
    let own_roots = props.literal.iter().any(|(k, _)| k.starts_with("ssl.ca."))
        || props.credentials.iter().any(|(k, _)| k.starts_with("ssl.ca."));
    let ssl = props
        .literal
        .iter()
        .any(|(k, v)| k == "security.protocol" && v.to_ascii_uppercase().ends_with("SSL"));
    if tls.is_none() && ssl && !own_roots {
        config.set("ssl.ca.pem", super::trust::pem(None)?);
    }
    // librdkafka asks a token endpoint over its own HTTPS client, which trusts nothing it is not given. A token
    // endpoint is a public identity provider, not the brokers, so it gets the platform's roots, and only where the
    // brokers do: a connector's private authorities are the brokers' business.
    let oidc = props
        .literal
        .iter()
        .any(|(k, v)| k == "sasl.oauthbearer.method" && v == "oidc");
    let own_https_roots = props.literal.iter().any(|(k, _)| k.starts_with("https.ca."))
        || props.credentials.iter().any(|(k, _)| k.starts_with("https.ca."));
    let workload = entra::workload_config(props).filter(|_| in_workload);
    if tls.is_none() && oidc && workload.is_none() && !own_https_roots {
        config.set("https.ca.pem", super::trust::pem(None)?);
    }
    apply_props(config, props, secrets)?;
    if let Some(workload) = workload {
        // librdkafka's own sign-in steps aside, so it asks the client's context for each token.
        config
            .remove("sasl.oauthbearer.method")
            .remove("sasl.oauthbearer.metadata.authentication.type")
            .set("sasl.oauthbearer.config", workload);
    } else if let Some(query) = managed_identity_query(props, super::entra::managed_identity_client_id().as_deref()) {
        config.set("sasl.oauthbearer.config", query);
    }
    Ok(())
}

/// The `sasl.oauthbearer.config` a client signing in as the host's Azure managed identity takes, when the host names
/// which of its identities that is: Azure's own clients read `AZURE_CLIENT_ID` for a user-assigned identity, and so
/// does this one. `None` leaves the properties as they are: no managed identity, one the properties already name,
/// or a host naming none, which is its system-assigned identity.
fn managed_identity_query(props: &KafkaProps, client_id: Option<&str>) -> Option<String> {
    let client_id = client_id.map(str::trim).filter(|c| !c.is_empty())?;
    let literal = |key: &str| props.literal.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str());
    if literal("sasl.oauthbearer.metadata.authentication.type") != Some("azure_imds") {
        return None;
    }
    let config = literal("sasl.oauthbearer.config")?;
    let query = config.strip_prefix("query=").filter(|q| !q.contains(','))?;
    if query.split('&').any(|pair| pair.starts_with("client_id=")) {
        return None;
    }
    Some(format!("{config}&client_id={client_id}"))
}

/// Sets the inline PEM properties the connector's TLS carries. librdkafka reads each of these from memory, so
/// nothing is written to the worker's disk. The chain and key come through `trust::presented`, so one librdkafka
/// could not read is refused in the words every other client uses, before any handshake.
///
/// The security protocol is not set here: the TLS says what this client presents and trusts, not whether SASL is
/// also in play, and load-time validation has already refused TLS of the connector's own under a protocol that
/// negotiates none.
fn apply_tls(config: &mut ClientConfig, tls: Option<&Tls>, secrets: &HeldSecrets) -> Result<(), IoError> {
    let Some(tls) = tls else { return Ok(()) };
    config.set("ssl.ca.pem", super::trust::pem(Some(tls))?);
    if let Some(presented) = super::trust::presented(tls, secrets)? {
        config.set("ssl.certificate.pem", presented.chain_pem);
        config.set("ssl.key.pem", presented.key_pem);
    }
    Ok(())
}

/// Applies an operator's librdkafka properties, resolving the credentials against what this worker holds. One it
/// cannot obtain is an error, not a skipped property: a credential silently dropped would look exactly like a broker
/// refusing it.
fn apply_props(config: &mut ClientConfig, props: &KafkaProps, secrets: &HeldSecrets) -> Result<(), IoError> {
    for (k, v) in &props.literal {
        config.set(k, v);
    }
    for (k, reference) in &props.credentials {
        config.set(
            k,
            super::held(secrets, reference, &format!("kafka property {k:?}"), "value")?,
        );
    }
    Ok(())
}

/// A Kafka source: its topics, read through the deployment's shared client (`client`) under ordinary group
/// membership, so the group's members share the partitions. `KafkaSourceConfig::start` places each partition the group
/// hands over (`client::resolve_starts`); `stop`, when set, ends each partition at it. Offsets are keyed by
/// `io::TopicPartition`, so topics sharing a partition number commit independently.
pub struct KafkaSource {
    client: Arc<client::Client>,
    /// What this source and the client thread share: its inbox, its partitions, and what the group did to them.
    slot: Arc<client::Slot>,
    topic: String,
    /// Partitions the group took since `take_revoked` last reported, for the reader.
    revoked: Vec<TopicPartition>,
    /// Where each partition ends, before the record there. `None` never ends.
    stop: Option<StopPosition>,
    /// When `place_held` may next ask the broker.
    place_retry_at: Instant,
    /// Commits dropped because the group had moved on (`is_stale_commit`). Logged once per burst rather than per
    /// commit.
    stale_commits: u64,
    stale_commit_logged: bool,
    /// When the last burst began. A deploy rebalances the group once, which is the at-least-once path working; a burst
    /// soon after another means the group keeps rebalancing.
    stale_burst_at: Option<Instant>,
    /// The broker's latest word on this source's commits: `Ok` once one landed or was dropped as stale, the refusal
    /// otherwise. `commit` answers with it, so a refusal is reported on every call until a later commit lands.
    commit_verdict: Result<(), KafkaError>,
    /// How its payloads read, where they are Avro.
    avro: Option<crate::io::avro::AvroReader>,
}

/// `msg`'s headers, when `keep` asks for them. A header with no value is left out.
fn headers_of(msg: &impl Message, keep: bool) -> Option<Vec<(String, Vec<u8>)>> {
    keep.then(|| match msg.headers() {
        Some(headers) => headers
            .iter()
            .filter_map(|h| Some((h.key.to_string(), h.value?.to_vec())))
            .collect(),
        None => Vec::new(),
    })
}

/// The part of `offsets` whose partitions are in `owned`, and how many were left out. A cooperative rebalance moves
/// a partition away while batches read from it are still in flight, and the broker takes a commit for any partition
/// from a current member: committing one this consumer no longer owns would move its new owner's offset back. The
/// reader forgets a revoked partition once `take_revoked` reports it; this covers a revoke it has not heard of yet.
fn owned_only(offsets: &CommitMap, owned: &HashSet<(String, i32)>) -> (Vec<(TopicPartition, i64)>, usize) {
    let mut kept = Vec::with_capacity(offsets.len());
    for (tp, &offset) in offsets {
        if owned.contains(&(tp.topic.to_string(), tp.partition)) {
            kept.push((tp.clone(), offset));
        }
    }
    let dropped = offsets.len() - kept.len();
    (kept, dropped)
}

impl KafkaSource {
    /// A source on a client of its own.
    pub fn new(config: &KafkaSourceConfig, secrets: &HeldSecrets) -> Result<KafkaSource, IoError> {
        KafkaSource::open(config, secrets, &Arc::new(Clients::default()))
    }

    /// A source on the client of `clients` its configuration names.
    pub fn open(
        config: &KafkaSourceConfig,
        secrets: &HeldSecrets,
        clients: &Arc<Clients>,
    ) -> Result<KafkaSource, IoError> {
        if config.topics.is_empty() {
            return Err(IoError::transient("a source must name at least one topic".to_string()));
        }
        let avro = (config.avro.as_ref())
            .map(|a| crate::io::avro::AvroReader::new(a, config.tls.as_ref(), secrets))
            .transpose()?;
        let (client, slot) = clients.attach(config, secrets)?;
        Ok(KafkaSource {
            avro,
            client,
            slot,
            topic: config.topics[0].clone(),
            revoked: Vec::new(),
            stop: config.stop,
            place_retry_at: Instant::now(),
            stale_commits: 0,
            stale_burst_at: None,
            stale_commit_logged: false,
            commit_verdict: Ok(()),
        })
    }

    fn consumer(&self) -> &BaseConsumer<client::Routes> {
        &self.client.consumer
    }

    /// The reader is reading. A source whose reader had stopped goes back into the subscription.
    fn touch(&self) {
        if self.slot.touch() {
            tracing::info!(
                target: "kafka.reader_resumed",
                subject = %self.topic,
                "a Kafka source's reader is reading again; its topics rejoin the subscription"
            );
            self.client.routes.changed();
        }
    }

    /// Take the partitions the group revoked since the last call, and answer whether there were any.
    fn note_rebalance(&mut self, state: &mut client::SlotState) -> Result<bool, IoError> {
        let revoked = std::mem::take(&mut state.revoked);
        for (topic, partition) in &revoked {
            self.revoked
                .push(TopicPartition::new(self.slot_topic(topic), *partition));
        }
        Ok(!revoked.is_empty())
    }

    /// This source's name for `topic`, interned.
    fn slot_topic(&self, topic: &str) -> Arc<str> {
        self.slot
            .topics
            .iter()
            .find(|t| t.as_ref() == topic)
            .cloned()
            .unwrap_or_else(|| Arc::from(topic))
    }

    /// Places the partitions the group handed over that the broker could not place then, and lets them fetch. Asks
    /// the broker at most every `PLACE_RETRY_EVERY`, and never under the slot's lock; until it answers they stay
    /// paused.
    fn place_held(&mut self) -> Result<(), IoError> {
        let held: Vec<((String, i32), client::Placing)> = {
            let state = self.slot.lock();
            if state.unplaced.is_empty() || Instant::now() < self.place_retry_at {
                return Ok(());
            }
            state
                .unplaced
                .iter()
                .map(|k| (k.clone(), self.slot.placing()))
                .collect()
        };
        self.place_retry_at = Instant::now() + PLACE_RETRY_EVERY;
        let resolved = match client::resolve_starts(self.consumer(), &held) {
            Ok(resolved) => resolved,
            Err(e) => {
                tracing::debug!(target: "kafka.placement", error = %e, "still cannot place held partitions");
                return Ok(());
            }
        };
        // One seek for them all, so one timeout bounds the poll however many partitions are held.
        let mut placed = Vec::new();
        let mut seeks = TopicPartitionList::new();
        let mut tags = HashMap::new();
        for ((topic, partition), off, tag) in resolved {
            tags.insert((topic.clone(), partition), tag);
            if matches!(off, Offset::Invalid) {
                // Nothing to seek to: the partition starts where the client puts it.
                placed.push((topic, partition));
            } else {
                seeks.add_partition_offset(&topic, partition, off)?;
            }
        }
        if seeks.count() > 0 {
            match self.consumer().seek_partitions(seeks, PLACE_TIMEOUT) {
                Ok(sought) => {
                    for e in sought.elements() {
                        if e.error().is_err() {
                            continue;
                        }
                        placed.push((e.topic().to_string(), e.partition()));
                    }
                }
                Err(e) => tracing::debug!(target: "kafka.placement", error = %e, "could not seek held partitions"),
            }
        }
        let mut state = self.slot.lock();
        for (topic, partition) in placed {
            // Revoked meanwhile: no longer this source's to place.
            if !state.unplaced.remove(&(topic.clone(), partition)) {
                continue;
            }
            let key = (topic.clone(), partition);
            let tag = tags.get(&key).cloned().unwrap_or_default();
            state.tags.insert(key, tag);
        }
        self.slot.sync(self.consumer(), &state)
    }

    /// Marks a partition exhausted and pauses it so librdkafka stops fetching from it. Idempotent.
    fn mark_exhausted(&self, state: &mut client::SlotState, topic: &str, partition: i32) -> Result<(), IoError> {
        if state.exhausted.insert((topic.to_string(), partition)) {
            self.slot.sync(self.consumer(), state)?;
        }
        Ok(())
    }

    /// Whether a record of `topic`'s `partition` stamped `timestamp` is at or past the partition's stop, marking the
    /// partition exhausted when it is. Such a record is not read. A time stop ends a partition at its first record
    /// stamped at or after it, the record Kafka's offsets-for-times would name; a record with no timestamp does not
    /// end it.
    fn past_stop(
        &self,
        state: &mut client::SlotState,
        topic: &str,
        partition: i32,
        timestamp: Option<i64>,
    ) -> Result<bool, IoError> {
        if state.exhausted.contains(&(topic.to_string(), partition)) {
            return Ok(true);
        }
        let at_stop = matches!((self.stop, timestamp), (Some(StopPosition::Timestamp(s)), Some(t)) if t >= s);
        if at_stop {
            self.mark_exhausted(state, topic, partition)?;
        }
        Ok(at_stop)
    }

    /// Hands `tpl` to librdkafka with the consumer's own queue as the reply queue, so the broker's answer comes back
    /// through the client thread's `poll` as an offset-commit event, which the client hands to
    /// `Routes::commit_callback`. The client's own asynchronous commit mode names no reply queue, and librdkafka
    /// then posts the answer only to a legacy callback the client never registers: nobody would hear it.
    fn commit_async(&self, tpl: &TopicPartitionList) -> Result<(), KafkaError> {
        let rk = self.consumer().client().native_ptr();
        // SAFETY: `rk` is the live client `self.client` owns, and `tpl` outlives the call, which copies the list
        // before it returns. The queue handle is one more reference to the consumer queue, taken and released here;
        // the answer goes to the queue itself, which lives as long as the consumer.
        let err = unsafe {
            let queue = bindings::rd_kafka_queue_get_consumer(rk);
            if queue.is_null() {
                return Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::UnknownGroup));
            }
            let err = bindings::rd_kafka_commit_queue(rk, tpl.ptr(), queue, None, std::ptr::null_mut());
            bindings::rd_kafka_queue_destroy(queue);
            err
        };
        if err.is_error() {
            Err(KafkaError::ConsumerCommit(err.into()))
        } else {
            Ok(())
        }
    }

    /// One commit's answer. A landed commit ends a stale burst; a stale one is dropped and counted; anything else
    /// is a refusal, kept as the verdict and handed back to the call that read it.
    /// `count` commits dropped because the group moved these partitions away, logged once per burst.
    fn note_stale_commits(&mut self, count: u64, cause: &str) {
        self.stale_commits += count;
        if !self.stale_commit_logged {
            self.stale_commit_logged = true;
            let repeated = self.stale_burst_at.is_some_and(|at| at.elapsed() < STALE_BURST_REPEAT);
            self.stale_burst_at = Some(Instant::now());
            if repeated {
                tracing::warn!(
                    target: "kafka.commit_stale",
                    subject = %self.topic,
                    error = cause,
                    "dropping an offset commit, in the second rebalance within ten minutes: the group is \
                     rebalancing often. Their new owner resumes from the last committed offset, so records between \
                     it and here are redelivered rather than lost. Look at session.timeout.ms and how long startup \
                     takes."
                );
            } else {
                tracing::info!(
                    target: "kafka.commit_stale",
                    subject = %self.topic,
                    error = cause,
                    "dropping an offset commit: the consumer group rebalanced and these partitions are no \
                     longer this consumer's. Their new owner resumes from the last committed offset, so \
                     records between it and here are redelivered rather than lost."
                );
            }
        }
    }

    fn settle_commit(&mut self, answer: Result<(), KafkaError>) -> Option<KafkaError> {
        self.commit_verdict = match answer {
            Ok(()) => {
                if self.stale_commit_logged {
                    tracing::info!(
                        target: "kafka.commit_current",
                        subject = %self.topic,
                        dropped = self.stale_commits,
                        "commits are landing again after some were dropped while the consumer group rebalanced"
                    );
                    self.stale_commit_logged = false;
                }
                Ok(())
            }
            // Dropped, not failed: the group moved on and these partitions are no longer this consumer's. Failing would
            // pause the source over an ordinary rebalance, and `CommitTracker::retire` has already taken the tokens, so
            // the commit is gone either way.
            Err(e) if is_stale_commit(&e) => {
                self.note_stale_commits(1, &e.to_string());
                Ok(())
            }
            Err(e) => Err(e),
        };
        self.commit_verdict.clone().err()
    }
}

impl Drop for KafkaSource {
    /// Leaves the client: its topics leave the subscription. The last source to leave closes the client.
    fn drop(&mut self) {
        self.client.detach(&self.slot);
    }
}

impl Source for KafkaSource {
    /// `true` while this source owns partitions, every one is at its stop, and every record read has been handed
    /// out. Always `false` with no stop.
    fn is_exhausted(&self) -> bool {
        if self.stop.is_none() {
            return false;
        }
        let state = self.slot.lock();
        state.inbox.is_empty() && !state.owned.is_empty() && state.owned.iter().all(|k| state.exhausted.contains(k))
    }

    /// Its inbox: what the client routed here and the reader has not taken.
    fn held_bytes(&self) -> Option<u64> {
        Some(self.slot.held() as u64)
    }

    /// Every partition this source owns.
    fn assigned_partitions(&self) -> Vec<(String, i32)> {
        self.slot.lock().owned.iter().cloned().collect()
    }

    fn poll_batch(
        &mut self,
        max_rows: usize,
        max_bytes: usize,
        timeout: Duration,
    ) -> Result<Vec<SourceRecord>, IoError> {
        if max_rows == 0 {
            return Ok(Vec::new());
        }
        self.touch();
        {
            let mut state = self.slot.lock();
            if state.keeping {
                state.keeping = false;
                self.slot.sync(self.consumer(), &state)?;
            }
        }
        self.place_held()?;

        let deadline = Instant::now() + timeout;
        let mut out = crate::io::RecordChunkBuilder::new();
        let slot = Arc::clone(&self.slot);
        let mut state = slot.lock();
        'poll: loop {
            if let Some(e) = state.fatal.clone() {
                return Err(e.into());
            }
            // A revoke ends the batch: what was read before it is forgotten with the partition, and what is read
            // after it may be under a new assignment of the same partition, which must commit.
            if self.note_rebalance(&mut state)? {
                break;
            }
            // The first always goes out, however large, as a fresh one would; the rest only while they fit.
            while !state.refetch && out.len() < max_rows {
                let Some(len) = state.inbox.front().map(client::Pending::size) else {
                    break;
                };
                if !out.is_empty() && out.size() + len > max_bytes {
                    break;
                }
                // A record at or past its partition's stop ends that partition, and one from a partition not yet
                // placed is not where it starts: both are dropped unread, and the others read on.
                let front = state.inbox.front().expect("a front");
                let (topic, partition, timestamp) = (Arc::clone(&front.topic), front.partition, front.timestamp);
                if state.unplaced.contains(&(topic.to_string(), partition))
                    || self.past_stop(&mut state, &topic, partition, timestamp)?
                {
                    slot.take(self.consumer(), &mut state);
                    continue;
                }
                // The registry is asked before the record leaves the inbox, so a registry that does not answer
                // leaves it there to be read again.
                let unasked = (self.avro.as_ref()).and_then(|a| a.unasked(&state.inbox.front().expect("a front").raw));
                if let (Some(id), Some(avro)) = (unasked, self.avro.as_mut()) {
                    drop(state);
                    let asked = avro.ask(id);
                    state = slot.lock();
                    match asked {
                        Ok(()) => continue 'poll,
                        Err(_) if !out.is_empty() => break 'poll,
                        Err(e) => return Err(e),
                    }
                }
                let record = slot.take(self.consumer(), &mut state).expect("a front");
                out.push_stamped(
                    &record.raw,
                    record.offset,
                    record.partition,
                    record.topic,
                    record.headers,
                    record.timestamp.and_then(|millis| millis.checked_mul(1000)),
                );
            }
            // Records in hand go out once nothing more has arrived: waiting to fill the batch would only delay them.
            let remaining = deadline.saturating_duration_since(Instant::now());
            if !out.is_empty() || remaining.is_zero() {
                break;
            }
            state = slot.arrived.wait_timeout(state, remaining).unwrap().0;
        }
        drop(state);
        let records = out.finish();
        Ok(match &self.avro {
            Some(avro) if !records.is_empty() => avro.read(records),
            _ => records,
        })
    }

    /// Pauses every partition this source owns, and each one the group assigns while blocked. The client keeps
    /// polling, so a block is not an eviction and a rebalance.
    fn block(&mut self) -> Result<(), IoError> {
        let mut state = self.slot.lock();
        state.blocked = true;
        self.slot.sync(self.consumer(), &state)
    }

    /// Records already in the inbox when headers are first asked for were copied without them: the client fetches
    /// them again before the reader takes any more.
    fn keep_headers(&mut self, keep: bool) {
        let mut state = self.slot.lock();
        if keep && !state.keep_headers && !state.inbox.is_empty() {
            state.refetch = true;
            self.client.routes.request_refetch();
        }
        state.keep_headers = keep;
    }

    fn release(&mut self) -> Result<(), IoError> {
        let mut state = self.slot.lock();
        state.blocked = false;
        self.slot.sync(self.consumer(), &state)
    }

    /// Pauses every partition this source owns until the next `poll_batch`. The client thread serves the group's
    /// rebalances and the poll interval, so there is nothing more to serve here. What arrives anyway waits in the
    /// inbox for that call.
    fn keep_membership(&mut self, timeout: Duration) -> Result<(), IoError> {
        self.touch();
        {
            let mut state = self.slot.lock();
            if !state.keeping {
                state.keeping = true;
                self.slot.sync(self.consumer(), &state)?;
            }
        }
        if !timeout.is_zero() {
            std::thread::sleep(timeout);
        }
        Ok(())
    }

    fn take_revoked(&mut self) -> Vec<TopicPartition> {
        let slot = Arc::clone(&self.slot);
        let mut state = slot.lock();
        let revoked = std::mem::take(&mut state.revoked);
        drop(state);
        for (topic, partition) in revoked {
            self.revoked
                .push(TopicPartition::new(self.slot_topic(&topic), partition));
        }
        std::mem::take(&mut self.revoked)
    }

    /// What the client's sampler measured of this source since the last call. Never a broker round trip on this
    /// thread.
    fn lag(&mut self) -> Vec<PartitionLag> {
        std::mem::take(&mut self.slot.lock().lag)
    }

    /// Hands the commit to the client and returns at once. The broker's answer arrives through the client thread's
    /// `poll`, and a later call reads it: a refusal reaches the caller late, on the call that reads it whatever else
    /// that call reads, and stays the answer until a commit lands. Several commits may be in flight, and across a
    /// coordinator change an older one may land after a newer one, so the committed offset can step back until the
    /// next lands. A commit unanswered when the client closes is read by nobody: closing waits for it to land, and one
    /// that does not is a dropped commit like any other, its records redelivered from the commit before it.
    fn commit(&mut self, offsets: &CommitMap) -> Result<(), IoError> {
        let (answers, owned, tags) = {
            let mut state = self.slot.lock();
            let answers = std::mem::take(&mut state.commit_results);
            let owned = (!offsets.is_empty()).then(|| (state.owned.is_empty(), owned_only(offsets, &state.owned)));
            (answers, owned, state.tags.clone())
        };
        // A refusal among this call's answers is this call's even when a later answer landed, so every refused
        // commit is reported once; the verdict keeps the latest word for the calls after.
        let mut refused = None;
        for answer in answers {
            if let Some(e) = self.settle_commit(answer) {
                refused = Some(e);
            }
        }
        if let Some((owns_none, (kept, dropped))) = owned {
            // Every partition in `offsets` was owned at poll time, so owning none now means ownership was lost
            // between poll and commit: an error, not nothing to do.
            if owns_none {
                return Err(IoError::transient(
                    "commit: consumer has no current partition assignment".to_string(),
                ));
            }
            if dropped > 0 {
                self.note_stale_commits(dropped as u64, "partitions no longer assigned to this consumer");
            }
            let mut tpl = TopicPartitionList::new();
            for (tp, offset) in &kept {
                // Kafka commits the next offset to read; `CommitMap` holds the last processed one.
                tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(offset + 1))?;
                // The tag travels with every commit: the next owner reads from it which `start` the offset descends
                // from, and under which deployed commit.
                if let Some(tag) = tags.get(&(tp.topic.to_string(), tp.partition))
                    && let Some(mut e) = tpl.find_partition(&tp.topic, tp.partition)
                {
                    e.set_metadata(tag.clone());
                }
            }
            // `Ok` here means handed over, not landed; only the callback's answer counts as landed. Nothing left to
            // commit sends nothing: an empty list is an error to the client.
            if !kept.is_empty()
                && let Err(e) = self.commit_async(&tpl)
                && let Some(e) = self.settle_commit(Err(e))
            {
                refused = Some(e);
            }
        }
        match refused {
            Some(e) => Err(e.into()),
            None => self.commit_verdict.clone().map_err(IoError::from),
        }
    }

    /// Why this source reads nothing, if another source reads one of its topics in its group; otherwise the client's
    /// most recent connectivity error (E4), `None` once a fetch or an assignment has proven it reachable since.
    fn connection_error(&self) -> Option<String> {
        self.slot
            .lock()
            .refused
            .clone()
            .or_else(|| self.client.routes.last_error())
    }
}

/// Per-batch delivery accounting for the producer. `ProducerContext`'s default `delivery` callback is a no-op and
/// `Producer::flush` returns once the queue is empty whether or not each message arrived; the runtime commits only once
/// a batch is acknowledged (design doc, Runtime stages), so failures are recorded here.
#[derive(Default)]
struct Completion {
    /// Unreported records per send-sequence. Keyed by sequence rather than batch id: records are counted as they are
    /// queued, before the hand-over is sealed under its batch.
    outstanding: HashMap<u64, u64>,
    /// Sealed sequences and their batch ids. A sequence not here is still accumulating and must not be reported
    /// complete.
    sealed: HashMap<u64, BatchId>,
    /// What each sequence's records that did not land said.
    failed: HashMap<u64, Failed>,
    /// Sequences a hand-over that failed part way left records queued under. Its batch goes again whole, under a
    /// sequence of its own, so what is reported for these answers for nothing.
    abandoned: HashSet<u64>,
    /// Sealed and wholly reported, waiting to be returned.
    done: Vec<BatchCompletion>,
}

/// What a sequence's records that did not land said.
#[derive(Default)]
struct Failed {
    /// The rows that may land if sent again, under the first such failure.
    undelivered: Option<Undelivered>,
    /// The records the broker refuses however often they are sent, for the destination's answer to `Refused`.
    refused: Vec<Rejection>,
}

/// Why a record did not land.
enum Failure {
    /// It may land if sent again.
    Undelivered(String),
    /// It never will, as sent (`is_rejection`).
    Refused(String),
}

impl Completion {
    /// Row `row` of sequence `seq` was reported: landed, or `failure`.
    fn reported(&mut self, seq: u64, row: usize, failure: Option<Failure>) {
        if let Some(failure) = failure.filter(|_| !self.abandoned.contains(&seq)) {
            let failed = self.failed.entry(seq).or_default();
            match failure {
                Failure::Undelivered(why) => failed
                    .undelivered
                    .get_or_insert_with(|| Undelivered {
                        error: IoError::transient(format!("a produced message failed to deliver: {why}")),
                        rows: Vec::new(),
                    })
                    .rows
                    .push(row),
                Failure::Refused(reason) => failed.refused.push(Rejection {
                    doc: row,
                    reason,
                    record: None,
                }),
            }
        }
        if let Some(n) = self.outstanding.get_mut(&seq) {
            *n = n.saturating_sub(1);
        }
        self.settle(seq);
    }

    /// A sequence completes once it is sealed and has nothing outstanding; either can happen last. One abandoned is
    /// forgotten once nothing of it is outstanding.
    fn settle(&mut self, seq: u64) {
        if self.outstanding.get(&seq).is_some_and(|n| *n > 0) {
            return;
        }
        if self.abandoned.remove(&seq) {
            self.outstanding.remove(&seq);
            return;
        }
        if let Some(batch) = self.sealed.remove(&seq) {
            self.outstanding.remove(&seq);
            let failed = self.failed.remove(&seq).unwrap_or_default();
            self.done.push(BatchCompletion {
                batch,
                rejections: failed.refused,
                throttled: false,
                undelivered: failed.undelivered,
            });
        }
    }

    /// The hand-over under `seq` failed part way, and its batch goes again whole.
    fn abandon(&mut self, seq: u64) {
        self.failed.remove(&seq);
        self.abandoned.insert(seq);
        self.settle(seq);
    }
}

/// How often librdkafka reports its own statistics.
///
/// A window is reset every time it is reported (`rd_avg_rollover`), so the interval costs no throttle time -- a longer
/// one reports the same total in fewer, larger pieces. What it does decide is how sharply the queue gauge tracks a
/// destination filling up, which is why this is close to the heartbeat rather than far above it.
const STATS_INTERVAL: Duration = Duration::from_secs(2);

/// Publish what librdkafka reports about this producer.
///
/// Summed across brokers and partitions rather than labeled by either. A `broker` label would multiply every series
/// by the size of the cluster, which is not this topology's to bound, and the question an operator asks is whether
/// this destination is being held back -- not by which node.
fn publish_stats(stats: &Statistics, destination: &str, metrics: &MetricsHandle) {
    // Broker-enforced quota throttling (KIP-13): the time brokers delayed handling this producer's requests. It is
    // Kafka's whole backpressure signal, and it reaches a client as latency rather than as a refusal.
    //
    // A window covers the interval since it was last reported, so its `sum` is a delta and accumulates. It is in
    // milliseconds.
    let throttled: i64 = stats
        .brokers
        .values()
        .filter_map(|b| b.throttle.as_ref())
        .map(|w| w.sum)
        .sum();
    metrics.incr_seconds(
        "smeltr_destination_throttled_seconds_total",
        &[("destination", destination)],
        throttled.max(0) as f64 / 1e3,
    );

    // What `bufferBytes` bounds, as librdkafka currently holds it: waiting for a partition, and ready to transmit.
    // Partition -1 is the internal unassigned queue, which a message leaves for a real partition, so nothing is
    // counted twice.
    let buffered: u64 = stats
        .topics
        .values()
        .flat_map(|t| t.partitions.values())
        .map(|p| p.msgq_bytes + p.xmit_msgq_bytes)
        .sum();
    metrics.gauge(
        "smeltr_destination_buffered_bytes",
        &[("destination", destination)],
        buffered as f64,
    );

    // The link as the producer sees it: the mean round trip over the brokers that answered a request this interval. What `bufferBytes` reaches over it is bytes in flight over this figure, and the leader draws
    // that beside the queue.
    let answered: Vec<i64> = stats
        .brokers
        .values()
        .filter_map(|b| b.rtt.as_ref())
        .filter(|w| w.cnt > 0)
        .map(|w| w.avg)
        .collect();
    if !answered.is_empty() {
        let micros = answered.iter().sum::<i64>() as f64 / answered.len() as f64;
        metrics.gauge(
            "smeltr_destination_rtt_seconds",
            &[("destination", destination)],
            micros / 1e6,
        );
    }
}

#[derive(Clone)]
struct DeliveryTracker {
    state: Arc<Mutex<Completion>>,
    metrics: MetricsHandle,
    /// This destination's own label, as every `smeltr_destination_*` series carries it.
    destination: String,
    reach: Arc<Mutex<Reach>>,
    /// Whether this producer sends gzip to Event Hubs, whose Basic and Standard tiers refuse it.
    gzip_to_event_hubs: bool,
    /// Bytes the queued records cost, by `held_bytes`. Written by `produce` and the delivery reports, both on egress's
    /// thread, since only its polls run them.
    held: Arc<std::sync::atomic::AtomicUsize>,
}

/// Whether the producer reaches a broker, as its callbacks tell it.
///
/// A connection error alone does not make a destination unreachable: one broker of several can be down while the rest
/// take every record. So an error counts only while the latest statistics show no broker up, and the next statistics
/// that show one clear it. Statistics come every `STATS_INTERVAL`, sent or not, so an idle destination recovers too.
#[derive(Default)]
struct Reach {
    /// The latest connection error since a broker was last seen up.
    error: Option<String>,
    /// Whether the latest statistics showed a broker up. `false` until the first arrive.
    up: bool,
}

impl Reach {
    /// What `KafkaDestination::connection_error` reads.
    fn unreachable(&self) -> Option<String> {
        if self.up { None } else { self.error.clone() }
    }
}

/// The global errors that say the producer could not reach a broker. Every other global error is about something
/// once connected, and reaches egress through a delivery report.
fn is_connection_error(error: &KafkaError) -> bool {
    matches!(
        error.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::AllBrokersDown
                | RDKafkaErrorCode::BrokerTransportFailure
                | RDKafkaErrorCode::Resolve
                | RDKafkaErrorCode::SSL
                | RDKafkaErrorCode::Authentication
        )
    )
}

/// Whether a snapshot shows a broker this producer is connected to. The client's own internal and logical brokers
/// are left out: they report a state without ever touching the network.
fn any_broker_up(stats: &Statistics) -> bool {
    stats
        .brokers
        .values()
        .any(|b| matches!(b.source.as_str(), "configured" | "learned") && b.state == "UP")
}

impl ClientContext for DeliveryTracker {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(&self, config: Option<&str>) -> Result<OAuthToken, Box<dyn std::error::Error>> {
        entra::token(config)
    }

    /// Runs on librdkafka's own thread, not egress's. `MetricsHandle` is two `Arc`s over a locked registry, so both
    /// may write.
    fn stats(&self, stats: Statistics) {
        publish_stats(&stats, &self.destination, &self.metrics);
        let mut reach = self.reach.lock().unwrap();
        reach.up = any_broker_up(&stats);
        if reach.up {
            reach.error = None;
        }
    }

    /// Seen within seconds, where a record sent to an unreachable broker fails only once `message.timeout.ms` runs
    /// out. The most recent error wins, as on a Kafka source's client (`client::Routes`).
    fn error(&self, error: KafkaError, reason: &str) {
        if is_connection_error(&error) {
            self.reach.lock().unwrap().error = Some(format!("{error}: {reason}"));
        }
    }
}

impl ProducerContext for DeliveryTracker {
    type DeliveryOpaque = Box<Sent>;

    fn delivery(&self, result: &DeliveryResult<'_>, sent: Self::DeliveryOpaque) {
        self.held
            .fetch_sub(sent.held as usize, std::sync::atomic::Ordering::Relaxed);
        let failure = result.as_ref().err().map(|(e, _)| {
            let why = match e {
                KafkaError::MessageProduction(RDKafkaErrorCode::UnsupportedForMessageFormat)
                    if self.gzip_to_event_hubs =>
                {
                    format!("{e}. The namespace does not take gzip; set compression to none")
                }
                _ => e.to_string(),
            };
            if is_rejection(e) {
                Failure::Refused(why)
            } else {
                Failure::Undelivered(why)
            }
        });
        self.state
            .lock()
            .unwrap()
            .reported(sent.seq, sent.row as usize, failure);
    }
}

/// A Kafka producer.
///
/// `BaseProducer`, not `ThreadedProducer`: the threaded one's background poll races `flush`'s for the same delivery
/// events. Nothing polls this producer except `deliver`, `poll_completions` and `flush`.
pub struct KafkaDestination {
    producer: BaseProducer<DeliveryTracker>,
    default_topic: Arc<str>,
    /// Budget for one `flush`, and for one record blocked on a full queue.
    stall_after: Duration,
    /// Shared with the producer's own `DeliveryTracker`.
    state: Arc<Mutex<Completion>>,
    /// Shared with the producer's own `DeliveryTracker`.
    reach: Arc<Mutex<Reach>>,
    /// The send-sequence records are tagged with. Bumped once a hand-over is sealed, so every record of one batch
    /// shares one tag.
    seq: u64,
    /// How its rows are written, where they are written as Avro.
    avro: Option<crate::io::avro::AvroWriter>,
    /// Shared with the producer's own `DeliveryTracker`.
    held: Arc<std::sync::atomic::AtomicUsize>,
    /// What `held` stays under: the producer's share of `bufferBytes`, or of the most its queue may hold when the
    /// worker sizes this destination. The egress queue in front of it is what paces it; this is the backstop.
    budget: usize,
}

/// Bytes one consumer fetch may carry: half of `fetchBytes`, so a fetch always lands with room behind it.
fn fetch_request_bytes(fetch_bytes: usize) -> usize {
    (fetch_bytes / 2).max(1)
}

/// librdkafka's own `receive.message.max.bytes`.
const DEFAULT_RECEIVE_MESSAGE_BYTES: usize = 100_000_000;

/// The framing librdkafka wants between a fetch and the message that carries it.
const RECEIVE_FRAMING_BYTES: usize = 512;

/// What one received message may be: librdkafka's default, or the fetch plus its framing when the fetch is larger.
fn receive_message_bytes(fetch_bytes: usize) -> usize {
    (fetch_request_bytes(fetch_bytes) + RECEIVE_FRAMING_BYTES).max(DEFAULT_RECEIVE_MESSAGE_BYTES)
}

/// The Kafka message ceiling: the topic's effective `max.message.bytes`, asked before the producer is built, with
/// where it came from. What the topic set, else what the broker gives every topic: one DescribeConfigs on the topic
/// answers both, since the broker fills in its default where the topic set nothing, and says which it was. `None`
/// when it is not readable: an ACL that refuses Describe, a broker too old for the call, or no answer in time.
fn message_ceiling(base: &ClientConfig, topic: &str) -> Option<(usize, &'static str)> {
    let admin: AdminClient<DefaultClientContext> = base.create().ok()?;
    entra::sign_in_unpolled(admin.inner(), base.get("sasl.oauthbearer.config"));
    let wanted = [ResourceSpecifier::Topic(topic)];
    let options = AdminOptions::new().request_timeout(Some(MESSAGE_CEILING_TIMEOUT));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .ok()?;
    let answers = runtime.block_on(admin.describe_configs(wanted.iter(), &options)).ok()?;
    answers.into_iter().flatten().find_map(|resource| {
        resource.entries.iter().find_map(|entry| {
            if entry.name != "max.message.bytes" {
                return None;
            }
            let value = entry.value.as_deref()?.parse::<usize>().ok()?;
            let from = match entry.source {
                rdkafka::admin::ConfigSource::DynamicTopic => "topic",
                _ => "broker",
            };
            Some((value, from))
        })
    })
}

/// The `message.max.bytes` and `batch.size` for a broker that takes requests up to `ceiling`: a record as large as it
/// takes, and a batch no larger than librdkafka's own. Both are brought inside the range librdkafka accepts, since a
/// producer outside it never builds.
fn request_sizes(ceiling: usize) -> (usize, usize) {
    let message = ceiling.clamp(MIN_MESSAGE_BYTES, MAX_MESSAGE_BYTES);
    (message, message.min(DEFAULT_BATCH_BYTES))
}

impl KafkaDestination {
    pub fn new(
        config: &KafkaDestinationConfig,
        id: DestId,
        metrics: MetricsHandle,
        secrets: &HeldSecrets,
    ) -> Result<KafkaDestination, IoError> {
        let tracker = DeliveryTracker {
            state: Arc::new(Mutex::new(Completion::default())),
            metrics,
            destination: id.to_string(),
            reach: Arc::default(),
            gzip_to_event_hubs: config.service == crate::plan::topology::KafkaService::EventHubs
                && config.compression == crate::plan::topology::Compression::Gzip,
            held: Arc::default(),
        };
        let held = tracker.held.clone();
        let state = tracker.state.clone();
        let reach = tracker.reach.clone();
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("bootstrap.servers", &config.brokers)
            // Not idempotent: an idempotent producer keeps at most five requests in flight and, at librdkafka 2.12
            // to 2.16, sends about one batch a round trip while its broker thread spins a core, so a link 200ms away
            // carries a fifth of what it can. Without it a retried produce may duplicate a record, as a batch
            // smeltr sends again already may (at-least-once), or reorder it against its neighbors.
            .set("acks", config.acks.as_property())
            // `produce` bounds the queue at the producer's share of `bufferBytes`, or at the pacing target, which
            // never passes the share it is built with; it counts what librdkafka holds for each record besides its
            // payload. This only stops librdkafka's own 1 GB default from being the bound
            // should that count ever fall short. The egress queue in front of this one holds the other half of
            // `bufferBytes` and counts rows a little differently, so a full producer queue is reachable from egress:
            // `deliver` polls it down, and a queue that stays full is the stall it already reports.
            .set(
                "queue.buffering.max.kbytes",
                (config.producer_bytes() / 1024).to_string(),
            )
            // `produce` bounds the queue by what its records hold, so librdkafka's count, 100,000 by default, cannot
            // hold a far link to fewer records a round trip than the buffer has room for.
            .set("queue.buffering.max.messages", i32::MAX.to_string())
            .set("client.id", CLIENT_ID);
        // From the destination's own field; the passthrough reserves `compression.type`.
        producer_config.set("compression.type", config.compression.as_property());
        // Broker-enforced throttling and the producer queue's depth are reported nowhere else; see `publish_stats`.
        producer_config.set("statistics.interval.ms", STATS_INTERVAL.as_millis().to_string());
        apply_client_config(&mut producer_config, &config.props, config.tls.as_ref(), secrets)?;
        // `batch.size`, `linger.ms` and `max.in.flight.requests.per.connection` stay at librdkafka's own unless the
        // operator sets them. Pacing bounds what is in flight through `produce`, and measures its round trip from
        // handover to delivery report, so a longer linger would wait inside the round trip it measures. A record may
        // be as large as the broker takes, and a broker that takes less than librdkafka's batch lowers it, since it
        // refuses a request over its ceiling. An operator's own `batch.size` or `message.max.bytes` stands. Asked
        // before the producer exists, because librdkafka fixes its sizes when a client is built, and under the
        // producer's own configuration, so a secured broker answers.
        let (ceiling, ceiling_from) =
            message_ceiling(&producer_config, &config.default_topic).unwrap_or((DEFAULT_BATCH_BYTES, "default"));
        let (message_bytes, batch_bytes) = request_sizes(ceiling);
        tracing::info!(
            target: "kafka.message_ceiling",
            destination = %id,
            topic = %config.default_topic,
            ceiling_bytes = ceiling,
            ceiling_from,
            message_bytes,
            batch_bytes,
            "sized the producer's requests for its broker"
        );
        for (key, bytes) in [("message.max.bytes", message_bytes), ("batch.size", batch_bytes)] {
            if producer_config.get(key).is_none() {
                producer_config.set(key, bytes.to_string());
            }
        }
        let avro = (config.avro.as_ref())
            .map(|r| crate::io::avro::AvroWriter::new(r, config.tls.as_ref(), secrets))
            .transpose()?;
        let producer: BaseProducer<DeliveryTracker> = producer_config.create_with_context(tracker)?;
        // The first token now, so the first delivery does not wait on a sign-in. Later ones come through its polls.
        entra::sign_in_unpolled(producer.client(), producer_config.get("sasl.oauthbearer.config"));
        Ok(KafkaDestination {
            producer,
            default_topic: Arc::from(config.default_topic.as_str()),
            stall_after: config.stall_after,
            state,
            reach,
            seq: 0,
            avro,
            held,
            budget: config.producer_bytes(),
        })
    }
}

impl KafkaDestination {
    /// Undoes the `outstanding` increment `produce` makes up front, for a record that never entered the queue.
    fn uncount(&mut self) {
        let mut state = self.state.lock().unwrap();
        if let Some(n) = state.outstanding.get_mut(&self.seq) {
            *n = n.saturating_sub(1);
        }
        let seq = self.seq;
        state.settle(seq);
    }

    /// Queue one record with librdkafka under the batch being handed over. `record.doc_id` is ignored. `key` is the
    /// partition key, and routing a document id into it would change partitioning and ordering on every Kafka
    /// destination in a deployment that only meant to turn on Elasticsearch dedupe.
    fn produce(&mut self, row: usize, out: OutgoingRecord<'_>) -> Result<(), IoError> {
        use std::sync::atomic::Ordering::Relaxed;
        let topic = out.placement.unwrap_or(&self.default_topic);
        let held = held_bytes(out.value.len(), out.key.map_or(0, <[u8]>::len));
        let sent = Box::new(Sent {
            seq: self.seq,
            held: u32::try_from(held).expect("a record is under message.max.bytes"),
            row: u32::try_from(row).expect("a batch has fewer rows than u32 counts"),
        });
        let mut record = BaseRecord::with_opaque_to(topic, sent).payload(out.value);
        if let Some(k) = out.key {
            record = record.key(k);
        }
        // Counted before the send: a delivery report can fire under the queue-full poll below, and decrementing a
        // record not yet counted would complete the batch early.
        *self.state.lock().unwrap().outstanding.entry(self.seq).or_insert(0) += 1;
        let deadline = Instant::now() + self.stall_after;
        loop {
            // A record larger than the whole budget goes alone, rather than never.
            let queued = self.held.load(Relaxed);
            if queued == 0 || queued + held <= self.budget {
                match self.producer.send(record) {
                    // Added here, not before: only this thread's polls run delivery reports.
                    Ok(()) => {
                        self.held.fetch_add(held, Relaxed);
                        return Ok(());
                    }
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), rejected)) => record = rejected,
                    // Refused synchronously, before queuing: a rejection of this record (`is_rejection`), distinct
                    // from a delivery failure reported through `DeliveryTracker`. No callback will come for it, so
                    // take the count back off.
                    Err((e, _)) => {
                        self.uncount();
                        return Err(e.into());
                    }
                }
            }
            // A full queue is ordinarily momentary backpressure: poll to drain, since `BaseProducer` has no thread of
            // its own. Bounded, because a queue full for the whole budget means the destination is not draining, and
            // blocking would stop the consumer polling (`MAX_POLL_INTERVAL`).
            if Instant::now() >= deadline {
                self.uncount();
                return Err(IoError::transient(format!(
                    "destination {} is not draining: its local queue stayed full for {:?}",
                    self.default_topic, self.stall_after
                )));
            }
            // `poll` waits out its whole timeout, so a short one: the send is tried again soon after the queue has
            // room.
            self.producer.poll(Duration::from_millis(10));
        }
    }
}

/// Each of `rows` written as Avro, or why it is refused. Every schema is registered for its topic before any row is
/// written, so a registry that does not answer fails the delivery before anything is produced.
fn avro_values(
    avro: &mut crate::io::avro::AvroWriter,
    default_topic: &str,
    rows: &[OwnedRecord],
) -> Result<Vec<Result<Vec<u8>, String>>, IoError> {
    let topic = |row: &OwnedRecord| row.placement.as_deref().unwrap_or(default_topic).to_string();
    for row in rows.iter().filter(|r| r.unfit.is_none()) {
        if let Some(columns) = &row.columns {
            avro.register(&topic(row), columns.chunk.batch().schema_ref())?;
        }
    }
    let mut encoder = avro.encoder();
    Ok(rows
        .iter()
        .map(|row| {
            let Some(columns) = (row.unfit.is_none()).then_some(row.columns.as_ref()).flatten() else {
                return Err("the row has no columns to write as Avro".to_string());
            };
            if let Some(why) = columns.chunk.unfit(columns.row) {
                return Err(why.to_string());
            }
            encoder.encode(&topic(row), columns.chunk.batch(), columns.row as usize)
        })
        .collect())
}

impl Destination for KafkaDestination {
    fn deliver(
        &mut self,
        batch: BatchId,
        rows: Cow<'_, [OwnedRecord]>,
    ) -> Result<(Dispatched, Vec<Rejection>), IoError> {
        let mut rejections = Vec::new();
        let encoded = match &mut self.avro {
            Some(avro) => Some(avro_values(avro, &self.default_topic, &rows)?),
            None => None,
        };
        for (i, row) in rows.iter().enumerate() {
            if let Some(why) = &row.unfit {
                rejections.push(Rejection::of(&rows, i, why.clone()));
                continue;
            }
            let mut out = row.as_outgoing();
            if let Some(encoded) = &encoded {
                match &encoded[i] {
                    Ok(value) => out.value = value,
                    Err(why) => {
                        rejections.push(Rejection::of(&rows, i, why.clone()));
                        continue;
                    }
                }
            }
            match self.produce(i, out) {
                Ok(()) => {}
                // Refused before queuing: this record is refused, and the batch goes on without it.
                Err(e) if e.is_rejection() => rejections.push(Rejection::of(&rows, i, e.message)),
                // The batch goes again whole, so what is already queued of it answers for nothing.
                Err(e) => {
                    self.state.lock().unwrap().abandon(self.seq);
                    self.seq += 1;
                    return Err(e);
                }
            }
        }
        {
            let mut state = self.state.lock().unwrap();
            state.sealed.insert(self.seq, batch);
            // A batch acknowledged before it was sealed, or one that sent nothing, is already done.
            state.settle(self.seq);
        }
        self.seq += 1;
        self.producer.poll(Duration::ZERO);
        Ok((Dispatched::InFlight, rejections))
    }

    fn flush(&mut self) -> Result<(), IoError> {
        // Bounded: a flush that never returns is a consumer that never polls (`MAX_POLL_INTERVAL`). Timing out fails
        // the tick; the batch stays uncommitted and is retried.
        //
        // Not `BaseProducer::flush`: it polls in 100ms slices and `poll` waits out each one, so every flush with
        // anything in flight would cost at least 100ms.
        let deadline = Instant::now() + self.stall_after;
        while self.producer.in_flight_count() > 0 {
            if Instant::now() >= deadline {
                return Err(KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut).into());
            }
            self.producer.poll(Duration::from_millis(1));
        }
        // The barrier form: everything sent so far is reported, so settle whatever is still waiting. A record that
        // failed comes back with its batch's completion.
        let mut state = self.state.lock().unwrap();
        state.outstanding.clear();
        state.abandoned.clear();
        let sealed: Vec<u64> = state.sealed.keys().copied().collect();
        for seq in sealed {
            state.settle(seq);
        }
        Ok(())
    }

    fn poll_completions(&mut self) -> Result<Vec<BatchCompletion>, IoError> {
        self.producer.poll(Duration::ZERO);
        Ok(std::mem::take(&mut self.state.lock().unwrap().done))
    }

    /// Set by the producer's own error callback, which `poll_completions` runs.
    fn connection_error(&self) -> Option<String> {
        self.reach.lock().unwrap().unreachable()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::topology::StartPosition;
    use rdkafka::consumer::ConsumerContext;

    /// The reason a save gives for refusing `max.poll.interval.ms` names this interval in minutes; it is text, so
    /// nothing else ties the two.
    #[test]
    fn the_reserved_poll_interval_reason_names_the_interval() {
        let (_, why) = crate::config_file::RESERVED_KAFKA_PROPS
            .iter()
            .find(|(key, _)| *key == "max.poll.interval.ms")
            .expect("reserved");
        let minutes = MAX_POLL_INTERVAL.as_secs() / 60;
        assert!(why.starts_with(&format!("fixed at {minutes} minutes,")), "{why}");
    }

    /// A commit names only the partitions this consumer still owns. One the group moved away is left out, and
    /// counted: the broker would take it from any current member, and it would move the new owner's offset back.
    #[test]
    fn a_commit_leaves_out_the_partitions_this_consumer_no_longer_owns() {
        let tp = |topic: &str, partition: i32| TopicPartition::new(Arc::from(topic), partition);
        let offsets: CommitMap = [(tp("a", 0), 10), (tp("a", 1), 20), (tp("b", 0), 30)]
            .into_iter()
            .collect();
        let owned: HashSet<(String, i32)> = [("a".to_string(), 1), ("b".to_string(), 0)].into_iter().collect();

        let (mut kept, dropped) = owned_only(&offsets, &owned);
        kept.sort_by(|x, y| (x.0.topic.as_ref(), x.0.partition).cmp(&(y.0.topic.as_ref(), y.0.partition)));
        assert_eq!(kept, vec![(tp("a", 1), 20), (tp("b", 0), 30)]);
        assert_eq!(dropped, 1, "a/0 moved away");
    }

    /// A record may be as large as the broker takes, and a batch is librdkafka's own unless the broker takes less. A
    /// ceiling outside what librdkafka accepts at all is brought inside it.
    #[test]
    fn a_record_is_as_large_as_the_broker_takes_and_a_batch_no_larger_than_librdkafkas() {
        assert_eq!(request_sizes(8 << 20), (8 << 20, DEFAULT_BATCH_BYTES));
        assert_eq!(
            request_sizes(DEFAULT_BATCH_BYTES),
            (DEFAULT_BATCH_BYTES, DEFAULT_BATCH_BYTES)
        );
        assert_eq!(request_sizes(500_000), (500_000, 500_000));
        assert_eq!(request_sizes(0), (MIN_MESSAGE_BYTES, MIN_MESSAGE_BYTES));
        assert_eq!(request_sizes(usize::MAX), (MAX_MESSAGE_BYTES, DEFAULT_BATCH_BYTES));
    }

    /// What a queued record costs is no more than `held_bytes` counts it at: librdkafka's own share of a record, and
    /// its delivery tag, measured as the resident memory a queue of one-byte records takes. Queued for a broker that is
    /// never reached, so every record stays queued. A librdkafka that holds more per record fails it. Linux only, and
    /// read from `/proc/self/status`, which is this test's alone because nextest runs each test in a process of its
    /// own, as `tests/adversarial-memory-tests.rs` does.
    #[cfg(target_os = "linux")]
    #[test]
    fn per_message_overhead() {
        const RECORDS: usize = 500_000;
        fn resident() -> usize {
            let status = std::fs::read_to_string("/proc/self/status").unwrap();
            let line = status.lines().find(|l| l.starts_with("VmRSS:")).unwrap();
            line.split_whitespace().nth(1).unwrap().parse::<usize>().unwrap() * 1024
        }
        let tracker = DeliveryTracker {
            state: Arc::default(),
            metrics: MetricsHandle::new(Arc::new(crate::metrics::Metrics::new()), "test"),
            destination: "test".into(),
            reach: Arc::default(),
            gzip_to_event_hubs: false,
            held: Arc::default(),
        };
        let producer: BaseProducer<DeliveryTracker> = ClientConfig::new()
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("queue.buffering.max.messages", i32::MAX.to_string())
            .set("queue.buffering.max.kbytes", i32::MAX.to_string())
            .set("message.timeout.ms", "600000")
            .create_with_context(tracker)
            .unwrap();
        let before = resident();
        for seq in 0..RECORDS {
            let sent = Box::new(Sent {
                seq: seq as u64,
                held: held_bytes(1, 0) as u32,
                row: 0,
            });
            let record = BaseRecord::<(), [u8], _>::with_opaque_to("t", sent).payload(b"x");
            producer.send(record).map_err(|(e, _)| e).unwrap();
        }
        let per_record = (resident() - before) / RECORDS;
        assert!(
            per_record <= held_bytes(1, 0),
            "a queued one-byte record holds {per_record} bytes, where it is counted at {}",
            held_bytes(1, 0)
        );
        eprintln!("a queued one-byte record holds {per_record} bytes");
    }

    #[test]
    fn a_fetch_carries_half_the_queue_and_the_message_that_carries_it_is_never_smaller() {
        assert_eq!(fetch_request_bytes(64 << 20), 32 << 20);
        assert_eq!(fetch_request_bytes(1), 1);
        assert_eq!(receive_message_bytes(64 << 20), DEFAULT_RECEIVE_MESSAGE_BYTES);
        assert_eq!(receive_message_bytes(512 << 20), (256 << 20) + RECEIVE_FRAMING_BYTES);
    }

    /// The largest `fetchBytes` the loader allows builds a consumer: librdkafka accepts the fetch beside the message
    /// size that carries it.
    #[test]
    fn the_largest_fetch_buffer_still_builds_a_consumer() {
        let mut config = ClientConfig::new();
        config
            .set("bootstrap.servers", "127.0.0.1:1")
            .set("group.id", "g")
            .set("fetch.max.bytes", fetch_request_bytes(512 << 20).to_string())
            .set("fetch.message.max.bytes", fetch_request_bytes(512 << 20).to_string())
            .set(
                "receive.message.max.bytes",
                receive_message_bytes(512 << 20).to_string(),
            );
        config.create_native_config().expect("librdkafka accepts the pair");
    }

    /// The arithmetic, against a payload rather than a broker: a real quota throttle needs a cluster enforcing one,
    /// and what can go wrong here is the summing.
    #[test]
    fn stats_are_summed_across_brokers_and_partitions() {
        use crate::metrics::{MetricValue, Metrics};

        let broker = |throttle_sum: i64| rdkafka::statistics::Broker {
            throttle: Some(rdkafka::statistics::Window {
                sum: throttle_sum,
                ..Default::default()
            }),
            // Microseconds, averaged over the requests this interval; `cnt` says whether any went.
            rtt: Some(rdkafka::statistics::Window {
                avg: if throttle_sum == 30 { 120_000 } else { 80_000 },
                cnt: if throttle_sum == 30 { 4 } else { 0 },
                ..Default::default()
            }),
            ..Default::default()
        };
        let partition = |msgq: u64, xmit: u64| rdkafka::statistics::Partition {
            msgq_bytes: msgq,
            xmit_msgq_bytes: xmit,
            ..Default::default()
        };
        let topic = |partitions: Vec<(i32, rdkafka::statistics::Partition)>| rdkafka::statistics::Topic {
            partitions: partitions.into_iter().collect(),
            ..Default::default()
        };

        let stats = Statistics {
            brokers: [("b1".to_string(), broker(30)), ("b2".to_string(), broker(12))]
                .into_iter()
                .collect(),
            topics: [
                (
                    "t".to_string(),
                    topic(vec![(0, partition(100, 5)), (1, partition(20, 0))]),
                ),
                // The internal unassigned queue, which a message leaves for a real partition.
                ("u".to_string(), topic(vec![(-1, partition(7, 0))])),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };

        let registry = Arc::new(Metrics::new());
        let metrics = MetricsHandle::new(registry.clone(), "egress-3");
        publish_stats(&stats, "3", &metrics);

        let find = |name: &str| {
            registry
                .snapshot()
                .into_iter()
                .find(|s| s.key.name == name)
                .map(|s| s.value)
        };
        assert!(
            matches!(find("smeltr_destination_throttled_seconds_total"), Some(MetricValue::FloatCounter(v)) if v == 0.042),
            "every broker's throttle time counts: {:?}",
            find("smeltr_destination_throttled_seconds_total")
        );
        assert!(
            matches!(find("smeltr_destination_buffered_bytes"), Some(MetricValue::Gauge(v)) if v == 132.0),
            "queued plus ready to transmit, every partition: {:?}",
            find("smeltr_destination_buffered_bytes")
        );

        assert!(
            matches!(find("smeltr_destination_rtt_seconds"), Some(MetricValue::Gauge(v)) if v == 0.12),
            "the mean over the brokers that answered a request, in seconds; b2 sent none: {:?}",
            find("smeltr_destination_rtt_seconds")
        );

        // A second report accumulates the counter and replaces the gauge.
        publish_stats(&stats, "3", &metrics);
        assert!(
            matches!(find("smeltr_destination_throttled_seconds_total"), Some(MetricValue::FloatCounter(v)) if v == 0.084),
            "a window is a delta, so throttle time accumulates"
        );
        assert!(
            matches!(find("smeltr_destination_buffered_bytes"), Some(MetricValue::Gauge(v)) if v == 132.0),
            "the queue is a level, not a total"
        );
    }

    /// A single-broker mock cluster (`rdkafka::mocking`, no Docker needed) with `topic` already created, so a
    /// source that subscribes to it can rebalance and be assigned the moment the broker is reachable.
    fn mock_cluster_with_topic(
        topic: &str,
    ) -> rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext> {
        let mock = rdkafka::mocking::MockCluster::new(1).unwrap();
        mock.create_topic(topic, 1, 1).unwrap();
        mock
    }

    /// Polls `source` until `connection_error()` matches `want_present`, or `budget` runs out -- whichever it
    /// settles at is the return value, so a timeout reads as a clear assertion failure rather than a hang.
    fn poll_until_connection_error(source: &mut KafkaSource, want_present: bool, budget: Duration) -> Option<String> {
        let deadline = Instant::now() + budget;
        loop {
            let _ = source.poll_batch(10, 1_000_000, Duration::from_millis(300));
            let current = source.connection_error();
            if current.is_some() == want_present || Instant::now() >= deadline {
                return current;
            }
        }
    }

    /// The recovery half of E4, end to end through the real `poll_batch`/`post_rebalance` path rather than the
    /// tracker in isolation (`only_a_completed_assignment_clears_the_recorded_error` already covers that unit):
    /// a mock broker taken down is captured as a connectivity error, and brought back up is what must clear it --
    /// the requirement most likely to be got wrong, so it gets the most faithful test available
    /// without a live fleet.
    #[test]
    fn connection_error_clears_once_the_broker_comes_back() {
        let mock = mock_cluster_with_topic("recovery-topic");
        mock.broker_down(1).unwrap();

        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: vec!["recovery-topic".to_string()],
            group_id: "recovery-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

        let reason = poll_until_connection_error(&mut source, true, Duration::from_secs(10));
        assert!(
            reason.is_some(),
            "expected a connectivity error while the mock broker is down"
        );

        mock.broker_up(1).unwrap();
        let after = poll_until_connection_error(&mut source, false, Duration::from_secs(20));
        assert_eq!(after, None, "the error must clear once the broker is reachable again");
    }

    /// A source with a start and a stop starts with its broker down, says why it reads nothing, and once the broker
    /// is back reads its range to the stop.
    #[test]
    fn a_range_starts_without_its_broker_and_reads_once_it_is_back() {
        let topic = "range-down-topic";
        let mock = mock_cluster_with_topic(topic);
        mock.broker_down(1).unwrap();

        let mut source =
            KafkaSource::new(&range_config(&mock, &[topic], "range-down-group"), &Default::default()).unwrap();
        poll_until_connection_error(&mut source, true, Duration::from_secs(10))
            .expect("a source whose broker is down must say so");
        assert!(!source.is_exhausted(), "a range not yet read is not finished");

        mock.broker_up(1).unwrap();
        produce_range(&mock, topic, 0);
        let mut read = Vec::new();
        drain_until_exhausted(&mut [&mut source], &mut read);
        assert_eq!(
            offsets_of(&read, topic, 0),
            vec![0, 1, 2, 3],
            "the range up to its stop"
        );
        assert_eq!(source.connection_error(), None);
    }

    /// A range over two topics reads each topic's range, and ends once both reach their stop.
    #[test]
    fn a_range_reads_every_topic_it_names() {
        let mock = mock_cluster_with_topic("range-a");
        mock.create_topic("range-b", 1, 1).unwrap();
        produce_range(&mock, "range-a", 0);
        produce_range(&mock, "range-b", 0);

        let mut source = KafkaSource::new(
            &range_config(&mock, &["range-a", "range-b"], "range-two-topics"),
            &Default::default(),
        )
        .unwrap();
        let mut read = Vec::new();
        drain_until_exhausted(&mut [&mut source], &mut read);
        assert_eq!(offsets_of(&read, "range-a", 0), vec![0, 1, 2, 3]);
        assert_eq!(offsets_of(&read, "range-b", 0), vec![0, 1, 2, 3]);
    }

    /// Two members of one group share a range's partitions: each partition's range is read once, by one of them.
    #[test]
    fn two_members_of_one_group_share_a_range_without_duplicates() {
        let topic = "range-shared";
        let mock = rdkafka::mocking::MockCluster::new(1).unwrap();
        mock.create_topic(topic, 2, 1).unwrap();
        produce_range(&mock, topic, 0);
        produce_range(&mock, topic, 1);

        let mut config = range_config(&mock, &[topic], "range-shared-group");
        // When both join at once, librdkafka's mock broker can refuse the second member's SyncGroup, and it holds the
        // rejoin that follows open for the session timeout less a second. Shortened, that ends within the deadline.
        config
            .props
            .literal
            .push(("session.timeout.ms".to_string(), "6000".to_string()));
        let mut first = KafkaSource::new(&config, &Default::default()).unwrap();
        let mut second = KafkaSource::new(&config, &Default::default()).unwrap();
        // Held until each owns a partition, so neither reads what the group is about to hand the other.
        first.block().unwrap();
        second.block().unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while first.assigned_partitions().len() != 1 || second.assigned_partitions().len() != 1 {
            assert!(Instant::now() < deadline, "the group never shared its two partitions");
            assert!(
                first
                    .poll_batch(10, 1 << 20, Duration::from_millis(100))
                    .unwrap()
                    .is_empty()
            );
            assert!(
                second
                    .poll_batch(10, 1 << 20, Duration::from_millis(100))
                    .unwrap()
                    .is_empty()
            );
        }
        first.release().unwrap();
        second.release().unwrap();

        let mut read = Vec::new();
        drain_until_exhausted(&mut [&mut first, &mut second], &mut read);
        assert_eq!(offsets_of(&read, topic, 0), vec![0, 1, 2, 3]);
        assert_eq!(offsets_of(&read, topic, 1), vec![0, 1, 2, 3]);
    }

    /// A partition the broker cannot place when it is handed over waits, paused, and is placed at `start` on a later
    /// poll: it does not start at the earliest record instead.
    #[test]
    fn a_partition_the_broker_cannot_place_waits_for_its_start() {
        let topic = "range-unplaced";
        let mock = mock_cluster_with_topic(topic);
        produce_range(&mock, topic, 0);
        // The hand-over's own lookup of the committed offset is the first to ask, and is refused.
        mock.request_errors(
            rdkafka::types::RDKafkaApiKey::OffsetFetch,
            &[rdkafka::types::RDKafkaRespErr::RD_KAFKA_RESP_ERR_GROUP_AUTHORIZATION_FAILED],
        );

        let mut source = KafkaSource::new(
            &range_config(&mock, &[topic], "range-unplaced-group"),
            &Default::default(),
        )
        .unwrap();
        let mut read = Vec::new();
        drain_until_exhausted(&mut [&mut source], &mut read);
        assert_eq!(
            offsets_of(&read, topic, 0),
            vec![0, 1, 2, 3],
            "the range from its start, not the earliest record"
        );
    }

    /// What `group` has committed for each of `topic`'s first `partitions`: the offset and its metadata.
    fn committed_of(
        mock: &rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        group: &str,
        topic: &str,
        partitions: i32,
    ) -> Vec<(Offset, String)> {
        let asker: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .set("group.id", group)
            .create()
            .unwrap();
        let mut query = TopicPartitionList::new();
        for p in 0..partitions {
            query.add_partition(topic, p);
        }
        let committed = asker.committed_offsets(query, Duration::from_secs(10)).unwrap();
        committed
            .elements()
            .iter()
            .map(|e| (e.offset(), e.metadata().to_string()))
            .collect()
    }

    /// Reads from `source` until it has `n` records, committing each batch, and answers them.
    fn read_and_commit(source: &mut KafkaSource, topic: &str, n: usize) -> Vec<SourceRecord> {
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut read = Vec::new();
        while read.len() < n {
            assert!(Instant::now() < deadline, "read {} of {n}", read.len());
            let batch = source.poll_batch(100, 1 << 20, Duration::from_millis(100)).unwrap();
            let mut commit = CommitMap::new();
            for r in &batch {
                commit.insert(TopicPartition::new(Arc::from(topic), r.partition), r.offset);
            }
            if !commit.is_empty() {
                source.commit(&commit).unwrap();
            }
            read.extend(batch);
        }
        read
    }

    /// Polls `source` for a while and answers what it read, which a test expects to be nothing new.
    fn read_for(source: &mut KafkaSource, how_long: Duration) -> Vec<SourceRecord> {
        let deadline = Instant::now() + how_long;
        let mut read = Vec::new();
        while Instant::now() < deadline {
            read.extend(source.poll_batch(100, 1 << 20, Duration::from_millis(100)).unwrap());
        }
        read
    }

    /// A commit from a build without tags is read on from. A `start` changed in a newer deployed commit moves every
    /// partition there once, committed at once under the new tag; a worker still on an older commit reads on from what
    /// is committed and keeps the tag, and a newer commit with the same `start` reads on under its own.
    #[test]
    fn a_changed_start_moves_every_partition_once_and_an_older_commit_never_moves_it_back() {
        let topic = "start-topic";
        let group = "start-group";
        let mock = mock_cluster_with_topic("start-unused");
        mock.create_topic(topic, 2, 1).unwrap();
        produce_to(&mock, topic, 0, 5);
        produce_to(&mock, topic, 1, 5);
        let committer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .set("group.id", group)
            .create()
            .unwrap();
        let mut read_before = TopicPartitionList::new();
        read_before.add_partition_offset(topic, 0, Offset::Offset(5)).unwrap();
        read_before.add_partition_offset(topic, 1, Offset::Offset(5)).unwrap();
        committer
            .commit(&read_before, rdkafka::consumer::CommitMode::Sync)
            .unwrap();
        drop(committer);

        // Each source here joins as the last one leaves, and the mock broker holds the rejoin open until the session
        // of the member that left times out. Shortened, each hand-over ends within the deadline.
        let mut props = KafkaProps::default();
        props
            .literal
            .push(("session.timeout.ms".to_string(), "6000".to_string()));
        let config = |start: StartPosition, deployed_at: i64| KafkaSourceConfig {
            start,
            stop: None,
            deployed_at,
            member: None,
            rack: None,
            props: props.clone(),
            ..range_config(&mock, &[topic], group)
        };
        let both = |tag: &str, at: [i64; 2]| at.map(|v| (Offset::Offset(v), tag.to_string())).to_vec();

        // Untagged: read on, and tagged from now on.
        produce_to(&mock, topic, 0, 1);
        let mut source = KafkaSource::new(&config(StartPosition::Latest, 100), &Default::default()).unwrap();
        let read = read_and_commit(&mut source, topic, 1);
        assert_eq!(offsets_of(&read, topic, 0), [5], "an untagged commit is read on from");
        drop(source);
        assert_eq!(
            committed_of(&mock, group, topic, 2)[0],
            (Offset::Offset(6), "smeltr-start=latest;at=100".to_string())
        );

        // A newer commit with another start: both partitions move there once.
        let moved = "smeltr-start=earliest;at=200";
        let mut source = KafkaSource::new(&config(StartPosition::Earliest, 200), &Default::default()).unwrap();
        let read = read_and_commit(&mut source, topic, 11);
        assert_eq!(offsets_of(&read, topic, 0), [0, 1, 2, 3, 4, 5]);
        assert_eq!(offsets_of(&read, topic, 1), [0, 1, 2, 3, 4]);
        drop(source);
        assert_eq!(committed_of(&mock, group, topic, 2), both(moved, [6, 5]));

        // An older commit, and a newer one with the same start: each reads on, the newer under its own tag.
        for (n, (start, at, tag)) in [
            (StartPosition::Latest, 150, moved),
            (StartPosition::Earliest, 300, "smeltr-start=earliest;at=300"),
        ]
        .into_iter()
        .enumerate()
        {
            produce_to(&mock, topic, 0, 1);
            let mut source = KafkaSource::new(&config(start, at), &Default::default()).unwrap();
            let read = read_and_commit(&mut source, topic, 1);
            assert_eq!(
                offsets_of(&read, topic, 0),
                [6 + n as i64],
                "{start:?} at {at} reads on"
            );
            assert!(
                read_for(&mut source, Duration::from_millis(500)).is_empty(),
                "{start:?} at {at} read again"
            );
            drop(source);
            assert_eq!(committed_of(&mock, group, topic, 2), both(tag, [7 + n as i64, 5]));
        }

        // A newer commit, to the end: committed there at once, before any record is read.
        let mut source = KafkaSource::new(&config(StartPosition::Latest, 400), &Default::default()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let latest = "smeltr-start=latest;at=400";
        while committed_of(&mock, group, topic, 2) != both(latest, [8, 5]) {
            assert!(Instant::now() < deadline, "{:?}", committed_of(&mock, group, topic, 2));
            let _ = source.poll_batch(100, 1 << 20, Duration::from_millis(100)).unwrap();
        }
        produce_to(&mock, topic, 1, 1);
        let read = read_and_commit(&mut source, topic, 1);
        assert_eq!(offsets_of(&read, topic, 1), [5], "only what came after the move");
    }

    #[test]
    fn a_tag_is_read_only_from_metadata_smeltr_wrote() {
        for start in [
            StartPosition::Earliest,
            StartPosition::Latest,
            StartPosition::Timestamp(1_791_763_200_000),
        ] {
            let tag = client::Tag::new(start, 1_791_849_600);
            assert_eq!(client::Tag::parse(&tag.metadata()), Some(tag), "{start:?}");
        }
        for other in [
            "",
            "earliest",
            "smeltr-start=",
            "smeltr-start=earliest",
            "smeltr-start=earliest;at=x",
            "smeltr-reset=1791849600000",
            "someone else's",
        ] {
            assert_eq!(client::Tag::parse(other), None, "{other:?}");
        }
    }

    /// Two sources on one shared client each read their own topic, and a block holds only its own source: the other
    /// keeps reading while the first's records wait for its release.
    #[test]
    fn sources_sharing_a_client_read_their_own_topics_and_block_alone() {
        let mock = mock_cluster_with_topic("shared-a");
        mock.create_topic("shared-b", 1, 1).unwrap();
        let clients = Arc::new(Clients::default());
        let config = |topic: &str| KafkaSourceConfig {
            topics: vec![topic.to_string()],
            start: StartPosition::Earliest,
            stop: None,
            ..range_config(&mock, &[topic], "shared-group")
        };
        let mut a = KafkaSource::open(&config("shared-a"), &Default::default(), &clients).unwrap();
        let mut b = KafkaSource::open(&config("shared-b"), &Default::default(), &clients).unwrap();
        assert!(Arc::ptr_eq(&a.client, &b.client), "one configuration, one client");

        a.block().unwrap();
        produce_to(&mock, "shared-a", 0, 3);
        produce_to(&mock, "shared-b", 0, 3);
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut read_b = Vec::new();
        while read_b.len() < 3 {
            assert!(Instant::now() < deadline, "b never read its topic");
            assert!(
                a.poll_batch(10, 1 << 20, Duration::from_millis(50)).unwrap().is_empty(),
                "a blocked source reads nothing"
            );
            read_b.extend(b.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap());
        }
        assert!(read_b.iter().all(|r| r.topic.as_ref() == "shared-b"));

        a.release().unwrap();
        let mut read_a = Vec::new();
        while read_a.len() < 3 {
            assert!(Instant::now() < deadline, "a never read its topic once released");
            read_a.extend(a.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap());
        }
        assert_eq!(offsets_of(&read_a, "shared-a", 0), vec![0, 1, 2]);
    }

    /// Offsets 0 to 3 of every partition of `topics` that `produce_range` filled: the fifth record is at the stop.
    fn range_config(
        mock: &rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        topics: &[&str],
        group: &str,
    ) -> KafkaSourceConfig {
        KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: topics.iter().map(|t| t.to_string()).collect(),
            group_id: group.to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: Some(StopPosition::Timestamp(RANGE_STOP)),
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        }
    }

    /// Polls every source in turn until each is exhausted, adding what they read to `read`.
    fn drain_until_exhausted(sources: &mut [&mut KafkaSource], read: &mut Vec<SourceRecord>) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !sources.iter().all(|s| s.is_exhausted()) {
            assert!(Instant::now() < deadline, "the range never ended");
            for source in sources.iter_mut() {
                read.extend(source.poll_batch(10, 1 << 20, Duration::from_millis(100)).unwrap());
            }
        }
    }

    /// The offsets read from one partition, in the order they were read.
    fn offsets_of(read: &[SourceRecord], topic: &str, partition: i32) -> Vec<i64> {
        read.iter()
            .filter(|r| r.topic.as_ref() == topic && r.partition == partition)
            .map(|r| r.offset)
            .collect()
    }

    /// Where `range_config` stops, in milliseconds since the Unix epoch.
    const RANGE_STOP: i64 = 1_700_000_004_000;

    /// Produces five records to one partition of `topic` on `mock`, a second apart: four before `RANGE_STOP`, and
    /// the fifth at it.
    fn produce_range(
        mock: &rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        topic: &str,
        partition: i32,
    ) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .create()
            .unwrap();
        for n in 0..5 {
            producer
                .send(
                    BaseRecord::<(), [u8]>::to(topic)
                        .partition(partition)
                        .payload(b"x")
                        .timestamp(RANGE_STOP - 4_000 + n * 1_000),
                )
                .unwrap();
        }
        producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// Produces `n` one-byte records to one partition of `topic` on `mock` and waits for every delivery.
    fn produce_to(
        mock: &rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        topic: &str,
        partition: i32,
        n: usize,
    ) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .create()
            .unwrap();
        for _ in 0..n {
            producer
                .send(BaseRecord::<(), [u8]>::to(topic).partition(partition).payload(b"x"))
                .unwrap();
        }
        producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// The producer's half: a destination whose only broker goes down says so within seconds, not once a record's
    /// `message.timeout.ms` runs out, and says it is reachable again once the broker is back. Nothing is sent, so
    /// the recovery is the statistics callback's, the only one an idle destination has.
    #[test]
    fn a_destination_reports_its_broker_down_and_back_without_sending() {
        let mock = mock_cluster_with_topic("down-topic");
        let mut dest = KafkaDestination::new(
            &KafkaDestinationConfig {
                payload: crate::plan::config::Payload::Record,
                compression: crate::plan::topology::Compression::None,
                acks: crate::plan::topology::Acks::All,
                stall_after: Duration::from_secs(30),
                brokers: mock.bootstrap_servers(),
                default_topic: "down-topic".to_string(),
                service: crate::plan::topology::KafkaService::Kafka,
                tls: None,
                buffer_bytes: 64 * 1024 * 1024,
                props: Default::default(),
                avro: None,
            },
            7,
            MetricsHandle::new(Arc::new(crate::metrics::Metrics::new()), "egress-7"),
            &Default::default(),
        )
        .unwrap();
        let mut poll_until = |want_present: bool| {
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                let _ = dest.poll_completions();
                let current = dest.connection_error();
                if current.is_some() == want_present || Instant::now() >= deadline {
                    return current;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        };
        assert_eq!(poll_until(false), None, "a reachable broker is not an error");

        mock.broker_down(1).unwrap();
        let reason = poll_until(true).expect("the broker went down and the destination never said so");
        assert!(reason.starts_with("Global error: "), "{reason}");

        mock.broker_up(1).unwrap();
        assert_eq!(
            poll_until(false),
            None,
            "the error must clear once the broker is reachable again"
        );
    }

    /// A sequence completes once, after its last record is reported, naming the rows that may land if sent again and
    /// refusing the ones that never will; one a failed hand-over abandoned answers for nothing.
    #[test]
    fn a_completion_names_the_rows_that_did_not_land() {
        let mut state = Completion::default();
        state.outstanding.insert(0, 3);
        state.reported(0, 0, None);
        state.reported(0, 1, Some(Failure::Undelivered("timed out".into())));
        state.reported(0, 2, Some(Failure::Refused("too large".into())));
        assert!(state.done.is_empty(), "not before it is sealed");
        state.sealed.insert(0, 7);
        state.settle(0);
        let done = std::mem::take(&mut state.done);
        assert_eq!(done.len(), 1);
        assert_eq!(done[0].batch, 7);
        assert_eq!(done[0].rejections.iter().map(|r| r.doc).collect::<Vec<_>>(), vec![2]);
        let undelivered = done[0].undelivered.as_ref().expect("a row did not land");
        assert_eq!(undelivered.rows, vec![1]);
        assert!(undelivered.error.message.contains("timed out"), "{}", undelivered.error);

        state.outstanding.insert(1, 2);
        state.reported(1, 0, Some(Failure::Undelivered("timed out".into())));
        state.abandon(1);
        state.reported(1, 1, None);
        state.outstanding.insert(2, 1);
        state.sealed.insert(2, 8);
        state.reported(2, 0, None);
        let done = std::mem::take(&mut state.done);
        assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![8]);
        assert!(
            done[0].undelivered.is_none(),
            "the abandoned sequence's failure is not the next one's"
        );
        assert!(state.outstanding.is_empty() && state.abandoned.is_empty() && state.failed.is_empty());
    }

    /// A record the broker failed is that batch's alone: a failure that may pass names its rows to send again, one
    /// that never will refuses them, and a batch beside it lands.
    #[test]
    fn a_failed_record_comes_back_with_its_own_batch() {
        use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
        let mock = mock_cluster_with_topic("failing-topic");
        let mut dest = KafkaDestination::new(
            &KafkaDestinationConfig {
                payload: crate::plan::config::Payload::Record,
                compression: crate::plan::topology::Compression::None,
                acks: crate::plan::topology::Acks::All,
                stall_after: Duration::from_secs(10),
                brokers: mock.bootstrap_servers(),
                default_topic: "failing-topic".to_string(),
                service: crate::plan::topology::KafkaService::Kafka,
                tls: None,
                buffer_bytes: 64 * 1024 * 1024,
                props: Default::default(),
                avro: None,
            },
            7,
            MetricsHandle::new(Arc::new(crate::metrics::Metrics::new()), "egress-7"),
            &Default::default(),
        )
        .unwrap();
        let rows =
            |n: usize| -> Vec<OwnedRecord> { (0..n).map(|_| OwnedRecord::from(&OutgoingRecord::new(b"x"))).collect() };
        let mut answer = |batch: BatchId, error: Option<RDKafkaRespErr>| {
            if let Some(error) = error {
                mock.request_errors(RDKafkaApiKey::Produce, &[error]);
            }
            dest.deliver(batch, Cow::Owned(rows(2))).unwrap();
            dest.flush().unwrap();
            let done = dest.poll_completions().expect("a failed record fails no poll");
            assert_eq!(done.iter().map(|c| c.batch).collect::<Vec<_>>(), vec![batch]);
            let c = &done[0];
            (
                c.undelivered.as_ref().map(|u| u.rows.clone()),
                c.rejections.iter().map(|r| r.doc).collect::<Vec<_>>(),
            )
        };
        assert_eq!(
            answer(1, Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_POLICY_VIOLATION)),
            (Some(vec![0, 1]), vec![]),
            "a failure that may pass names the rows to send again"
        );
        assert_eq!(
            answer(2, Some(RDKafkaRespErr::RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE)),
            (None, vec![0, 1]),
            "one that never will refuses them"
        );
        assert_eq!(answer(3, None), (None, vec![]), "and the next batch lands");
    }

    /// A producer whose broker is gone queues records only while what they hold fits its share of `bufferBytes`,
    /// counted with their overhead, then says it is not draining once `stallAfterSecs` runs out.
    #[test]
    fn the_queue_holds_no_more_than_the_buffer_counting_each_record() {
        let mock = mock_cluster_with_topic("full-topic");
        let value = [b'x'; 1000];
        let fits = 100;
        let mut dest = KafkaDestination::new(
            &KafkaDestinationConfig {
                payload: crate::plan::config::Payload::Record,
                compression: crate::plan::topology::Compression::None,
                acks: crate::plan::topology::Acks::All,
                stall_after: Duration::from_secs(1),
                brokers: mock.bootstrap_servers(),
                default_topic: "full-topic".to_string(),
                service: crate::plan::topology::KafkaService::Kafka,
                tls: None,
                buffer_bytes: 2 * fits * held_bytes(value.len(), 0),
                props: Default::default(),
                avro: None,
            },
            7,
            MetricsHandle::new(Arc::new(crate::metrics::Metrics::new()), "egress-7"),
            &Default::default(),
        )
        .unwrap();
        mock.broker_down(1).unwrap();
        let queued = (0..10 * fits)
            .take_while(|_| dest.produce(0, OutgoingRecord::new(&value)).is_ok())
            .count();
        assert_eq!(queued, fits);
        assert_eq!(
            dest.held.load(std::sync::atomic::Ordering::Relaxed),
            fits * held_bytes(value.len(), 0)
        );
        let refused = dest.produce(0, OutgoingRecord::new(&value)).unwrap_err();
        assert!(refused.message.contains("not draining"), "{}", refused.message);
    }

    /// Produces `n` one-byte records to `topic` on `mock` and waits for every delivery.
    fn produce_n(
        mock: &rdkafka::mocking::MockCluster<'static, rdkafka::producer::DefaultProducerContext>,
        topic: &str,
        n: usize,
    ) {
        let producer: BaseProducer = ClientConfig::new()
            .set("bootstrap.servers", mock.bootstrap_servers())
            .create()
            .unwrap();
        for _ in 0..n {
            producer.send(BaseRecord::<(), [u8]>::to(topic).payload(b"x")).unwrap();
        }
        producer.flush(Duration::from_secs(10)).unwrap();
    }

    /// Lag keeps updating while the source is blocked (E6). The reader stops consuming on a block, and its partitions
    /// are paused, so librdkafka fetches nothing and its own statistics would freeze the high watermark. The
    /// sampler's own broker queries do not. This runs across one full `LAG_SAMPLE_INTERVAL`, since the first
    /// sample lands before anything is committed.
    #[test]
    fn a_held_source_still_reports_fresh_lag() {
        let topic = "lag-topic";
        let mock = mock_cluster_with_topic(topic);
        produce_n(&mock, topic, 3);

        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: vec![topic.to_string()],
            group_id: "lag-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

        // Read the first three, commit only the first, then block: two read but not durable, and everything produced
        // from here on unread.
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut got = 0;
        while got < 3 && Instant::now() < deadline {
            got += source
                .poll_batch(10, 1_000_000, Duration::from_millis(300))
                .unwrap()
                .len();
        }
        assert_eq!(got, 3, "the three produced records never arrived");
        let mut commit = CommitMap::new();
        commit.insert(TopicPartition::new(Arc::from(topic), 0), 0);
        source.commit(&commit).unwrap();
        source.block().unwrap();
        produce_n(&mock, topic, 5);

        // High watermark 8, committed 1: lag 7, and it must arrive while blocked, with `poll_batch` returning nothing.
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut seen = Vec::new();
        while Instant::now() < deadline {
            let batch = source.poll_batch(10, 1_000_000, Duration::from_millis(200)).unwrap();
            assert!(batch.is_empty(), "a blocked source must not consume");
            seen.extend(source.lag());
            if seen.iter().any(|l| l.lag == 7) {
                break;
            }
        }
        let sample = seen
            .iter()
            .find(|l| l.lag == 7)
            .unwrap_or_else(|| panic!("no sample with lag 7 while blocked; saw {seen:?}"));
        assert_eq!(sample.partition.topic.as_ref(), topic);
        assert_eq!(sample.partition.partition, 0);
    }

    /// Dropping a source joins its sampler, so the consumer leaves the group when the source goes and the thread
    /// does not outlive it. Against a reachable broker the queries are quick, so the join is too.
    #[test]
    fn dropping_a_source_joins_its_lag_sampler() {
        let mock = mock_cluster_with_topic("drop-topic");
        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: vec!["drop-topic".to_string()],
            group_id: "drop-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        let _ = source.poll_batch(10, 1_000_000, Duration::from_millis(300));
        let consumer = Arc::downgrade(&source.client.consumer);

        let started = Instant::now();
        drop(source);
        assert!(
            started.elapsed() < LAG_SAMPLE_INTERVAL,
            "drop waited out the sampler's interval instead of waking it"
        );
        assert!(
            consumer.upgrade().is_none(),
            "the sampler thread still holds the consumer"
        );
    }

    /// The idle half of E4: a source whose broker was reachable the whole time, subscribed to a topic that never
    /// received a record, must never report a connectivity error -- the false positive this mechanism must not
    /// introduce. `poll_batch` returning nothing here is the same "nothing to read" every source sees between
    /// records; only a genuine `error()` callback may set the fact, and none fires.
    #[test]
    fn an_idle_but_reachable_source_reports_no_connection_error() {
        let mock = mock_cluster_with_topic("idle-topic");

        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: vec!["idle-topic".to_string()],
            group_id: "idle-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

        for _ in 0..10 {
            let batch = source.poll_batch(10, 1_000_000, Duration::from_millis(300)).unwrap();
            assert!(batch.is_empty(), "nothing was ever produced to this topic");
            assert_eq!(
                source.connection_error(),
                None,
                "an idle topic on a reachable broker must never read as a connectivity failure"
            );
        }
    }

    /// End to end against a mock cluster: librdkafka emits statistics on its own thread, and they reach the registry
    /// the destination was built with. The arithmetic is `stats_are_summed_across_brokers_and_partitions`; this is
    /// about the callback being wired at all.
    #[test]
    fn a_producer_reports_its_statistics_to_the_registry() {
        use crate::metrics::Metrics;

        let mock = mock_cluster_with_topic("stats-topic");
        let registry = Arc::new(Metrics::new());
        let mut dest = KafkaDestination::new(
            &KafkaDestinationConfig {
                payload: crate::plan::config::Payload::Record,
                compression: crate::plan::topology::Compression::None,
                acks: crate::plan::topology::Acks::All,
                stall_after: Duration::from_secs(30),
                brokers: mock.bootstrap_servers(),
                default_topic: "stats-topic".to_string(),
                service: crate::plan::topology::KafkaService::Kafka,
                tls: None,
                buffer_bytes: 64 * 1024 * 1024,
                props: Default::default(),
                avro: None,
            },
            7,
            MetricsHandle::new(registry.clone(), "egress-7"),
            &Default::default(),
        )
        .unwrap();

        let row = OwnedRecord::from(&OutgoingRecord::new(b"row"));
        dest.deliver(0, Cow::Owned(vec![row])).unwrap();

        // Statistics arrive through `poll`, which this producer only does when asked.
        let deadline = Instant::now() + STATS_INTERVAL * 4;
        let published = loop {
            let found = registry
                .snapshot()
                .into_iter()
                .any(|s| s.key.name == "smeltr_destination_buffered_bytes");
            if found || Instant::now() >= deadline {
                break found;
            }
            let _ = dest.poll_completions();
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(published, "librdkafka's statistics never reached the registry");

        let labeled = registry.snapshot().into_iter().any(|s| {
            s.key.name == "smeltr_destination_buffered_bytes"
                && s.key.labels.iter().any(|(k, v)| *k == "destination" && v == "7")
        });
        assert!(labeled, "and they carry this destination's own label");
    }

    /// A commit's answer that names no partition, so it is every source's on the client.
    fn empty_tpl() -> TopicPartitionList {
        TopicPartitionList::new()
    }

    /// A commit is answered from `poll`, and the answer is read by the next `commit`: a landed one is what the broker
    /// then reports as committed; a stale one is dropped and counted; a refusal reaches the caller one call late. The
    /// stale and refused answers are pushed by hand, since a mock broker rebalances nobody and refuses nothing.
    #[test]
    fn a_commit_is_answered_from_poll_and_read_by_the_next_commit() {
        let mock = mock_cluster_with_topic("commit-topic");
        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: mock.bootstrap_servers(),
            topics: vec!["commit-topic".to_string()],
            group_id: "commit-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();

        // Joining the group is what makes the partition this consumer's to commit.
        let deadline = Instant::now() + Duration::from_secs(20);
        while source.assigned_partitions().is_empty() {
            assert!(Instant::now() < deadline, "never assigned");
            let _ = source.poll_batch(10, 1_000_000, Duration::from_millis(200));
        }
        let mut offsets = CommitMap::new();
        offsets.insert(TopicPartition::new(Arc::from("commit-topic"), 0), 4);
        source.commit(&offsets).expect("handed to the client");

        // The answer is an event the client handles inside `poll`, and it must not end the poll early.
        let polled = Instant::now();
        let _ = source.poll_batch(10, 1_000_000, Duration::from_millis(300));
        assert!(
            polled.elapsed() >= Duration::from_millis(290),
            "a commit's answer cut the poll short: {:?}",
            polled.elapsed()
        );
        let deadline = Instant::now() + Duration::from_secs(20);
        while source.slot.lock().commit_results.is_empty() {
            assert!(Instant::now() < deadline, "the broker never answered");
            let _ = source.poll_batch(10, 1_000_000, Duration::from_millis(200));
        }
        assert_eq!(source.slot.lock().commit_results, vec![Ok(())]);
        let committed = source
            .client
            .consumer
            .committed_offsets(source.client.consumer.assignment().unwrap(), Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            committed.elements()[0].offset(),
            Offset::Offset(5),
            "the next offset to read, one past the last processed"
        );

        source.client.routes.commit_callback(
            Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::RebalanceInProgress)),
            &empty_tpl(),
        );
        // An empty map sends nothing, so no real answer can land between the injected ones.
        source
            .commit(&CommitMap::new())
            .expect("a stale commit is dropped, not failed");
        assert_eq!(source.stale_commits, 1);
        assert!(source.stale_commit_logged);

        source.client.routes.commit_callback(
            Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::GroupAuthorizationFailed)),
            &empty_tpl(),
        );
        let err = source
            .commit(&CommitMap::new())
            .expect_err("the refusal before this call is this call's answer");
        assert!(err.to_string().contains("authorization"), "{err}");
        source
            .commit(&CommitMap::new())
            .expect_err("no answer since, so the refusal stands");

        source.client.routes.commit_callback(Ok(()), &empty_tpl());
        source
            .commit(&CommitMap::new())
            .expect("nothing to commit, and the last answer landed");
        assert!(!source.stale_commit_logged, "a landed commit ends the stale burst");

        // Two answers in one window. A refusal is this call's whichever side of a landing it came, and the
        // landing is the word for the calls after.
        let refusal = || Err(KafkaError::ConsumerCommit(RDKafkaErrorCode::GroupAuthorizationFailed));
        source.client.routes.commit_callback(refusal(), &empty_tpl());
        source.client.routes.commit_callback(Ok(()), &empty_tpl());
        source
            .commit(&CommitMap::new())
            .expect_err("a refusal read beside a landing is still reported");
        source
            .commit(&CommitMap::new())
            .expect("and the landing is the word after it");
        source.client.routes.commit_callback(Ok(()), &empty_tpl());
        source.client.routes.commit_callback(refusal(), &empty_tpl());
        source
            .commit(&CommitMap::new())
            .expect_err("a refusal after a landing is the answer");
        source
            .commit(&CommitMap::new())
            .expect_err("and stands until a commit lands");
    }

    /// The defect this whole mechanism exists to fix (E4), reproduced without a live broker: `kafka-nowhere.invalid`
    /// is guaranteed by RFC 2606 never to resolve. Without the client's context (`client::Routes`) this reads as an
    /// idle topic, since `Source::connection_error`'s default answers `None` either way; the client thread swallows
    /// the same consumption error this test's sibling (`poll_batch_against_an_unreachable_broker_times_out_empty_not_hung`
    /// in `kafka-tests.rs`) polls past, so only the context -- fed from `poll`'s separate `RD_KAFKA_EVENT_ERROR`
    /// channel -- can tell the two apart.
    #[test]
    fn poll_batch_against_a_broker_that_does_not_resolve_reports_the_reason() {
        let config = KafkaSourceConfig {
            service: crate::plan::topology::KafkaService::Kafka,
            encoding: crate::format::encoding::Encoding::Auto,
            fetch_bytes: 64 * 1024 * 1024,
            brokers: "kafka-nowhere.invalid:9092".to_string(),
            topics: vec!["a-topic".to_string()],
            group_id: "a-group".to_string(),
            tls: None,
            start: StartPosition::Earliest,
            stop: None,
            deployed_at: 0,
            member: None,
            rack: None,
            props: Default::default(),
            avro: None,
        };
        let mut source = KafkaSource::new(&config, &Default::default()).unwrap();
        assert_eq!(
            source.connection_error(),
            None,
            "construction alone must not read as a connectivity failure"
        );

        let deadline = Instant::now() + Duration::from_secs(10);
        let mut reason = None;
        while reason.is_none() && Instant::now() < deadline {
            source.poll_batch(10, 1_000_000, Duration::from_millis(500)).unwrap();
            reason = source.connection_error();
        }
        let reason = reason.expect("polling an unresolvable broker for 10s never reported a connection error");
        // That it came through the global-error channel is the property, not which global error won the race.
        // librdkafka emits several as a broker goes unreachable and `error()` keeps the most recent, so the first
        // one a 500ms poll sees is a matter of timing: Linux reports `Global error: Resolve (Local: Host resolution
        // failure): ... Failed to resolve ...`, Windows the rollup `Global error: AllBrokersDown (Local: All broker
        // connections are down): 1/1 brokers are down`. Both say the broker cannot be reached, which is what E4
        // asks for; pinning either one pins a platform.
        assert!(
            reason.starts_with("Global error: "),
            "the reason should be librdkafka's own global error, as `ClientContext::error` formats it: {reason}"
        );
    }

    #[test]
    fn a_rebalance_makes_a_commit_stale_rather_than_failed() {
        for code in [
            RDKafkaErrorCode::IllegalGeneration,
            RDKafkaErrorCode::UnknownMemberId,
            RDKafkaErrorCode::RebalanceInProgress,
            RDKafkaErrorCode::UnknownTopicOrPartition,
        ] {
            assert!(
                is_stale_commit(&KafkaError::ConsumerCommit(code)),
                "{code:?} is a group or a topic that moved on, not a commit that failed"
            );
        }
    }

    #[test]
    fn a_commit_that_genuinely_failed_is_not_treated_as_stale() {
        for e in [
            KafkaError::ConsumerCommit(RDKafkaErrorCode::BrokerTransportFailure),
            KafkaError::ConsumerCommit(RDKafkaErrorCode::RequestTimedOut),
            KafkaError::ConsumerCommit(RDKafkaErrorCode::GroupAuthorizationFailed),
            // Not a commit error at all.
            KafkaError::MessageProduction(RDKafkaErrorCode::MessageSizeTooLarge),
        ] {
            assert!(!is_stale_commit(&e), "{e} should stay an error");
        }
    }

    /// Kerberos is offered on both platforms smeltr ships: Linux links Cyrus through `gssapi-vendored`, Windows selects
    /// SSPI on `_WIN32`. One assertion covers the probe and its wiring: a Linux build that lost Cyrus fails it, and so
    /// does a target that accepted the config without a provider.
    #[test]
    fn selecting_gssapi_is_accepted_exactly_where_the_build_carries_it() {
        let props = KafkaProps {
            literal: vec![("sasl.mechanism".into(), "GSSAPI".into())],
            credentials: Vec::new(),
        };
        let mut errors = Vec::new();
        validate_literal_props(&props, "test", &mut errors);
        assert_eq!(
            errors.is_empty(),
            cfg!(any(target_os = "linux", windows)),
            "this build disagrees with the platforms Cargo.toml claims: {errors:?}"
        );
    }

    /// What a form is offered and what a save accepts are the same answer, asked once. A build that carries no
    /// Kerberos provider offers four mechanisms; one that does offers five.
    #[test]
    fn the_mechanisms_offered_are_the_ones_a_save_accepts() {
        let offered = available_sasl_mechanisms();
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] {
            assert!(offered.contains(&mechanism), "{mechanism} is in every librdkafka build");
        }
        for mechanism in &offered {
            let props = KafkaProps {
                literal: vec![("sasl.mechanism".into(), (*mechanism).to_string())],
                credentials: Vec::new(),
            };
            let mut errors = Vec::new();
            validate_literal_props(&props, "test", &mut errors);
            assert!(errors.is_empty(), "offered {mechanism} and refused it: {errors:?}");
        }
        assert_eq!(
            offered.contains(&"GSSAPI"),
            cfg!(any(target_os = "linux", windows)),
            "this build disagrees with the platforms Cargo.toml claims"
        );
    }

    fn literal(pairs: &[(&str, &str)]) -> KafkaProps {
        KafkaProps {
            literal: pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            credentials: Vec::new(),
        }
    }

    /// A host naming its user-assigned identity names it in the managed identity's token request; anything else is
    /// left as the properties say.
    #[test]
    fn a_managed_identity_signs_in_as_the_identity_its_host_names() {
        let imds = [
            ("sasl.oauthbearer.metadata.authentication.type", "azure_imds"),
            (
                "sasl.oauthbearer.config",
                "query=api-version=2018-02-01&resource=https://acme",
            ),
        ];
        assert_eq!(
            managed_identity_query(&literal(&imds), Some("c-1")).as_deref(),
            Some("query=api-version=2018-02-01&resource=https://acme&client_id=c-1")
        );
        assert_eq!(managed_identity_query(&literal(&imds), None), None, "system-assigned");
        assert_eq!(
            managed_identity_query(&literal(&imds), Some(" ")),
            None,
            "system-assigned"
        );
        let named = [
            imds[0],
            ("sasl.oauthbearer.config", "query=api-version=2018-02-01&client_id=c-2"),
        ];
        assert_eq!(
            managed_identity_query(&literal(&named), Some("c-1")),
            None,
            "the properties' own"
        );
        assert_eq!(
            managed_identity_query(&literal(&imds[1..]), Some("c-1")),
            None,
            "no managed identity"
        );
    }

    /// librdkafka takes the properties an Event Hubs sign-in loads as, with what the worker adds to them: each
    /// builds a client.
    #[test]
    fn an_entra_sign_in_builds_a_client() {
        let principal = literal(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "OAUTHBEARER"),
            ("sasl.oauthbearer.method", "oidc"),
            ("sasl.oauthbearer.client.id", "c-1"),
            ("sasl.oauthbearer.client.secret", "s-1"),
            (
                "sasl.oauthbearer.token.endpoint.url",
                "https://127.0.0.1:1/t-1/oauth2/v2.0/token",
            ),
            ("sasl.oauthbearer.scope", "https://acme.servicebus.windows.net/.default"),
        ]);
        // The metadata service's own address, moved to this host so the test asks nothing of the network.
        let managed = literal(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "OAUTHBEARER"),
            ("sasl.oauthbearer.method", "oidc"),
            ("sasl.oauthbearer.metadata.authentication.type", "azure_imds"),
            (
                "sasl.oauthbearer.token.endpoint.url",
                "http://127.0.0.1:1/metadata/identity/oauth2/token",
            ),
            (
                "sasl.oauthbearer.config",
                "query=api-version=2018-02-01&resource=https://acme",
            ),
        ]);
        for props in [principal, managed] {
            let mut config = ClientConfig::new();
            config.set("bootstrap.servers", "127.0.0.1:1");
            apply_client_config(&mut config, &props, None, &HeldSecrets::default()).expect("applies");
            assert!(
                config.get("https.ca.pem").is_some(),
                "the token endpoint is trusted as a broker is"
            );
            let built: Result<BaseProducer, _> = config.create();
            assert!(built.is_ok(), "{:?}: {:?}", props.literal, built.err());
        }
    }

    /// Counts what librdkafka asks it for a token with.
    #[derive(Default)]
    struct Asked(Mutex<Vec<Option<String>>>);

    impl ClientContext for Asked {
        const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

        fn generate_oauth_token(&self, config: Option<&str>) -> Result<OAuthToken, Box<dyn std::error::Error>> {
            self.0.lock().unwrap().push(config.map(str::to_string));
            Err("asked".into())
        }
    }

    impl rdkafka::consumer::ConsumerContext for Asked {}

    #[test]
    fn a_pod_with_a_workload_identity_asks_the_clients_context_for_its_token() {
        let managed = literal(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "OAUTHBEARER"),
            ("sasl.oauthbearer.method", "oidc"),
            ("sasl.oauthbearer.metadata.authentication.type", "azure_imds"),
            (
                "sasl.oauthbearer.config",
                "query=api-version=2018-02-01&resource=https://acme.servicebus.windows.net",
            ),
        ]);
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", "127.0.0.1:1").set("group.id", "g");
        apply_client_config_on(&mut config, &managed, None, &HeldSecrets::default(), true).expect("applies");
        assert_eq!(config.get("sasl.oauthbearer.method"), None);
        assert_eq!(config.get("sasl.oauthbearer.metadata.authentication.type"), None);
        assert_eq!(config.get("https.ca.pem"), None, "librdkafka asks no token endpoint");
        let consumer: BaseConsumer<Asked> = config.create_with_context(Asked::default()).expect("builds");
        let deadline = Instant::now() + Duration::from_secs(5);
        while consumer.context().0.lock().unwrap().is_empty() && Instant::now() < deadline {
            consumer.poll(Duration::from_millis(50));
        }
        assert_eq!(
            consumer.context().0.lock().unwrap().first(),
            Some(&Some(
                "smeltr.workload.scope=https://acme.servicebus.windows.net/.default".to_string()
            ))
        );
    }
}
