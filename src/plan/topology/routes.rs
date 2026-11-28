use std::collections::{HashMap, HashSet};

use crate::core::batch::{Batch, SLOT_LIVE};
use crate::core::column::{Column, DataType};
use crate::core::ctx::Ctx;
use crate::core::error::ExceptionKind;
use crate::core::id::{DestId, SourceId};
use crate::expr::ast::Span;
use crate::expr::eval::eval;
use crate::expr::ir::{Ir, IrKind, flatten};
use crate::ops::BinOp;
use crate::plan::compile::parse;
use crate::plan::compile::{Plan, compile, ingest_fields};
use crate::plan::config::{CompileError, OnException, Payload, PipelineConfig};
use crate::plan::resolve::{ResourceBuilder, SymbolTable, resolve};

use super::*;

/// One entry in a source's ordered route list — design doc, Scope: "Multiple routes per source."
#[derive(Clone, Debug)]
pub struct RouteConfig {
    /// The author's id (`RouteDto::id`). A route has no positional id the way a source or destination does; this is
    /// what names its edge on the map and what a diff tells an edit from a delete-and-insert by.
    pub id: String,
    /// How every screen and message names the route (`config_file::route_name`).
    pub name: String,
    pub source: SourceId,
    /// `None` matches every row of its source.
    pub cond: Option<String>,
    /// `PipelineConfig::id` of the pipeline this route dispatches to.
    pub pipeline: usize,
    /// Where this route's output goes — design doc, Runtime stages: "Delivery belongs to the route". Rules are
    /// **additive, not first-match**: a row matching two rules is delivered to the union of their destinations, once each, which is
    /// what makes rule order carry no hidden meaning.
    ///
    /// Empty is legal and means the route delivers nowhere: it runs its pipeline and discards the result. Unhelpful,
    /// but a real configuration rather than an error.
    pub deliver: Vec<DeliverRule>,
    /// The answer to each exception its pipeline raises: the group's, with the route's own laid over it.
    pub on_exception: OnException,
}

/// A `DeliverRule` with its `when` compiled.
///
/// Compiled in `compile_topology` rather than `compile_route`, because a `when` resolves against the target
/// *pipeline's* output slots — which is the whole point of it, and the thing `RouteConfig::cond` cannot do (that runs
/// on the ingest batch and only ever sees the record as it arrived). `compile_route` has no pipeline in scope; `compile_topology` has
/// both.
#[derive(Clone, Debug)]
pub(crate) struct CompiledDeliverRule {
    /// `None` matches every surviving row — the catch-all.
    pub(crate) when: Option<Ir>,
    pub(crate) to: Vec<DestId>,
    /// Regex literals in `when`, compiled once. Evaluation points `Ctx::patterns` at this, exactly as `route_masks`
    /// does for `cond`.
    pub(crate) compiled_patterns: crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
}

/// One delivery rule: an optional condition over the pipeline's *output*, and the destinations a matching row goes to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeliverRule {
    /// Evaluated after the pipeline runs, so it can see computed fields — unlike `RouteConfig::cond`, which runs on the
    /// ingest batch and only ever sees the record as it arrived. `None` matches every surviving row: the catch-all, conventionally last.
    ///
    /// It also sees the route's tag field (`Plan::tag_slot`), so a tagged row is placed by a rule like any other
    /// (`compile_deliver_rule`).
    pub when: Option<String>,
    pub to: Vec<DestId>,
}

/// The sources whose `fetchBytes` and the destinations whose `bufferBytes` the bundle leaves out. Each holds its default
/// here, and a worker with a memory figure sizes it to that memory (`worker::memory::fit`).
#[derive(Clone, Debug, Default)]
pub(crate) struct Fitted {
    pub(crate) sources: std::collections::BTreeSet<SourceId>,
    pub(crate) destinations: std::collections::BTreeSet<DestId>,
}

#[derive(Clone, Debug, Default)]
pub struct TopologyConfig {
    pub sources: Vec<SourceConfig>,
    pub destinations: Vec<DestinationConfig>,
    /// What the bundle leaves for the worker to size. See [`Fitted`].
    pub(crate) fitted: Fitted,
    pub routes: Vec<RouteConfig>,
    /// The group's answer to every exception kind. See `CompiledTopology::on_exception`.
    pub on_exception: OnException,
    /// Every credential this bundle names (design doc, Secrets boundary: a bundle holds the *name* of the place a
    /// secret lives, never the value), read from the bundle at load. This is what makes "a named secret that is
    /// missing is a startup failure" checkable before a deployment is torn down: the answer depends only on the
    /// bundle, so a worker can ask it of a new commit while still running the old one.
    pub named_secrets: Vec<NamedSecret>,
    /// Every certificate this bundle names, read from the bundle at load as `named_secrets` is.
    pub named_certificates: Vec<NamedCertificate>,
}

impl TopologyConfig {
    /// Whether a row entering `source` can be `Unmatched` (design doc, Exception taxonomy): no route on it matches
    /// unconditionally.
    ///
    /// A route with no `cond` takes every row, so one makes the source's routing total. Anything narrower answers
    /// `true`, a source with no routes at all included: whether a set of conditions is exhaustive is a question about
    /// the expression language, and a wrong `false` hides a path rows are really taking.
    pub(crate) fn falls_through(&self, source: SourceId) -> bool {
        !self.routes.iter().any(|r| r.source == source && r.cond.is_none())
    }
}

/// One credential a bundle names but does not carry: where its value lives, and what needs it.
#[derive(Clone, Debug)]
pub struct NamedSecret {
    pub reference: SecretRef,
    /// What to say when it is missing, e.g. `source "in": credentials`.
    pub what: String,
}

/// One certificate a bundle names but does not carry: the entry of the leader's list, and the connector presenting
/// it. The worker runs the entry's newest version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedCertificate {
    pub name: String,
    /// What to say when it is missing, e.g. `source "in"`.
    pub(crate) what: String,
}

/// A variable set to the empty string counts as unset, matching `startup_config`: that is what an unset
/// docker-compose variable expands to, and an empty credential authenticates as nobody.
fn usable(read: Option<String>) -> Option<String> {
    read.filter(|v| !v.is_empty())
}

/// The credentials a worker can obtain: its own environment, plus what the leader has sent it. Secrets are keyed by
/// name *and* version, because a rotation in flight means holding the version that is running alongside the one a
/// bundle is about to quote. Certificates are keyed by name: a worker runs the newest version the leader has sent.
#[derive(Clone, Default)]
pub struct HeldSecrets {
    stored: std::collections::BTreeMap<(String, i64), String>,
    certificates: std::collections::BTreeMap<String, HeldCertificate>,
}

/// The newest version of a certificate entry the leader has sent: its chain, PEM, and its private key.
#[derive(Clone)]
struct HeldCertificate {
    version: i64,
    chain: String,
    key: String,
}

/// Names and versions, never values. This holds every credential the deployment uses, so one `{:?}` reaching a
/// panic message or a log line would be all of them at once -- the reason `leader::secrets::EncryptionKey` derives
/// nothing at all.
impl std::fmt::Debug for HeldSecrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeldSecrets").field("holds", &self.versions()).finish()
    }
}

impl HeldSecrets {
    /// Replaces what the leader has sent with plain values: each entry is a name, a version, and that version's value.
    pub fn set_stored(&mut self, values: impl IntoIterator<Item = (String, i64, String)>) {
        self.stored = values.into_iter().map(|(n, v, value)| ((n, v), value)).collect();
    }

    /// Every credential in `wanted` and every certificate in `presented` that this worker holds, for handing to the
    /// process that will use them. The one place a value is read back out: it crosses to a runtime on a private
    /// pipe and nowhere else, and a runtime is given what its own bundle quotes rather than everything this worker
    /// has ever been sent.
    pub(crate) fn deliveries_for(
        &self,
        wanted: &[NamedSecret],
        presented: &[NamedCertificate],
    ) -> Vec<crate::heartbeat::SecretDelivery> {
        let secrets = self
            .stored
            .iter()
            .filter(|((name, version), _)| {
                wanted.iter().any(|s| {
                    matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
                })
            })
            .map(|((name, version), value)| crate::heartbeat::SecretDelivery {
                name: name.clone(),
                version: *version,
                value: value.clone(),
                chain: None,
            });
        let certificates = self
            .certificates
            .iter()
            .filter(|(name, _)| presented.iter().any(|c| &c.name == *name))
            .map(|(name, held)| crate::heartbeat::SecretDelivery {
                name: name.clone(),
                version: held.version,
                value: held.key.clone(),
                chain: Some(held.chain.clone()),
            });
        secrets.chain(certificates).collect()
    }

    /// Adds what the leader has just sent, keeping everything else. The leader answers with what this worker lacks,
    /// so an empty delivery means nothing changed rather than that nothing is held. A delivery with a chain is a
    /// certificate, kept by name at its newest version; the names whose version this replaced are answered, since
    /// what presents them has to be built again.
    pub fn take(&mut self, delivered: impl IntoIterator<Item = crate::heartbeat::SecretDelivery>) -> Vec<String> {
        let mut replaced = Vec::new();
        for d in delivered {
            let Some(chain) = d.chain else {
                self.stored.insert((d.name, d.version), d.value);
                continue;
            };
            let version = self.certificates.get(&d.name).map(|c| c.version);
            if version.is_some_and(|v| v >= d.version) {
                continue;
            }
            if version.is_some() {
                replaced.push(d.name.clone());
            }
            self.certificates.insert(
                d.name,
                HeldCertificate {
                    version: d.version,
                    chain,
                    key: d.value,
                },
            );
        }
        replaced
    }

    /// Drops everything `wanted` and `presented` do not name -- what a worker does once a deployment has loaded, so
    /// the versions it carries are the ones something runs on rather than every one it has ever been sent.
    pub(crate) fn keep_only(&mut self, wanted: &[NamedSecret], presented: &[NamedCertificate]) {
        self.stored.retain(|(name, version), _| {
            wanted.iter().any(|s| {
                matches!(&s.reference, SecretRef::LeaderStored { name: n, version: v } if n == name && v == version)
            })
        });
        self.certificates
            .retain(|name, _| presented.iter().any(|c| &c.name == name));
    }

    /// Every `(name, version)` held, certificates included, which is what a worker tells the leader so it is sent
    /// only what it lacks.
    pub(crate) fn versions(&self) -> Vec<(String, i64)> {
        let certificates = self.certificates.iter().map(|(n, c)| (n.clone(), c.version));
        self.stored.keys().cloned().chain(certificates).collect()
    }

    /// The value, or `None` when this worker cannot obtain it. A leader-stored reference matches one version and no
    /// other: a bundle quoting version 7 runs on version 7, never on whatever arrived since.
    pub fn value(&self, reference: &SecretRef) -> Option<String> {
        match reference {
            SecretRef::WorkerLocal(var) => usable(std::env::var(var).ok()),
            SecretRef::LeaderStored { name, version } => self.stored.get(&(name.clone(), *version)).cloned(),
        }
    }

    /// The version of the certificate `name` this holder has, which is its newest.
    pub(crate) fn certificate_version(&self, name: &str) -> Option<i64> {
        self.certificates.get(name).map(|held| held.version)
    }

    /// The chain and private key of the certificate `name`, PEM both, at the newest version this worker holds.
    pub(crate) fn certificate(&self, name: &str) -> Option<(String, String)> {
        let held = self.certificates.get(name)?;
        Some((held.chain.clone(), held.key.clone()))
    }
}

/// Why `tls`'s certificate cannot be presented from the side its bundle presents it, once the worker holds it.
/// `None` when there is nothing to check yet: no certificate named, or one this holder does not have at all, which
/// is [`TopologyConfig::missing_secrets`]' concern.
///
/// A free function rather than a method, so [`crate::worker::capture`]'s cold path -- which builds one source and
/// never has a whole [`TopologyConfig`] to ask -- can run the same check on the one it has.
pub(crate) fn certificate_error(tls: &Tls, held: &HeldSecrets) -> Option<String> {
    let (chain, _) = held.certificate(tls.certificate.as_ref()?)?;
    crate::io::trust::refuse_unless_usable(&chain, tls.side)
        .err()
        .map(|why| {
            let verb = if tls.side == crate::io::trust::Side::Server {
                "serves"
            } else {
                "presents"
            };
            format!("{} {verb} a certificate {why}", tls.of)
        })
}

impl TopologyConfig {
    /// The TLS every connector of this bundle speaks, where it speaks any of its own.
    fn tls(&self) -> impl Iterator<Item = &Tls> {
        let sources = self.sources.iter().filter_map(|s| s.kind.tls());
        let destinations = self.destinations.iter().filter_map(|d| d.kind.tls());
        sources.chain(destinations)
    }

    /// The named credentials and certificates this worker cannot obtain, as ready-to-print lines. Empty means the
    /// bundle can be deployed here.
    pub(crate) fn missing_secrets(&self, held: &HeldSecrets) -> Vec<String> {
        let secrets = self
            .named_secrets
            .iter()
            .filter(|s| held.value(&s.reference).is_none())
            .map(|s| {
                format!(
                    "{} names {}, which this worker does not have",
                    s.what,
                    s.reference.describe()
                )
            });
        let certificates = self
            .named_certificates
            .iter()
            .filter(|c| held.certificate(&c.name).is_none())
            .map(|c| {
                format!(
                    "{} names the certificate {:?}, which this worker does not have",
                    c.what, c.name
                )
            });
        secrets.chain(certificates).collect()
    }

    /// Every connector whose certificate cannot be presented the way it presents it, among the certificates this
    /// worker holds. Checked wherever a commit is about to run, so a certificate a far end would refuse is refused
    /// before anything is torn down rather than discovered at the handshake, after it already has been.
    pub(crate) fn certificate_errors(&self, held: &HeldSecrets) -> Vec<String> {
        self.tls().filter_map(|tls| certificate_error(tls, held)).collect()
    }
}

// -- compiling ----------------------------------------------------------

/// A `RouteConfig` with its condition (if any) resolved to `Ir` — compiled against a minimal, ingest-fields-only
/// `SymbolTable` (`plan::ingest_fields()` gives exactly `__live`/`__source.offset`/`_raw`), not any target pipeline's
/// declared fields. Routing has to decide which pipeline (and therefore which parser, which declared fields)
/// applies before any of that exists.
#[derive(Clone, Debug)]
pub struct CompiledRoute {
    /// `RouteConfig::id`, carried through compiling so a caller holding only the compiled form can still name the
    /// route -- the map, and route preview's own report of which route a row matched.
    pub(crate) id: String,
    /// `RouteConfig::name`.
    pub(crate) name: String,
    pub(crate) source: SourceId,
    pub(crate) cond: Option<Ir>,
    pub(crate) pipeline: usize,
    /// This route's own delivery rules, destinations resolved to `DestId`s and each `when` compiled against the target
    /// pipeline's *output*.
    pub(crate) deliver: Vec<CompiledDeliverRule>,
    /// Every regex literal `cond` interned, compiled once here — this route's own counterpart to
    /// `Plan::compiled_patterns`. A route condition resolves against its own disjoint `ResourceBuilder` (see this
    /// struct's doc), so it cannot share a target pipeline's `compiled_patterns`; `route_masks` points `Ctx::patterns`
    /// at this before evaluating `cond`, the same way `plan::exec::run` points it at `Plan::compiled_patterns`.
    pub(crate) compiled_patterns: crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
    /// `RouteConfig::on_exception`.
    pub(crate) on_exception: OnException,
}

impl CompiledRoute {
    /// Whether this route sends rows to `dest`: a `deliver` rule's or an exception answer's target.
    pub(crate) fn delivers_to(&self, dest: DestId) -> bool {
        self.deliver.iter().any(|rule| rule.to.contains(&dest))
            || self.on_exception.answers.iter().any(|a| a.targets().contains(&dest))
    }
}

impl CompiledTopology {
    /// Every destination a row entering through `source` could reach: the union of that source's routes' `deliver`
    /// targets, plus the DLQ each of those salvages its refusals to.
    ///
    /// A worker thread serves exactly one source, so this is what it actually needs to connect to. Building a producer
    /// per destination *in the bundle* instead costs a client and a background thread for every destination the thread
    /// can never write to, times every thread.
    pub fn destinations_reachable_from(&self, source: SourceId) -> Vec<DestId> {
        // The group's answers to `Unmatched` and `Unprocessable`, which no route names and a source with no routes
        // at all still reaches.
        let delivery = [ExceptionKind::Unmatched, ExceptionKind::Unprocessable].map(|k| self.on_exception.answer(&k));
        let mut out: Vec<DestId> = delivery.iter().flat_map(|a| a.targets()).copied().collect();
        out = self.with_dlqs(out);
        for r in self.routes.iter().filter(|r| r.source == source) {
            out.extend(self.written_by(r));
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// Every destination `route` writes a row to: those it delivers to, those its exceptions' answers deliver to, and
    /// down each one's DLQ chain, since a DLQ answers for what it refuses with its own policy.
    pub(crate) fn written_by(&self, route: &CompiledRoute) -> Vec<DestId> {
        let delivered = route.deliver.iter().flat_map(|rule| rule.to.iter().copied());
        let answered = route.on_exception.answers.iter().flat_map(|a| a.targets()).copied();
        self.with_dlqs(delivered.chain(answered).collect())
    }

    /// `dests` and every DLQ down their chains, each once. `dlq_of` already holds each whole chain.
    pub(crate) fn with_dlqs(&self, mut dests: Vec<DestId>) -> Vec<DestId> {
        let salvage: Vec<DestId> = dests
            .iter()
            .filter_map(|d| self.dlq_of.get(d))
            .flatten()
            .copied()
            .collect();
        dests.extend(salvage);
        dests.sort_unstable();
        dests.dedup();
        dests
    }

    /// The destinations `source`'s routes deliver to: what the flow goes to, and none of the places a row goes
    /// when it fails, matches nothing, or cannot be processed. Those are in `destinations_reachable_from` and carry a
    /// trickle beside this.
    pub(crate) fn delivery_destinations_from(&self, source: SourceId) -> Vec<DestId> {
        let mut out: Vec<DestId> = self
            .routes
            .iter()
            .filter(|r| r.source == source)
            .flat_map(|r| r.deliver.iter().flat_map(|rule| rule.to.iter().copied()))
            .collect();
        out.sort_unstable();
        out.dedup();
        out
    }
}

fn compile_routes(routes: &[RouteConfig], metas: &SourceMetas) -> Result<Vec<CompiledRoute>, Vec<CompileError>> {
    let mut out = Vec::with_capacity(routes.len());
    let mut errors = Vec::new();
    for (i, r) in routes.iter().enumerate() {
        match compile_route(r, &meta_of(metas, r.source)) {
            Ok(c) => out.push(c),
            Err(e) => errors.push(CompileError { route: Some(i), ..e }),
        }
    }
    if errors.is_empty() { Ok(out) } else { Err(errors) }
}

/// `ir` as a condition: `bool`, or null, which reads false and is wrapped so the column the runtime reads is one.
pub(crate) fn condition(ir: Ir, src: &str, what: &str) -> Result<Ir, CompileError> {
    if !crate::plan::resolve::is_condition(&ir.ty) {
        return Err(CompileError::new(
            None,
            format!("{what} `{src}` must be bool, resolved to {}", ir.ty.spell()),
        ));
    }
    Ok(if ir.ty == DataType::Null {
        flatten(crate::plan::compile::is_true(ir))
    } else {
        ir
    })
}

fn compile_route(r: &RouteConfig, meta: &SourceMeta) -> Result<CompiledRoute, CompileError> {
    let mut res = ResourceBuilder::default();
    let cond = match &r.cond {
        None => None,
        Some(src) => {
            let ast = parse(src, None)?;
            let mut syms = SymbolTable::new(&ingest_fields(meta));
            let ir = flatten(resolve(&ast, &mut syms, &mut res)?);
            Some(condition(ir, src, "route condition")?)
        }
    };
    let compiled_patterns = crate::plan::compile::compile_patterns(&res.patterns)?;
    Ok(CompiledRoute {
        id: r.id.clone(),
        name: r.name.clone(),
        source: r.source,
        cond,
        // Filled in by `compile_topology`, which has the pipeline a `when` must resolve against.
        deliver: Vec::new(),
        pipeline: r.pipeline,
        compiled_patterns,
        on_exception: r.on_exception.clone(),
    })
}

/// Every route, compiled, plus every pipeline it can reference, compiled — what `runtime::Worker` needs to actually run
/// a topology. Validates that every route's `pipeline` id resolves to a real compiled pipeline, so `runtime`'s driver
/// loop can assume that rather than handle it as a per-batch runtime error.
pub struct CompiledTopology {
    pub routes: Vec<CompiledRoute>,
    /// Keyed by the source whose metadata the plan was compiled against, and the pipeline id. Keyed by the metadata a
    /// plan was compiled against, so sources that declare the same fields share one. Reach it with `plan_for`.
    pub(crate) pipelines: HashMap<(SourceMeta, usize), Plan>,
    /// What each source declared when these plans were compiled. The runtime reads it from here rather than being told
    /// separately, so a plan's `reads_meta` and the fields the batch is built from cannot disagree.
    pub(crate) metas: SourceMetas,
    /// The group's answer to every exception kind (design doc, Exception taxonomy). The runtime reads `Unmatched` and
    /// `Unprocessable` here: neither reaches a route that could answer for it.
    pub on_exception: OnException,
    /// Each destination that delivers what it refuses, and every DLQ a refusal there can reach: the ones it names, theirs,
    /// and so on down each chain (`dlq_chain`). A destination's own setting, not the compiler's, so
    /// `config_file::LoadedSystem::compile_topology` fills it; `destinations_reachable_from` reads it so a DLQ is
    /// connected wherever a refusal can send a row to it.
    pub dlq_of: HashMap<DestId, Vec<DestId>>,
    /// What each Parquet destination writes, route by route. Filled with `dlq_of`, since a DLQ takes the same rows.
    pub records: HashMap<DestId, Records>,
    /// Each destination that may hold a variant as Parquet's variant (`DestinationKind::holds_native_variants`).
    /// Filled with `dlq_of`.
    pub(crate) native_variants: HashSet<DestId>,
    /// What each destination writes of a row (`DestinationKind::payload`). Filled with `dlq_of`; one missing writes the
    /// record.
    pub payloads: HashMap<DestId, Payload>,
    /// Each destination that names its documents by a field of the row, and that field
    /// (`DestinationKind::id_field`). Filled with `dlq_of`.
    pub id_fields: Vec<(DestId, String)>,
}

impl std::fmt::Debug for CompiledTopology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledTopology")
            .field("routes", &self.routes.len())
            .finish_non_exhaustive()
    }
}

impl CompiledTopology {
    /// What `dest` writes of a row. One no destination config names, a worker's built by hand, writes the record.
    pub(crate) fn payload(&self, dest: DestId) -> Payload {
        self.payloads.get(&dest).cloned().unwrap_or(Payload::Record)
    }

    /// The forms a row bound for `dest` keeps for `dlqs`, the DLQs down its chain: each one's, but `dest`'s own.
    pub(crate) fn kept<'d>(&self, dest: DestId, dlqs: impl IntoIterator<Item = &'d DestId>) -> Vec<Payload> {
        let own = self.payload(dest);
        let mut kept = Vec::new();
        for &dlq in dlqs {
            let p = self.payload(dlq);
            if p != own && !kept.contains(&p) {
                kept.push(p);
            }
        }
        kept
    }

    /// Every field a destination writes as text, each once, sorted.
    pub(crate) fn text_fields(&self) -> Vec<String> {
        let mut fields: Vec<String> = Vec::new();
        for p in self.payloads.values() {
            if let Payload::Text { field } = p
                && !fields.iter().any(|f| **f == **field)
            {
                fields.push(field.to_string());
            }
        }
        fields.sort();
        fields
    }

    /// What `source`'s records carry, as its plans were compiled to expect.
    pub fn meta_for(&self, source: SourceId) -> SourceMeta {
        meta_of(&self.metas, source)
    }

    /// The plan a row entering through `source` runs under `pipeline`.
    pub fn plan_for(&self, source: SourceId, pipeline: usize) -> Option<&Plan> {
        self.pipelines.get(&(self.meta_for(source), pipeline))
    }

    /// Each route that sends rows to `dest`, with its index.
    pub(crate) fn delivering(&self, dest: DestId) -> impl Iterator<Item = (usize, &CompiledRoute)> {
        self.routes.iter().enumerate().filter(move |(_, r)| r.delivers_to(dest))
    }

    /// A route's finished record, as a condition over it reads it.
    pub(crate) fn output_of(&self, route: &CompiledRoute) -> SymbolTable {
        let meta = self.meta_for(route.source);
        let plan = (self.plan_for(route.source, route.pipeline))
            .expect("compile_topology validated every route's pipeline id");
        output_symbols(plan, &route.on_exception.tag_field, &meta)
    }

    /// A capture filter at `dest`, compiled against the output of each route delivering there, by route index. A
    /// filter runs on each of them it compiles on, since they may give `dest` rows of different shapes.
    pub(crate) fn capture_filters(
        &self,
        dest: DestId,
        src: &str,
    ) -> Vec<(usize, Result<OutputCondition, CompileError>)> {
        self.delivering(dest)
            .map(|(idx, r)| {
                let plan = (self.plan_for(r.source, r.pipeline))
                    .expect("compile_topology validated every route's pipeline id");
                (
                    idx,
                    output_condition(
                        src,
                        "a capture filter",
                        plan,
                        &r.on_exception.tag_field,
                        &self.meta_for(r.source),
                    ),
                )
            })
            .collect()
    }

    /// Whether anything `source`'s records reach reads its field `field`: a route's condition, its pipeline or its
    /// delivery rules.
    pub fn reads(&self, source: SourceId, field: &str) -> bool {
        let Some(i) = self
            .meta_for(source)
            .source_fields()
            .iter()
            .position(|(name, _)| *name == field)
        else {
            return false;
        };
        self.routes
            .iter()
            .filter(|r| r.source == source)
            .any(|r| self.plan_for(source, r.pipeline).is_some_and(|p| p.reads_meta[i]))
    }
}

/// `compile_topology_with` over sources that all declare Kafka's fields. The assumption is stated here, at a call site
/// that has no sources to ask, rather than inside the compiler where every caller would inherit it.
pub fn compile_topology(
    routes: &[RouteConfig],
    pipelines: &[PipelineConfig],
) -> Result<CompiledTopology, Vec<CompileError>> {
    let metas: SourceMetas = routes.iter().map(|r| (r.source, SourceMeta::KAFKA)).collect();
    compile_topology_with(routes, pipelines, OnException::default(), &metas)
}

/// The compiler's entry point: every production path reaches it through `LoadedSystem::compile_topology`, with the
/// group's answers and the metadata each source declares. `compile_topology` is the two-argument form for a
/// caller with neither to pass.
pub fn compile_topology_with(
    routes: &[RouteConfig],
    pipelines: &[PipelineConfig],
    on_exception: OnException,
    metas: &SourceMetas,
) -> Result<CompiledTopology, Vec<CompileError>> {
    let mut errors = Vec::new();

    let by_id: HashMap<usize, &PipelineConfig> = pipelines.iter().map(|p| (p.id, p)).collect();

    // What a plan depends on is the names in scope, which is the source's metadata -- not the source. Two Kafka sources
    // routed to one pipeline would compile it to the same instructions, the same slots and the same loaded lookup
    // tables, so they share one plan. The metadata carries the encoding, so two that differ only there compile apart,
    // to the same plan twice. Route order, deduped, so the errors a config produces stay in the same order from run
    // to run.
    let mut seen = std::collections::HashSet::new();
    let pairs: Vec<(SourceMeta, usize)> = routes
        .iter()
        .map(|r| (meta_of(metas, r.source), r.pipeline))
        .filter(|pair| seen.insert(pair.clone()))
        .collect();

    let mut compiled_pipelines: HashMap<(SourceMeta, usize), Plan> = HashMap::new();
    for (meta, pipeline) in pairs {
        let Some(p) = by_id.get(&pipeline) else {
            continue; // a route naming no pipeline is reported below
        };
        match compile(p, &meta) {
            Ok(plan) => {
                compiled_pipelines.insert((meta, pipeline), plan);
            }
            // A save compiles every pipeline in one call, so a bare `function` id needs `p.id` alongside it to
            // say which pipeline it came from.
            Err(e) => errors.extend(e.into_iter().map(|err| err.in_pipeline(p.id))),
        }
    }

    // An unrouted pipeline is checked against `SourceMeta::KAFKA` -- see its doc for why.
    for p in pipelines {
        if !routes.iter().any(|r| r.pipeline == p.id)
            && let Err(e) = compile(p, &SourceMeta::KAFKA)
        {
            errors.extend(e.into_iter().map(|err| err.in_pipeline(p.id)));
        }
    }

    let mut compiled_routes = match compile_routes(routes, metas) {
        Ok(r) => r,
        Err(mut e) => {
            errors.append(&mut e);
            Vec::new()
        }
    };

    // A `when` resolves against its target pipeline's *output*, so it can only be compiled once that pipeline has been
    // -- which is why this happens here rather than in `compile_route`.
    //
    // It also reads the batch the pipeline wrote, so a metadata field named only by a `when` still has to be built.
    // So does one named only by the route's condition: the route chose the row by it, so a repair it made must reach
    // the row's exceptions. `Plan::reads_meta` is derived from the pipeline's own instructions, which cannot see these
    // rules, so their reads are collected here and folded in below.
    let mut rule_reads: HashMap<(SourceMeta, usize), Vec<bool>> = HashMap::new();
    let mut mark = |key: (SourceMeta, usize), ir: &Ir| {
        let meta = key.0.clone();
        let reads = rule_reads
            .entry(key)
            .or_insert_with(|| vec![false; meta.source_fields().len()]);
        crate::plan::compile::walk_slots(ir, &mut |slot| {
            if let Some(i) = meta.source_field_at(slot) {
                reads[i] = true;
            }
        });
    };
    for (r, config) in routes.iter().enumerate() {
        let Some(compiled) = compiled_routes.get_mut(r) else {
            continue;
        };
        let meta = meta_of(metas, config.source);
        let Some(plan) = compiled_pipelines.get(&(meta.clone(), config.pipeline)) else {
            continue; // reported just below
        };
        if let Some(cond) = &compiled.cond {
            mark((meta.clone(), config.pipeline), cond);
        }
        for (i, rule) in config.deliver.iter().enumerate() {
            match compile_deliver_rule(rule, plan, &config.on_exception.tag_field, &meta) {
                Ok(c) => {
                    if let Some(ir) = &c.when {
                        mark((meta.clone(), config.pipeline), ir);
                    }
                    compiled.deliver.push(c);
                }
                Err(mut e) => {
                    e.route = Some(r);
                    e.rule = Some(i as u32);
                    errors.push(e);
                }
            }
        }
    }

    for (key, reads) in rule_reads {
        if let Some(plan) = compiled_pipelines.get_mut(&key) {
            for (built, wanted) in plan.reads_meta.iter_mut().zip(reads) {
                *built |= wanted;
            }
        }
    }

    for (r, route) in compiled_routes.iter().enumerate() {
        if !by_id.contains_key(&route.pipeline) {
            errors.push(CompileError::new(
                None,
                format!(
                    "route {r} (source {}) references pipeline id {}, which doesn't exist",
                    route.source, route.pipeline
                ),
            ));
        }
    }

    if errors.is_empty() {
        Ok(CompiledTopology {
            routes: compiled_routes,
            pipelines: compiled_pipelines,
            metas: metas.clone(),
            on_exception,
            dlq_of: HashMap::new(),
            records: HashMap::new(),
            native_variants: HashSet::new(),
            payloads: HashMap::new(),
            id_fields: Vec::new(),
        })
    } else {
        Err(errors)
    }
}

// -- evaluating -----------------------------------------------------------

/// Compile one `deliver` rule against its target pipeline's output.
///
/// The condition sees exactly what the finished batch holds: every field the pipeline left live, the ingest names,
/// and the route's tag field (`Plan::tag_slot`), written after the pipeline's last step. That is the capability
/// `RouteConfig::cond` lacks -- it runs on the ingest batch, before the pipeline, so it can only test `_raw`.
///
/// It reads the row as it will be sent: the tag field is the tag on a tagged row, and what the row holds there on any
/// other.
fn compile_deliver_rule(
    rule: &DeliverRule,
    plan: &Plan,
    tag_field: &str,
    meta: &SourceMeta,
) -> Result<CompiledDeliverRule, CompileError> {
    let Some(src) = &rule.when else {
        return Ok(CompiledDeliverRule {
            when: None,
            to: rule.to.clone(),
            compiled_patterns: Default::default(),
        });
    };
    let (ir, compiled_patterns) = output_condition(src, "condition", plan, tag_field, meta)?;
    Ok(CompiledDeliverRule {
        when: Some(ir),
        to: rule.to.clone(),
        compiled_patterns,
    })
}

/// The names an expression over `plan`'s finished batch reads, for a row from a source of `meta`: the pipeline's
/// output, the source's own fields, and the route's tag field.
pub(crate) fn output_symbols(plan: &Plan, tag_field: &str, meta: &SourceMeta) -> SymbolTable {
    SymbolTable::over_plan_output(
        &plan.delivery_layout(),
        &plan.tree,
        &ingest_fields(meta),
        &[(tag_field, plan.tag_slot())],
    )
}

/// A condition over a finished batch, and the patterns `Ctx::patterns` is pointed at to evaluate it.
type OutputCondition = (
    Ir,
    crate::core::id::ById<crate::core::id::PatternId, crate::core::ctx::CompiledPattern>,
);

/// Compile `src`, called `what` in an error, as a condition over `plan`'s finished batch: what a `deliver` rule reads,
/// and what a filter at a destination reads. Evaluated with `Ctx::patterns` pointed at the patterns it returns.
pub(crate) fn output_condition(
    src: &str,
    what: &str,
    plan: &Plan,
    tag_field: &str,
    meta: &SourceMeta,
) -> Result<OutputCondition, CompileError> {
    let mut res = ResourceBuilder::default();
    let ast = parse(src, None)?;
    let ir = flatten(resolve(&ast, &mut output_symbols(plan, tag_field, meta), &mut res)?);
    let ir = condition(ir, src, what)?;
    Ok((ir, crate::plan::compile::compile_patterns(&res.patterns)?))
}

/// Evaluate every route belonging to `source` against `batch` — which must have the source's `ingest_fields` layout,
/// its record filled (and `__source.offset`, if the source has one) and `__live` primed all-true. Routes are unordered:
/// each sees the whole batch, so a row matching two routes runs both pipelines, and `__live` is left as it came.
///
/// Returns, for each route belonging to `source`, its index into `routes` and the Bool mask of rows dispatched to it.
/// A route that matches nothing still gets an all-false mask, since a batch's routing shape can vary row to row.
pub fn route_masks<'a>(
    routes: &'a [CompiledRoute],
    source: SourceId,
    batch: &mut Batch,
    ctx: &mut Ctx<'a>,
) -> Vec<(usize, Column)> {
    let sp = Span::default();
    let live = || Ir::new(DataType::Bool, sp, IrKind::Field(SLOT_LIVE));

    let mut out = Vec::new();
    for (i, r) in routes.iter().enumerate() {
        if r.source != source {
            continue;
        }

        ctx.patterns = &r.compiled_patterns;

        let matched_ir = match &r.cond {
            None => live(),
            Some(cond) => Ir::new(
                DataType::Bool,
                sp,
                IrKind::Binary {
                    op: BinOp::And,
                    lhs: Box::new(live()),
                    rhs: Box::new(cond.clone()),
                },
            ),
        };
        let matched = eval(&matched_ir, batch, ctx);
        out.push((i, matched));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::id::Id;
    use crate::plan::config::Answer;

    use crate::core::bits;
    use crate::core::buffer::BufferPool;
    use crate::core::column::{StrBuilder, VarType};
    use crate::core::error::ErrorSink;

    /// Which of `records` the condition `meta` gives `key` takes, run as a route on `meta`'s source.
    fn taken_by_stream(meta: SourceMeta, key: &str, records: &[crate::io::SourceRecord]) -> Vec<bool> {
        let cond = meta.stream_condition(key).expect("a condition");
        let routes = compile_routes(
            &[route(SourceId::new(0), Some(&cond), 0)],
            &SourceMetas::from([(SourceId::new(0), meta.clone())]),
        )
        .unwrap_or_else(|e| panic!("`{cond}` compiles: {e:?}"));
        let layout: Vec<DataType> = crate::plan::compile::ingest_fields(&meta)
            .into_iter()
            .map(|(_, t)| t)
            .collect();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = Batch::new(&layout, records.len(), 0);
        let texts = vec![""; records.len()];
        meta.ingest(&mut batch, records, &texts, None, |_| true, &mut pool);
        let mut live = Column::alloc(&mut pool, &DataType::Bool, records.len());
        bits::fill_valid(live.bool_mut().unwrap().values, records.len());
        batch.put(SLOT_LIVE, live, &mut pool);
        let mut ctx = Ctx::new(&mut pool, &mut errors);
        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let mask = masks[0].1.bool().unwrap().values;
        let taken = (0..records.len()).map(|r| bits::get_row(mask, r)).collect();
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
        taken
    }

    #[test]
    fn a_streams_condition_takes_the_records_filed_under_its_key() {
        let record = |topic: &str, filed: Option<Vec<(String, Vec<u8>)>>| crate::io::SourceRecord {
            chunk: crate::io::RecordChunk::of_bytes(b"{}"),
            row: 0,
            offset: 0,
            partition: 0,
            topic: std::sync::Arc::from(topic),
            headers: filed,
        };
        let filed = |key: &str, value: &str| Some(vec![(key.to_string(), value.as_bytes().to_vec())]);
        let cases = [
            (
                SourceMeta::HEC_RECEIVER,
                [
                    record("10.0.0.9", filed("sourcetype", "okta:system")),
                    record("10.0.0.9", filed("sourcetype", "other")),
                ],
            ),
            (
                SourceMeta::HEC_RECEIVER,
                [
                    record("10.0.0.9", None),
                    record("10.0.0.9", filed("sourcetype", "other")),
                ],
            ),
            (
                SourceMeta::ELASTIC_RECEIVER,
                [
                    record("10.0.0.9", filed("index", "logs-\"okta\"")),
                    record("10.0.0.9", filed("index", "logs")),
                ],
            ),
            (
                SourceMeta::S3_SQS,
                [
                    record("landing/logs/okta/2026/10/14/a.json.gz", None),
                    record("landing/logs/duo/a.json", None),
                ],
            ),
            (
                SourceMeta::S3_SQS,
                [record("landing/2026/a.json", None), record("other/a.json", None)],
            ),
            (SourceMeta::KAFKA, [record("orders", None), record("returns", None)]),
            (
                SourceMeta::HTTP_RECEIVER,
                [record("10.0.0.9", None), record("10.0.0.10", None)],
            ),
        ];
        for (meta, records) in cases {
            // As `Streams::saw_unmatched` keys a record its kind files under nothing.
            let key = meta.stream_key(&records[0]).unwrap_or_default().into_owned();
            assert_eq!(
                taken_by_stream(meta.clone(), &key, &records),
                [true, false],
                "{key} on {}",
                meta.kind
            );
        }
    }

    fn local(var: &str) -> SecretRef {
        SecretRef::WorkerLocal(var.to_string())
    }

    fn stored(name: &str, version: i64) -> SecretRef {
        SecretRef::LeaderStored {
            name: name.to_string(),
            version,
        }
    }

    fn held(values: &[(&str, i64, &str)]) -> HeldSecrets {
        let mut h = HeldSecrets::default();
        h.set_stored(
            values
                .iter()
                .map(|(n, v, value)| (n.to_string(), *v, (*value).to_string())),
        );
        h
    }

    /// A bundle naming `reference` for `what`.
    fn naming(what: &str, reference: SecretRef) -> TopologyConfig {
        TopologyConfig {
            named_secrets: vec![NamedSecret {
                reference,
                what: what.to_string(),
            }],
            ..Default::default()
        }
    }

    /// The message has to name the credential *and* what wanted it. An operator reading it is being asked to supply
    /// something, and "a secret is missing" does not say which secret or on whose behalf.
    #[test]
    fn a_missing_secret_is_reported_with_its_credential_and_owner() {
        let t = naming(
            r#"destination "prod-es": auth"#,
            local("SMELTR_TEST_DEFINITELY_UNSET_AUTH"),
        );
        let missing = t.missing_secrets(&HeldSecrets::default());
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("prod-es"), "{missing:?}");
        assert!(missing[0].contains("auth"), "{missing:?}");
        assert!(missing[0].contains("SMELTR_TEST_DEFINITELY_UNSET_AUTH"), "{missing:?}");
    }

    /// A certificate and its key, its Extended Key Usage naming `purposes` where any are given.
    fn certificate_and_key(purposes: Vec<rcgen::ExtendedKeyUsagePurpose>) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec!["smeltr-test".to_string()]).unwrap();
        params.extended_key_usages = purposes;
        (params.self_signed(&key).unwrap().pem(), key.serialize_pem())
    }

    /// A connector presenting `acme`'s certificate from `side`.
    fn presenting(side: crate::io::trust::Side) -> Tls {
        Tls {
            of: "source \"in\"".to_string(),
            certificate: Some("acme".to_string()),
            authorities: None,
            trust_public: true,
            side,
            senders: Default::default(),
        }
    }

    /// What a worker holds once the leader has sent it the certificate `acme` at version 1.
    fn holding_certificate(chain: String, key: String) -> HeldSecrets {
        let mut h = HeldSecrets::default();
        h.take([crate::heartbeat::SecretDelivery {
            name: "acme".to_string(),
            version: 1,
            value: key,
            chain: Some(chain),
        }]);
        h
    }

    /// A certificate whose Extended Key Usage leaves out the side it is presented from is refused before anything
    /// is torn down, rather than at the handshake.
    #[test]
    fn a_certificate_is_held_to_the_side_its_bundle_presents_it_from() {
        use crate::io::trust::Side;
        let (chain, key) = certificate_and_key(vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth]);
        let held = holding_certificate(chain, key);
        let error =
            certificate_error(&presenting(Side::Client), &held).expect("a server certificate presented by a client");
        assert!(
            error.contains("source \"in\"") && error.contains("client authentication"),
            "{error}"
        );
        assert!(certificate_error(&presenting(Side::Server), &held).is_none());
    }

    #[test]
    fn a_certificate_with_no_extended_key_usage_is_presented_from_either_side() {
        use crate::io::trust::Side;
        let (chain, key) = certificate_and_key(Vec::new());
        let held = holding_certificate(chain, key);
        assert!(certificate_error(&presenting(Side::Client), &held).is_none());
        assert!(certificate_error(&presenting(Side::Server), &held).is_none());
    }

    /// A certificate this worker does not have yet is `missing_secrets`' concern.
    #[test]
    fn a_certificate_this_worker_lacks_is_not_this_checks_concern() {
        assert!(certificate_error(&presenting(crate::io::trust::Side::Client), &HeldSecrets::default()).is_none());
        let t = TopologyConfig {
            named_certificates: vec![NamedCertificate {
                name: "acme".to_string(),
                what: "source \"in\"".to_string(),
            }],
            ..Default::default()
        };
        let missing = t.missing_secrets(&HeldSecrets::default());
        assert_eq!(missing.len(), 1, "{missing:?}");
        assert!(missing[0].contains("certificate \"acme\""), "{missing:?}");
        assert!(
            t.missing_secrets(&holding_certificate("chain".into(), "key".into()))
                .is_empty()
        );
    }

    /// The chain travels with its key, and both reach the runtime; the newest version is the one held, and a
    /// version replacing an older one is said, since what presents it has to be built again.
    #[test]
    fn a_certificate_is_held_at_its_newest_version_with_its_chain() {
        let mut held = holding_certificate("chain".into(), "key".into());
        assert_eq!(held.certificate("acme"), Some(("chain".to_string(), "key".to_string())));
        let presented = [NamedCertificate {
            name: "acme".to_string(),
            what: "source \"in\"".to_string(),
        }];
        let handed = held.deliveries_for(&[], &presented);
        assert_eq!(handed.len(), 1);
        assert_eq!(handed[0].chain.as_deref(), Some("chain"));
        assert_eq!(handed[0].version, 1);

        let newer = |version: i64| crate::heartbeat::SecretDelivery {
            name: "acme".to_string(),
            version,
            value: format!("key{version}"),
            chain: Some(format!("chain{version}")),
        };
        assert_eq!(held.take([newer(2)]), vec!["acme".to_string()]);
        assert_eq!(
            held.certificate("acme"),
            Some(("chain2".to_string(), "key2".to_string()))
        );
        assert!(
            held.take([newer(1)]).is_empty(),
            "an older version does not replace the newest"
        );
        assert_eq!(held.versions(), vec![("acme".to_string(), 2)]);
        held.keep_only(&[], &[]);
        assert_eq!(held.certificate("acme"), None);
    }

    /// A deployment runs the version its bundle quotes or it does not run. Neither an older value nor a newer one
    /// stands in: storing a value is staging it, and the commit quoting the number is what deploys it.
    #[test]
    fn a_bundle_runs_the_version_it_quotes_and_no_other() {
        let t = naming(r#"destination "prod-es": auth"#, stored("es-auth", 7));
        assert!(t.missing_secrets(&held(&[("es-auth", 7, "ApiKey abc")])).is_empty());
        assert_eq!(
            t.missing_secrets(&held(&[("es-auth", 8, "ApiKey abc")])).len(),
            1,
            "a newer value is not what this bundle was built for"
        );
        assert_eq!(t.missing_secrets(&held(&[("es-auth", 6, "ApiKey abc")])).len(), 1);
        assert_eq!(t.missing_secrets(&held(&[("other", 7, "x")])).len(), 1);
        assert_eq!(t.missing_secrets(&HeldSecrets::default()).len(), 1);
    }

    /// Holding both is the ordinary state mid-rotation: the version that is running, and the one the next commit
    /// will quote.
    #[test]
    fn two_versions_of_one_name_are_held_apart() {
        let h = held(&[("es-auth", 7, "old"), ("es-auth", 8, "new")]);
        assert_eq!(h.value(&stored("es-auth", 7)).as_deref(), Some("old"));
        assert_eq!(h.value(&stored("es-auth", 8)).as_deref(), Some("new"));
        assert_eq!(h.value(&stored("es-auth", 9)), None);
    }

    /// What a deployment does not name is dropped, so a worker carries the credentials something runs on rather
    /// than every one it has ever been sent.
    #[test]
    fn keeping_only_what_a_deployment_names_drops_the_rest() {
        let mut h = held(&[("es-auth", 7, "old"), ("es-auth", 8, "new"), ("gone", 1, "x")]);
        let t = naming(r#"destination "prod-es": auth"#, stored("es-auth", 8));
        h.keep_only(&t.named_secrets, &[]);
        assert_eq!(h.versions(), vec![("es-auth".to_string(), 8)]);
    }

    #[test]
    fn a_variable_set_to_nothing_is_a_credential_this_worker_does_not_have() {
        assert_eq!(usable(Some("hunter2".to_string())), Some("hunter2".to_string()));
        assert_eq!(usable(Some(String::new())), None);
        assert_eq!(usable(None), None);
    }

    /// The leader answers with what this worker lacks, so an empty delivery has to mean "nothing changed" rather
    /// than "hold nothing".
    #[test]
    fn taking_a_delivery_keeps_what_it_does_not_mention() {
        use crate::heartbeat::SecretDelivery;

        let mut h = held(&[("a", 1, "one"), ("b", 2, "two")]);
        h.take([SecretDelivery {
            name: "b".to_string(),
            version: 3,
            value: "three".to_string(),
            chain: None,
        }]);
        assert_eq!(h.value(&stored("a", 1)).as_deref(), Some("one"));
        assert_eq!(
            h.value(&stored("b", 2)).as_deref(),
            Some("two"),
            "the running one stays"
        );
        assert_eq!(h.value(&stored("b", 3)).as_deref(), Some("three"));

        h.take([]);
        assert_eq!(
            h.versions(),
            vec![("a".to_string(), 1), ("b".to_string(), 2), ("b".to_string(), 3)],
            "a delivery adds; forgetting is what a deploy does"
        );
    }

    /// The type holds every credential a deployment uses, so its own `Debug` must not be the way they escape.
    #[test]
    fn debug_shows_what_is_held_and_never_a_value() {
        let shown = format!("{:?}", held(&[("es-auth", 7, "ApiKey-SECRET")]));
        assert!(shown.contains("es-auth"), "{shown}");
        assert!(shown.contains('7'), "{shown}");
        assert!(!shown.contains("ApiKey-SECRET"), "{shown}");
    }

    fn route(source: SourceId, cond: Option<&str>, pipeline: usize) -> RouteConfig {
        RouteConfig {
            id: format!("route-{source}-{pipeline}"),
            name: format!("route-{source}-{pipeline}"),
            source,
            cond: cond.map(|s| s.to_string()),
            pipeline,
            deliver: Vec::new(),
            on_exception: Default::default(),
        }
    }

    /// A plan depends on the names in scope, so two sources declaring the same fields share one rather than compiling
    /// it twice. Compiling twice would also load the pipeline's lookup tables twice.
    #[test]
    fn sources_declaring_the_same_fields_share_one_plan() {
        let pipelines = vec![crate::plan::config::PipelineConfig {
            id: 7,
            name: "p".into(),
            body: Vec::new(),
            step_ids: Default::default(),
            lookup_tables: Default::default(),
            grok_patterns: Default::default(),
            produces: Vec::new(),
        }];
        let metas = SourceMetas::from([
            (SourceId::new(0), SourceMeta::KAFKA),
            (SourceId::new(1), SourceMeta::KAFKA),
        ]);
        let compiled = compile_topology_with(
            &[route(SourceId::new(0), None, 7), route(SourceId::new(1), None, 7)],
            &pipelines,
            OnException::default(),
            &metas,
        )
        .unwrap();

        assert_eq!(compiled.pipelines.len(), 1, "one plan, not one per source");
        // Both sources still reach it.
        assert!(compiled.plan_for(SourceId::new(0), 7).is_some());
        assert!(compiled.plan_for(SourceId::new(1), 7).is_some());

        // Declaring different fields is what forces a second plan.
        let mixed = SourceMetas::from([
            (SourceId::new(0), SourceMeta::KAFKA),
            (SourceId::new(1), SourceMeta::METRICS),
        ]);
        let compiled = compile_topology_with(
            &[route(SourceId::new(0), None, 7), route(SourceId::new(1), None, 7)],
            &pipelines,
            OnException::default(),
            &mixed,
        )
        .unwrap();
        assert_eq!(compiled.pipelines.len(), 2, "different names in scope");
    }

    /// A worker thread serves one source and should connect only to what that source's own routes can reach. Building
    /// every destination in the bundle instead cost a Kafka client *and* a background thread per destination per worker
    /// thread -- the largest single multiplier in the fan-out, and invisible until you count producers rather than
    /// consumers.
    #[test]
    fn a_source_reaches_only_the_destinations_its_own_routes_name() {
        // Built as `CompiledRoute`s directly rather than through `compile_routes`: that function deliberately leaves
        // `deliver` empty, since a rule's `when` compiles against its target pipeline's output and only
        // `compile_topology` has the plan to do that. The worker calls `compile_topology`, so its routes are fully
        // populated -- but a test that used `compile_routes` would be asserting against rules that were never filled
        // in.
        let mk = |source: SourceId, to: Vec<DestId>| CompiledRoute {
            id: format!("route-{source}"),
            name: format!("route-{source}"),
            source,
            cond: None,
            pipeline: 0,
            deliver: vec![CompiledDeliverRule {
                when: None,
                to,
                compiled_patterns: Default::default(),
            }],
            compiled_patterns: Default::default(),
            on_exception: Default::default(),
        };

        let compiled = CompiledTopology {
            routes: vec![
                mk(SourceId::new(0), vec![DestId::new(2)]),
                mk(SourceId::new(1), vec![DestId::new(7)]),
            ],
            pipelines: std::collections::HashMap::new(),
            metas: SourceMetas::new(),
            on_exception: {
                let mut group = OnException::default();
                for (kind, to) in [
                    (ExceptionKind::Unmatched, DestId::new(4)),
                    (ExceptionKind::Unprocessable, DestId::new(8)),
                ] {
                    group.answers[kind.index()] = Answer::Deliver(vec![to]);
                }
                group
            },
            // Destination 2 salvages its refusals to 5, and 8 to 3. `dlq_of` holds whole chains, so reachability reads
            // one entry per destination.
            dlq_of: std::collections::HashMap::from([
                (DestId::new(2), vec![DestId::new(5)]),
                (DestId::new(8), vec![DestId::new(3)]),
            ]),
            records: HashMap::new(),
            native_variants: Default::default(),
            payloads: HashMap::new(),
            id_fields: Vec::new(),
        };

        // Source 0: its own delivery target plus the DLQ that target salvages to -- a source can write to a
        // destination no route names.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(0)),
            vec![
                DestId::new(2),
                DestId::new(3),
                DestId::new(4),
                DestId::new(5),
                DestId::new(8)
            ]
        );
        // Source 1 reaches neither of those.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(1)),
            vec![DestId::new(3), DestId::new(4), DestId::new(7), DestId::new(8)]
        );
        // A source with no routes reaches only where its unmatched and unprocessable rows go, and the chain behind
        // them.
        assert_eq!(
            compiled.destinations_reachable_from(SourceId::new(9)),
            vec![DestId::new(3), DestId::new(4), DestId::new(8)]
        );

        // Where the flow goes: the delivery targets alone, no DLQ of any kind.
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(0)),
            vec![DestId::new(2)]
        );
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(1)),
            vec![DestId::new(7)]
        );
        assert_eq!(
            compiled.delivery_destinations_from(SourceId::new(9)),
            Vec::<DestId>::new()
        );
    }

    /// The metadata `seed_batch`'s layout assumes, for a route condition over `__source.offset`.
    fn kafka_metas(source: SourceId) -> SourceMetas {
        SourceMetas::from([(source, SourceMeta::KAFKA)])
    }

    /// `rows` raw byte strings, in a batch laid out for a Kafka source, with `__live` primed all-true and `__source.offset`
    /// set to the row index.
    fn seed_batch(pool: &mut BufferPool, docs: &[&str]) -> Batch {
        use crate::core::batch::{RAW_SLOTS, SLOT_RAW, raw_layout};

        let offset_field = SourceMeta::KAFKA
            .source_fields()
            .iter()
            .position(|f| f.0 == "__source.offset")
            .unwrap();
        let slot_offset = crate::core::id::SlotId::at(RAW_SLOTS + offset_field);

        let rows = docs.len();
        let mut layout = raw_layout();
        layout.extend(SourceMeta::KAFKA.source_fields().iter().map(|f| f.1.clone()));
        let mut batch = Batch::new(&layout, rows, 0);

        let mut live = Column::alloc(pool, &DataType::Bool, rows);
        bits::fill_valid(live.bool_mut().unwrap().values, rows);
        batch.put(SLOT_LIVE, live, pool);

        // Row index, not a real Kafka offset -- just something per-row and distinct for route conditions in these tests
        // to key on, since `_raw` equality/comparison support isn't exercised here.
        let mut offset = Column::alloc(pool, &DataType::I64, rows);
        for (i, v) in offset.i64_mut().unwrap().values.iter_mut().enumerate() {
            *v = i as i64;
        }
        batch.put(slot_offset, offset, pool);

        let mut b = StrBuilder::new(pool, VarType::Str, rows);
        b.reserve(pool, docs.iter().map(|d| d.len()).sum());
        for d in docs {
            b.push(d.as_bytes());
        }
        batch.put(SLOT_RAW, b.finish(), pool);

        batch
    }

    #[test]
    fn a_single_route_with_no_condition_catches_everything() {
        let routes = compile_routes(&[route(SourceId::new(0), None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a", "b", "c"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        assert_eq!(masks.len(), 1);
        let rows: Vec<bool> = (0..3)
            .map(|r| bits::get_row(masks[0].1.bool().unwrap().values, r))
            .collect();
        assert_eq!(rows, vec![true, true, true]);
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }

    #[test]
    fn routes_for_a_different_source_are_skipped() {
        let routes = compile_routes(&[route(SourceId::new(1), None, 7)], &SourceMetas::new()).unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["a"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        assert!(masks.is_empty(), "route belongs to source 1, not 0");
        batch.reclaim(&mut pool);
    }

    #[test]
    fn every_route_sees_the_whole_batch() {
        // Route on __source.offset (here just the row index -- see seed_batch) rather than a parsed field, since routes only
        // see ingest fields. Row 0 matches both routes, so it runs both pipelines.
        let routes = compile_routes(
            &[
                route(SourceId::new(0), Some("__source.offset == 0"), 1),
                route(SourceId::new(0), None, 2),
            ],
            &kafka_metas(SourceId::new(0)),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x", "yy"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let rows = |m: &Column| {
            (0..2)
                .map(|r| bits::get_row(m.bool().unwrap().values, r))
                .collect::<Vec<_>>()
        };
        assert_eq!(masks.iter().map(|(i, _)| *i).collect::<Vec<_>>(), vec![0, 1]);
        assert_eq!(
            rows(&masks[0].1),
            vec![true, false],
            "route 0 (__source.offset==0) matches only row 0"
        );
        assert_eq!(
            rows(&masks[1].1),
            vec![true, true],
            "the route with no condition takes every row"
        );
        assert_eq!(
            rows(batch.get(SLOT_LIVE)),
            vec![true, true],
            "routing leaves __live as it came"
        );
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }

    #[test]
    fn a_non_bool_route_condition_is_a_compile_error() {
        let err = compile_routes(&[route(SourceId::new(0), Some("_raw"), 1)], &SourceMetas::new());
        assert!(err.is_err());
    }

    /// Regression test: a route condition using a regex literal (`match`/ `replace`) must actually match at runtime.
    /// `compile_route` resolves `cond` against its own disjoint `ResourceBuilder`, so its interned patterns live in
    /// `CompiledRoute::compiled_patterns`, not any target pipeline's `Plan::compiled_patterns` — `route_masks` has to
    /// point `Ctx::patterns` there before evaluating `cond`, or every `match`/ `replace` in a route condition silently
    /// resolves against an empty pattern table and never matches.
    #[test]
    fn a_route_condition_using_a_regex_literal_actually_matches() {
        let routes = compile_routes(
            &[
                route(SourceId::new(0), Some("test(_raw, /^x/)"), 1),
                route(SourceId::new(0), None, 2), // catch-all
            ],
            &SourceMetas::new(),
        )
        .unwrap();
        let mut pool = BufferPool::new();
        let mut errors = ErrorSink::new();
        let mut batch = seed_batch(&mut pool, &["x1", "y2"]);
        let mut ctx = Ctx::new(&mut pool, &mut errors);

        let masks = route_masks(&routes, SourceId::new(0), &mut batch, &mut ctx);
        let (_, m0) = &masks[0];
        assert_eq!(
            (0..2)
                .map(|r| bits::get_row(m0.bool().unwrap().values, r))
                .collect::<Vec<_>>(),
            vec![true, false],
            "route 0 (test(_raw, /^x/)) should match only row 0"
        );
        batch.reclaim(&mut pool);
        masks.into_iter().for_each(|(_, c)| c.reclaim(&mut pool));
    }
}
