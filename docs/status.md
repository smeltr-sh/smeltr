# Smeltr — implementation status

What is built and what is not. [`smeltr-design.md`](smeltr-design.md) says
what must be true; `src/` says how. A row here names an area, says whether
it is built, and lists what is not. Names in parentheses are the design
document's systems.

## By area

### Language and compiler

| area | status |
|---|---|
| Columnar core, compiler, `Code` blocks, inlined functions, tail recursion, `while`, `break`/`continue`, early `return` (Columnar data model, Compiler, Language) | built |
| Trailing commas, shorthand object keys (Language) | built |
| `delete`, `move`, `drop()`, `unroll()`, `try`/`catch`/`finally`, `switch`, braceless `if`, `$ = e`, `Object.assign`; `json`, `kv`, `csv`, `grok`, `lookup`, `rest` as functions (Language, Compiler) | built; every GUI step lowers to code (`plan::lower`) |
| Types: `i64`, `f64`, `variant`, exceptions, function parameter and return types (Language, Compiler) | built |
| The name tree: leaves, objects, unclaimed fields, open and closed objects (Compiler, Language, Columnar data model) | built |
| Stdlib: variadic `push`/`insert`/`concat`/`min`/`max`, `+` on arrays; address functions; Elastic's enrichments (`parseUrl`, `registeredDomain`, `parseUserAgent`, hashes, `communityId`, ...) (Language) | built |
| Bytes are not text: `decode`, `encode`, `toString(bytes, form)`, `parseBytes` (Language, Columnar data model) | built |
| Functions as values: `let`-bound arrows, `map`/`filter` (Language) | built. Not built: a function type annotation, `(x: T) => R`; a function chosen per row, `(c ? f : g)(x)`; a `let`-bound arrow calling itself |
| A key is a key; `splitDots` (Language) | built |
| Compile warnings and formatting (Compiler) | built (`Plan::warnings`, `expr::pretty`, `POST /api/format`). Warnings show in the pipeline editor only; a review or a deploy does not list them |
| CSV column table, `skip` (Config plane) | built |
| Grok: `.` matches a newline (Runtime stages) | built |
| Lookup: CSV, inline, MMDB; exact, CIDR, regex (Runtime stages) | built; files live in the leader's lookup store as Git LFS pointers |
| Data dictionary: a pipeline's record as an Elasticsearch mapping (Compiler, Runtime stages) | built. A mapping is per pipeline; an index fed by several is merged by hand |
| Non-JSON output: Avro, CSV (Scope) | built. Avro (`io::avro`): to Kafka and Event Hubs framed as Confluent frames it, each route's record registered under `<topic>-value`; to object stores and SFTP as container files holding their schema. CSV (`io::csv`): to object stores and SFTP, a column per leaf of the route's record under a header row. Not built: CSV to any other destination; Avro over HTTP or to Splunk; lz4 in a container file, which Avro has no codec for |

### Destinations

| area | status |
|---|---|
| Elasticsearch via the Bulk API, `docId` policies (Runtime stages) | built, on the HTTP destination core (`io::http`) |
| Splunk HEC: event or raw endpoint, indexer acknowledgement (Runtime stages) | built (`io::splunk`) |
| Event Hubs (`eventHubs`), through its Kafka endpoint (Runtime stages, Config plane, Security) | built, as a Kafka destination signed in with a connection string, a service principal, the host's managed identity or, in an AKS pod, its workload identity. Not built: the managed identities of App Service, Container Apps and Functions, which Azure's instance metadata service does not serve; a live test of Entra sign-in, which the emulator lacks. Compression is none or gzip, which only the Premium and Dedicated tiers take; the emulator refuses gzip, so the live fuzzer checks it only as refused |
| HTTP (`http`) (Runtime stages, Config plane, Secrets boundary) | built (`io::http_post`) |
| Log Analytics workspace (`logAnalytics`), through Azure Monitor's Logs Ingestion API (Runtime stages, Config plane, Security) | built, on the HTTP destination core (`io::http_post`), signed in with Entra as a service principal, the host's managed identity or, in an AKS pod, its workload identity (`io::entra`), with its Destinations page form. Not built: a stream chosen per row; the managed identities of App Service, Container Apps and Functions |
| Prometheus remote_write 1.0 and 2.0 (`prometheus`) (Runtime stages, Config plane, Secrets boundary) | built (`io::remote_write`). Not built: sharding by series; SigV4 or OAuth sign-in |
| Object stores: S3, GCS, Azure Blob (Runtime stages, Config plane) | built (`io::files`, `io::objects`). A batch sent again after a failure may leave a duplicate object |
| SFTP (`sftp`) (Runtime stages, Config plane, Security) | built, on the object store destinations. A write cut off may leave its hidden temporary file |
| Destination formats: JSON, Parquet, Avro, text, bytes (Runtime stages, Config plane) | built (`Payload`, `io::avro`) |
| Tables: Iceberg through a REST catalog, Delta path tables and Unity Catalog external tables (Runtime stages, Config plane, Secrets boundary) | built (`io::iceberg`, `io::delta`). Not built: Iceberg format version 3 (row lineage) and encrypted tables; Delta tables Unity Catalog manages; a live test against a Databricks workspace; aborting stale multipart uploads under a table |

### Sources

| area | status |
|---|---|
| Kafka, start and stop positions; changing `start` moves the source (Runtime stages, Config plane) | built |
| Avro read into declared fields, none or more, every undeclared top-level field kept as the root's unclaimed: from Kafka and Event Hubs with each writer's schema from a schema registry, from object sources as container files (Runtime stages, Config plane, Compiler) | built (`io::avro`). A container file's values are decoded twice, once to find where each ends. The GUI edits the format, the registry and the fields, and declares the fields from a capture's writer schema (`io::avro::fields_of`). Not built: Protobuf and JSON Schema |
| Event Hubs (`eventHubs`), through its Kafka endpoint (Runtime stages, Config plane, Security) | built, signed in as the destination is |
| SQS/S3 (`s3Sqs`), Queue/Azure Blob (`azureBlobQueue`), Pub/Sub/GCS (`gcsPubSub`) (Runtime stages, Config plane) | built, on the object source core (`io::object_source`). Pub/Sub reports no lag |
| Listing sources: `s3List`, `gcsList`, `azureBlobList`, `sftp` (Runtime stages, Config plane) | built (`io::listing`). Not built: a key `object_store` cannot parse fails every listing of its prefix until renamed; delete and move are not conditional, so a key overwritten between the ETag check and the delete loses its new version; SFTP does not see a file rewritten within the same second at the same size |
| HTTP API (`httpApi`) and Okta System Log (`oktaLogs`) (Runtime stages, Config plane, Secrets boundary) | built, on the API poller core (`io::api`). HTTP API asks with a GET or a POST of a fixed JSON body, and signs in as an app with OAuth client credentials, a secret or a private key, at a token URL or through an OpenID issuer (`io::app_sign_in`) |
| PostgreSQL (`postgres`) (Runtime stages, Config plane, Control plane, Secrets boundary) | built, on the SQL source engine (`io::sql_source`), with its website page. Not built: offering `:last` as a one-click fix, SQL Server and Oracle |
| MySQL and MariaDB (`mysql`) (Runtime stages, Config plane, Control plane, Secrets boundary) | built, on the SQL source engine through its MySQL driver (`io::mysql`), with its Sources page form and its website page |
| HTTP receiver (`httpReceiver`) (Runtime stages, Config plane, Control plane, Security) | built, on the receiver core (`io::receiver`); socket hand-over across runtimes is unix only. A worker's heartbeat names where each receiver it runs listens, and its detail on Status shows it |
| HEC receiver (`hecReceiver`) (Runtime stages, Config plane, Security) | built |
| S2S receiver (`s2sReceiver`) (Runtime stages, Config plane, Security) | built. Not built: heartbeats to the forwarder |
| OTLP receiver (`otlpReceiver`): logs, metrics, traces over HTTP and gRPC (Runtime stages, Config plane, Security) | built |
| Prometheus receiver (`prometheusReceiver`), remote_write 1.0 and 2.0 (Runtime stages, Config plane, Security) | built |
| Syslog receiver (`syslogReceiver`), TCP, TLS and UDP (Runtime stages, Config plane, Security) | built; delivery is best-effort, as syslog has no acknowledgement |
| Elasticsearch receiver (`elasticReceiver`), the Bulk API (Runtime stages, Config plane, Security) | built. Not built: `update` and `delete`, the single-document paths such as `/{index}/_doc`, `filter_path` |
| Single-worker sources: holds and progress (Control plane, Runtime stages) | built (`worker::held`, `leader::holds`) |
| Text encoding at ingest, `auto` detection (Runtime stages) | built |
| The record object, `RecordFields`, `SourceMeta` (Runtime stages, Compiler) | built. No source declares typed fields yet, and a Parquet destination refuses a typed record no pipeline ran on. The ack direction (per-source nack, non-Kafka commit shapes) is not built |

### Runtime

| area | status |
|---|---|
| Delivery: routes, exception answers, `Unmatched`/`Unprocessable`/`Refused` (Runtime stages) | built |
| Reader/transform split, `Lane`s (Runtime stages) | built |
| Egress: one thread per destination behind a `bufferBytes` queue (Runtime stages) | built |
| Commit tracker, `commit.rs` (Runtime stages) | built |
| A worker sized to its memory, `worker::memory::fit` (Runtime stages) | built. Lookup tables are not counted: they are mapped files |
| `MemoryBudget`, split on refusal (Runtime stages) | built; chunked materialization not built |
| A batch run in slices sized to the CPU's L2, `cpu_cache` (Runtime stages) | built on Linux and Windows |
| Bundle format, `Call`, shared lookup tables (Runtime stages) | built |
| Shared grok patterns (Config plane) | built (`grok-patterns.json`) |
| Kafka client settings, derived sizes, the property passthrough (Control plane) | built |
| Kafka sources share a client (Runtime stages) | built (`io::kafka::Clients`) |
| A worker's zone, handed to connectors that can prefer their own zone (Config plane) | built for Kafka consumers, as their rack, and for Elasticsearch, Splunk HEC and HTTP POST destinations, as an address per zone, and for an Elasticsearch destination that sniffs, as the nodes it sends to first |
| A receiver's listen address, from its worker (Config plane) | built |
| A Kafka consumer's static membership, from its worker (Config plane) | built for Kafka; Event Hubs sources join as dynamic members |
| Kafka topic patterns: a topic that starts with `^` (Runtime stages) | built |
| Kafka record time: the time Kafka stamped a record, as `__source.time` (Runtime stages) | built |
| Authenticating to sources and destinations (Control plane) | Kafka built (TLS, SASL/PLAIN, SASL/SCRAM, OAuth/OIDC, Kerberos/GSSAPI); Elasticsearch `auth` only |
| Self-observability: `Metrics` source, logs to the console, the leader and a `smeltr logs` source (Observability) | built |
| A worker's metrics endpoint: `GET /metrics` in Prometheus's text format, with optional TLS, client certificates and basic auth (Observability, Security) | built (`worker::metrics_endpoint`, `metrics::exposition`). Not built: the leader listing workers for Prometheus's HTTP service discovery |

### Control plane

| area | status |
|---|---|
| Worker groups, registry, heartbeat, `/api/health`, Cluster page (Control plane) | built; the Cluster page is not the Status area `gui-design.md` describes. Windows does not read a container's job-object limit |
| A default worker group (Control plane) | built |
| Worker↔leader authentication, TLS, enrollment codes, authority rotation for every certificate source (Security) | built |
| A certificate ordered from a certificate authority over ACME and renewed (Security) | built: TLS-ALPN-01, HTTP-01, and DNS-01 through Cloudflare |
| One list of certificates beside the secrets, the leader serving straight from it (Security) | built (`leader::newest_certificate`, `/api/certificates`, `tls::serve`), and identities name a certificate from it by name and version. The leader issues entries under its own authority and renews them unattended (`leader::renew_issued`), each ACME order is a version of the `acme` entry (`acme::list`), a start that cannot serve the chosen version serves the leader's own (`tls::unserved`), and an identity on an older version warns (`warnings::identities_behind`). Not built: the leader's own authority's certificate as an entry |
| Config distribution (Control plane) | built; serving smart-HTTP still spawns `git`, so the leader needs it installed. A corrupt worker clone is never re-cloned |
| One repository, one `main`, a subtree per group and `shared/` (Control plane) | built |
| Upgrades (Control plane) | built. Not built: config file converters (there is one `system.json` version), the *Upgrade config files* action, and a supported floor |
| Damaged stores (Control plane) | built. Not built: a copy taken while the leader runs, and rebuilding `config.git` from the host or a worker's clone |
| Worker recovery: a supervisor, the deployment in a child process (Control plane) | built. Not built: terminating a candidate that hangs during preparation |
| Worker process and offline behavior (Control plane) | built |
| The deployed pointer is what the fleet runs; rollback holds (Control plane) | built |
| Following a git remote (Config plane) | built (`leader::remote`) |
| Worker restart, shut down and remove (Control plane, S2) | built |
| All-in-one deployment (Control plane) | built |
| Branches, review, deploy, rollback (Config plane) | built, API and GUI |
| Blast radius: an editor's connections (A5, `gui-design.md`, Connections) | built (`config_file::Links`, `leader::connections`) |
| GUI pipeline editing with sample data (Control plane) | built |
| Samples and tests as bundle files (Control plane) | built |
| Capture at a source and at a destination; route preview (Control plane, Runtime stages) | built. A reachability check is not built |
| Routing what arrives (`gui-design.md`, Routing what arrives) | built: the map's tags and panel, the Routes page's new route, and the workspace's capture with its parser proposed (`leader::onboard`). A line of a format with no whole-line Grok pattern is not proposed |
| Warnings (Control plane) | built (`leader::warnings`), with notices and a source's volume against its normal (`leader::throughput`). Not built: a weekly normal, which needs more history than the series keeps |
| A newer release (Control plane, Upgrades) | built (`leader::updates`, through smeltr.sh's `website/worker`), a notice and Admin -> System -> General |
| Metrics history (Control plane, E2) | built |
| Streams: what each Parser produces and what arrives unmatched, summed on the leader (Observability) | built, `GET /api/streams`, which the map, the pipeline editor's declared fields and the drift warnings (`leader::warnings`) read. Retention in `leader::series` |
| Lag (Control plane, E6) | built |
| The map (Control plane) | built (`leader::map`), stacks included |
| The GUI from a phone to a 4K display; four text registers (Control plane) | built |
| A lookup path relative to its bundle (Config plane) | built |
| Credentials in the leader store, by name and version (Secrets boundary) | built |
| The secrets encryption key: written at first start, a failure while it is missing or does not fit, regenerated on Admin › Secrets › Encryption key (Secrets boundary) | built (`leader::secrets`, `Store::key_standing`) |
| Identity: local accounts, OIDC, roles per area (Security) | built. File permissions on `leader.db` are set on unix only; on Windows they are whatever the data directory inherits |
| The AI agent as a principal: a person's agent permissions (Security) | built: agent permissions and the gate's judgment of an agent's requests (`access::AgentPermissions`, `access::ByAgent`), and their screen: Agent permissions from a person's name. The model endpoint: Admin › System › AI agents turns them on, and the leader's one client speaks Anthropic's Messages API, OpenAI's Responses API and OpenAI-compatible Chat Completions (`leader::model`). The agent: a person's conversations with it over `/api/agent/conversations`, each turn's calls sent through the gate to the routes its catalog names, and a conversation deleted once nobody has added to it for the days set (`leader::agent`), and the Agent page, from a person's name, where they talk to it and see each request it made, and its answer as the model writes it. Past three quarters of the model's context, which an administrator sets and Anthropic's Models API fills in, a summary stands in for a conversation's older turns: Anthropic's and OpenAI's compaction, or one the leader asks an OpenAI-compatible model for. Each message may pick any model the stored key reaches and an effort, which its answer keeps and the conversation carries on with (`/api/agent/models`); OpenAI's requests carry the conversation as their cache key and a hash of the person as their safety identifier. A circle beside the model shows how full the context is, and from it a person summarizes the conversation now or clears it: the model reads on from that summary, or from nothing, while the page still shows it all. The conversation also opens in a panel along the bottom of any page. The user docs this build shipped with, as Markdown, searched and read by section over `/api/docs` (`leader::docs`, written from the website's build by `just docs-markdown`) |
| License page (Control plane) | built: Admin › System › License states who must buy a license; no feature needs one |
| Release | built, and on Windows a service (`src/bin/smeltr/service.rs`) working in `%ProgramData%\Smeltr` and logging to `smeltr.log` there. Not built: public downloads of the archives, an MSI, code signing |

## Known gaps against the architecture

### Control plane

`main` is reconciled with, not owned (Runtime stages): a deployed commit
smeltr did not make is not detected.

**Deleting a group does not look at its workers** (S3). Once the delete
deploys, a worker still serving the group runs nothing and heartbeats with a
`groupNotDeployed` warning until it is moved or shut down. Refusing the
deploy while the group has live workers would close it.

**An enrollment code does not carry the leader's address.** The Workers page
fills one in from where the browser reached the leader. A leader reached over
a bastion offers an address its workers cannot use.

**What normal is has no answer yet** (E3). The map's color channel and the
catalog's volume entry both wait on it.

### Worker recovery

- **A non-worker leader keeps its own logs only in its own store.** Nothing ships them anywhere else: no syslog emitter, and no worker to run a smeltr logs source.
- **A tree confirmed dead by its pipe's end-of-file assumes every member still holds that pipe.** A descendant that closed its copy before wedging is not waited on, so `kill_and_confirm` can report a tree gone while it remains. No runtime spawns a descendant today.
- **A capture invitation can be routed to a runtime about to be replaced.** `route_invitations` acts before the same beat's redeploy decision, so a capture racing a redeploy can be lost.
- **A heartbeat that times out after the leader persists it can inflate a durable counter.** Closing it needs an idempotency key across the heartbeat protocol.
- **A worker's bare clone is never repacked.** `gix` has no maintenance primitive, so a worker redeployed often accumulates packs without bound.
- **A runtime releases a capture session once it writes the report to its own pipe, not once the leader has it,** so a slow upload can see the session re-admitted and re-captured. Closing it needs an acknowledgment frame back from the supervisor.
- **A runtime confirmed dead with a descendant holding its output pipe is reported as still running.** `RuntimeHandle::exited` does not tell "alive" from "gone, tree unconfirmed".
- **The heartbeat's body limit assumes a worst case nothing enforces.** `thread_limit` and the reader count have no ceiling, so a large deployment's report can outgrow `HEARTBEAT_BODY_LIMIT`. Closing it needs per-beat windowing for metrics.
- **Capture sessions are retained without a cap; only the outbox (`CAPTURES_WAITING`) is capped.**
- **The deployment warning's grace does not cover the stop it waits through.** The stop and startup budgets add up to exactly `NOT_DEPLOYING_AFTER_MS`, leaving no slack.
- **A capture queued while its own upload is in flight is uploaded twice.**
- **A capture with a start other than the latest can hold a record twice.** Workers join its consumer group a beat apart, and nothing commits, so a partition the rebalance moves is read again from the start. The leader does not drop a record two workers sent.
- **A queued capture invitation can be served while the worker cannot reach its leader.**
- **An offline stop is not queued while a runtime is starting.**
- **A supervisor spawns whatever binary now sits at its own path,** so a rollback to a build that does not read `SMELTR_RUNTIME_CHILD` times out with nothing to say why. Undecided between a preserved image and a version exchanged at startup.

### Runtime and compiler

- **Chunked materialization is not built.** `MemoryBudget`'s split stops at a single record.
- **Some memory parts report no fill:** a Kafka client's queue, which only librdkafka's statistics measure, and a destination's senders, whose size is an estimate.
- **Two steps are not yet code** (Compiler). An `if` step compiles from `Step::If` rather than lowering to a code `if`. There is no `delete` step.
- **Unroll's output is not bounded at hand-over.** One poll's records can fan out without bound, and a turn's whole output for a destination is handed over in one call; only `MemoryBudget` bounds it.
- **`overwrite` is not ordered across in-flight batches.** Two versions of one `_id` can land in either order.
- **Elasticsearch: not attempted.** Index templates and ILM, `update`/`upsert` actions, OpenSearch-specific behavior.
- **The registry never evicts a retired thread** of a permanent worker, and `thread_id` is whatever the caller sends, so one credential can grow the map.
- **A decode exception is not counted:** `GuessedEncoding` and `InvalidUtf8` mark the row, but no counter rates them per source.
- **Mutual recursion and escaping closures** (Language) are outside the language.
- **Scaling past one bundle** (Scope): nothing tells an operator a source is a consumer group rather than a topic.
- **Browser tests reach only what a user story names,** and none reads Kafka or Elasticsearch.
- **The array functions take an array of scalars** (Language). `slice`, `remove`, `resize`, `concat`, `+`, `includes`, `indexOf` and `join` refuse an array of arrays or of objects.

### GUI

- **A finished capture names neither its source nor its filter.**
- **A finished capture's workers stay "filling"** (`AddSampleDialog.svelte`) until the capture is saved or discarded.
- **A capture open when a batch blocks holds its blocking records twice:** once as read, and once as held, with why.
- **A capture of a blocked source shows only the records that blocked first:** a batch stops at the first slice and route that blocks, so records a later one would have blocked are not shown.
- **A row a destination refused and keeps names no route** in a capture at it: egress knows the row, not the route that sent it.
- **A captured record shows only its start,** so for syslog the text a filter matched is not visible.
- **Route preview names a route by its position** (`RoutePreviewPage.svelte`). Routes have ids and no order.
- **History clips its last column at a 1440 px window.**
- **A worker's id is `hostname-pid-nanos`** (`worker::generate_worker_id`). No setting names a worker.
- **A leader started without a worker still seeds a worker group,** which a fleet that names its own groups carries empty until someone deletes it.
