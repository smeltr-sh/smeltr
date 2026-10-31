//! Every integration test, as one binary. Each test binary links the whole dependency tree, so fifty-odd of them
//! filled `target/` past a cloud session's disk; one links it once. The tests need nextest's process per test, so run
//! the whole suite with nextest. `cargo test` runs one module: `cargo test --test integration language_tests::`.
//!
//! The `[[test]]` targets beside this one in `Cargo.toml` are built under the `dist` or `release` profile to measure,
//! and stay binaries of their own so a measurement does not build every test with full LTO.

mod certs;
mod harness;
mod live;

#[path = "acme-live-tests.rs"]
mod acme_live_tests;
#[path = "adversarial-memory-tests.rs"]
mod adversarial_memory_tests;
#[path = "api-source-fuzz.rs"]
mod api_source_fuzz;
#[path = "api-source-tests.rs"]
mod api_source_tests;
#[path = "code-tests.rs"]
mod code_tests;
#[path = "compile-fuzz.rs"]
mod compile_fuzz;
#[path = "compile-tests.rs"]
mod compile_tests;
#[path = "config-file-tests.rs"]
mod config_file_tests;
#[path = "crypto-provider-tests.rs"]
mod crypto_provider_tests;
#[path = "csv-parser-tests.rs"]
mod csv_parser_tests;
#[path = "delta-live-fuzz.rs"]
mod delta_live_fuzz;
#[path = "delta-live-tests.rs"]
mod delta_live_tests;
#[path = "egress-tests.rs"]
mod egress_tests;
#[path = "elastic-fuzz.rs"]
mod elastic_fuzz;
#[path = "elastic-live-tests.rs"]
mod elastic_live_tests;
#[path = "elastic-receiver-fuzz.rs"]
mod elastic_receiver_fuzz;
#[path = "elastic-sniff-tests.rs"]
mod elastic_sniff_tests;
#[path = "elastic-tests.rs"]
mod elastic_tests;
#[path = "elastic-tls-tests.rs"]
mod elastic_tls_tests;
#[path = "enrollment-tests.rs"]
mod enrollment_tests;
#[path = "eval-tests.rs"]
mod eval_tests;
#[path = "event-hubs-live-fuzz.rs"]
mod event_hubs_live_fuzz;
#[path = "exec-tests.rs"]
mod exec_tests;
#[path = "file-tests.rs"]
mod file_tests;
#[path = "fuzz-untrusted-input.rs"]
mod fuzz_untrusted_input;
#[path = "gate-tests.rs"]
mod gate_tests;
#[path = "git-remote-live-tests.rs"]
mod git_remote_live_tests;
#[path = "grok-parser-tests.rs"]
mod grok_parser_tests;
#[path = "gui-build-inputs-tests.rs"]
mod gui_build_inputs_tests;
#[path = "heartbeat-retry-tests.rs"]
mod heartbeat_retry_tests;
#[path = "hec-receiver-fuzz.rs"]
mod hec_receiver_fuzz;
#[path = "http-fuzz.rs"]
mod http_fuzz;
#[path = "http-post-tests.rs"]
mod http_post_tests;
#[path = "http-receiver-fuzz.rs"]
mod http_receiver_fuzz;
#[path = "iceberg-live-fuzz.rs"]
mod iceberg_live_fuzz;
#[path = "iceberg-live-tests.rs"]
mod iceberg_live_tests;
#[path = "json-parser-tests.rs"]
mod json_parser_tests;
#[path = "kafka-avro-fuzz.rs"]
mod kafka_avro_fuzz;
#[path = "kafka-destination-fuzz.rs"]
mod kafka_destination_fuzz;
#[path = "kafka-fuzz.rs"]
mod kafka_fuzz;
#[path = "kafka-tests.rs"]
mod kafka_tests;
#[path = "kafka-tls-tests.rs"]
mod kafka_tls_tests;
#[path = "kerberos-tests.rs"]
mod kerberos_tests;
#[path = "kv-parser-tests.rs"]
mod kv_parser_tests;
#[path = "language-tests.rs"]
mod language_tests;
#[path = "leader-accounts-tests.rs"]
mod leader_accounts_tests;
#[path = "leader-tests.rs"]
mod leader_tests;
#[path = "log-analytics-fuzz.rs"]
mod log_analytics_fuzz;
#[path = "logs-carry-no-password.rs"]
mod logs_carry_no_password;
#[path = "logs-delivery.rs"]
mod logs_delivery;
#[path = "logs-delivery-guard.rs"]
mod logs_delivery_guard;
#[path = "logs-fields.rs"]
mod logs_fields;
#[path = "logs-leader-local.rs"]
mod logs_leader_local;
#[path = "logs-shutdown.rs"]
mod logs_shutdown;
#[path = "logs-source-tests.rs"]
mod logs_source_tests;
#[path = "lookup-tests.rs"]
mod lookup_tests;
#[path = "memory-budget-tests.rs"]
mod memory_budget_tests;
#[path = "metrics-source-tests.rs"]
mod metrics_source_tests;
#[path = "mysql-live-tests.rs"]
mod mysql_live_tests;
#[path = "nested-tests.rs"]
mod nested_tests;
#[path = "object-source-fuzz.rs"]
mod object_source_fuzz;
#[path = "object-source-tests.rs"]
mod object_source_tests;
#[path = "objects-fuzz.rs"]
mod objects_fuzz;
#[path = "objects-live-tests.rs"]
mod objects_live_tests;
#[path = "objects-tests.rs"]
mod objects_tests;
#[path = "okta-live-tests.rs"]
mod okta_live_tests;
#[path = "ops.rs"]
mod ops;
#[path = "otlp-receiver-fuzz.rs"]
mod otlp_receiver_fuzz;
#[path = "parquet-tests.rs"]
mod parquet_tests;
#[path = "pipeline-integration-tests.rs"]
mod pipeline_integration_tests;
#[path = "pipelined-tests.rs"]
mod pipelined_tests;
#[path = "postgres-live-tests.rs"]
mod postgres_live_tests;
#[path = "prometheus-receiver-fuzz.rs"]
mod prometheus_receiver_fuzz;
#[path = "rebuild-fuzz.rs"]
mod rebuild_fuzz;
#[path = "receiver-tests.rs"]
mod receiver_tests;
#[path = "remote-write-fuzz.rs"]
mod remote_write_fuzz;
#[path = "remote-write-tests.rs"]
mod remote_write_tests;
#[path = "role-tests.rs"]
mod role_tests;
#[path = "runtime-tests.rs"]
mod runtime_tests;
#[path = "s2s-live-tests.rs"]
mod s2s_live_tests;
#[path = "s2s-receiver-fuzz.rs"]
mod s2s_receiver_fuzz;
#[path = "s2s-receiver-tests.rs"]
mod s2s_receiver_tests;
#[path = "split-threads-tests.rs"]
mod split_threads_tests;
#[path = "splunk-fuzz.rs"]
mod splunk_fuzz;
#[path = "splunk-live-tests.rs"]
mod splunk_live_tests;
#[path = "splunk-tests.rs"]
mod splunk_tests;
#[path = "sql-live-fuzz.rs"]
mod sql_live_fuzz;
#[path = "stdlib-tests.rs"]
mod stdlib_tests;
#[path = "store-access.rs"]
mod store_access;
#[path = "streams-tests.rs"]
mod streams_tests;
#[path = "syslog-receiver-fuzz.rs"]
mod syslog_receiver_fuzz;
#[path = "syslog-receiver-tests.rs"]
mod syslog_receiver_tests;
#[path = "website-reference-tests.rs"]
mod website_reference_tests;
#[path = "worker-tests.rs"]
mod worker_tests;
