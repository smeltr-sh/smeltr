//! The routes the AI agent can call, as its system prompt describes them (`leader::agent`).
//!
//! Every gated route is here or in [`LEFT_OUT`], which a test holds to the router. What is left out is what an agent
//! cannot send as a JSON request, or what is the person's own: signing in and out, and their conversations.

use smeltr::leader::agent::Route;

pub const CATALOG: &[Route] = &[
    // -- config --
    Route {
        method: "GET",
        path: "/api/areas/:area",
        what: "The whole bundle of one area on a branch, and the commit it was read at. Query: branch, at.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/unreadable",
        what: "Files in the area that do not parse, each with why and whether it can be deleted. Query: branch, at.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/files/*path",
        what: "One file's raw text and commit, by its path in the area (unreadable lists shared/... or groups/<id>/...: the rest after the area). Query: branch.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/files/*path",
        what: "Overwrite one file's raw text on a branch (repairs an unreadable file). Query: branch.",
        body: "{ text: string }",
    },
    Route {
        method: "DELETE",
        path: "/api/areas/:area/files/*path",
        what: "Delete one file on a branch. Query: branch.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/sources",
        what: "Replace the area's whole sources list on a branch. Query: branch.",
        body: "SourceDto[] (each { id, type, ...kind fields }, as GET /api/areas/:area returns sources)",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/destinations",
        what: "Replace the area's whole destinations list on a branch. Query: branch.",
        body: "DestinationDto[] (each { id, exceptions?, type, ...kind fields }, as GET /api/areas/:area returns them)",
    },
    Route {
        method: "POST",
        path: "/api/preview/certificate",
        what: "Describe the certificates in a PEM (subject, expiry, fingerprint). No query.",
        body: "{ pem: string }",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/routes",
        what: "Replace the area's whole routes list on a branch. Query: branch.",
        body: "{ id, source, condition?, pipeline, deliver?: { when?, to: string[] }[], exceptions?: OnException }[]",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/grok-patterns",
        what: "Replace the area's named grok patterns on a branch. Query: branch.",
        body: "{ id: string, pattern: string }[]",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/lookups",
        what: "Replace the area's lookup tables on a branch; a CSV table may carry its file's new text. Query: branch.",
        body: "({ id, type: 'csv', path, delimiter, headers? } | { id, type: 'inline', headers, rows } | { id, type: 'mmdb', path }) & { contents?: string }[]",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/manifest",
        what: "Set a group's answer to every exception kind (refused for shared). Query: branch.",
        body: "{ exceptions?: { categories?: {[k]: answer}, kinds?: {[k]: answer}, tagField?: string } | null }",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/pipelines",
        what: "The area's pipelines on a branch. Query: branch.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/pipelines/:id",
        what: "One pipeline's config. Query: branch.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/pipelines/:id",
        what: "Create or replace a pipeline by id on a branch; the path's id wins. Query: branch.",
        body: "{ id: string, name: string, steps: StepDto[] (the pipeline's steps, as GET returns them) }",
    },
    Route {
        method: "DELETE",
        path: "/api/areas/:area/pipelines/:id",
        what: "Delete a pipeline on a branch. Query: branch.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/pipeline-callers",
        what: "Map of pipeline id to the pipelines that call it directly. Query: branch.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/:section/:id/connections",
        what: "What a change to one object reaches in each group that runs it; section is sources, routes, pipelines, destinations, lookups or grok-patterns. Query: branch.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/areas/:area/undo",
        what: "Put one object back as it was where the branch left main, as one save. Query: branch.",
        body: "{ section: 'sources'|'destinations'|'routes'|'lookupTables'|'grokPatterns'|'pipelines', id: string }",
    },
    Route {
        method: "POST",
        path: "/api/areas/:area/preview/pipeline",
        what: "Run a draft bundle's pipeline over a saved sample and show the rows; nothing is written. Query: branch.",
        body: "{ config: string (bundle JSON as a string), pipeline?: id, sample?: id, show_internal?: bool, at_micros?: number }",
    },
    Route {
        method: "POST",
        path: "/api/areas/:area/preview/routes",
        what: "Route a saved sample through a draft bundle's routes and pipelines; nothing is written. Query: branch.",
        body: "{ config: string (bundle JSON as a string), sample: id, at_micros?: number }",
    },
    Route {
        method: "POST",
        path: "/api/preview/grok",
        what: "List the capture names grok patterns declare, with no sample. No query.",
        body: "{ patterns: string[], customPatterns?: { name, pattern }[], grokPatterns?: { id, pattern }[] }",
    },
    Route {
        method: "POST",
        path: "/api/preview/line-breaker",
        what: "Check whether a save would accept this line-breaker pattern. No query.",
        body: "{ pattern: string }",
    },
    Route {
        method: "POST",
        path: "/api/preview/format",
        what: "Lay pipeline code out in the house style; code is null where it does not parse. No query.",
        body: "{ code: string, kind: 'expr'|'block' }",
    },
    Route {
        method: "POST",
        path: "/api/preview/onboard",
        what: "Propose first steps and a route condition for captured records. No query.",
        body: "{ records: object[], filed?: object[] (each record's __source) }",
    },
    Route {
        method: "POST",
        path: "/api/preview/sql",
        what: "Show the SQL statements an SQL source would run (first, later, run) and their binds. No query.",
        body: "{ source: SourceDto (an SQL source) }",
    },
    // -- branches and changes --
    Route {
        method: "GET",
        path: "/api/branches",
        what: "Every branch except main, with its change, base and remote push state, filtered to what the caller sees. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/branches",
        what: "Create a branch off main. No query.",
        body: "{ name: string }",
    },
    Route {
        method: "DELETE",
        path: "/api/branches/:name",
        what: "Delete a branch. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/branches/:name/diff",
        what: "A branch's changed files and unified diff against main. Query: path (one file).",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/main/diff",
        what: "Files and unified diff between two commits. Query: from, to, path.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/main/history",
        what: "main's history newest first, with the deployed commit and any hold. Query: limit (default 50, max 500).",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/open",
        what: "Open a branch for review against main; refused if the branch changes nothing. No query.",
        body: "{ title: string, description?: string }",
    },
    Route {
        method: "PATCH",
        path: "/api/branches/:name",
        what: "Retitle an open change. No query.",
        body: "{ title: string, description?: string }",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/close",
        what: "Close a change without deploying; the branch stays. No query.",
        body: "{ reason?: string } (body optional)",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/comments",
        what: "Comment on a change, optionally anchored or as a reply. No query.",
        body: "{ body: string, anchor?: { kind:'object', scope, section, objectId, step? } | { kind:'line', path, line }, replyTo?: commentId }",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/threads/:id",
        what: "Resolve or reopen the thread that comment :id started. No query.",
        body: "{ resolved: boolean }",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/reviews",
        what: "Approve a change or request changes; needs deployer over everything the change touches. No query.",
        body: "{ verdict: 'approve'|'request-changes', body?: string }",
    },
    Route {
        method: "GET",
        path: "/api/branches/:name/timeline",
        what: "A change's events oldest first: opening, renames, comments, saves, closes. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/bring-current",
        what: "Merge main into the branch; on conflict it writes nothing and returns the conflicts. No query.",
        body: "{ choices?: { main: commit, work: commit, picks: { path, id?, side: 'main'|'work' }[] } }",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/deploy",
        what: "Approve and deploy: squash the branch onto main (or push to the remote), point the fleet at it, drop the branch. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/main/deploy",
        what: "Deploy a commit already in main's history (this is also rollback). No query.",
        body: "{ commit: string, confirm?: boolean (deploy even if a group does not load) }",
    },
    // -- captures and samples --
    Route {
        method: "POST",
        path: "/api/captures",
        what: "Open a capture of one source or at one destination in a group; returns the session id and clamped bounds. No query.",
        body: "{ group, source?: SourceDto, destination?: id, filter?, start?: StartPosition, maxMatches?, maxBytes?, deadlineSecs? }",
    },
    Route {
        method: "GET",
        path: "/api/captures/:session",
        what: "A capture's status: workers, batches, end reasons, time left, truncated. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/captures/:session/stop",
        what: "End a running capture and keep what it holds. No query.",
        body: "",
    },
    Route {
        method: "DELETE",
        path: "/api/captures/:session",
        what: "Give up on a capture without saving; answers { ok }. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/samples",
        what: "The area's sample summaries (id, name, source, record count). Query: branch.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/areas/:area/samples",
        what: "Save a finished capture as a sample on a branch and forget the session. Query: branch.",
        body: "{ capture: session, id?: string, name: string, source?: string (required for a source capture) }",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/samples/:id",
        what: "Create or replace a sample on a branch; the path's id wins. Query: branch.",
        body: "{ name, source?, columns?: CaptureColumn[], records: object[] }",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/samples/:id",
        what: "One sample with its records. Query: branch, at.",
        body: "",
    },
    Route {
        method: "DELETE",
        path: "/api/areas/:area/samples/:id",
        what: "Delete a sample on a branch. Query: branch.",
        body: "",
    },
    // -- lookups --
    Route {
        method: "GET",
        path: "/api/areas/:area/lookups/file",
        what: "A CSV lookup file's text and commit (path must be under lookups/). Query: branch, path, at.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/lookups/mmdb",
        what: "What an MMDB lookup file says about itself, and the record for ip when given. Query: branch, path, ip, at.",
        body: "",
    },
    // -- tests --
    Route {
        method: "GET",
        path: "/api/areas/:area/tests",
        what: "The area's test summaries. Query: branch.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/areas/:area/tests/:id",
        what: "Lock (create or replace) a test on a branch; the path's id wins. Query: branch.",
        body: "{ name, pipeline?: id (absent = end-to-end by sample source), sample: id, lockedAt: micros, rowsIn: number, expected: (object|null)[] }",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/tests/:id",
        what: "One test. Query: branch, at.",
        body: "",
    },
    Route {
        method: "DELETE",
        path: "/api/areas/:area/tests/:id",
        what: "Delete a test on a branch. Query: branch.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/areas/:area/test-runs",
        what: "Run the area's tests on a branch and report each result; nothing is written. Query: branch.",
        body: "{ ids?: string[] (empty = all), touched?: boolean (narrow to what the change reaches) }",
    },
    Route {
        method: "GET",
        path: "/api/branches/:name/tests",
        what: "The test summaries of every area the branch's change touches. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/test-runs",
        what: "Run the tests of every area the branch's change touches and report each result; nothing is written. No query.",
        body: "{ ids?: string[] (empty = all), touched?: boolean (narrow to what the change reaches) }",
    },
    // -- cluster and workers --
    Route {
        method: "GET",
        path: "/api/cluster",
        what: "The cluster page: deployed commit, worker groups, workers, health, filtered to the caller. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/map",
        what: "The fleet's compiled topology for a branch as nodes and edges by group, filtered to visible groups. Query: branch.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/map/measurements",
        what: "What the map's nodes report now, plus the branch commit to spot a stale topology. Query: branch.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/areas/:area/stats",
        what: "One node's counts in a group. Query: branch, kind (source|pipeline|destination), id, windowMinutes (default 10).",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/flow",
        what: "Fleet flow series over a window, filtered to the caller's groups. Query: windowMinutes (default 60, max 30 days).",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/streams",
        what: "Fields each parser produced and unmatched row keys per source, filtered to visible groups. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/warnings",
        what: "The warning catalog, filtered to what the caller sees. `info=true` adds its info entries: facts that \
               ask nothing of anyone.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/logs",
        what: "One page of fleet log rows. Query: level (comma-separated), worker, group, site, q, sinceMillis, untilMillis, limit, before, after.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/health",
        what: "Leader health snapshot; also sweeps forgotten ephemeral workers. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/system",
        what: "Version, release, a newer published release and the last look for one, and the Kafka SASL mechanisms \
               this build offers. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/worker-groups",
        what: "Worker groups on a branch (live or pending), the hue for shared, and the commit. Query: branch.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/worker-groups",
        what: "Create a worker group's subtree on a branch. Query: branch.",
        body: "{ id: string, name: string, hue?: number (degrees) }",
    },
    Route {
        method: "PUT",
        path: "/api/worker-groups/:id",
        what: "Rename a worker group on a branch. Query: branch.",
        body: "{ name: string }",
    },
    Route {
        method: "DELETE",
        path: "/api/worker-groups/:id",
        what: "Delete a worker group's subtree on a branch. Query: branch.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/worker-groups/:id/hue",
        what: "Set a group's (or shared's) display hue right away, with no branch. No query.",
        body: "{ hue: number }",
    },
    Route {
        method: "GET",
        path: "/api/workers",
        what: "The worker roster and credentials. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/workers/:worker_id/revoke",
        what: "Revoke a worker's credential; its single-worker sources move to another worker. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/workers/:worker_id/restart",
        what: "Ask a heartbeating worker to restart its deployment on its next heartbeat. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/workers/:worker_id/shutdown",
        what: "Ask a heartbeating worker to stop and exit on its next heartbeat. No query.",
        body: "",
    },
    Route {
        method: "DELETE",
        path: "/api/workers/:worker_id",
        what: "Remove a worker that is revoked or silent, for good; refused while it heartbeats. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/enrollment-codes",
        what: "Enrollment codes and their state. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/enrollment-codes",
        what: "Mint an enrollment code (returns the secret and a join command). No query.",
        body: "{ groupId?: string, kind?: 'permanent'|'ephemeral', maxUses?: number, lifetimeHours?: number }",
    },
    Route {
        method: "DELETE",
        path: "/api/enrollment-codes/:id",
        what: "Withdraw an enrollment code. No query.",
        body: "",
    },
    // -- accounts and roles --
    Route {
        method: "GET",
        path: "/api/users",
        what: "Every account with its roles. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/users",
        what: "Create a local administrator; returns a one-time temporary password. No query.",
        body: "{ username: string, displayName: string }",
    },
    Route {
        method: "PATCH",
        path: "/api/users/:id",
        what: "Change one account's display name or enabled state, one field per request. No query.",
        body: "{ displayName?: string, enabled?: boolean } (exactly one)",
    },
    Route {
        method: "POST",
        path: "/api/users/:id/reset-password",
        what: "Give an account a new temporary password (returned) and end its sessions. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/users/:id/revoke-sessions",
        what: "End every session an account holds; answers { ok, ended }. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/security-events",
        what: "Account security events, newest first (up to 500). No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/docs",
        what: "Every page of the user docs with its headings, or with q the sections holding every word of q, each with a snippet. Query: q.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/docs/page",
        what: "One page of the user docs as text, or with section one heading's text and what is under it. Query: path (/docs/...), section (a heading's id).",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/me/agent-permissions",
        what: "What the caller lets their own agent use. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/roles",
        what: "Every role by name. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/roles/:name",
        what: "Create a role or replace it whole. No query.",
        body: "{ administrator?: boolean, areas?: { [area]: 'viewer'|'editor'|'deployer' } }",
    },
    Route {
        method: "DELETE",
        path: "/api/roles/:name",
        what: "Delete a role. No query.",
        body: "",
    },
    // -- settings --
    Route {
        method: "GET",
        path: "/api/settings/sessions",
        what: "Session idle and absolute lifetimes in force, with the allowed min and max. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/sessions",
        what: "Set session lifetimes. No query.",
        body: "{ idleSeconds: number, absoluteSeconds: number }",
    },
    Route {
        method: "GET",
        path: "/api/settings/warnings",
        what: "The percent of a source's past windows its expected throughput holds, with the default and allowed min and max. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/warnings",
        what: "Set the percent of a source's past windows its expected throughput holds. No query.",
        body: "{ sourceRowsBandPercent: number }",
    },
    Route {
        method: "GET",
        path: "/api/settings/updates",
        what: "Whether this leader looks for a newer smeltr release once a day. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/updates",
        what: "Turn the daily look for a newer smeltr release on or off. No query.",
        body: "{ check: boolean }",
    },
    Route {
        method: "GET",
        path: "/api/settings/oidc",
        what: "OIDC sign-in settings (whether a secret is set, and the redirect URI). No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/oidc",
        what: "Set OIDC sign-in; turning it on checks the provider first. No query.",
        body: "{ enabled: boolean, issuer: string, clientId: string, clientSecret?: string (absent keeps, '' removes), groupsClaim?: string }",
    },
    Route {
        method: "GET",
        path: "/api/settings/agent",
        what: "AI agent settings (whether an API key is set). No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/agent",
        what: "Set AI agent provider settings; turning on probes the endpoint. No query.",
        body: "{ enabled: boolean, provider: 'anthropic'|'openai'|'openai-compatible', endpoint: string, model: string, apiKey?: string (absent keeps, '' removes) }",
    },
    Route {
        method: "GET",
        path: "/api/settings/workers",
        what: "How long a silent ephemeral worker is kept before it is forgotten. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/workers",
        what: "Set how long a silent ephemeral worker is kept. No query.",
        body: "{ ephemeralForgetAfterSeconds: number }",
    },
    Route {
        method: "GET",
        path: "/api/settings/git-remote",
        what: "Git remote settings and sync state (whether a token is set, poll bounds). No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/git-remote",
        what: "Set or clear the git remote the leader follows and pushes to. No query.",
        body: "{ url: string ('' disconnects), user?: string, token?: string (absent keeps, '' removes), pollSeconds: number (0 = manual), approvePushes?: boolean }",
    },
    Route {
        method: "POST",
        path: "/api/settings/git-remote/sync",
        what: "Fetch and follow the remote now; returns the settings. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/branches/:name/overwrite",
        what: "Force-push a branch the remote refused, leased on the tips the caller was shown. No query.",
        body: "{ tip: commit, hostTip?: commit | null }",
    },
    Route {
        method: "GET",
        path: "/api/settings/tls/acme",
        what: "Ordering the leader's certificate over ACME: the settings, and what the last order left. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/tls/acme",
        what: "Set ordering the leader's certificate over ACME. No query.",
        body: "{ enabled: boolean, directory: url, contact?: string, names?: string[], challenge: 'tls-alpn-01'|'http-01'|'dns-01', authority?: pem, eabKeyId?: string, eabHmac?: string (absent keeps, '' removes), dnsProvider?: 'cloudflare' (absent keeps), cloudflareToken?: string (absent keeps, '' removes) }",
    },
    Route {
        method: "POST",
        path: "/api/settings/tls/acme/order",
        what: "Order the leader's certificate now and serve it. No query.",
        body: "",
    },
    Route {
        method: "GET",
        path: "/api/settings/tls",
        what: "The leader's TLS state: authority, served certificate, whether supplied, any rotation. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/tls",
        what: "Serve a certificate from /api/certificates. No query.",
        body: "{ certificate: string (the certificate's name) }",
    },
    Route {
        method: "POST",
        path: "/api/settings/tls/rotation",
        what: "Start rotating the authority, waiting up to waitHours for workers to hold the new root. No query.",
        body: "{ waitHours: number (1..336), authorityValidityDays?: number }",
    },
    Route {
        method: "DELETE",
        path: "/api/settings/tls/rotation",
        what: "Abandon an open authority rotation. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/settings/tls/rotation/cutover",
        what: "Sign under the incoming authority now; workers without it stop verifying. No query.",
        body: "",
    },
    Route {
        method: "PUT",
        path: "/api/settings/tls/rotation/deadline",
        what: "Move when an open rotation cuts over without waiting for the fleet. No query.",
        body: "{ deadlineMillis: number (after now, no later than the rotation can wait) }",
    },
    // -- secrets --
    Route {
        method: "GET",
        path: "/api/secrets",
        what: "Secret names, custody, versions and where each version is deployed; never values. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/secrets",
        what: "Create a secret. No query.",
        body: "{ name: string, custody: 'worker-local'|'leader-stored', description?: string, value?: string (required for leader-stored) }",
    },
    Route {
        method: "POST",
        path: "/api/secrets/:name/versions",
        what: "Store a new version of a secret's value; prunes versions retention no longer keeps. No query.",
        body: "{ value: string }",
    },
    Route {
        method: "DELETE",
        path: "/api/secrets/:name",
        what: "Delete a secret or a certificate; refused while the deployed bundle or main's head names it, or the leader serves it. No query.",
        body: "",
    },
    Route {
        method: "DELETE",
        path: "/api/secrets/:name/versions/:version",
        what: "Forget one version of a secret; refused while it is deployed or on main. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/secrets/:name/verify",
        what: "Check whether a candidate equals a stored version (5 tries per secret per minute); answers { matches }. No query.",
        body: "{ version: number, candidate: string }",
    },
    Route {
        method: "POST",
        path: "/api/secrets/:name/references",
        what: "On a branch, point every reference to this secret at one version and open a change. No query.",
        body: "{ version: number, branch: string (not main) }",
    },
    Route {
        method: "GET",
        path: "/api/settings/encryption-key",
        what: "Whether the secrets encryption key is held, missing or mismatched, and which values it cannot open. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/settings/encryption-key/regenerate",
        what: "Write a new secrets encryption key in place of a missing or mismatched one; every value it lists must then be entered again. No query.",
        body: "",
    },
    // -- certificates --
    Route {
        method: "GET",
        path: "/api/certificates",
        what: "Certificates: name, description, versions, the newest version's chain described, and kept (issued or acme) where this leader renews it; never a key. No query.",
        body: "",
    },
    Route {
        method: "POST",
        path: "/api/certificates",
        what: "Add a certificate: supplied, with its private key; or issued, which this leader issues under its own authority for the DNS names and IP addresses given, valid for validityDays (365 if left out), and renews on its own. No query.",
        body: "{ kind: 'supplied', name: string, description?: string, certificate: pem, privateKey: pem } | { kind: 'issued', name: string, description?: string, names: string[], validityDays?: number }",
    },
    Route {
        method: "PUT",
        path: "/api/certificates/:name",
        what: "Store a new version of a certificate and its private key; where the leader serves it, the version must be one the leader can serve, and it serves it. No query.",
        body: "{ certificate: pem, privateKey: pem }",
    },
];

#[cfg(test)]
/// Gated paths the agent cannot call: its person's conversations and the models they pick from, a file upload that
/// is no JSON request, and what a person sets for their own agent, which no agent changes.
pub const LEFT_OUT: &[(&str, &str)] = &[
    ("POST", "/api/agent/conversations"),
    ("GET", "/api/agent/conversations"),
    ("GET", "/api/agent/conversations/:id"),
    ("DELETE", "/api/agent/conversations/:id"),
    ("GET", "/api/agent/models"),
    ("POST", "/api/agent/conversations/:id/messages"),
    ("POST", "/api/agent/conversations/:id/stop"),
    ("POST", "/api/agent/conversations/:id/context"),
    ("POST", "/api/areas/:area/lookups/upload"),
    ("PUT", "/api/me/agent-permissions"),
];

#[cfg(test)]
mod tests {
    use super::*;

    /// Routes outside the gate: signing in, and what workers and enrollment use, each with its own credential.
    const UNGATED: &[(&str, &str)] = &[
        ("GET", "/api/auth/session"),
        ("POST", "/api/auth/login"),
        ("DELETE", "/api/auth/session"),
        ("POST", "/api/auth/session/activity"),
        ("PUT", "/api/auth/password"),
        ("GET", "/api/auth/oidc"),
        ("GET", "/api/auth/oidc/start"),
        ("GET", "/api/auth/oidc/callback"),
        ("POST", "/api/worker/heartbeat"),
        ("POST", "/api/worker/capture-report"),
        ("GET", "/api/worker/lookup-store/:oid"),
        ("GET", "/api/worker/ledger"),
        ("POST", "/api/worker/enroll"),
        ("GET", "/api/worker/authority"),
        ("GET", "/api/worker/repo/*path"),
        ("POST", "/api/worker/repo/*path"),
    ];

    /// Every `(method, path)` the leader's sources register under `/api/`, read from the sources: axum lists no
    /// routes of a router.
    fn registered() -> Vec<(String, String)> {
        let path = regex::Regex::new(r#"^\s*"(/api/[^"]+)""#).unwrap();
        let method = regex::Regex::new(r"(?:^|[\s.(:])(get|post|put|patch|delete)\(").unwrap();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(root.join("src/bin/smeltr"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        files.push(root.join("src/leader/mod.rs"));
        let mut found = Vec::new();
        for file in files.iter().filter(|f| f.extension().is_some_and(|e| e == "rs")) {
            let text: String = std::fs::read_to_string(file)
                .unwrap()
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .collect::<Vec<_>>()
                .join("\n");
            // Each registration runs to the next, or to the end of its statement.
            for chunk in text.split(".route(").skip(1) {
                let chunk = chunk.split(';').next().unwrap_or_default();
                let Some(named) = path.captures(chunk) else {
                    continue;
                };
                for verb in method.captures_iter(chunk) {
                    found.push((verb[1].to_ascii_uppercase(), named[1].to_string()));
                }
            }
        }
        found.sort();
        found.dedup();
        found
    }

    #[test]
    fn every_registered_route_is_in_the_catalog_or_left_out() {
        let registered = registered();
        assert!(registered.len() > 100, "the routes were not found: {registered:?}");
        let known = |method: &str, path: &str| {
            CATALOG.iter().any(|r| r.method == method && r.path == path)
                || LEFT_OUT.iter().chain(UNGATED).any(|(m, p)| *m == method && *p == path)
        };
        let unknown: Vec<_> = registered.iter().filter(|(m, p)| !known(m, p)).collect();
        assert!(unknown.is_empty(), "neither in the catalog nor left out: {unknown:?}");
        let stale: Vec<_> = CATALOG
            .iter()
            .filter(|r| !registered.iter().any(|(m, p)| m == r.method && p == r.path))
            .map(|r| (r.method, r.path))
            .collect();
        assert!(stale.is_empty(), "in the catalog but registered nowhere: {stale:?}");
    }
}
