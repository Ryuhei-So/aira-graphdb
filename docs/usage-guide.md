# Usage Guide (English)

## 1. Rust API

### Handshake

```rust
use aira_graphdb::protocol::{HandshakeRequest, negotiate};

let response = negotiate(&HandshakeRequest {
    protocol_version: "protocol-p0@1.0.0".into(),
    canonical_type_system_version: "canonical-types@1.0.0".into(),
})?;
assert!(response.accepted);
# Ok::<(), aira_graphdb::errors::GraphDbError>(())
```

### Query Execution

```rust
use aira_graphdb::graph::InMemoryGraphStore;
use aira_graphdb::query::{execute_query, execute_query_with_dialect, CypherDialect};

let mut store = InMemoryGraphStore::new();
execute_query(&mut store, "CREATE (n:Paper {title:'GraphDB'})")?;
execute_query(&mut store, "MERGE (n:Paper {title:'GraphDB'}) ON MATCH SET n.status='existing'")?;
let _ = execute_query(&mut store, "MATCH (n:Paper) WITH n RETURN n ORDER BY n.id SKIP 0 LIMIT 1")?;
let _ = execute_query_with_dialect(
    &mut store,
    "MATCH (n) RETURN n UNION MATCH (m) RETURN m",
    CypherDialect::Neo4jCompat,
)?;
# Ok::<(), aira_graphdb::errors::GraphDbError>(())
```

## 2. Server Transport (JSON Lines)

Execution boundary is strict: `CONNECTED -> TLS_OK -> AUTH_OK -> APP_READY`.

```json
{"type":"handshake","protocol_version":"protocol-p0@1.0.0","canonical_type_system_version":"canonical-types@1.0.0"}
{"type":"auth","bearer_token":"<jwt>"}
{"type":"begin_tx"}
{"type":"query","tx_id":"tx-1","query":"CREATE (n:Paper {title:'GraphDB'})"}
{"type":"commit_tx","tx_id":"tx-1"}
```

Before `APP_READY`, application requests are rejected with `AUTH_REQUIRED`.

### Data registration and indexing via native JSON-RPC

Start sidecar:

```bash
cargo run --bin aira-graphdb-native -- --db /path/to/aira-graphdb-native.db
```

Register nodes/edges:

```json
{"id":1,"method":"upsert_nodes","params":{"nodes":[{"nodeId":"n1","corpusId":"c1","layer":"paper","ref":{},"label":"Paper"}]}}
{"id":2,"method":"upsert_edges","params":{"edges":[{"edgeId":"e1","corpusId":"c1","sourceNodeId":"n1","targetNodeId":"n1","relation":"SELF","weight":1.0}]}}
```

Register index data:

```json
{"id":3,"method":"vector_upsert","params":{"vectors":[{"id":"v1","corpusId":"c1","namespace":"default","values":[0.1,0.2,0.3],"metadata":{"documentId":"d1"}}]}}
{"id":4,"method":"lexical_index_passages","params":{"passages":[{"passageId":"p1","corpusId":"c1","documentId":"d1","text":"graph database"}]}}
```

Search:

```json
{"id":5,"method":"vector_search","params":{"corpusId":"c1","namespace":"default","queryVector":[0.1,0.2,0.3],"topK":10}}
{"id":6,"method":"lexical_search","params":{"corpusId":"c1","query":"graph database","topK":10}}
```

`create index` is not a separate RPC today. In-memory graph/vector/lexical indexes are refreshed automatically when upsert/delete methods succeed.

### Available queries (RPC methods)

| Method | Description |
|---|---|
| `ping` | Health check (`{"pong":true}`) |
| `upsert_nodes` / `upsert_edges` | Insert or update nodes/edges |
| `get_node` / `get_nodes` | Fetch one node / list nodes with filters |
| `get_edges` / `get_adjacent` | List edges / get adjacent edges for a node |
| `delete_nodes` / `delete_edges` | Delete selected nodes/edges |
| `delete_by_document` / `delete_by_corpus` | Bulk delete by document or corpus |
| `vector_upsert` / `vector_search` / `vector_delete_by_document` | Vector insert-search-delete operations |
| `lexical_index_passages` / `lexical_search` / `lexical_delete_by_document` | Lexical index insert-search-delete operations |
| `memory_save` / `memory_load` | Save and load memory snapshots |
| `memory_upsert` / `memory_activate_facts_by_schema_ids` | Bounded indexing-memory mutations (delta upsert, fact activation) |
| `memory_get_schemas_by_ids` / `memory_get_active_facts` | Bounded indexing-memory reads |
| `memory_get_passages_by_ids` / `memory_get_facts_by_ids` | Targeted query-path reads: full objects for the requested ids, in request order, unknown ids omitted |
| `memory_find_facts_by_entities` | Facts whose `headEntity` or `tailEntity` case-folds (Unicode 16) to a requested entity, `factId` ascending |
| `memory_section_counts` | `{passages, facts, schemas}` item counts for one corpus |
| `memory_save_checkpoint` / `memory_load_checkpoint` | Save and load checkpoints |
| `memory_validate_integrity` | Memory integrity check (currently returns an empty list) |
| `projection_get_transitions` / `projection_get_dangling_nodes` / `projection_get_node_count` | Projection reads: transitions, dangling nodes, and node count |
| `projection_get_transitions_page` | Paged transitions for one committed generation (see below) |

### Method policy (generated)

The table below is generated from the native `METHOD_SPECS` table, which is the single policy authority for `protocol_info.methods[]`, WAL admission, and per-method wire limits. Regenerate it with:

```bash
cargo run --bin aira-graphdb-native -- --print-method-policy-table
```

A unit test (`usage_guide_method_policy_table_matches_method_specs`) fails when this block drifts from the binary.

<!-- METHOD_SPECS:BEGIN (generated; do not edit) -->
| Method | Classification | WAL | Wire profile |
|---|---|---|---|
| `ping` | health | false | normal |
| `protocol_info` | health | false | normal |
| `blob_lineage` | health | false | normal |
| `batch_begin` | transaction | false | normal |
| `batch_prepare_commit` | transaction | false | normal |
| `batch_commit` | commit | false | normal |
| `recovery_discard` | recovery | false | normal |
| `upsert_nodes` | mutation | true | normal |
| `upsert_edges` | mutation | true | normal |
| `get_node` | read | false | normal |
| `get_nodes` | read | false | normal |
| `get_edges` | read | false | normal |
| `get_adjacent` | read | false | normal |
| `delete_nodes` | mutation | true | normal |
| `delete_edges` | mutation | true | normal |
| `delete_by_document` | mutation | true | normal |
| `delete_by_corpus` | mutation | true | normal |
| `vector_upsert` | mutation | true | normal |
| `vector_search` | read | false | normal |
| `vector_delete_by_document` | mutation | true | normal |
| `memory_upsert` | mutation | true | bounded-indexing |
| `memory_save` | mutation | true | normal |
| `memory_save_file` | mutation | true | normal |
| `memory_load` | read | false | normal |
| `memory_get_schemas_by_ids` | read | false | bounded-indexing |
| `memory_get_active_facts` | read | false | bounded-indexing |
| `memory_get_passages_by_ids` | read | false | bounded-indexing |
| `memory_get_facts_by_ids` | read | false | bounded-indexing |
| `memory_find_facts_by_entities` | read | false | bounded-indexing |
| `memory_section_counts` | read | false | bounded-indexing |
| `memory_activate_facts_by_schema_ids` | mutation | true | bounded-indexing |
| `memory_save_checkpoint` | mutation | true | normal |
| `memory_load_checkpoint` | read | false | normal |
| `memory_validate_integrity` | read | false | normal |
| `projection_get_transitions` | read | false | normal |
| `projection_get_transitions_page` | read | false | bounded-indexing |
| `projection_get_dangling_nodes` | read | false | normal |
| `projection_get_node_count` | read | false | normal |
| `lexical_index_passages` | mutation | true | normal |
| `lexical_search` | read | false | normal |
| `lexical_delete_by_document` | mutation | true | normal |
| `cypher_query` | read | false | normal |
| `__debug_force_panic__` | debug | false | normal |
<!-- METHOD_SPECS:END -->

Bounded-indexing methods are capped at `limits.indexingMemory.maxRequestBytes` (64 MiB) per request and `limits.indexingMemory.maxResponseBytes` (8 MiB) per reply, enforced before serialization. The targeted memory reads additionally advertise `limits.memoryRead` (`schema`, `maxIdsPerRequest`, `maxEntitiesPerRequest`, `maxLimit`); consumers must read those values from `protocol_info` rather than assume them.

`projection_get_transitions_page {corpusId, generation, offset}` returns `{generation, offset, nextOffset, totalEntries, entries}`. Entries are the same `{sourceNodeId, targetNodeId, weight}` objects as `projection_get_transitions`, in the total order `(sourceNodeId, targetNodeId, edge key)` by bytes (`limits.projectionRead.order = source-target-key@1`). Each page holds as many entries as fit `limits.projectionRead.maxResponseBytes`, and always at least one. Pass `generation: null` only with `offset: 0`: the reply names the committed generation, and every later page must pin it by passing that generation back, with `offset` equal to the previous `nextOffset`. A page is rejected when the pinned generation is no longer the committed one or a batch is open, so a read that spans a commit fails closed and restarts from offset 0. `nextOffset` is `null` on the last page.

## 3. Conformance Report

`build_and_persist_conformance_report` writes:

```text
target/conformance/opencypher9-report.json
```

The report includes:

- `pass_rate`
- `unresolved_tck_ids`
- `mandatory_negative_cases_satisfied`
- `failed_test_ids`
- clause-level and feature-level PASS/FAIL

Neo4j-compatible Cypher is guarded: `FOREACH`, variable-length paths, and `shortestPath(...)` are rejected with `UNSUPPORTED_FEATURE` plus an `unsupported_clause` detail.

## 4. Audit Events

Server/runtime audit events are appended to `<db-file>.audit.log`.
Native JSON-RPC request anomaly audit events are appended to `<db-file>.native-audit.log`.

Implemented event types:

- `AUTH_FAILED`
- `AUTH_REQUIRED_REJECTED`
- `ROLLBACK_EXECUTED`
- `REFERENTIAL_INTEGRITY_VIOLATION`
- `DETERMINISTIC_CONFLICT`

Native request anomaly entries include required fields:

- `errorCode`
- `failureClass` (`INTERNAL_BUG | IO_FAILURE | OOM | TIMEOUT | CLIENT_INPUT`)
- `requestId`
- `timestamp`

Native crash entries are auto-recorded as `PROCESS_CRASH` with:

- `errorCode`
- `timestamp`
- `processExitCode`
- `signal`
- `lastRequestId`
- `uptimeSec`
- `cause` (if available)

## 5. Native Perf/Soak Quality Gate Artifacts

The native gate suite writes:

```text
artifacts/native-bench-report.json
artifacts/native-soak-report.json
artifacts/native-audit-events.json
```

`native-soak-report.json` includes:

- `profile` (`P0-NATIVE-SOAK-SMOKE` for pull requests, `P0-NATIVE-SOAK` for schedule/release)
- `durationMinutes` (30 or 1440)
- `crashCount` (must be `0`)
- `internalFailureRate` (must be `<= 0.001`)
- `requiredFieldsValid`
- `gatePass`

## 6. openCypher Coverage Status (Current)

| Area | Status | Notes |
|---|---|---|
| `MATCH`, `OPTIONAL MATCH`, `WHERE`, `WITH`, `RETURN` | Supported (profile subset) | Includes `WITH` alias scope validation |
| `ORDER BY`, `SKIP`, `LIMIT` | Supported | Strategy switch: ordered vs multiset (`resolve_row_comparison_strategy`) |
| `CREATE`, `MERGE`, `SET`, `REMOVE`, `DELETE`, `DETACH` | Supported (profile subset) | `MERGE` on-create/on-match semantics implemented |
| `UNWIND` + aggregation | Supported | `count/sum/avg/min/max/collect` |
| `CALL` + APOC subset (`apoc.meta.schema`, `apoc.coll.toSet`, `apoc.text.join`, `apoc.refactor.rename.label`) | Supported (manifest-based) | Allowed set is fixed by `spec/contracts/apoc-procedure-manifest.v1.0.0.yaml` |
| Neo4j-compatible Cypher dialect | Supported (guarded) | `execute_query_with_dialect(..., CypherDialect::Neo4jCompat)` supports `UNION` / `UNION ALL` / `CASE` and rejects unsupported extensions with `UNSUPPORTED_FEATURE` |
| Relationship traversal pattern (`()-[]->()`, `()-[]-()`) | Supported | Single-hop traversal with `OPTIONAL MATCH/WHERE/WITH/ORDER BY/SKIP/LIMIT` contract cases |

## 7. Storage Port Compatibility with aira-synapse

Canonical contract:

```text
spec/contracts/aira-synapse-storage-ports.v1.0.0.json
```

Phase 4 implementation includes:

- AST-based method parity checks for `IGraphStore / IVectorIndex / IMemoryStore / IGraphProjection / ILexicalRetriever`
- `aira-graphdb` backend selection in `memgraphrag` storage factory
- storage-port compatibility integration tests (`graph/vector/lexical/memory/projection`)
- vector/lexical compatibility evaluator with fixed validation error codes:
  - `INVALID_TOP_K`
  - `INVALID_THRESHOLD`
  - `INVALID_CORPUS_ID`
  - `INVALID_NAMESPACE`

Compatibility workflow references:

- `.github/workflows/aira-synapse-backend-compat.yml`
- `spec/contracts/p0-compat-test-map.v1.0.0.json`
- `spec/contracts/backend-compat-failure-report.v1.0.0.json`
- `spec/contracts/branch-protection-policy.v1.0.0.json`
- `spec/contracts/event-scope-map.v1.0.0.json`

### Native transport path

`backend=aira-graphdb` now uses the native Rust sidecar process:

- Rust binary: `aira-graphdb-native` (`src/bin/aira-graphdb-native.rs`)
- Transport: JSON-RPC over stdin/stdout
- Persistent state: `--db <path>` compact binary snapshot/WAL file
- Legacy JSON snapshots are auto-migrated to the compact binary format on load

This replaces the previous SQLite compatibility fallback for the `aira-graphdb` backend path.

## 8. Native RPC resilience contract

The native resilience contract test validates that invalid JSON, unknown methods, and execution failures return fixed error codes while the sidecar process stays alive.

```bash
cargo test --test native_rpc_resilience -- --nocapture
```

The same test suite also includes a forced panic scenario to verify automatic `PROCESS_CRASH` audit logging.

## 9. External watchdog crash tracking

Kill-level exits (e.g. SIGKILL) are tracked through the external watchdog path and persisted as:

```text
artifacts/watchdog-crash-report.json
```

Run locally:

```bash
cargo test --test native_watchdog -- --nocapture
```
