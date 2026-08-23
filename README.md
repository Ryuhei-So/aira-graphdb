# aira-graphdb

Rust-based GraphDB for AIRA ecosystem services, designed to support both:

- **Embedded mode** (file-based, SQLite/LadybugDB-like)
- **Server mode** (daemon/TCP, Neo4j-like)

This repository follows SDD artifacts and currently includes Phase 1–4 implementation, including native Rust transport performance and stability gates.

## Current status

- Requirements: `spec/REQ-AIRA-GRAPHDB-001.md`
- Design: `spec/DES-AIRA-GRAPHDB-001.md`
- ADR: `spec/ADR-AGDB-001.md`
- Task breakdown: `spec/PLAN-AIRA-GRAPHDB-001.md`
- Immutable contracts:
  - `spec/contracts/agdb-typemap-p0.v1.0.0.json`
  - `spec/contracts/agdb-cypher-p0-grammar.v1.0.0.json`
  - `spec/contracts/agdb-error-codes.v1.0.0.json`

## Repository layout

```text
src/
  auth.rs
  audit.rs
  bench.rs
  contracts.rs
  conformance.rs
  errors.rs
  graph.rs
  lock.rs
  native_bench.rs
  protocol.rs
  query.rs
  runtime.rs
  server.rs
  storage.rs
  tx.rs
  bin/
    aira-graphdb-native.rs
tests/
  cypher_conformance.rs
  integration_flow.rs
  native_perf_gate.rs
  native_rpc_resilience.rs
  native_soak_gate.rs
sdk/
  node/
  python/
spec/
  REQ-*.md
  DES-*.md
  ADR-*.md
  PLAN-*.md
  contracts/*.json
```

## Prerequisites

- Rust (stable, with `cargo`)
- Node.js (for Node SDK tests)
- Python 3.10+ (for Python SDK tests)
- Linux with `renameat2(RENAME_NOREPLACE)` support on the database filesystem
  for `aira-graphdb-native` durable generation persistence. The native process
  probes this capability in the database directory before loading state or
  accepting mutations and fails without changing canonical data when it is
  unavailable.

## Build and test

Run Rust tests:

```bash
cargo test
```

Run native transport contract/perf/soak tests:

```bash
cargo test --test native_rpc_resilience
cargo test --test native_perf_gate
cargo test --test native_soak_gate
```

Run Node SDK tests:

```bash
cd sdk/node
npm test
```

Run Python SDK tests:

```bash
cd sdk/python
PYTHONPATH=. python -m unittest discover -s tests -v
```

## Implemented capabilities

- Contract loading for type mapping / Cypher P0 grammar / error codes
- Centralized error registry and code mapping
- In-memory graph domain model (node/edge CRUD)
- Compact binary snapshot + WAL-based persistence/recovery
- Legacy JSON snapshots auto-migrate on load
- Transaction manager (begin/commit/rollback)
- File write lock guard (`WRITE_LOCK_CONFLICT`)
- Minimal query execution for P0 subset surface
- Protocol handshake negotiation
- Auth boundary validation logic (TLS/JWT policy checks)
- Embedded and server runtime scaffolding
- Native benchmark and soak profile helpers
- Native request anomaly audit logging (`<db>.native-audit.log`)
- Native runtime crash auto logging (`PROCESS_CRASH`)
- External watchdog crash tracking for kill-level exits (`artifacts/watchdog-crash-report.json`)
- Native CI quality gates:
  - perf gate artifact: `artifacts/native-bench-report.json`
  - soak gate artifact: `artifacts/native-soak-report.json`
  - audit artifact: `artifacts/native-audit-events.json`

## CI gate policy (native transport)

- `pull_request`: `P0-NATIVE-SOAK-SMOKE` (30 min profile contract)
- `schedule` / `release`: `P0-NATIVE-SOAK` (24h profile contract)
- Mandatory thresholds:
  - `crashCount == 0`
  - `internalFailureRate <= 0.001`
  - required audit fields present for native request anomaly events

## Native crash forensics

When the native sidecar panics or exits abnormally, `<db>.native-audit.log` receives a `PROCESS_CRASH` event with:

- `errorCode`
- `timestamp`
- `processExitCode`
- `signal`
- `lastRequestId`
- `uptimeSec`
- `cause` (when available)

Supported CALL/APOC subset is manifest-driven (`spec/contracts/apoc-procedure-manifest.v1.0.0.yaml`), and relationship traversal `()-[]->()/()-[]-()` is now covered by conformance cases.

## Data registration and indexing (native JSON-RPC)

Start sidecar:

```bash
cargo run --bin aira-graphdb-native -- --db /path/to/aira-graphdb-native.db
```

Register graph data:

```json
{"id":1,"method":"upsert_nodes","params":{"nodes":[{"nodeId":"n1","corpusId":"c1","layer":"paper","ref":{},"label":"Paper"}]}}
{"id":2,"method":"upsert_edges","params":{"edges":[{"edgeId":"e1","corpusId":"c1","sourceNodeId":"n1","targetNodeId":"n1","relation":"SELF","weight":1.0}]}}
```

Register vector/lexical index data:

```json
{"id":3,"method":"vector_upsert","params":{"vectors":[{"id":"v1","corpusId":"c1","namespace":"default","values":[0.1,0.2,0.3],"metadata":{"documentId":"d1"}}]}}
{"id":4,"method":"lexical_index_passages","params":{"passages":[{"passageId":"p1","corpusId":"c1","documentId":"d1","text":"graph database"}]}}
```

Query indexes:

```json
{"id":5,"method":"vector_search","params":{"corpusId":"c1","namespace":"default","queryVector":[0.1,0.2,0.3],"topK":10}}
{"id":6,"method":"lexical_search","params":{"corpusId":"c1","query":"graph database","topK":10}}
```

Note: graph/vector/lexical in-memory indexes are refreshed automatically on upsert/delete. No separate `create index` command is required for the current native runtime.

### Available queries (RPC methods)

| Method | Description |
|---|---|
| `ping` | Health check |
| `upsert_nodes`, `upsert_edges` | Insert/update graph nodes and edges |
| `get_node`, `get_nodes`, `get_edges`, `get_adjacent` | Graph read operations |
| `delete_nodes`, `delete_edges`, `delete_by_document`, `delete_by_corpus` | Graph delete operations |
| `vector_upsert`, `vector_search`, `vector_delete_by_document` | Vector insert/search/delete |
| `lexical_index_passages`, `lexical_search`, `lexical_delete_by_document` | Lexical index insert/search/delete |
| `memory_save`, `memory_load`, `memory_save_checkpoint`, `memory_load_checkpoint`, `memory_validate_integrity` | Memory snapshot/checkpoint operations |
| `projection_get_transitions`, `projection_get_dangling_nodes`, `projection_get_node_count` | Projection read operations |
