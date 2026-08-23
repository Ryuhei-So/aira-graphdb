# Bounded retrieval data plane

Status: design checkpoint only.  This document does not authorize native
method or validator implementation.

This design replaces the first issue-4 `retrieve_bounded` contract.  Aira
Synapse is the sole query-policy authority.  Aira GraphDB provides three
versioned, bounded data operations over one committed generation.  Literature
Hub owns the generation session, availability policy, and retry circuit.

## Decision

One semantic retrieval uses three bulk operations inside one exclusive owner
reader lease:

1. `candidate_search_bounded@1` performs explicitly requested vector searches
   and materializes only their domain objects.
2. `fact_expand_bounded@1` performs an explicit bounded entity-match plan when
   Synapse requests one.
3. `ppr_materialize_bounded@1` runs an explicit graph plan and materializes only
   its selected passages and facts.

The operations do not name, default, or validate a Synapse profile.  Their
`protocol_info` entries advertise operation schema digests, hard limits,
`classification=read`, and `wal=false`.  A profile id may be echoed as opaque
audit data but never changes native behavior.

Rejected alternatives:

- A single query-pipeline RPC makes Rust a second query-policy authority.
- Per-node/per-edge RPCs make queue and lease duration proportional to graph
  fan-out.
- Full immutable reader snapshots retain multi-GiB memory and transfer costs.
- Larger timeouts, response lines, cgroups, or restart delays do not establish
  correctness or bounded work.

## Authorities

- Canonical JSON and its referenced vector blob are the committed GraphDB data
  authority.  The adjacent owner generation manifest records owner admission
  state; it does not replace the canonical JSON publication pointer.  WAL is
  unpublished recovery input and is never reader-visible.
- The owner alone reaches native stdio and owns roles, writer exclusion, reader
  lease admission/renewal/release, publication blocking, and native-death
  classification.  Native requests contain no caller role or lease claim.
- Native owns storage lookup, exact vector arithmetic, graph arithmetic,
  domain-object lookup, work counters, checked allocation, and response
  framing for the requested data operations.
- Synapse owns a machine-readable `V15RetrievalPlan` and the pure functions that
  produce it.  The legacy JS path and bounded adapter call the same functions.
  Synapse alone decides normalization, embedding, search slots, comparison
  mode, expansion parameters, seeds, PPR parameters, output limits, context,
  answer, and provenance.
- Literature Hub owns a `GenerationSession` around the complete retrieval and
  an infrastructure retry budget independent of document failure counts.

No native default may fill a missing policy value.  Unknown fields and absent
required operation values fail before corpus-sized allocation or work.

## Generation session

Generation is a JSON safe integer in `0..9007199254740991` at every existing
boundary: owner hello/status/lease, native request/response, commit token, and
owner manifest.  Native rejects a larger persisted or requested generation
before conversion or increment.  A future u64 migration must change all of
these boundaries atomically; issue 4 does not introduce a string/number split.

Bridge v1 deliberately supports one active semantic query.  Its bounded queue
may hold waiting HTTP requests, but a single `GenerationSession` owns one
control connection, one reader connection, and one lease for the active query.
The session:

1. acquires committed generation N;
2. heartbeats before TTL and awaits any in-flight renewal;
3. passes N to every bulk operation and rejects any response not equal to N;
4. permits a queued writer to mark the owner manifest dirty only at base N,
   while native mutation/publication remains blocked;
5. performs final renewal/validation after the last operation; and
6. releases in `finally` exactly once.

Lease loss, generation mismatch, reader disconnect, or failed final validation
discards the whole retrieval.  A later concurrency version must use distinct
query sessions or a reviewed reference-counted lease manager; it may not share
one mutable acquire/release pair between concurrent queries.

Native admits each operation only while its state is Idle at N.  RecoveryPending
and stale generation fail closed.  Read operations never write WAL, populate a
persistent cache, change manifest/canonical artifacts, or advance generation.

## Synapse policy authority and parity ledger

Before native implementation, Synapse must extract the active production path
(`VectorMemoryFilter`, `SimpleNodeInitializer`, `SimplePPR`) into shared pure
plan/order helpers.  Both the legacy and bounded paths consume the same helper
output.  Hybrid lexical RRF, dictionary injection, subquery decomposition, and
other feature profiles remain unsupported by the bounded adapter until their
own plan versions and parity fixtures exist.  Synapse, not native, fails closed
when active feature flags lack a bounded plan.

The v15 parity ledger is explicit:

| Behavior | Current behavior | Issue-4 contract |
| --- | --- | --- |
| vector slots | passage `topK`, fact `topM`, schema `10` | preserve; values supplied by Synapse |
| threshold | finite exact-vector domain, including negative | preserve |
| missing domain object for a vector hit | silently skipped | harden to integrity failure only after a copied-production audit proves zero; otherwise block rollout |
| fact expansion population | all facts, including inactive | preserve |
| entity normalization | ECMAScript `toLowerCase()` | move to one Synapse-owned helper with cross-language vectors; native operation implements that digest only |
| expansion scoring | max matching seed-entity score times `0.3`, top 20 | preserve; all values explicit in the plan |
| expansion tie | snapshot-order stable sort | harden to fact id ascending after legacy JS adopts the same order |
| seed score | any finite signed vector score | preserve; do not require nonnegative values |
| PPR order and rank ties | incidental map/projection insertion order | harden to canonical order after legacy JS adopts it |
| dangling mass | dropped | preserve |
| context materialization | selected passages; fact-layer entries from ranked entities | preserve complete domain shapes; separately test the existing index-to-fact association |

Every hardening is first implemented in the legacy JS reference, evaluated on
the copied production generation, and recorded with before/after ids, ranks,
and scores.  It is not described as parity until that evidence is accepted.

## Data operations

### Candidate search

Synapse supplies an ordered list of slots containing namespace, finite query
vector, threshold, and result limit.  Native returns one ordered result list
per slot with exact cosine score, native id, and the complete corresponding
Passage, Fact, or Schema object.  It returns a missing-object integrity error
only after the parity-ledger gate above selects that hardening.

Candidate order is score descending then native id ascending.  The same order
must already be active in the legacy JS reference.  Candidate and domain object
shapes come from a SHA-pinned cross-repository fixture generated from Synapse
types, not an independently invented GraphDB shape.

### Fact expansion

Synapse decides whether expansion applies and supplies:

- normalized seed entity keys and their highest finite signed seed scores;
- excluded seed fact ids;
- explicit attenuation and result limit; and
- the normalization-contract digest produced by the Synapse authority.

Native scans the requested corpus facts, applies only that operation, and
returns derived score descending then fact id ascending.  It includes inactive
facts to preserve v15 behavior.  It does not detect comparison queries or
choose normalization, attenuation, or limits.

### PPR and materialization

Synapse supplies the final bounded node-id/finite-score seed map,
`teleportProbability`, `convergenceEpsilon`, `maxIterations`,
`hubDegreeThreshold`, and separate passage/entity rank limits.

The shared reference and native use these canonical orders:

- seed entries: node id ascending;
- graph nodes: node id ascending;
- outgoing edges: source id, target id, edge id ascending;
- floating-point accumulation: the above serial order with no parallel
  reduction or fused alternative;
- ranked passages/entities: score descending, then node id ascending.

Arithmetic follows current `SimplePPR`: only graph endpoint nodes exist; absent
seeds are ignored; teleport is L1-normalized when its sum is positive and is
uniform otherwise; non-dangling nodes distribute `(1-teleportProbability)` by
normalized outgoing weight; dangling mass is dropped; schema targets above the
degree threshold receive `1/log2(totalDegree+2)` damping; convergence uses L1
delta after a complete iteration.

Cross-language golden tests require identical ids/ranks/iteration/convergence
and finite scores within absolute `1e-12`; they do not claim bit identity across
JavaScript and Rust.  Copied-production comparison records maximum score delta
and every top-rank difference.  Native returns ranked node ids/scores plus full
Passage/Fact objects required by the existing context/provenance path; it never
returns a full projection or memory snapshot and never invents `Fact.value`.

## Hard resource model

One Rust contract module is the authority for validation, `protocol_info`,
checked allocation formulas, and tests.  Request values may reduce but never
raise hard maxima.  Initial ceilings are reviewed against the copied production
generation and provide at least 2x count headroom while remaining below the
owner cgroup headroom:

- vector dimensions 4096; total vector comparisons 1,500,000;
- graph nodes 1,500,000; graph edges 4,000,000; iterations 128;
- search result limit 100 per slot; expansion results 64; seeds 512;
- returned passages 100 and facts 100, combined objects 128;
- one encoded object 256 KiB; complete JSON-RPC line 2 MiB;
- transient request memory 512 MiB; hard operation deadline 60 seconds.

Production configuration starts lower where the current query requires it and
alerts at 80% of any count, byte, deadline, `MemoryHigh`, or owner queue limit.
The 2 MiB native frame must remain below both owner per-line and aggregate queue
limits.  The 512 MiB transient ceiling plus measured native steady PSS and
concurrent service allowance must remain below `MemoryHigh`; otherwise deploy
is blocked rather than raising the cgroup boundary.

At generation load, native builds an immutable read catalog containing domain
id references, encoded-size metadata, corpus/namespace counts, and canonical
adjacency.  This is part of steady-state PSS and is measured separately from
per-request transient memory.  It must not duplicate domain text or vectors.

Counters use checked integers.  Preflight counts reject a plan before work when
the corpus cannot fit its requested/hard bound.  One unit is charged for each
vector compared, fact inspected, node initialized per iteration, edge visited
per iteration, and object considered for encoding.  Allocation checks cover
both array capacity and bytes before allocation.  Response serialization uses
borrowed typed objects and an upper-bounded writer; no full `serde_json::Value`
result clone is allowed.  If one complete object cannot fit, the whole request
fails with a response-limit error.  Bytes include the JSON-RPC envelope and
newline.  No truncation or partial success is valid.

Version 1 promises a hard monotonic deadline, not immediate client
cancellation.  Work checks the deadline at least every 1024 units and before
every allocation/materialization step.  A disconnect may leave work running
only until that deadline; the owner does not reuse the native request slot or
report success meanwhile.  Immediate cancellation requires a later reviewed
worker/cancel protocol.

## Failure and retry authority

Native returns structured errors only while alive.  Death, SIGKILL, cgroup OOM,
or a partial frame is synthesized by the owner/client from the existing native
transport/death authority.  Every failure leaves canonical JSON, referenced
blob, owner manifest generation, and WAL unchanged.

Central error behavior is:

| Class | Retry policy |
| --- | --- |
| validation, unsupported operation/digest, hard cap | nonretryable configuration error; service stays unavailable until corrected |
| stale generation or lease loss | at most 2 attempts per HTTP request with bounded backoff and a fresh session |
| operation deadline | no same-request replay; return 503 and record work counters |
| RecoveryPending or durability failure | no automatic loop; operator reconciliation required |
| native death or OOM | open circuit immediately; owner fails closed; systemd start limit is the restart bound |

Indexing infrastructure errors never consume the document-content failure
budget, but that exemption is not an unlimited retry.  The worker records a
separate durable infra-attempt timestamp/count, requeues the document once,
exits, and relies on the bounded service start limit.  Repeated recovery/OOM
opens an operator-visible circuit instead of a 30-second reload loop.

## Negative-test and rollout gates

Before typed operation implementation:

- delete the superseded single-RPC contract, fixtures, ignored test, and local
  error definitions so only this design remains authoritative;
- land the Synapse `V15RetrievalPlan`/ordering helpers and cross-repository
  domain/parity fixtures without weakening its existing test gates;
- test the exclusive `GenerationSession`: renewal ordering, lease loss,
  `finally` release, queued writer, and two simultaneous HTTP requests where
  only one session is active.

Before native algorithm wiring:

- executable validator tests cover every type, unknown field, safe-generation
  boundary, signed/non-finite score, count/byte/allocation formula, operation
  digest, and limit at/above the boundary;
- real `protocol_info` advertises each operation as read/WAL-free with the exact
  schema/limit digest;
- real-native tests cover nonzero generation, stale generation,
  RecoveryPending, writer waiting across all three operations, deadline via
  fake monotonic clock, allocation failpoints, response budget, partial frame,
  kill/OOM classification, and byte-invariant canonical/manifest/blob state.

CI uses deterministic clock/counter/allocation seams.  A separate copied-state
cgroup run measures actual PSS/RSS/VmSwap, p50/p95 latency, work counters,
response bytes, ranks, and maximum score delta.  Production activation is
blocked unless it fits the existing owner timeout, lease heartbeat, cgroup
headroom, and HTTP latency budget with margin.

Migration keeps the canonical format unchanged and retains legacy reads until
bounded production E2E passes.  The production bridge has no implicit legacy
fallback: missing capability/digest or unsupported active policy fails closed.
Rollback selects the prior code while worker/bridge are drained; it never
rewrites or downgrades canonical data.
