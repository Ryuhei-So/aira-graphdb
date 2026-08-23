# Bounded retrieval data plane

Status: design checkpoint only.  This document does not authorize native
method or validator implementation.

This design replaces the first issue-4 `retrieve_bounded` contract.  Aira
Synapse is the sole query-policy authority.  Aira GraphDB provides two or three
versioned, bounded data operations over one committed generation, depending on
whether Synapse requests fact expansion.  Literature Hub owns the generation
session, availability policy, and retry circuit.

## Decision

One semantic retrieval uses two required and one optional bulk operation inside
one exclusive owner reader lease:

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
- Synapse owns a machine-readable pre-acquire `V15RetrievalRequestPlan` and the
  pure functions that derive each later operation plan from the preceding
  validated response.  A complete downstream plan is deliberately not an
  input to the session: candidate results do not exist before candidate
  search, and final PPR seeds do not exist before optional fact expansion.
  The legacy JS path and bounded adapter call the same stage builders.
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

1. validates the static request plan and unsupported-feature gate before owner
   acquisition or any legacy memory/vector read;
2. acquires committed generation N;
3. heartbeats before TTL and awaits any in-flight renewal;
4. performs candidate search, validates the exact per-slot response, and lets
   Synapse derive the optional fact-expansion plan from those candidates;
5. validates fact-expansion output when requested, then lets the shared
   Synapse node-initialization helper derive the final PPR seed plan from the
   candidate and expansion results;
6. passes N to every bulk operation and rejects any response not equal to N;
7. permits a queued writer to mark the owner manifest dirty only at base N,
   while native mutation/publication remains blocked;
8. performs final renewal/validation after the last operation; and
9. releases in `finally` exactly once.

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

The versioned plan boundary is staged:

- `V15RetrievalRequestPlan` contains only policy known before data access:
  version/profile, corpus, context limit, exact ordered candidate slots,
  comparison choice, PPR scalars, and output limits.
- `V15FactExpansionPlan` is produced only from the validated candidate response
  and the static comparison policy.  It is `null` when expansion is not
  requested or no fact candidate can seed it.
- `V15PprMaterializationPlan` is produced only after candidate search and any
  requested expansion.  One shared pure node-initialization helper combines
  those bounded results into the final signed seed map.  The legacy initializer
  uses the same helper after obtaining its legacy data; the bounded path never
  invokes `memory_load` or reconstructs a full snapshot first.

All unsupported-feature checks and all static request validation happen before
lease acquisition.  Dynamic operation plans are validated immediately after
derivation and before their RPC.  No native default supplies a missing stage,
and no caller may submit a fabricated complete plan that bypasses an earlier
response.

The v15 parity ledger is explicit:

| Behavior | Current behavior | Issue-4 contract |
| --- | --- | --- |
| vector slots | passage `topK`, fact `topM`, schema `10` | preserve; values supplied by Synapse |
| threshold | finite exact-vector domain, including negative | preserve |
| missing domain object for a vector hit | silently skipped | harden to integrity failure only after a copied-production audit proves zero; otherwise block rollout |
| fact expansion population | all facts, including inactive | preserve |
| entity normalization | ECMAScript `toLowerCase()` | move to one Synapse-owned helper with cross-language vectors; native operation implements that digest only |
| expansion scoring | `max(0, matching seed similarities)` times `0.3`, top 20 | preserve; the zero floor and all values are explicit in the plan |
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

Synapse supplies an ordered list of slots containing a unique stable `slotId`,
namespace, finite query vector, threshold, and result limit.  V15 requires the
exact `passage`, `fact`, and `schema` slot set in canonical order; a later plan
version may declare a different set.  Native returns one ordered result list
per slot, echoing `slotId` and namespace, with exact cosine score, native id,
and the complete corresponding Passage, Fact, or Schema object.  It returns a
missing-object integrity error only after the parity-ledger gate above selects
that hardening.

The response cardinality, slot order, echoed ids, and namespaces must match the
request exactly before Synapse uses any hit.  Fixed namespace-wide response
arrays are not the contract because they cannot preserve repeated or reordered
slots in a future version.

Each slot returns at most its requested limit.  Every hit has a finite cosine
score in `[-1,1]` that satisfies that slot's threshold, and the list is strictly
ordered by score descending then native id ascending with no duplicate native
id.  The native id prefix, domain-object kind, embedded object id, and object
`corpusId` must agree with the requested namespace and corpus.  Synapse checks
the complete response envelope and every hit before deriving the next stage;
native enforces the same schema while constructing the bounded response.

Candidate order is score descending then native id ascending.  The same order
must already be active in the legacy JS reference.  Candidate and domain object
shapes come from a SHA-pinned cross-repository fixture generated from Synapse
types, not an independently invented GraphDB shape.

### Fact expansion

Synapse decides whether expansion applies and supplies:

- normalized seed entity keys and `max(0, matching finite seed similarities)`;
- excluded seed fact ids;
- explicit attenuation and result limit; and
- the normalization-contract digest produced by the Synapse authority.

Native scans the requested corpus facts, applies only that operation, and
returns derived score descending then fact id ascending.  It includes inactive
facts to preserve v15 behavior.  It does not detect comparison queries or
choose normalization, attenuation, or limits.

The digest is the literal
`v15-entity-normalization-ecmascript-tolowercase-unicode16.0.0@1`, not an
arbitrary non-empty string.  It means full-string, locale-insensitive Unicode
16.0.0 lowercase with no extra normalization.  Its data authority is the
Unicode 16.0.0 UCD `UnicodeData.txt` SHA-256
`ff58e5823bd095166564a006e47d111130813dcf8bf234ef79fa51a870edb48f`,
`SpecialCasing.txt` SHA-256
`8d5de354eef79f2395a54c9c7dcebbaf3d30fc962d0f85611ea97aa973a0c451`,
and `DerivedCoreProperties.txt` SHA-256
`39d35161f2954497f69e08bdb9e701493f476a3d30222de20028feda36c1dabd`.

One generator owned by Synapse emits the JavaScript lookup helper, the native
lookup artifact, and a manifested conformance fixture from those exact files.
Neither JavaScript `String.prototype.toLowerCase()` nor Rust's host Unicode
tables are semantic authorities for this profile.  The fixture exhausts
every Unicode scalar's default lowercase mapping and every non-locale
conditional `SpecialCasing` context, including all generated Final_Sigma
Cased/Case_Ignorable boundary cases; it also covers invalid-scalar rejection
at the JSON/string boundary.  Every supported Node runtime must prove the
generated JavaScript helper equals the complete fixture; native must use the
generated Rust table and prove the same.  Ambient runtime lowercasing is only
a diagnostic comparison and may differ without becoming authority.  Both
repositories pin the byte-identical generator inputs, fixture, native lookup
artifact, and manifest SHA; Synapse additionally pins its generated JavaScript
helper SHA.  Any UCD, generator, supported runtime, or generated output change
requires a reviewed digest and plan-version decision.  Unknown digests fail in
Synapse before the owner/native call and fail again in native as defense in
depth.  This issue changes only the v15 entity-comparison helper; unrelated
indexing keys, lexical text, answer normalization, and other lowercase uses
are outside this contract and are not migrated implicitly.

### PPR and materialization

Synapse supplies the final bounded node-id/finite-score seed map,
`teleportProbability`, `convergenceEpsilon`, `maxIterations`,
`hubDegreeThreshold`, and separate passage/entity rank limits.

`hubDegreeThreshold` has one authority: the QueryService PPR policy placed in
the shared PPR request/plan.  `SimplePPR` consumes that request value; it has no
independent constructor default that can disagree with the emitted native
plan.

The shared reference and native use these canonical orders:

- seed entries: node id ascending;
- graph nodes: node id ascending;
- outgoing edges: source id, target id, then IEEE-754 weight total order;
- floating-point accumulation: the above serial order with no parallel
  reduction or fused alternative;
- ranked passages/entities: score descending, then node id ascending.

Arithmetic follows current `SimplePPR`: only graph endpoint nodes exist; absent
seeds are ignored; the signed algebraic seed sum `S` is accumulated in node-id
order, scores are divided by `S` when `S > 0`, and teleport is uniform when
`S <= 0`; non-dangling nodes distribute `(1-teleportProbability)` by normalized
outgoing weight; dangling mass is dropped; schema targets above the degree
threshold receive `1/log2(totalDegree+2)` damping; convergence uses L1 delta
after a complete iteration.  This is not absolute-value L1 normalization.
`TransitionEntry` does not expose edge ids, so edge ids are deliberately absent
from the canonical order.  Duplicate source/target/weight entries are
arithmetically indistinguishable; no backend-specific insertion ordinal enters
the contract.

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
- at most 8 search slots; facts inspected 1,500,000;
- graph nodes 1,500,000; graph edges 4,000,000; iterations 128;
- search result limit 100 per slot; expansion results 64; seeds 512;
- returned passages 100 and facts 100, combined objects 128;
- complete input JSON-RPC line 256 KiB;
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
the corpus cannot fit its requested/hard bound.  Aggregate work is exactly
`vectorComparisons + factsInspected + iterations * (nodesInitialized +
edgesVisited) + objectsConsideredForEncoding`; every multiplication and sum is
checked before allocation/work.  `maxTransientBytes` is the checked sum of all
request-owned vector/seed/result capacities, two PPR score arrays, heaps, and
the response buffer; the immutable read catalog is steady-state memory and is
reported separately.  Allocation checks cover both capacity and bytes before
allocation.  Response serialization uses borrowed typed objects and an
upper-bounded writer; no full `serde_json::Value` result clone is allowed.  If
one complete object cannot fit, the whole request fails with a response-limit
error.  Bytes include the JSON-RPC envelope and newline.  No truncation or
partial success is valid.

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

The sole error-code authority is `AGDB-ERROR-CODES@1.0.0`.  The first contract
change corrects its stale `INVALID_THRESHOLD` description to the exact vector
primitive's finite `[-1,1]` domain and adds reviewed retry/failure metadata
there, rather than defining operation-local codes.

Central error behavior is:

| Class | Retry policy |
| --- | --- |
| validation, unsupported operation/digest, hard cap | nonretryable configuration error; service stays unavailable until corrected |
| stale generation or lease loss | at most 2 attempts per HTTP request with bounded backoff and a fresh session |
| operation deadline | no same-request replay; return 503 and record work counters |
| RecoveryPending or durability failure | no automatic loop; operator reconciliation required |
| native death or OOM | open circuit immediately; owner fails closed; systemd start limit is the restart bound |

Indexing infrastructure errors never consume the document-content failure
budget, but that exemption is not an unlimited retry.  For v1, stale/lease loss
gets one replay (2 total attempts) after 100 ms; deadline gets no replay.
Recovery/durability opens an operator-reset circuit immediately.  Three
owner/native death or OOM events in 10 minutes open a 10-minute circuit, and
the tracked owner unit uses the same `StartLimitIntervalSec=10min` and
`StartLimitBurst=3`.  A successful owner start plus 10 completed retrievals
closes the timed circuit.  The worker records a separate durable infra event,
requeues the document once, exits, and resets that document's infra state only
after an exact generation commit.  It never increments the document-content
failure counter.

The bridge queue holds at most 8 requests, permits one active generation
session, and gives a queued request 2 seconds to start.  Timeout returns 503.
HTTP disconnect removes queued work before activation.  Once active, a
disconnect is governed by the hard native/session deadline; v1 does not promise
immediate cancellation.  Production session deadline is 8 seconds (below the
10-second API caller budget); every bulk operation receives the remaining
monotonic budget, never a fresh 8 seconds.

## Negative-test and rollout gates

Before typed operation implementation:

- delete the superseded single-RPC contract, fixtures, ignored test, and local
  error definitions so only this design remains authoritative;
- freeze the Synapse staged request/operation-plan helpers and ordering helpers
  without weakening its existing test gates, but do not approve or merge them
  until the following copied-production and cross-repository evidence exists;
- record an immutable parity artifact naming the copied canonical generation,
  canonical/manifest/blob hashes, Synapse base and candidate SHAs, GraphDB SHA,
  fixture SHA, exact command and supported runtimes, plus before/after ids,
  ranks, scores, maximum score delta, absent-seed behavior, signed sum `S <= 0`,
  dangling loss, hub damping, and convergence;
- add SHA-pinned cross-repository fixtures for candidate/domain shapes and the
  normalization vectors described above, and verify byte-identical fixture
  authority in both repositories;
- replace the context-builder index zip with an id-keyed association shared by
  legacy and bounded paths; treat this score/provenance correction as semantic
  hardening covered by the same pre-merge parity artifact;
- test the exclusive `GenerationSession`: renewal ordering, lease loss,
  `finally` release, queued writer, and two simultaneous HTTP requests where
  only one session is active.

Synapse owns the versioned `V15CopiedProductionParityArtifact@1` schema and
its generate/check tooling.  Literature Hub's private repository owns the
detailed copied-production artifact when it contains production identifiers.
The detailed schema requires the exact generation and
canonical/owner-manifest/vector-blob hashes; the *evaluated* Synapse base and
candidate commits/tree digests and GraphDB commit/tree digest; domain and
normalization manifest SHAs; Node/V8/ICU/Unicode/OS/architecture; tool SHA and
normalized arguments; before/after candidate, expansion, PPR, and
id-association ids, ranks, and scores; maximum absolute score and rank deltas;
missing-object audit; absent-seed, signed-sum `S <= 0`, dangling-loss,
hub-damping, and convergence cases; accepted semantic changes; and a final
pass/fail verdict.  The evaluated source authorities are frozen before any
later commit that pins their attestation, avoiding a self-referential commit
hash cycle.  CI verifies the behavior-bearing paths and tree digests of those
evaluated commits, not the later attestation-pin commit as if it had been the
evaluated implementation.

Synapse and GraphDB separately pin byte-identical
`V15CopiedProductionParityAttestation@1` JSON.  Its public schema contains only
the detailed artifact SHA, evaluated source authorities, public domain and
normalization manifest SHAs, required-case names with pass/fail booleans,
aggregate count/delta bounds, accepted semantic-change identifiers, and the
final verdict.  It contains no production ids, paths, query text, scores that
can identify a document, or canonical artifact hashes.  Public CI validates
the exact attestation bytes/schema, public hashes, evaluated source ancestry
and behavior-path tree digests; it does not claim to reconstruct or validate
private evidence from a hash alone.  Literature Hub CI is the authority that
validates the detailed artifact, read-only copy manifest, required cases, and
the equality between its redacted projection and the public attestation.

The schema and checker have one executable authority.  Synapse owns a strict
Zod contract, the generated JSON Schema, the deterministic artifact builder,
and the private/public checker CLI.  The generated schema must be byte-derived
from that Zod contract in CI; Literature Hub and GraphDB do not maintain a
second field allow-list or a second comparison validator.  Literature Hub
invokes the checker from an exact-SHA Synapse checkout, verifies that checkout
and its behavior-path tree digest before passing any private artifact path, and
records that evaluated Synapse authority in the artifact.  The checker emits
stable error classes only and never logs private values.  Public repositories
pin only the generated public schema and exact public-attestation bytes.

The artifact builder is not a public boundary that accepts caller-assembled
comparison rows, case booleans, or behavioral observations.  Synapse owns one
`V15ParityEvaluator` which selects the versioned fixture cases, invokes the
evaluated legacy and candidate operations through typed read-only ports, and
derives candidate, expansion, PPR, association, missing-object, absent-seed,
signed-sum, dangling-loss, hub-damping, convergence, and tie-order traces from
those calls.  Only that evaluator can call the internal builder.  A Hub driver
may provide copied-state and exact-runtime operation ports, source metadata,
and held manifest bytes; it cannot provide a precomputed trace, required-case
map, verdict, or public-manifest digest.  Tests use instrumented ports to prove
the evaluator issued every required call and that omitted, reordered,
short-circuited, or substituted calls cannot produce an artifact.  The Linux
copied-production gate then uses real base/candidate ports and records the
operation-transcript digest and per-case call counts in the private artifact.

Execution authority is established outside the checker before any private
path is disclosed.  The Hub-owned bootstrap receives only public repository
locations and expected commits, creates or selects clean detached checkouts,
verifies commit existence, strict ancestry, tracked cleanliness, and tree
digests, force-builds the checker from the candidate checkout, and launches
the absolute script inside that same checkout.  Only after those checks pass
does it provide copied-root, artifact, manifest, or fixture paths to the child.
The child also compares its `import.meta.url` real path with the expected
checkout as defense in depth, but an already-loaded checker never treats a
caller-provided decoy checkout as proof of its own identity.  Bootstrap tests
execute a checker from checkout A while presenting clean checkout B and require
rejection before B receives any private path.  A stale incremental build and a
modified ignored `dist` tree are also rejected or overwritten by a forced
fresh emit before launch.

The source fields and CI inputs are named by lifecycle, not by an ambiguous
`toolSha`.  The detailed private artifact records
`evaluatedSynapseBaseCommit/treeDigest`,
`evaluatedSynapseCandidateCommit/treeDigest`,
`evaluatedGraphDbCommit/treeDigest`, and the private-only
`evaluatedHubDriverCommit/treeDigest`.  Its `synapseCheckerCommit` is required
to equal `evaluatedSynapseCandidateCommit`; normalized arguments are recorded
separately.  Base is a strict ancestor of candidate.  The later public
attestation contains only the evaluated Synapse and GraphDB authorities, never
the private Hub driver authority.  A public CI run obtains
`synapseAttestationPinCommit` or `graphDbAttestationPinCommit` from its own
checked-out `HEAD` rather than artifact JSON, requires the corresponding
evaluated commit to be a strict ancestor, and recomputes the declared
behavior-path tree digest from the evaluated commit.  Pin commits are never
fields inside the attestation they add, so no self-addressed hash is possible.

Hashes bind bytes rather than caller assertions.  The copy-manifest hash is
computed by the checker from the exact held manifest bytes; the fixture hash
is computed from the exact held fixture bytes; and the public detailed-artifact
hash is computed from the exact private artifact bytes supplied to projection.
No API accepts any of those digests as an unverified replacement for the
corresponding bytes.  Synapse's builder owns one deterministic UTF-8 JSON
serialization (`JSON.stringify` over its fixed insertion-order DTO, two-space
indent, one trailing LF, finite numbers only, and `-0` normalized to `0`).  A
checker reserializes a parsed manifest, detailed artifact, or public
attestation and requires byte equality before hashing it.  This avoids both a
self-hash field and a second ad-hoc canonical-JSON implementation.

All Git/source-authority subprocesses have an explicit monotonic deadline,
bounded stdout and stderr, and process-group termination and reap on timeout.
A timeout is a stable fail-closed source-authority error and never a partial
success.  Hosted CI runs the parity schema, evaluator, bootstrap, and CLI
negative suites on both Node 22 and the supported production Node 24 runtime;
Unicode-only Node 24 coverage is not sufficient for this boundary.

Comparison evidence is derived, not declared.  Each result list has unique
IDs, ranks exactly `1..N`, array order equal to rank order, finite scores, and
the operation-specific deterministic ID tie order.  The checker associates
before/after rows by ID and recomputes per-operation `beforeCount`,
`afterCount`, `matchedCount`, `changedRankCount`, maximum absolute score delta,
and maximum rank delta.  Empty result pairs have both maxima `0`.  V1 never
permits an added or removed candidate, expansion, or PPR ID, so the matched
population is the complete identical ID set rather than an intersection that
can hide a removal.  Candidate IDs, ranks, and scores must satisfy exact V15
parity.  Expansion and PPR keep the same ID set and scores within the
versioned Synapse-owned tolerance; rank changes are permitted only within an
equal-score tie group and only when the matching hardening enum is present.
The after-list must then be score descending and ID ascending, and the checker
derives every changed rank from those two lists.

The closed V1 accepted-change enum is exactly
`semantic-expansion-tie-order`, `semantic-ppr-tie-order`, and
`semantic-id-keyed-context-association`.  The first two authorize only their
operation's equal-score rank changes.  The third applies to a separately typed
association comparison whose row key is the ranked result ID: before and after
row-key sets remain identical, and every changed passage/fact association must
equal the output of the shared ID-keyed context helper.  It does not authorize
candidate additions, removals, or score changes.  Adding an enum or changing
its allowed difference is a plan-version and parity-review change.

The production missing-object audit records its measured count even when it
blocks rollout.  The negative fixture case `missing-object-fail-closed` proves
the hardened path rejects a missing domain object; the production case
`production-missing-object-zero` passes only when the copied generation audit
finds exactly zero missing references.  A nonzero production count therefore
produces a valid, inspectable artifact with verdict `fail`, never a passing
artifact.  This preserves the ledger rule that the hardening and rollout may
proceed only after the audit proves zero.

The builder derives the final verdict.  `pass` is valid only when every
required case passes, every comparison and missing-object invariant above
holds, all requested accepted changes belong to the closed V1 enum, and no
unaccepted difference remains.  The checker independently derives the same
verdict and rejects a serialized mismatch.  The public checker repeats all
independently checkable shape, closed-enum, required-case, summary, source, and
byte checks; only Literature Hub's private check is allowed to open the
detailed artifact and copied inputs.  Evaluated implementation commits and
behavior-path tree digests remain distinct from any later attestation-pin
commit, so adding the attestation cannot change what was claimed to have been
evaluated.

Generation consumes a user-supplied read-only copied-state root and never
opens the live canonical path.  The copy manifest contains exactly three
entries in canonical order (`canonical`, `ownerManifest`, `vectorBlob`), with
normalized unique relative paths and descriptor fields tied to the named
vector entry.  Before reading, the tool rejects the live path, symlinks,
non-regular files, unexpected hard links, any repeated held `(dev, ino)`, and
any copy manifest whose resolved files escape that root; it revalidates held
file identity and hashes after measurement.  Thus no two roles can alias the
same copied inode and no copied role can alias the live canonical generation.
The Hub driver obtains the exact live canonical, owner-manifest, and
descriptor-selected vector-blob identities from the configured owner boundary,
opens those three roles metadata-only with no-follow authority, and holds their
`(dev, ino, mount-id)` capabilities until copied-root validation finishes.
Every held copied role is compared against every held live role, not merely
against live path strings or `realpath`; equal `(dev, ino)` is rejected even
when the mount ID and pathname differ.  Linux mount metadata is also checked so
a copy root whose mount source is the live generation subtree is rejected
before any copied payload byte is read.  The copy-driver negative suite injects
a different pathname with the live `(dev, ino)` under a distinct mount ID (the
bind-mount case), and the supported Linux E2E uses a real bind mount when its
isolated mount namespace is available.  A platform unable to establish or
compare the live identity capabilities fails closed.
Logs and public artifacts redact copy roots, absolute
paths, production identifiers, query text, and raw errors.  Private `--check`
verifies the evaluated source authorities, detailed artifact and fixture
hashes, required cases, and redacted projection without regenerating
production data; public `--check` verifies only the independently checkable
attestation contract described above.

Before native algorithm wiring:

- prove that a bounded request can be constructed before candidate access and
  then proceeds candidate -> optional expansion -> shared seed builder -> PPR
  without `memory_load`, global projection transfer, or a legacy vector scan;
- reject wrong slot cardinality/order/id/namespace and a wrong normalization
  digest before the next owner/native operation;
- test that the legacy `SimplePPR` execution and emitted PPR plan consume the
  same `hubDegreeThreshold` authority, including dependency-injected values;

- executable validator tests cover every type, unknown field, safe-generation
  boundary, signed/non-finite score, count/byte/allocation formula, operation
  digest, and limit at/above the boundary;
- generation `MAX_SAFE-1 -> MAX_SAFE` commits once, while the next write is
  rejected before manifest dirtying, WAL append, or native mutation; canonical
  JSON, referenced blob, owner manifest, WAL, lock, and audit authority remain
  byte-identical on rejection;
- real `protocol_info` advertises each operation as read/WAL-free with the exact
  schema/limit digest;
- real-native tests cover nonzero generation, stale generation,
  RecoveryPending, writer waiting across all three operations, deadline via
  fake monotonic clock, allocation failpoints, response budget, partial frame,
  kill/OOM classification, and byte-invariant canonical/manifest/blob/WAL
  state.

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
