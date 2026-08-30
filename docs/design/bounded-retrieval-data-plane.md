# Bounded retrieval data plane

Status: design/contract checkpoint only.  This document authorizes the bounded
contract validation boundary, but no bounded retrieval algorithm dispatch.

This design replaces the first issue-4 `retrieve_bounded` contract.  Aira
Synapse is the sole query-policy authority.  Aira GraphDB defines two or three
versioned, bounded data operations over one committed generation, depending on
whether Synapse requests fact expansion.  Literature Hub owns the generation
session, availability policy, and retry circuit.  The current issue-10
checkpoint defines and validates that contract but does not expose the
operations as executable capabilities.

## Decision

One semantic retrieval uses two required and one optional bulk operation inside
one exclusive owner reader lease:

1. `candidate_search_bounded@1` performs explicitly requested vector searches
   and materializes only their domain objects.
2. `fact_expand_bounded@1` performs an explicit bounded entity-match plan when
   Synapse requests one.
3. `ppr_materialize_bounded@1` runs an explicit graph plan and materializes only
   its selected passages and facts.

The operations do not name, default, or validate a Synapse profile.  At the
current checkpoint, `protocol_info.boundedRetrieval.methods` is empty.  The
contract schema/digest metadata is exposed only in
`boundedRetrieval.checkpoint.unavailableMethods`, alongside
`status=checkpoint`, `availability=unavailable`, and `executable=false`.
Dispatch returns `REQUEST_EXECUTION_FAILED` until algorithm wiring is reviewed.
A profile id may be echoed as opaque audit data but never changes native
behavior.

Rejected alternatives:

- A single query-pipeline RPC makes Rust a second query-policy authority.
- Per-node/per-edge RPCs make queue and lease duration proportional to graph
  fan-out.
- Full immutable reader snapshots retain multi-GiB memory and transfer costs.
- Larger timeouts, response lines, cgroups, or restart delays do not establish
  correctness or bounded work.

## Authorities

- Canonical JSON and its referenced vector blob are the committed GraphDB data
  authority.  The sole scoped legacy exception is the owner-minted
  `LegacyGeneration0Binding@1`, which is only the canonical/blob association
  authority for a clean descriptor-less generation zero; it never replaces
  the canonical publication pointer, and a copy manifest remains subordinate
  evidence.  The binding is ignored and forbidden once a descriptor generation
  exists.  The adjacent owner generation manifest records owner admission
  state; it does not replace the canonical JSON publication pointer.  WAL is
  unpublished recovery input and is never reader-visible.
- The owner alone reaches native stdio and owns roles, writer exclusion, reader
  lease admission/renewal/release, publication blocking, and native-death
  classification.  Native requests contain no caller role or lease claim.
- The pinned producer `refinementNodes` declaration is the sole structural
  authority for refinement node roles and field markers.  Native supplies only
  its executable opcode-handler inventory and supported primitive marker
  inventory, and rejects any parsed opcode set that is not exactly equal to
  the handler set.  Producer-owned assertion witnesses are likewise pinned and
  executed in declaration-derived order; native does not restate their rule or
  mutation catalog.
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

One Rust contract module is the authority for validation, executable
`protocol_info` inventory, checked allocation formulas, and tests.  Its
refinement structure is parsed from the pinned producer declaration; it does
not restate producer node roles or field grammar.  Request values may reduce
but never raise hard maxima.  Initial ceilings are reviewed against the copied
production generation and provide at least 2x count headroom while remaining
below the owner cgroup headroom:

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
comparison rows, case booleans, behavioral observations, or generic operation
ports.  Synapse owns one `V15ParityEvaluator` which selects the versioned
fixture cases and evaluates two isolated operation-runner processes minted and
owned by the trusted Hub bootstrap from the exact base and candidate build
capabilities described below.  The evaluator, not
the Hub driver, assigns semantic runner roles and verifies that the two process IDs,
implementation commits, trees, and build digests are distinct where required.
It rejects swapped roles, two roles connected to one process/build, an
unattested implementation root, or a runner response whose authority handshake
does not match the execution manifest.  Hub supplies only held copied-state
capabilities and source bytes after both runners are authoritative and ready;
it cannot provide a runner callback, precomputed trace, required-case map,
verdict, or public-manifest digest.

The evaluator derives candidate, expansion, PPR, association, missing-object,
absent-seed, signed-sum, dangling-loss, hub-damping, convergence, and tie-order
evidence from the actual runner requests and responses.  Only it can call the
internal builder.  Unit tests use deliberately swapped, shared, fabricated,
short-circuited, and hanging runner processes and require failure.  The Linux
copied-production gate uses the same process path and records the complete
private operation transcript rather than accepting a test-only port path as
production evidence.

Execution authority is established outside the checker before any private
path is disclosed.  The Hub repository pins a strict
`V15ParityExecutionManifest@1`; command-line expected commits never override
it.  The manifest has first-class `evaluator`, `base`, `candidate`, and
`copiedGraphDbNative` records.  Each names its owning commits/trees, repository
identities, role-specific
package-lock hash, dependency/build roots, implementation entrypoint,
candidate-owned adapter entrypoint, exact install/bundle argv, bundler identity
and version, input-graph manifest hash, and sealed-bundle byte hash where
applicable.  The copied-native record instead pins the GraphDB native
commit/tree, build-manifest hash, sealed executable hash, descriptor-only read-
generation protocol, and exact read-method inventory digest.  The evaluator record is bound to the exact candidate
Synapse commit/tree and has its own entrypoint, input graph, bundle hash, and
launch argv; it is not inferred from the runner records.  The manifest also
pins public-manifest byte hashes; the Git, Node, npm-cli, and bundler executable
identities with file hashes and versions; the environment allowlist; and all
byte/time/disk limits.  Git runs with system/global/local config, hooks,
replacement refs, alternates, object quarantine, and caller Git environment
disabled; the bootstrap independently hashes the extracted tracked tree rather
than trusting a worktree status string.  The evaluated Hub driver commit and
execution-manifest byte hash are private-artifact fields.

An outer Hub build coordinator never receives a private path, copied file,
fixture, or artifact and is orchestration-only: it starts, waits for, and
validates one role-scoped dedicated build unit at a time, but never writes a
source, dependency, build, HOME, cache, temporary, package-store, or bundle
byte itself.  Inside that capped unit, the builder creates unique mode-0700
roots, materializes the exact commit without reused ignored files, verifies
the extracted tracked tree, performs a lockfile-clean install and forced build
in isolated dependency/output roots, and uses the manifest-pinned bundler to
produce the self-contained ESM role or evaluator bundle.  Each input-
graph manifest covers the exact adapter, implementation, transitive
JavaScript, and embedded data bytes used by that bundle, but never lists or
hashes itself; its exact serialized-byte hash and the output-bundle hash are
held by the execution manifest.  Dynamic imports, runtime filesystem module
resolution, native addons, and undeclared external data are rejected from each
runner input graph.  GraphDB operations and copied-state access remain external
only through the Hub-brokered copied-native capability and bounded protocol;
no runner is given a DB path or filesystem permission.
The candidate adapter therefore cannot resolve base dependencies through the
candidate dependency root or ambient parent paths.  The bootstrap invokes
absolute runtime and tool paths with `PATH`, `NODE_OPTIONS`, preload/import
hooks, package-manager configuration, and unrelated environment removed;
only the manifest allowlist exists.  Every writable build `HOME`, cache,
temporary directory, and package-manager store is below the same charged
unique `/dev/shm` root.
Dependency installation uses `npm ci --ignore-scripts` unless the pinned
dependency graph proves a lifecycle step is required.  An allowed lifecycle
step has its package-integrity and script bytes in the role input manifest and
runs inside that same role-scoped transient user-systemd build unit under the
same sanitized environment and output/disk/overall deadline.  Every such unit
uses `KillMode=control-group` with bounded `TasksMax`, `MemoryMax`, and
`MemorySwapMax=0`.  Source/dependency/build
roots live in a unique `/dev/shm` directory, so generated bytes are charged to
that cgroup's hard memory ceiling; capacity is checked before start.  Timeout,
failure, or cgroup OOM stops the whole unit, waits for an empty cgroup, removes
the unique root, and verifies cleanup before any private input is opened.  A
lifecycle step therefore cannot select an ambient runtime, escape by
`setsid`/double-fork, or leave a descendant.  The coordinator publishes only
the public bundle, input-graph, runtime, and tool outputs named by the execution
manifest.  The coordinator waits for the successful build cgroup to become
empty before independently validating those published outputs.

The separately tracked network-filtered evaluation bootstrap does not contact
the user-systemd manager or run an installer/build.  It independently
revalidates source, lockfile, runtime/tool identity, every published input
graph, and bundle bytes before opening a private input.  It then writes each
verified bundle, the Node executable, and the GraphDB native
executable into separate Linux `memfd_create` files and applies
`F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL`; it verifies the
seal mask and byte hash from every held descriptor before launch.  It executes
the sealed Node or GraphDB ELF through `/proc/self/fd/<runtime-fd>`, so neither
rename nor in-place mutation of the original user-writable inode can change
executed bytes.  System dynamic-loader and
shared-library identities are recorded and revalidated as the explicit host
runtime trust boundary; they are not copied into, or confused with, Synapse
implementation authority.  Unsupported `memfd`/seal/held-exec semantics fail
closed before private disclosure.  This supported-DGX boundary deliberately
does not depend on an unprivileged user or mount namespace.

The evaluation entrypoint is a tracked one-shot user-systemd unit with
`SystemCallFilter=~@network-io`.  Before opening a private input, the Hub
bootstrap proves the effective classic-seccomp boundary with a real child
negative: `socket`, `socketpair`, `connect`, `bind`, `listen`, `accept`,
`send*`, and `recv*` fail, while anonymous-pipe `read`/`write` succeeds.  A
platform where the filter or probe is unavailable fails closed; the ineffective
`IPAddressDeny=` cgroup-eBPF property is not used as authority.  The execution
manifest and transcript bind the tracked unit/drop-in bytes, kernel release,
architecture, systemd version, the fully expanded architecture-specific
`@network-io` syscall-list digest, and the per-run raw-syscall probe result;
any drift requires a reviewed manifest update.

The same evaluation unit pins `TasksMax=32`, `MemoryHigh=48G`,
`MemoryMax=56G`, and `MemorySwapMax=0` for the first copied-production
generation.  These values are based on the measured approximately 17 GiB
read-only GraphDB baseline plus bounded four-process/workspace headroom, not on
making the live service unlimited.  Its private artifact records
`memory.current`, `memory.peak`, `memory.swap.current`, and `memory.events`
before/after and at peak.  A later generation must recompute the boundary from
copied-production PSS plus the declared transient-work formula before approval;
OOM or `memory.max` ends the run without an artifact and with copied hashes
unchanged.

The Hub bootstrap is the sole process and channel owner.  It launches the
candidate-owned evaluator and the base/candidate runners as three direct
children from their
sealed bundles and sealed runtime, using fixed `--permission --input-type=module
-` argv and the bundle descriptor as standard input.  It retains every process
handle and creates each anonymous protocol/stdout/stderr pipe itself; no shell,
caller executable, mutable entrypoint pathname, caller callback, socket, or
caller-provided pipe is accepted.  The standard-input descriptor is used only
for module loading.
Runners receive no filesystem, child-process, worker-thread, native-addon, or
WASI permission and inherit the unit's network-IO syscall denial.  Supported
Node 22 and Node 24 E2E must prove pathname reads, every denied network syscall,
subprocesses, workers, addons, and WASI fail while inherited anonymous protocol
pipes remain usable.

Hub starts both runners before the evaluator and gives the evaluator an
unordered pair of inherited one-shot runner pipe endpoints at its own spawn.
Hub retains every child handle; only non-authoritative parent-observed PIDs,
nonces, sealed bundle hashes, and input-graph hashes cross the evaluator
protocol.  The evaluator derives base/candidate roles by
matching those authorities to its first-class execution-manifest records; it
does not accept a Hub/caller role label.  Self-reported PID or role is
informational only.  Base and candidate require distinct live child handles,
PIDs, bundle descriptors, and bundle/input-graph hashes.  A valid handshake
binds implementation commit/tree, adapter commit, input-graph and bundle
hashes, runtime hash/version, and protocol version to the already-owned
channel.  Only then does the evaluator mint the semantic role capabilities.
If any child exits or the evaluator rejects authority, Hub terminates and
reaps all remaining direct children under the non-resetting deadline; runners
cannot create descendants.

The candidate evaluator and version-specific adapters are Synapse-owned; an
adapter bundles the named operation implementation only from its attested base
or candidate input graph.  Thus an older base commit need not contain the new
adapter, but the embedded legacy implementation bytes remain bound to that
base commit, input graph, and sealed bundle.  After both capabilities are
minted, Hub opens and validates the copied canonical, owner-manifest, and blob
files as held `O_RDONLY | O_NOFOLLOW` descriptors, streams bounded hashes while
retaining their identities and sizes without materializing whole-file copies,
and launches the sealed GraphDB native as the fourth direct child.  A dedicated
descriptor-only read mode receives only the GraphDB-owned canonical and blob
descriptors plus Hub's already-validated `expectedGeneration` at spawn; the Hub-
owned owner-manifest descriptor or bytes never cross into native.  Native
receives no DB pathname or parent-directory descriptor, never opens a WAL/audit/cache,
never performs recovery or persistence, and exposes only the exact pinned read-
method inventory.  Every mutation, transaction, commit, recovery, import, and
save method is rejected by native dispatch before work.  Native first proves
its process/executable/build/protocol authority, then validates and acknowledges
the expected generation and blob digest from the inherited GraphDB descriptors.
For generation greater than zero, the canonical descriptor is mandatory and
its size/hash must match the blob FD.  For the current legacy generation zero,
Hub must explicitly request `legacyGeneration0` and supply the hash of the
owner-minted `LegacyGeneration0Binding@1` described below; native requires the
canonical state to lack a blob descriptor, never searches an adjacent pathname,
hashes the supplied blob FD, and returns that measured digest.  Any other
legacy/descriptor/generation combination fails closed.  Hub requires the copy
manifest to equal the binding's exact generation-zero canonical/blob authority,
or for later generations to equal the canonical descriptor, and independently
binds the acknowledgement to its held
owner-manifest generation/hash.  Only after that
second acknowledgement does Hub broker evaluator/runner operation frames over
its directly-owned anonymous pipes.  Hub passes no process handle or file
descriptor after spawn.  On completion the checker requires byte-identical
held copied-state identities/hashes, failed write/pwrite/truncate attempts on
the O_RDONLY copied descriptors, no write-capable filesystem open, rename,
unlink, WAL, audit, cache, sidecar, or filesystem mutation event.  Bounded
writes to protocol/diagnostic pipe descriptors are explicitly allowed and are
the only successful writes.  The
normal canonical GraphDB service remains the sole live-data owner; this short-
lived native is an immutable-copy reader and cannot publish a generation by
construction.
The child reports the already-parent-bound bundle/runtime identities only as
defense-in-depth handshake fields.  Bootstrap negatives execute checkout A
while presenting clean checkout B; substitute a runner that echoes every
expected handshake field; swap role handshakes; alias both roles to one
process; mutate source, `node_modules`, or bundle output before sealing; try to
write/truncate/grow the sealed descriptor; replace or overwrite the original
Node/native executable; set `NODE_OPTIONS`; escape the systemd syscall filter;
and attempt a dynamic import, pathname read, subprocess, network syscall, or
direct DB open.  Each must fail before any private copied byte or operation
frame is released, or prove that the
parent-owned child executes only unchanged sealed bytes and communicates only
through Hub-owned anonymous pipes.

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

Every Git, dependency-install, build, evaluator, adapter, and base/candidate
operation subprocess has an explicit monotonic deadline, bounded stdin/stdout/
stderr, and its own process group.  The execution manifest pins positive
deadlines no greater than 30 seconds for source-authority commands, 15 minutes
per install/build, 2 minutes per operation, and 45 minutes overall.  The
overall deadline is never reset by a successful sub-operation.  Timeout or
output overflow sends TERM then bounded KILL to the process group, waits for
close/reap, and returns a stable fail-closed class; it never becomes a partial
success.  Hang and forked-child tests prove no runner remains and no later
artifact can be published.  Hosted CI runs the parity schema, evaluator,
bootstrap, and CLI negative suites on both Node 22 and the supported production
Node 24 runtime; Unicode-only Node 24 coverage is not sufficient for this
boundary.

V1 numeric limits are part of the execution manifest and may only be lower
than these schema maxima: 2 MiB per newline-delimited evaluator or runner
or copied-native request/response frame; 64 MiB cumulative protocol input and
64 MiB cumulative protocol output, plus 1 MiB cumulative diagnostic stdout and
1 MiB cumulative stderr, for each evaluator, runner, or copied native; 4,096
protocol frames per process; 64 MiB total fixture input;
64 MiB cumulative output per Git authority command; 16 MiB cumulative output
per install/build command; 256 transcript events; 64 MiB canonical transcript
bytes; 64 MiB canonical detailed-artifact bytes; 2 MiB canonical public-
attestation bytes; 500,000 input-graph entries; and 64 MiB canonical bytes per
input-graph manifest.  Exact source materialization is limited to 250,000
entries and 1 GiB; each isolated dependency/build root is limited to 1,000,000
entries and 8 GiB; each sealed JavaScript bundle is limited to 8 MiB.  Copied
production state is held rather than embedded and is limited to 16 GiB per
copy-manifest file entry and 32 GiB total, with exact entry sizes recorded
before any payload read.
The sealed Node executable is limited to 512 MiB and the sealed GraphDB native
to 512 MiB, both checked before memfd allocation.  The build unit's 12 GiB
`MemoryMax` jointly bounds process memory and `/dev/shm` build bytes; the 8 GiB
post-build directory limit remains the stricter acceptance bound.
Execution/copy/public manifests are each limited to 2 MiB unless a stricter
schema cap above applies.  Runner and operation-specific result/copy caps
remain the stricter limits where applicable.  Readers
reserve capacity incrementally and stop at limit plus one; they never buffer an
unbounded line or process output before checking.  Boundary tests exercise
each maximum and maximum-plus-one.  On overflow the owning process group is
terminated and reaped under the same non-resetting overall deadline, and no
partial frame, transcript, input-graph manifest, or artifact is accepted.

The private artifact contains a canonical `V15ParityExecutionTranscript@1`,
not only a caller-provided digest.  Each bounded event has consecutive
`ordinal`, closed `caseId`, exact `role` (`base` or `candidate`), runner process
and build authority, closed operation name, canonical request payload, and
exactly one canonical success response or stable error class; it contains no
timestamps or ambient paths.  Request/response byte hashes are derived from
those payloads.  The transcript header binds the execution-manifest hash,
copied-generation manifest hash, fixture hash, evaluator commit/build digest,
both runner handshakes, and the parent-observed copied-native PID/channel
authority, sealed native hash, GraphDB commit/build-manifest hash,
`protocol_info` inventory digest, descriptor-only mode, and validated exact
generation/blob digest together with Hub's owner-manifest hash.  A generation-
zero transcript additionally binds the exact `LegacyGeneration0Binding@1`
bytes/hash, `legacyGeneration0` admission, and canonical descriptor-absence
proof.  The private checker reparses the raw transcript,
recomputes every event hash, call count, comparison, required case, aggregate,
transcript digest, and verdict, and rejects missing, duplicate, reordered,
role-changed, authority-changed, or extra events.  Public projection includes
only the detailed-artifact hash and already-defined redacted aggregate; it does
not expose the private transcript.

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

Before the current descriptor-less generation zero can be copied, the
configured live owner must atomically mint one Hub-owned
`LegacyGeneration0Binding@1`.  While holding the sole owner lock it requires a
clean committed owner manifest at generation zero, native `idle` at generation
zero, no WAL/recovery/dirty state, and no existing mismatched binding.  It opens
the configured canonical and the existing V1 deterministic legacy-blob path as
held no-follow regular single-link files, proves the canonical has no blob
descriptor, streams their hashes, and revalidates identity/size/metadata after
hashing.  The binding records version/generation, canonical hash and live
`(dev, ino, mount-id)`, exact raw owner-manifest hash, blob format/size/hash and
live identity, and the legacy naming-rule version.  Publication creates a
mode-0600 temporary file with `O_CREAT | O_EXCL | O_NOFOLLOW`, performs a
bounded complete write, `fdatasync`/`fsync`, re-reads and verifies its exact
bytes/hash from the held FD, uses `renameat2(RENAME_NOREPLACE)`, and fsyncs the
parent directory.  Any unsupported primitive or incomplete cleanup fails
closed; an existing binding is idempotent only for byte-identical authority.
Binding creation never writes canonical/blob and is
disabled once a descriptor generation exists.  The copy is made from those
same held descriptors before the owner lock is released and must reproduce the
binding hashes.  Missing, rewritten, wrong-blob, dirty, descriptor-present, or
generation-greater-than-zero legacy binding attempts fail closed.

Binding crash recovery runs only under the same sole owner lock and recognizes
one fixed same-directory mode-0600 temporary name.  An exact final binding is
idempotent and the parent directory is re-synced.  When final is absent and the
single temp is a current-user regular single-link file, owner reopens it no-
follow, holds its identity, and freshly derives the expected binding from the
still-held canonical/blob/manifest authority.  Exact complete bytes are file-
synced and resume `RENAME_NOREPLACE` plus directory fsync.  A trusted partial
or mismatched temp is first claimed by identity-preserving no-replace rename to
a second fixed same-directory cleanup name, revalidated against the held FD,
unlinked, directory-synced, and recreated.  Startup always reconciles final,
then the fixed cleanup name, then the fixed temp name.  A cleanup entry is
removable only when a held no-follow FD proves the reserved exact pathname is a
current-user mode-0600 regular single-link file within the binding byte cap;
it is identity-rechecked after a no-replace claim before unlink.  Because both
reserved names are fixed, crash repetition cannot create an unbounded family
of retired artifacts.  Any extra candidate, symlink, hard link, foreign owner,
unexpected mode/type/identity, or failed claim is never deleted and requires
operator intervention.  Startup and fault-injection tests kill after create,
partial/full write, file sync, rename, and directory sync; each restart either
publishes the one exact binding or safely resumes without an unbounded temp or
payload leak.  The matrix also kills after temp-to-cleanup claim rename and
after cleanup unlink, then proves restart converges with at most one reserved
temp/cleanup artifact.

Generation consumes a read-only copied-state root whose authority is the live
descriptor generation or that owner-minted legacy binding; the user-supplied
copy manifest never becomes the source of truth.  It never opens the live
canonical path.  The copy manifest contains exactly three
entries in canonical order (`canonical`, `ownerManifest`, `vectorBlob`), with
normalized unique relative paths and descriptor fields tied to the named
vector entry.  Before reading, the tool rejects the live path, symlinks,
non-regular files, unexpected hard links, any repeated held `(dev, ino)`, and
any copy manifest whose resolved files escape that root; it revalidates held
file identity and hashes after measurement.  Thus no two roles can alias the
same copied inode and no copied role can alias the live canonical generation.
The Hub driver obtains the exact live canonical, owner-manifest, and either
descriptor-selected or legacy-binding-selected vector-blob identities from the configured owner boundary,
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
- real `protocol_info` keeps the bounded executable method list empty and
  exposes each contract operation only as unavailable checkpoint metadata with
  its exact schema/semantic digest;
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
