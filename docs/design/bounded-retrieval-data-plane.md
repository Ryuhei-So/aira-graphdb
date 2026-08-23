# Bounded retrieval data plane

Status: design checkpoint; no native method is authorized by this document.

This design supersedes the single `retrieve_bounded` pipeline proposed in the
first issue-4 contract checkpoint.  Aira Synapse remains the query-policy
authority.  Aira GraphDB executes a small set of bounded read plans over one
exact committed generation; it does not independently choose retrieval,
fusion, seed, ranking, or context policy.

## Decision and alternatives

The selected boundary is three bulk phases under one Literature Hub owner
reader lease:

1. `candidate_search_bounded` executes the requested passage, fact, and schema
   vector searches and materializes only those candidates.
2. Synapse applies its versioned node-initialization policy to those bounded
   candidates.  When the active policy requests comparison expansion,
   `fact_expand_bounded` returns a bounded, stable set of matching facts.
3. `ppr_materialize_bounded` executes the explicit PPR plan and materializes
   only the ranked passages and facts required by the caller.

All three requests carry the same decimal-string `expectedGeneration`.  The
owner holds and renews one reader lease around the whole sequence, rejects a
different generation, and final-validates the generation before releasing the
result.  Native also checks the expected generation at admission.  Publication
of generation N+1 remains blocked until the lease for N is released.

Alternatives rejected:

- A single query-pipeline RPC duplicates Synapse policy in Rust and can silently
  change ranking when either repository evolves.
- Per-node or per-edge RPCs preserve authority but amplify queueing and lease
  duration at production graph size.
- Full immutable snapshots preserve generation consistency but duplicate
  multi-GiB state and retain the transfer/cache failure mode.
- Larger deadlines, response lines, cgroups, or restart delays do not bound
  work or establish generation authority.

## Authority and supported profile

- Canonical JSON and its generation descriptor are the only committed data
  authority.  The owner is the only process allowed to reach native stdio.
- The owner owns roles, reader leases, writer exclusion, generation admission,
  native-death classification, and publication blocking.  Requests contain no
  caller-supplied role or lease identifier.
- Native owns storage lookup, exact vector scoring, graph traversal, domain
  object lookup, hard resource accounting, and response framing.
- Synapse owns normalization, embedding, query-mode detection, feature flags,
  candidate/seed policy, PPR parameters, context construction, answers, and
  provenance formatting.
- The first supported policy profile is the production v15 path:
  `VectorMemoryFilter` (passage/fact/schema), `SimpleNodeInitializer`, and
  `SimplePPR`.  Lexical RRF, dictionary injection, subquery decomposition, and
  other profiles are not silently approximated.  A client requesting an
  unadvertised profile fails before work with `PROTOCOL_VERSION_MISMATCH`.

The Synapse adapter supplies every policy value that native executes.  Native
must not fill missing policy values from its own defaults.  The request records
the Synapse profile version so cross-repository fixtures can detect drift.

## Generation and state machine

Generation values cross JSON boundaries as canonical unsigned decimal strings
(`"0"`, `"1"`, ...), with no leading zero except `"0"`.  This avoids loss of
integer identity in JavaScript.  Native converts to `u64` only after lexical
validation and overflow checking.

The sequence is:

1. Owner admits a control-client lease for committed generation N.
2. Each bulk RPC is admitted only when native is Idle at N.  RecoveryPending,
   partial generation state, N-1/N+1, and a dirty state with a different base
   generation fail closed.
3. A writer may become queued/dirty at base N, but no native mutation or N+1
   publication occurs while the lease is live and renewable.
4. Each response repeats N.  Synapse rejects a missing or different generation.
5. After the last response, the owner performs final lease renewal/validation,
   then releases the lease.  Only then may queued publication proceed.

No read RPC writes WAL, changes the manifest, advances a generation, populates
a persistent cache, or mutates canonical in-memory state.

## Bulk contracts

All request objects are versioned, reject unknown fields, and are validated
before corpus-sized allocation or work.  Contract limits are advertised by
`protocol_info`; clients may request smaller values but never enlarge native
hard maxima.

### Candidate search

The request contains an exact query vector and an ordered list of search slots.
Each slot names a supported namespace, threshold, and result limit.  The v15
profile requires exactly one passage slot, one fact slot, and one schema slot.
Results preserve the slot boundary and contain the complete Synapse domain
object plus exact cosine score.  Missing or malformed referenced objects fail
the whole request; they are not silently dropped.

Tie order is score descending then native node id ascending.  This formalizes a
stable order where the previous JavaScript sort depended on projection order;
golden parity must show unchanged scores and explicitly review equal-score
ordering as the only intentional semantic hardening.

### Comparison fact expansion

Synapse decides whether comparison expansion applies.  When enabled it submits
a bounded map of case-folded seed entity to its highest finite seed similarity,
plus the explicit attenuation and result limit from its versioned profile.
Native finds active facts sharing a head or tail entity, excludes existing
seeds, derives `max(matchingEntityScore) * attenuation`, and returns score
descending then fact id ascending up to the requested limit.  These operations
are an explicit data-plane plan; native neither detects comparison queries nor
chooses attenuation or limits.  This preserves the current highest-entity-score
and top-20 behavior without transferring every fact to Synapse.

### PPR and materialization

Synapse submits the final bounded map of node-id to finite, nonnegative seed
score plus all PPR values.  For the v15 profile native executes the current
`SimplePPR` semantics exactly:

- nodes are the endpoints present in the corpus graph projection;
- seed scores on absent nodes do not enter the teleport vector;
- teleport is L1-normalized, or uniform when no submitted seed is present;
- each non-dangling node distributes `(1 - teleportProbability)` by normalized
  outgoing edge weight;
- dangling mass is dropped, matching the current implementation;
- teleport contribution is `teleportProbability * teleport[v]`;
- schema targets whose total in+out degree exceeds `hubDegreeThreshold` receive
  damping `1 / log2(totalDegree + 2)`;
- convergence is the L1 delta threshold after each complete iteration;
- passages and non-passage entities are ranked separately by score descending,
  then node id ascending.

The response contains ranked node ids/scores and complete passage/fact domain
objects for the selected ranks.  It does not invent a generic `Fact.value` and
does not serialize schemas/entities that the v15 context builder discards.
Materialized objects retain the fields needed for citation and provenance.

## Hard resource accounting

Native hard maxima, not request values, are the allocation authority.  Initial
production values must be derived from a copied production generation and kept
in one Rust contract module shared by validation, `protocol_info`, and tests.
They include:

- query dimensions and total query bytes;
- vector comparisons per slot and total;
- seed count;
- facts scanned and expansion results;
- graph nodes, graph edges, and PPR iterations;
- returned passage count, fact count, per-object encoded bytes, and complete
  JSON-RPC frame bytes;
- monotonic elapsed deadline.

Counters use checked integer arithmetic.  A preflight count exceeding a hard or
requested work cap fails before work.  Each vector comparison, fact inspected,
node initialized, edge visited per iteration, and object considered for output
has one specified counter increment.  Deadline and cancellation checks occur at
least every 1024 work units and before materialization.

Output is encoded incrementally into a buffer whose capacity never exceeds the
hard frame maximum.  Before cloning or encoding variable-size text, arrays, or
metadata, native checks stored byte/count metadata against the remaining item
and frame budgets.  If the next complete item cannot fit, the entire RPC fails
with `RETRIEVE_BOUNDED_RESPONSE_LIMIT`; it never truncates a domain object and
never returns partial success.  The byte count covers the complete UTF-8
JSON-RPC line including envelope and newline.

## Failure ownership

- Native returns structured validation, version, stale-generation,
  RecoveryPending, work-limit, response-limit, and deadline errors while it is
  alive.
- A client disconnect does not authorize unbounded background work.  The owner
  cancellation/deadline path must cause native work to stop before the request
  slot is reused.
- Native death, SIGKILL, cgroup OOM, or a partial frame cannot be reported by
  dead native code.  The owner maps them to the existing authoritative native
  transport/death error, fails closed, and does not restart in a tight loop.
- Literature Hub treats all GraphDB/owner/durability failures as retryable
  infrastructure failures.  They never consume a document failure budget.
- Every failure path leaves canonical JSON, descriptor/blob, manifest, WAL,
  and generation unchanged.

New codes belong in the central AGDB error registry with one retryability and
failure-class definition.  Contract files may reference those codes but may
not define a second local registry.

## Test and rollout gates

Before algorithm implementation:

- freeze typed request/response structs, one Rust validator/cap authority, and
  cross-repository v15 fixtures derived from the actual Synapse domain shapes;
- make boundary tests execute the validator rather than inspect contract text;
- record the intentional equal-score tie hardening and otherwise require exact
  candidate, seed, PPR score, rank, domain-object, and generation parity.

Before method wiring:

- real native `protocol_info` advertises every method as `read`, `wal=false`,
  with the exact contract/profile and hard-limit digest;
- real-native negatives cover generation N, stale/overflow generation,
  RecoveryPending, policy drift, malformed/non-finite input, every cap at and
  above the boundary, deadline/cancel, response framing, native kill/OOM, and
  canonical/manifest/blob byte invariance;
- a writer is demonstrably blocked across the complete multi-RPC lease and one
  atomic N+1 commit proceeds only after final validation and release.

Before deploy:

- Aira Synapse uses the new data-plane only for the advertised v15 profile and
  has no generation-unaware snapshot cache on that path;
- copied-production tests record score/rank/domain parity, response bytes,
  latency, RSS/PSS/VmSwap, work counters, and exact generation;
- Literature Hub returns a real semantic result while acquisition,
  interpretation, and indexing continue, and restart/OOM counters remain flat.

Rollback is code-only: keep the canonical data format unchanged and retain the
old low-level reads during migration.  If the bounded capability or profile is
absent, production fails closed unless an explicit operator-controlled legacy
fallback is enabled for a copied/test database; it does not silently return to
the generation-mixing production path.
