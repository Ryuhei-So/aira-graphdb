# Issue 10 implementation checkpoint

This note authorizes implementation of the native boundary described by
`bounded-retrieval-data-plane.md`; it does not create another semantic
authority. The byte-pinned retrieval contract, fixture, and manifest under
`spec/contracts/bounded-retrieval/` are the semantic authority for the exact
operation set, producer refinement IR, and operation digests. The five
dependencies named by that manifest are pinned by source repository, branch,
commit, path, byte length, and SHA-256; the domain contract is consumed only
through that delegated dependency. Versioned Synapse plans remain the query-
policy authority.

## Authority and flow

- Synapse owns plan construction, normalization, ordering policy, and all
  retrieval semantics. The pinned retrieval artifact is the producer
  contract for those semantics; native accepts only explicit, complete
  operation requests and must not supply policy defaults.
- Native owns descriptor-backed lookup/arithmetic, checked allocation, the
  monotonic operation deadline, exact work counters, and bounded response
  framing for `candidate_search_bounded@1`, `fact_expand_bounded@1`, and
  `ppr_materialize_bounded@1`.
- Hub remains the generation-session, lease, availability, retry, and circuit
  authority. This issue adds no Hub behavior and no production cutover.

The implementation checkpoint is contract-first: freeze one Rust contract
module and executable negative tests before adding the three algorithms. That
module is the sole native authority for method inventory, request validation,
hard limits, checked work/allocation formulas, counters, deadline checks,
response bounds, and `protocol_info` metadata. Domain shapes are consumed from
the pinned Synapse artifacts rather than restated independently.

## Invariants and state

- Only an Idle descriptor reader at the requested committed generation admits
  an operation. Reads are WAL-free and never mutate canonical JSON, vector
  blobs, manifests, sidecars, recovery state, caches, or generation.
- Every request is validated and preflighted before corpus-sized allocation or
  work. Unknown fields, missing policy values, stale generation, unsupported
  digests, non-finite values, and checked-arithmetic overflow fail closed.
- Each operation receives Hub's decreasing session remainder and caps it at the
  native 60-second maximum. It must never start a fresh full budget for a later
  stage. Within that remainder, the complete native request has one
  non-resetting monotonic budget. Deadline checks occur at least every 1024 work
  units and before allocation/materialization. A deadline error returns
  counters for completed work but no result payload; truncation and partial
  success are forbidden.
- Response framing is capped before publication. Borrowed typed objects are
  serialized through an upper-bounded writer; a full snapshot or full
  `serde_json::Value` result clone is forbidden.
- Native remains single-request stdio. It introduces no shared mutable session,
  retry loop, cancellation worker, CAS, persistent cache, or idempotency token.

## Failure, privacy, compatibility, and rollback

Structured validation/cap/deadline errors leave every durable authority
byte-identical. Native death, OOM, and partial frames remain transport failures
for the owner to classify; native never converts them to success. Requests,
domain text, absolute paths, and raw objects must not be added to logs or public
artifacts; operational reporting is limited to method, generation, phase,
counters, bounds, bytes, duration, and redacted error class.

The canonical storage format is unchanged. Existing operations remain
available until Hub completes a separately reviewed bounded E2E, but there is
no implicit bounded-to-legacy fallback. Rollback selects the prior binary after
drain and does not rewrite data.

## Adversarial checkpoint

Before algorithm wiring, executable tests must prove:

1. exact method/digest/read/WAL-free inventory and rejection of unknown fields;
2. every count, byte, work, allocation, generation, and deadline boundary at
   the limit and immediately above it, including checked overflow;
3. wrong slot order/cardinality/id/namespace/corpus, unsupported normalization
   digest, malformed domain objects, signed/non-finite scores, and stale state
   fail before the next operation or corpus-sized work; each candidate hit must
   additionally prove finite `[-1,1]` threshold satisfaction, score-desc/id-asc
   ordering, uniqueness, and agreement among native-id prefix, object kind,
   embedded object id, namespace, and corpus;
4. fake-clock deadline failures expose completed work counters, expose no
   partial result, and do not reset the budget between phases;
5. response overflow, allocation failpoints, native death/partial frame, and
   RecoveryPending-shaped state cannot change canonical/blob/sidecar bytes;
6. no bounded code path calls `memory_load`, emits a global projection, scans a
   legacy response snapshot, or duplicates Synapse policy constants;
7. one decreasing session remainder is enforced across candidate, optional
   expansion, and PPR operations without reset, including exact-deadline ties;
8. while each real-native read operation runs, a queued writer may let the Hub
   owner mark its manifest dirty at base generation N, but native mutation and
   publication remain blocked: canonical JSON, vector blob, WAL, committed
   generation, and native sidecars stay byte-identical. The test must assert
   the permitted owner-manifest transition separately from native invariance.

Algorithm and real-native checkpoints may start only after fresh review of
this boundary and its negative tests.
