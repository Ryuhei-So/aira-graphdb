# Production committed-generation reads during indexing

Status: design for [aira-graphdb #45](https://github.com/Ryuhei-So/aira-graphdb/issues/45), informed by the test-only prototype in #43 / draft PR #44 and intended to unblock Literature Hub #481. No production capability is implemented or enabled by this document.

## Decision

Keep one native process and one canonical generation in memory. The current committed state at generation N remains the only reader-visible base while one writer accumulates a bounded overlay against N. Writer reads resolve overlay before base; reader operations resolve only the committed base. Publication stops new reader leases, drains existing leases, applies the already validated overlay in place, and uses the existing vector-blob-first / canonical-JSON-last commit path to publish N+1.

The first implementation boundary is smaller: add a generation-tagged, read-only committed catalog in normal native mode and use it for existing clean-state targeted memory lookup and document-deletion planning. Keep reads during dirty state unavailable. This boundary measures index retention and startup peak, audits duplicate IDs and shared provenance, and proves that every locator is tied to exactly one generation before mutation routing changes.

Do not start overlay integration if that boundary cannot meet the memory gates below. In particular, an `Arc<State>`, a second descriptor reader, or a whole-state clone is not an acceptable fallback.

## Evidence and current authorities

This design was checked against these exact revisions:

- aira-graphdb fork `83ef0eb309deec7e1ddc156ecda19eb4edb51010`;
- Literature Hub production worktree `35b20b3d106e70d17252c281489ed4a44821bab3`;
- deployed Synapse runtime `b99b61e273d269e9e9cbe5490d55bea36b4e64e3`; and
- merged Synapse `production-runtime` candidate `c50e0754caf207eba1a0e2e2f377c56b47c10c4b`. Its delta from the deployed revision is indexing-error annotation and tests, not a generation or storage contract.

The prototype at `116d255d761cb4420c824bb9659e68731c62da5e` proves only that a fixed document delta can be retained independently of base size in a synthetic `Arc<HashMap>` model. It explicitly does not model the production JSON arrays, WAL, vector lineage, caches, or owner.

The authority split remains:

| Concern | Authority |
| --- | --- |
| committed graph/memory data | canonical JSON and the exact vector-blob lineage it names |
| unpublished writer intent | native WAL bound to base generation, transaction nonce, exact inode/device, byte count, record count, and digest |
| writer exclusion, admission and leases | Literature Hub owner and adjacent generation manifest |
| storage lookup, vector arithmetic and bounded graph operations | aira-graphdb native |
| normalization, candidate plan, fact expansion, PPR policy and response validation | Synapse |
| SQLite dictionary/thesaurus snapshot | Synapse-owned SQLite read transaction; it is not part of GraphDB generation N |

WAL content is never reader-visible. The owner manifest is admission and recovery evidence; it never replaces canonical JSON as the data publication pointer.

Current facts that constrain the design:

- Production `State` owns hash maps plus `snapshots: HashMap<String, Value>` and separate generation/vector descriptors (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:181`).
- `Server` separately owns decoded `vector_values`, vector lineage, cache dirtiness, corpus/adjacency indexes, and bounded descriptor indexes (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:1405`). Startup currently copies decoded blob values back into every `VectorRecord` while retaining `vector_values` (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:2057`).
- `memory_upsert` validates the complete stored array target before WAL admission, then clones the incoming arrays and reverse-scans the stored arrays to merge (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:5830`, `83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:5888`, `83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:6045`, and `83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:7086`).
- Cache invalidation clears the live and bounded indexes, and rebuild scans the current mutable state (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:5082`).
- Commit verifies prepared evidence and the held WAL, publishes the immutable vector segment, atomically renames and syncs canonical JSON, retires the WAL, and only then updates in-memory generation metadata (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:3870` and `83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:3931`). A same-generation WAL on restart becomes `RecoveryPending`; it is not replayed into reader state (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:4849`).
- The owner already issues exact-generation TTL leases, but acquisition and renewal require the native to be idle. It blocks every mutation and commit behind any lease (`35b20b3d106e70d17252c281489ed4a44821bab3:scripts/graphdb/owner.mjs:1761`, `35b20b3d106e70d17252c281489ed4a44821bab3:scripts/graphdb/owner.mjs:1799`, and `35b20b3d106e70d17252c281489ed4a44821bab3:scripts/graphdb/owner.mjs:1976`).
- The owner lazily opens the native transaction on the first mutation, records its nonce in the manifest, then verifies prepared evidence and the N+1 token through exact CAS (`35b20b3d106e70d17252c281489ed4a44821bab3:scripts/graphdb/owner.mjs:2056`).
- Synapse already has an exclusive `BoundedGenerationSession` that validates every N/session envelope, retains results until final renewal, and releases once in `finally` (`c50e0754caf207eba1a0e2e2f377c56b47c10c4b:packages/memgraphrag/src/domain/retrieval/bounded.ts:337`). It has no production transport for this view.
- Synapse graph projection invalidation observes generation only at query entry (`c50e0754caf207eba1a0e2e2f377c56b47c10c4b:packages/memgraphrag/src/infrastructure/storage/cached/projectionVersionGate.ts:15`). Its SQLite lexicon cache is separately keyed by connection/corpus and `PRAGMA data_version` (`c50e0754caf207eba1a0e2e2f377c56b47c10c4b:packages/memgraphrag/src/infrastructure/storage/SQLiteLexiconStore.ts:224`). GraphDB-backed indexing currently disables dictionary indexing in the document pipeline (`c50e0754caf207eba1a0e2e2f377c56b47c10c4b:packages/memgraphrag/src/interface/runtime/MemGraphRagRuntime.ts:423`).

## Alternatives

| Alternative | Benefit | Reject or defer because |
| --- | --- | --- |
| second descriptor native pinned to N | reuses implemented bounded reader | duplicates a multi-GiB decoded state/vector working set and requires cross-process retirement coordination |
| `Arc<State>` copy-on-write | simple lease ownership | the first mutation clones corpus-scale maps, arrays, strings, and vector metadata |
| mutate live state plus an undo log | keeps current handlers | readers can observe partial changes unless every lookup and cache implements reverse overlay semantics; rollback after partial failure becomes a second persistence algorithm |
| retain old JSON file and remap it per query | strong disk snapshot | query latency and memory mapping/page-cache pressure are corpus-sized; native still needs generation indexes and vector/cache coherence |
| immutable committed base plus bounded writer overlay | fixed writer allocation, exact N reads, one native | requires deliberate mutation routing and a drain boundary; recommended |

The recommendation does not by itself make the current `HashMap`/JSON-array representation safe to update. `Vec` append or `HashMap` growth can allocate a new corpus-scale backing table at publication even when the delta is small. Storage normalization is therefore a gate, not an optimization to do later.

## Native model and data flow

The eventual native ownership model is:

```text
CommittedView N
  canonical State data
  decoded vector values and exact blob lineage
  GenerationCatalog(N): graph, vector, passage, memory-ID and document-provenance indexes
  committed query caches tagged N

WriterOverlay(base=N, transactionNonce)
  bounded keyed replacements and inserts
  bounded tombstones and association edits
  bounded vector values
  writer-only indexes and aggregate byte/item counters
  durable WAL evidence
```

A reader request contains generation N. The owner validates the connection-bound lease and passes only N plus the bounded operation to native. Native compares N with `CommittedView.generation` and returns N in the response. Lease identity never crosses into native.

A mutation is completely shape-, association-, byte-, item-, and aggregate-cap validated before WAL append or input cloning. Native durably appends the canonical mutation record, then applies it only to `WriterOverlay`. Writer reads merge overlay over base and therefore preserve read-your-writes. Committed operations never consult overlay storage, overlay caches, or WAL.

The transaction has an aggregate retained-byte and item ceiling across all RPCs, independent of per-request and WAL limits. Replacement is constructed off to the side and swapped only after final accounting. A rejected request leaves both the previous overlay and its counters byte-for-byte unchanged.

## Storage normalization and migration peak

Disk compatibility is the default: canonical JSON maps and memory-section arrays keep their existing shapes and vector descriptors. The in-memory representation may change behind custom `Deserialize`/`Serialize` implementations.

The target representation is ordered, bounded-growth storage:

- top-level keyed collections use a node-allocating ordered map or bounded shards whose insertion cannot reallocate a corpus-sized bucket array;
- each memory section stores records in stable bounded chunks and preserves legacy array order;
- a fixed-size digest of `(corpus, section, domain ID)` maps to one or more locators; lookup verifies the full ID in the located `Value`, so a digest collision cannot alias data;
- document membership maps a document digest to passage, fact, schema, node, edge, and vector locators; exact document text is verified at the locator;
- duplicates are represented in order and the ID index points at the last record, matching current reverse-merge behavior. Capability remains unavailable if a copied-production audit finds a duplicate/provenance shape that the migration cannot preserve exactly.

Legacy JSON must be streamed from a no-follow regular-file descriptor directly into the target representation. It must not first read the whole canonical file into a byte vector and then create a second corpus representation. Values move once into bounded chunks; text, associations, and vectors are not deep-copied for indexing. Serialization streams the same legacy JSON shape from the normalized representation.

The existing startup duplication of blob-backed `Vec<f64>` into both `VectorRecord.values` and `vector_values` must be removed or measured and explicitly budgeted before capability enablement. All vector consumers must first be proven to use the one committed vector-value authority.

Migration is code-level and restart-local, not an on-disk rewrite. Before deployment, tests must prove old and new binaries produce equivalent logical state from the same old-format fixture and that the old binary can still read JSON emitted by the new representation. Any order normalization requires a separate parity decision; it is outside this design.

Memory gates are allocator- and RSS-measured, not inferred from pointer identity:

1. synthetic 10k and 200k bases with a fixed delta report retained bytes, transient peak, allocation count, and RSS for open, catalog build, overlay admission, rejection, deletion planning, drain/apply, serialization, and post-publish rebuild;
2. catalog retention may grow with record/association count, but no index may retain document text, a domain object, vector values, or a second full ID string set;
3. fixed-delta mutation/admission/apply allocation must not grow with base size;
4. rejected oversize input must allocate at most the bounded parser/frame allowance and retain zero bytes;
5. a deployment memory budget is mandatory; capability advertisement fails closed when measured projected peak plus configured safety headroom exceeds it; and
6. a copied-production audit and benchmark are a later approval gate. This design does not copy or inspect production data.

If a standard collection operation still triggers a base-sized reallocation, the overlay integration is a no-go until that collection is normalized. Raising the memory limit is not a substitute.

## Deletion indexes and shared provenance

Document deletion is planned from `GenerationCatalog(N)`, not by corpus scans. The plan records exact locators and precondition digests before WAL append:

- passages and document-owned vectors are removed;
- graph nodes proven solely owned by the document are removed, and incident edges are found through adjacency indexes;
- a fact removes the target document and only passage associations owned by that document; it is deleted only when the accepted Synapse contract says no supporting provenance remains;
- a schema removes affected fact/document associations and adjusts frequency/state, but is not silently deleted. Current Synapse reports `deletedSchemas = 0` (`c50e0754caf207eba1a0e2e2f377c56b47c10c4b:packages/memgraphrag/src/application/indexing/DeleteDocumentService.ts:15`).

The authoritative shared-provenance semantics must be captured as cross-repository fixtures before implementation. The prototype behavior is evidence, not authority. Missing, contradictory, duplicate, or over-cap provenance fails atomically. Locators are generation-tagged; a plan for N cannot apply to N+1.

Physical removal from chunked sections may leave bounded tombstones until a reviewed compaction. Compaction cannot run while readers or a writer exist, cannot change logical order, and must have its own measured peak budget. Tombstone ratio and bytes are health metadata; automatic corpus-scale compaction is not part of the first implementation boundary.

## State transitions, leases, CAS and starvation

| State | Readers | Writer | Transition |
| --- | --- | --- | --- |
| `Committed(N)` | acquire/renew/read N | may begin at N | first accepted mutation -> `Writing(N,W)` |
| `Writing(N,W)` | acquire/renew/read committed N | mutate/read overlay W | commit request closes mutation admission -> `Draining(N,W)` |
| `Draining(N,W)` | no new lease; existing leases finish within absolute lifetime | no further mutation; prepared evidence may be recorded | zero leases -> `Publishing(N,W)` |
| `Publishing(N,W)` | denied | apply validated W and publish | success -> `Committed(N+1)`; any failure -> fatal exit |
| `RecoveryPending(N,E)` | denied except health | only digest-bound requeue/discard | discard returns to N; adopted durable commit returns N+1 |

Reader leases retain current connection binding and TTL, and add a non-extendable absolute session deadline. Renewal may extend the idle TTL only up to that deadline. Once drain begins, acquisition fails retryably; existing sessions may renew only within their original absolute deadline. Thus commit wait is bounded without discarding a successful response mid-operation, and an abandoned reader cannot starve publication.

Mutations no longer queue behind leases after overlay support is proven. Only prepare/publication drains readers. The owner may admit a lease while its manifest is dirty only when all of these match: native advertises the reviewed capability digest, native transaction base N, manifest base N, committed view N, and requested generation N. Unknown or mismatched state remains `GENERATION_DIRTY`.

Prepare and commit CAS bind base N, transaction nonce, WAL device/inode, bytes, record count, digest, next generation N+1, and the capability/representation version used to interpret the overlay. Duplicate mutation request IDs remain idempotent only within the active transaction. Repeated prepare with identical evidence returns identical evidence; any different evidence fails closed.

## Publication, WAL and crash atomicity

After drain, native applies the prevalidated overlay in place to normalized storage. No reader can observe this phase. Existing publication ordering remains vector segment, canonical JSON temp, JSON rename and directory sync, then exact WAL retirement.

The in-memory generation and committed cache generation change to N+1 only after durable JSON publication and after N+1 vector references/catalog/caches are coherent. Until then native must not return a successful commit token or serve another request. A failure after in-place apply is fatal; the process must exit without serving the mutated memory image.

| Crash/failure point | Reopen result |
| --- | --- |
| before JSON rename | canonical N; base-N WAL is `RecoveryPending`; no overlay data is readable |
| after JSON rename, before WAL retirement | canonical N+1 plus matching commit evidence; exact old WAL is retired, never replayed |
| after WAL retirement | canonical N+1 |
| malformed/future/mismatched WAL or vector lineage | fail closed without modifying canonical artifacts |

This preserves the existing recovery policy: an incomplete document batch is requeued and its exact WAL is quarantined/discarded through CAS rather than replayed into a different job. There is no dirty-state bypass and no mixed N/N+1 read.

## Vector values and caches

Committed vector metadata, decoded values, blob descriptor/lineage, graph/passage indexes, bounded memory catalog, and every native query cache carry the same generation tag N. A committed operation validates all tags before work. It never lazily rebuilds a cache from writer-overlay state.

Overlay vector inserts/replacements keep values only in overlay storage. Writer vector search merges committed and overlay candidates with overlay tombstones. At publication, vector references are resolved exactly as the current durable token requires. After JSON wins, the N+1 catalog and caches are patched from the validated delta or rebuilt while admission remains closed; native becomes Idle only after an invariant check proves all components report N+1.

Synapse's legacy `CachedGraphProjection` cannot be mixed with bounded N operations. The bounded path uses native materialization results for the entire retrieval. If a later consumer cache is added, its key includes generation, corpus, operation contract digest, and query-policy digest; a cache miss may compute, but a generation mismatch may not fall back to an unversioned cache.

## Synapse query policy and SQLite lexicon

`BoundedGenerationSession` remains the retrieval coordinator. It validates the static plan before acquisition, verifies N/session on every operation, performs final renewal, returns only after all stages validate, and discards partial results on any error.

GraphDB generation N does not version SQLite. A query that uses dictionary or thesaurus policy must open a dedicated read-only SQLite connection, begin a read transaction, force its snapshot with the first lexicon read, and record a local lexicon snapshot token such as `data_version` plus the dictionary-policy digest. Every dictionary/thesaurus read for that query uses that connection and transaction; its cache is session-local. The transaction closes after the graph lease releases.

The consistency claim is therefore the composite `(graph generation N, lexicon snapshot L, query-plan digest P)`. It must never be described as one cross-database generation. GraphDB-backed document indexing currently does not write the dictionary, so no atomic N/L commit is implied. If a future pipeline couples them, it requires a separate cross-store commit design.

Embedding/model configuration, normalization helpers, feature flags, and other query-policy caches are pinned into P before graph lease acquisition. Unknown or unsupported policy fails before acquisition. Answer generation may occur after retrieval, but its output is not part of the committed-generation evidence.

## First implementation boundary and go/no-go

The first code issue after this design is **CommittedCatalog and migration proof**, with no owner/Synapse wire change:

1. build `GenerationCatalog(N)` in normal native mode only from a validated Idle committed state;
2. index memory IDs and document provenance with collision verification, plus generation tags for existing graph/vector/passage indexes;
3. route existing clean-state `memory_get_schemas_by_ids`, `memory_get_active_facts`, and deletion *planning* through the catalog without changing their result shapes;
4. keep all dirty-state reads and all normal-mode bounded-retrieval methods unavailable;
5. add the streaming normalized-section loader/serializer as a compatibility and peak-memory test seam, without switching production storage until its equivalence and budget gates pass; and
6. advertise no committed-read-during-write capability.

Proceed to overlay mutation routing only if:

- old/new serialization round-trips are logically and order equivalent on adversarial fixtures;
- duplicate and shared-provenance fixtures have an explicit preserved result or fail before capability;
- allocator/RSS evidence passes the six memory gates above at both synthetic scales;
- catalog lookups reject stale generation and collision aliases;
- fixed-delta upsert and indexed deletion plans have base-independent transient allocation; and
- no existing clean-state result or durable artifact changes.

Otherwise stop with measured no-go evidence. Do not patch around a failed gate with a second native, `Arc<State>` clone, larger cgroup, or unbounded scan.

Later boundaries are separate reviewed issues: normalized storage activation; one-family memory overlay with writer read-your-writes; graph/vector/passage overlay and cache coherence; owner drain/admission capability; then Synapse transport and lexicon snapshot integration. #481 remains incomplete until the full path is deployed and measured.

## Privacy, compatibility and rollback

Logs and metrics may contain generation, lease count/age, transaction phase, operation name, bounded item/byte counts, catalog/tombstone counts, hashes, durations, and error class. They must not contain queries, document IDs, domain IDs, text, vector values, RPC payloads, SQLite terms, client file paths, or WAL contents. Digest values used as locators are not operational log fields.

Every boundary is capability-negotiated by exact version/digest. Older owner and Synapse continue to receive `GENERATION_DIRTY`; unknown consumers cannot invoke the new path. The first boundary changes no persisted schema or public response. Its rollback is binary rollback and restart. Later in-memory normalization must keep emitted JSON readable by the old binary; otherwise it becomes an explicit format migration with backup/restore and is not covered by this design.

Before enablement, deploy native with capability disabled, exercise clean reads and indexing, then enable owner admission separately. Rollback disables owner admission first, drains leases, and restarts the prior binary on the same canonical JSON/blob/WAL set. Never roll back by deleting WAL or choosing a generation manually.

## Non-goals

- implementing or deploying kernel changes in this design PR;
- concurrent writers, multi-generation historical reads, or long-lived snapshots;
- making WAL a reader source or replaying partial document work automatically;
- changing query ranking policy or claiming cross-store atomicity with SQLite;
- corpus clone, second native, real-data copy, GPU/LLM work, service/config changes, or automatic tombstone compaction; and
- claiming #481 complete from a design, prototype, or capability-disabled foundation.

## Adversarial acceptance suite

1. Hold an N lease, mutate every supported family in W, and prove byte-identical N results while writer reads its own inserts, replacements, activations, and tombstones.
2. Exercise vector/schema/fact/passage update and delete with shared provenance; no result may combine base and overlay associations.
3. Try stale, future, malformed, expired, disconnected, cross-connection, and wrong-session leases. No native work or partial result escapes.
4. Start continuous lease acquisition, request commit, and prove the drain barrier rejects new leases and reaches zero by the absolute lease deadline.
5. Kill/fail at every WAL append/sync, prepare, overlay apply, vector write/sync/rename, JSON write/sync/rename, directory sync, cache rebuild, and WAL-retire point. Reopen exposes exactly N or N+1.
6. Replay duplicate request IDs and repeated prepare/commit calls. Identical evidence is idempotent; any changed nonce/base/digest/inode/bytes/count/capability version is rejected.
7. Inject digest collisions, duplicate IDs, dangling passage/fact/schema associations, contradictory document provenance, invalid finite vectors, generation overflow, and over-cap fields. Rejection is atomic and payload-free in logs.
8. Mutate lexicon from another SQLite connection during a query. The session uses one L throughout; a later session may observe the new L. Reported evidence distinguishes N, L, and P.
9. Invalidate or race every native/Synapse cache during load. An N request may return only an N cache result or fail; it cannot publish a stale cache for a later generation.
10. Compare 10k and 200k bases with a fixed delta and rejected oversized delta. Mutation/apply peak is base-independent; rejected retention is zero; catalog cost is reported separately per record/association.
11. Parse old canonical JSON through the streaming normalized loader and serialize it back. Preserve logical data, array order, duplicate behavior, vector descriptor lineage, and canonical commit evidence without a second domain-object copy.
12. Keep the capability disabled and prove current owner/Synapse behavior remains `GENERATION_DIRTY` during indexing. Only a later integration issue may change that assertion.
