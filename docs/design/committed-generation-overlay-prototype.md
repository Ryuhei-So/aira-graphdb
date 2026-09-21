# Committed-generation overlay feasibility prototype

Status: test-only prototype for [aira-graphdb #43](https://github.com/Ryuhei-So/aira-graphdb/issues/43), informing Literature Hub #481. It is not a production feature and does not close either issue.

## Authority and boundary

The prototype is based on fork `main` at `83ef0eb309deec7e1ddc156ecda19eb4edb51010`. It adds only an integration-test target and this note. Native RPC dispatch, protocol inventory and digests, `State`, WAL, persistence, vector blobs, caches, and deployed binaries are unchanged.

The committed base is the sole readable generation authority. A committed lease must request the exact generation and owns an `Arc` to that immutable base. A writer has one bounded document overlay and reads overlay changes before the base. It cannot publish while any committed lease exists. Once readers drain, `Arc::get_mut` applies the prevalidated overlay in place and advances exactly `N -> N+1`. Discard removes the overlay without changing N.

The test records mirror the existing domain shapes rather than treating every item as document-owned:

- passages and vectors carry `metadata.documentId` equivalents;
- facts carry cumulative `passageIds` and `sourceDocumentIds`;
- schemas carry cumulative `factIds` and `sourceDocumentIds`.

## Invariants and state transitions

| State | Allowed operation | Result |
| --- | --- | --- |
| committed N, empty overlay | exact-N lease | immutable reader at N |
| committed N, empty/bound overlay | one-document `memory_upsert` | writer sees delta; lease still sees N |
| committed N, bounded overlay | discard | committed N, empty overlay |
| committed N, bounded overlay, live lease | publish | fails without mutation |
| committed N, bounded overlay, no lease | publish | applies once in place; committed N+1 |
| committed N, empty overlay | publish | fails: no pending changes |
| committed N | exact generation other than N | fails closed |

Mutation admission preflights item count, vector dimensions, deep record bytes, identifiers, association arrays, tombstones, and conservative hash-table capacity before cloning an input into the overlay. Candidate construction clones only the already bounded overlay. Final accounting runs again before replacement, so failure leaves the prior overlay unchanged. Replaying the same keyed delta is idempotent.

Empty deltas are rejected rather than publishing an empty generation. Non-finite vector values and generation overflow also fail before changing the overlay or base. Once delete metadata exists, a same-document upsert is rejected so a tombstone cannot be silently resurrected.

`memory_upsert` neither clones nor scans the base. Delete-by-document performs an allocation-free discovery/preflight scan of the keyed base, then creates a capped overlay. Its scan time is O(corpus) and remains an explicit missing index, while retained and transient allocation remain bounded by overlay caps.

## Delete semantics in this prototype

Deletion is deliberately narrow:

- target-owned passages and vectors become tombstones;
- a fact loses the target document and its target-owned passage associations, but remains when another source document supports it;
- a fact becomes a tombstone when no source document remains;
- a schema loses the target provenance and facts deleted with that document, adjusts frequency/state, and remains even with no source document, matching the current Synapse result contract where `deletedSchemas` is zero.

Mixing delete with pending upserts is rejected. Graph nodes/edges, lexical state, and SQLite dictionary/lexicon policy are outside this model. Production delete semantics still require a cross-repository contract decision.

## Measured bounded allocation

The integration-test binary wraps the system allocator and measures current bytes, peak live bytes, requested bytes, allocation count, and failures around only the fixed five-record upsert. It also samples Linux `VmRSS` before and after. Inputs and bases are built before the measurement. Separate processes prevent allocator history from mixing the profiles.

Measured on 2026-09-21 with the unoptimized test profile:

| Base (per collection) | Base collections | Retained delta | Peak delta | Requested | Semantic overlay | RSS before/after |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 10,000 | 2 | 5,463 B | 5,463 B | 5,463 B | 6,907 B | 17,256 / 17,264 KiB |
| 200,000 | 2 | 5,463 B | 5,463 B | 5,463 B | 6,907 B | 260,416 / 260,424 KiB |

The same fixed delta retained and requested exactly the same allocator bytes at both scales. The base `Arc` address stayed identical. Representative delete retained and peaked at 4,768 B, requested 4,913 B, and had a 5,387 B semantic overlay at both scales; allocation-free predicates replaced uncharged temporary ID sets. A text-cap-rejected five-record delta retained 0 B and had 0 B transient peak/requested allocation inside the operation because preflight rejected it before cloning. RSS is coarse corroboration only; allocator current/peak/requested bytes and source inspection are the acceptance evidence.

## What this does not prove

At the pinned base, production `State.snapshots` remains `HashMap<String, Value>` whose snapshot sections are JSON arrays (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:181`). `memory_upsert` prevalidation scans the complete stored target snapshot before WAL admission (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:5983`, call at line 6045), then clones incoming arrays and merges them into stored arrays (`83ef0eb309deec7e1ddc156ecda19eb4edb51010:src/bin/aira-graphdb-native.rs:7086`). Vector values and cache dirtiness are maintained separately from `State.vectors`.

Therefore, this `Arc<HashMap<...>>` result does not remove current production scan/merge costs and does not establish coherence for vector/search caches. Before any production implementation, a new approved design must cover:

1. initial array-to-keyed storage/index conversion and its migration/peak memory;
2. WAL and crash-atomic publication without a second native or whole-corpus clone;
3. vector blob, vector-value, graph, passage, lexical, and query-policy cache coherence;
4. reader drain/starvation, owner leases, concurrency/CAS, and process recovery;
5. Synapse `GenerationSession` plus versioned or explicitly external SQLite lexicon/dictionary policy;
6. bounded indexed document deletion and the final shared-provenance contract.

Unsupported mutation families are graph node/edge changes, lexical changes, memory activation/checkpoints, corpus-wide deletion, multi-document overlays, and concurrent writers. No production protocol, persistence, service, configuration, or data migration is proposed here.

All fixtures are synthetic. Metrics and errors contain counts and byte sizes only; no record text, identifiers, request payloads, or production data are logged.

## Verification and rollback

```text
cargo test --test committed_generation_overlay_prototype --no-run
cargo test --test committed_generation_overlay_prototype invariants -- --exact --nocapture --test-threads=1
OVERLAY_BASE_ITEMS=10000 cargo test --test committed_generation_overlay_prototype footprint -- --exact --nocapture --test-threads=1
OVERLAY_BASE_ITEMS=200000 cargo test --test committed_generation_overlay_prototype footprint -- --exact --nocapture --test-threads=1
cargo fmt --check
```

Rollback is deletion of the two test files and this note. No persisted format or runtime rollback is needed because the prototype has no production entrypoint.
