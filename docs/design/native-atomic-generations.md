# Native crash-atomic generation design

Issue: Ryuhei-So/aira-graphdb#1  
Consumers: Literature Hub issue Ryuhei-So/literature-hub#173  
Risk: kernel (persistence, crash recovery, exact generations)

## Decision

The JSON file is the sole publication pointer for a committed generation. Each
commit writes a new immutable, generation-named vector blob, makes that blob
durable, then atomically replaces and fsyncs the JSON that names it. A WAL
record carries the JSON generation on which it is based, so a WAL left behind
after JSON publication is recognized as already committed and is never replayed.

This boundary belongs in the native store. A caller-side hard-link snapshot was
rejected: it depends on implementation-specific rename behavior, breaks if the
native fallback copies in place, and forces the caller to duplicate the native
store's WAL, temp-file, fsync, and garbage-collection state machine. Raising a
consumer cgroup limit does not address mixed-generation corruption.

## Authority and data flow

- The canonical JSON is the only authority selecting a committed generation.
- `State.generation` is a monotonic unsigned integer, defaulting to zero for a
  legacy JSON file.
- `State.vectorBlob` contains a basename (never an absolute path or path with a
  separator), byte length, SHA-256, and format version. A described blob is
  immutable after publication.
- A legacy state without `vectorBlob` reads the adjacent legacy `.vblob`. Its
  first successful persist publishes generation 1 in the new format; the legacy
  blob remains a rollback artifact until a later, explicitly safe GC.
- WAL v2 records contain `{version, baseGeneration, request}`. The native store
  replays only records whose `baseGeneration` equals the JSON generation. Older
  records are already included and are retired; future-generation or malformed
  records fail closed.
- `batch_commit` returns the durable generation and blob identity only after the
  blob, JSON, WAL retirement, and parent directory have been synced.
- A versioned `protocol_info` RPC is the source of truth for the native method
  inventory and its health/read/transaction/mutation/commit classification.
  WAL classification calls the same implementation. Unknown methods are not
  implicitly read-only.

## Commit state machine and invariants

All mutation is owned by one explicit native transaction state machine:

```text
Idle(generation=N)
  -- batch_begin --> Active(base=N, mutation_seen=false)
Active
  -- canonicalized mutation + durable WAL + apply --> Active(mutation_seen=true)
Active(mutation_seen=true)
  -- batch_commit --> publish N+1 --> Idle(generation=N+1)

startup with base-N WAL
  --> RecoveryPending(base=N, wal_digest, record_count)
RecoveryPending
  -- recovery_discard(base, digest) --> quarantine WAL --> Idle(generation=N)
```

Mutation outside `Active` is rejected. A second `batch_begin` and a bare
`batch_commit` are rejected. Startup never applies WAL to the readable live
state. It parses and validates the WAL, computes a digest over its canonical
bytes, enters `RecoveryPending`, and rejects every read, transaction, mutation,
and commit other than health/status and a digest-matched `recovery_discard`.
The discard atomically quarantines the WAL and fsyncs its directory, leaving
canonical generation N unchanged. The owner then requeues the whole uncompleted
document from its durable source and begins a fresh batch. Stable IDs make that
full replay idempotent; an unrelated document can never publish an interrupted
prefix.

EOF, signal exit, WAL-size compaction, read-only open/close, and legacy format
migration never publish a generation. EOF with WAL or an active/recovery batch
leaves the canonical generation unchanged and preserves WAL for replay. The
only publication entrypoint is an explicit successful `batch_commit` and its
durable token.

For committed generation N and an active, mutated batch:

1. Serialize state N+1 and build the vector payload without changing the
   published in-memory generation.
2. Write a same-directory, exclusively-created blob temp; flush and `sync_all`.
3. Rename it to an immutable basename containing generation N+1 and the payload
   SHA-256. If that final name exists, accept it only after size/hash validation.
   Never copy into an existing canonical file as a rename fallback.
4. Fsync the parent directory. At this point generation N still selects its old
   blob and is complete.
5. Write the JSON temp containing generation N+1 and the exact blob descriptor;
   flush and `sync_all`.
6. Rename JSON temp over the canonical JSON and fsync the parent directory. This
   is the only publication point. A restart now opens a complete N+1 pair.
7. Retire a WAL only if every record has base generation N, then fsync the parent
   directory. A crash before retirement sees JSON N+1 and skips those records.
8. Update in-memory generation/blob metadata and return the durable token.

Required invariants:

- A reader can observe complete generation N or N+1, never JSON from one and a
  blob from the other.
- A successful commit response implies durable JSON, durable referenced blob,
  a durable directory entry, and no replayable WAL from generation N.
- Rename failure is an I/O error; there is no non-atomic copy fallback.
- The referenced blob must be a regular file in the canonical JSON directory;
  its basename, format, length, and SHA-256 must validate before serving data.
- Generation never advances after any failed validation or durability step.
- A successful mutator is acknowledged only after its WAL v2 record is
  `sync_data` durable. Requests are normalized and fully validated before any
  state change. The identical canonical request is written to WAL and applied;
  `memory_save_file` is expanded to a self-contained `memory_save` payload so
  replay never depends on a mutable external path. WAL failure or any mutator
  error is fatal/fail-closed; no later commit can publish an unlogged prefix.
- Validation and preparation are O(request delta). They must not clone `Server`,
  `State`, the vector map, or whole-corpus caches. After durable WAL append the
  prepared mutation applies in place; any unexpected apply error exits without
  EOF publication and is recovered from canonical N plus durable job requeue.
- Only the method-policy authority classifies WAL mutations. The debug panic and
  file-import RPC remain unavailable to an untrusted read client.
- The canonical JSON path is resolved once before sidecars are derived. An
  existing DB must be a regular, single-link file. A new relative/default path
  resolves its existing parent and uses `.` rather than an empty parent.
- Referenced blobs are opened with no symlink following; regular-file, link
  count, size, format, and hash are checked through that same descriptor. A
  pre-existing content-addressed blob is `sync_all`ed before JSON publication.

## Concurrency and idempotency

The native stdio server remains single-threaded. One request is completed before
the next is read. Dispatch first requires a `METHOD_SPECS` entry, so the policy
table gates the manual handler and unknown arms cannot execute. A mutation is
prepared and validated in full, written durably to WAL, then applied without a
fallible partial loop. As an interim fail-closed guard, any mutator error exits
without the EOF persistence path. The future Literature Hub owner supplies
process exclusivity, writer CAS, reader leases, and request-level generation
pinning. Stable node, edge, vector, passage, fact, schema, and checkpoint IDs
make replay replace existing logical records rather than duplicate them.

## Failure and rollback

- Before JSON rename: JSON N still selects blob N; orphan N+1 temp/final blobs
  are ignored. Recognized same-directory temp names include a cryptographic
  nonce and are conservatively removed on an exclusive later startup only when
  no committed JSON references them; unknown files fail closed or remain.
- After JSON rename but before WAL retirement: JSON N+1 opens its blob; WAL
  records with base N are already committed and are not replayed.
- Corrupt/missing/hash-mismatched referenced blob: startup fails closed without
  rewriting JSON or falling back to a different blob.
- Malformed/future WAL: startup fails closed and preserves files for diagnosis.
- Valid base-N WAL: startup exposes only `RecoveryPending` metadata (base,
  digest, record count), never its uncommitted content. Digest-matched discard
  quarantines rather than deletes the WAL; legacy `memory_save_file` WAL is
  reported as unsafe and requires the same explicit quarantine/preflight path.
- Legacy DB rollback: before deployment retain the existing JSON, `.vblob`, and
  WAL backup. The new binary is backward-readable; the old binary is not
  forward-readable after generation 1, so rollback restores that complete
  pre-migration set before starting the old binary.
- Blob GC is not part of the first boundary. Keeping old immutable blobs is a
  bounded short-term disk tradeoff during validation; safe reachability-based GC
  is a separate deliverable.
- Process-crash SIGKILL tests prove ordering with the host/page cache intact.
  A filesystem fault seam separately injects failure before each write, file
  sync, rename, directory sync, and WAL retirement; neither is described as a
  substitute for real power-loss testing.

## Privacy and logging

Generation, byte counts, hashes, method-class names, and error classes may be
logged. RPC params, queries, document text, file paths supplied by a client, and
snapshot contents must not be added to operational logs. Existing audit behavior
is unchanged unless a structured durability error class is required.

## Compatibility and non-goals

- Existing inline-vector and adjacent legacy `.vblob` snapshots remain readable.
- Existing RPC results remain unchanged except `batch_commit`, which changes
  from `null` to a backward-tolerable object containing the durable token.
- This issue does not implement multi-process ownership, UDS clients, reader
  leases, cgroup limits, systemd restart policy, semantic bridge behavior, or
  memory-cache reduction. Those are owned by Literature Hub #173 after this
  boundary is reviewed and green.

## Adversarial acceptance tests

Tests use the real native persistence code, not a fake that omits file writes.

1. Kill/fail after blob temp sync, blob rename, blob directory fsync, JSON temp
   sync, JSON rename, JSON directory fsync, WAL retirement, and final directory
   fsync. Reopen must yield exactly logical generation N or N+1.
2. Seed old vector z=[1,0], attempt N+1 z=[0,1] at every kill point, and prove a
   reopened search never interprets one generation's metadata with the other's
   values.
3. Leave a base-N WAL after publishing JSON N+1 and prove it is skipped, not
   replayed. Prove base-(N+1) records replay once. Reject future/malformed WAL.
4. Inject WAL append/sync failure and prove no success response and no continued
   request service.
5. Reject missing, truncated, hash-mismatched, non-regular, escaping-path, and
   wrong-format blobs without modifying canonical files.
6. Prove every injected write/sync/rename/directory-sync failure leaves N
   readable and returns no token. Prove the JSON publication rename never falls
   back to copy.
7. Migrate an existing inline-vector file and a legacy JSON + `.vblob` pair;
   reopen values and metadata exactly.
8. Compare `protocol_info` with the actual dispatch inventory and prove every
   expected method, class, and WAL flag exhaustively; unknown methods must be
   rejected before the handler match.
9. Replay the same document-shaped WAL/mutation sequence twice and prove stable
   logical IDs and counts (no duplicate completed document data).
10. Run the complete Cargo test suite on the supported Rust toolchain and a
    release binary smoke test before the consumer boundary starts.
11. Acknowledged `memory_save_file`, followed by deletion or replacement of its
    source and a crash, replays the exact acknowledged snapshot without logging
    or reopening the source path.
12. Mixed valid/invalid collection mutations return failure, exit fail-closed,
    and cannot publish the valid prefix after reopen. Cover every collection
    mutator family.
13. EOF mid-batch, EOF with replayed WAL, and legacy read-only open/close never
    advance generation or migrate format; only explicit commit does.
14. Canonical DB and blob symlink/hard-link aliases are rejected before write;
    tests include a swap attempt between path validation and publication.
15. Repeated temp-stage kills do not create a permanent PID/name collision or
    unbounded recognized-temp leak. Relative and default CLI DB paths work.
16. A base-N WAL at startup exposes no WAL-only vector, node, memory, lexical,
    or projection content. All non-health RPCs fail until a matching
    `recovery_discard(base, digest)` quarantines it; a wrong digest fails without
    mutation. A different document cannot commit the old prefix.
17. Production-sized validation performs no whole-state/vector/cache clone.
    A structural test rejects `Server: Clone` and exercises many small mutations
    under a bounded RSS-growth gate.

## Format 2: delta segments with a parent link (literature-hub #482)

Measured on the production corpus (2026-09-03, generation 326→327): one
document added 3,268,608 bytes of vectors, yet the commit rewrote the full
7.69 GB blob, re-read it to verify the hash, and reclaimed the previous 7.68 GB
copy. Publication cost was proportional to the corpus, not to the change.

Decision: a generation publishes a **segment** holding only the vectors first
written since the previous publication (those without a `blobRef`), plus a
parent link naming the previous generation's blob exactly
(basename, size, sha256, format). The descriptor shape
`{basename, size, sha256, format}` and the owner manifest contract are
unchanged; `format` is now `2`.

Layout, little-endian: `AGVB` | u16 version=2 | u64 segment generation |
u16 parent basename length | parent basename | u64 parent size |
32-byte parent sha256 | u16 parent format | f64 payload. A zero parent length
marks a base (the first generation from an empty store).

- `blobRef` gains `gen`, the generation of the segment holding the payload.
  A `blobRef` without `gen` points into a format 1 base. Existing format 1
  blobs are accepted as the base of a lineage; nothing is migrated.
- On open the native follows the descriptor through every parent link,
  verifying each segment against the descriptor that named it. Generations
  must strictly decrease, no basename may repeat, the chain is bounded by
  `MAX_VECTOR_BLOB_LINEAGE`, and any missing, truncated, tampered, or
  mismatched segment fails closed before a single vector is decoded.
- Publication refuses to extend a lineage already at the bound. Compaction
  (publishing a parentless full segment) is a separate change; until it lands
  the bound is the only ceiling, and it fails closed rather than growing.
- `protocol_info` reports `vectorBlobLineage` (descriptor first, base last)
  and `limits.vectorBlob`. Reclamation must retain every basename in the
  lineage; the previous "generation and its predecessor" rule would delete
  live ancestors.
- Descriptor read-only mode receives one inherited blob descriptor and cannot
  carry a lineage: a segment with a parent is rejected; a parentless format 2
  segment and a format 1 blob remain readable.
- Rollback to a binary that only knows format 1 fails closed on the first
  format 2 descriptor (format mismatch). Restore the pre-upgrade JSON and
  lineage set together; the old blobs are still on disk because the lineage
  retains them.

Negative tests: `tests/native_delta_vblob.rs`.
