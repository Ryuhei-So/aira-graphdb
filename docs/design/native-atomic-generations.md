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

For committed generation N:

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
  `sync_data` durable. WAL failure is fatal/fail-closed; the mutated in-memory
  process must not continue accepting work.
- Only the method-policy authority classifies WAL mutations. The debug panic and
  file-import RPC remain unavailable to an untrusted read client.

## Concurrency and idempotency

The native stdio server remains single-threaded. One request is completed before
the next is read, so publication and WAL retirement cannot interleave with
another request. The future Literature Hub owner supplies process exclusivity,
writer CAS, reader leases, and request-level generation pinning. Stable node,
edge, vector, passage, fact, schema, and checkpoint identifiers make replay and
document reprocessing replace existing logical records rather than duplicate
them; tests must prove this for the document-shaped mutation set.

## Failure and rollback

- Before JSON rename: JSON N still selects blob N; orphan N+1 temp/final blobs
  are ignored and may be garbage-collected only after a later successful open.
- After JSON rename but before WAL retirement: JSON N+1 opens its blob; WAL
  records with base N are already committed and are not replayed.
- Corrupt/missing/hash-mismatched referenced blob: startup fails closed without
  rewriting JSON or falling back to a different blob.
- Malformed/future WAL: startup fails closed and preserves files for diagnosis.
- Legacy DB rollback: before deployment retain the existing JSON, `.vblob`, and
  WAL backup. The new binary is backward-readable; the old binary is not
  forward-readable after generation 1, so rollback restores that complete
  pre-migration set before starting the old binary.
- Blob GC is not part of the first boundary. Keeping old immutable blobs is a
  bounded short-term disk tradeoff during validation; safe reachability-based GC
  is a separate deliverable.

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
6. Prove the JSON publication rename never falls back to copy and a rename
   failure leaves N readable.
7. Migrate an existing inline-vector file and a legacy JSON + `.vblob` pair;
   reopen values and metadata exactly.
8. Compare `protocol_info` with the actual dispatch inventory and prove every
   non-read method is fail-closed/writer-classified for the downstream owner.
9. Replay the same document-shaped WAL/mutation sequence twice and prove stable
   logical IDs and counts (no duplicate completed document data).
10. Run the complete Cargo test suite on the supported Rust toolchain and a
    release binary smoke test before the consumer boundary starts.
