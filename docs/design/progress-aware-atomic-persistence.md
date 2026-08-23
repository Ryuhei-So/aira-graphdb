# Progress-aware bounded-memory atomic persistence

Status: design checkpoint for #14 and Literature Hub #197. No implementation,
live database write, timeout change, or deploy is authorized by this document.

## Production evidence and root cause

On 2026-08-24 the Literature Hub semantic bridge was stopped and the single
owner had one writer. Two real indexing attempts each caused the owner to kill
the native child at the exact fixed 120-second request deadline. The documents
remained unmarked, the canonical manifest remained committed generation 0, and
the owner and worker restarted twice. Owner peak was about 19.4 GiB with zero
cgroup swap and zero host memory PSI, so these events were request-lifecycle
timeouts, not OOM.

The current persistence path also clones the complete `State`, materializes the
complete vector blob as a `Vec<u8>`, then materializes the complete canonical
JSON before publication. With the production-shaped 1.9-GB JSON and 5.5-GB
blob, progress alone would hide rather than remove a transient full-copy cost.
This design therefore couples progress-aware deadlines to streaming
bounded-memory persistence. The current on-disk format still requires O(total
persisted bytes) I/O; this issue does not relabel that rewrite as O(delta).

Before implementation, owner observability must identify the exact timed-out
method and phase using only method, request id, phase, and elapsed time. No
document id, payload, text, vector, path, or query is logged. If any method
other than the operations admitted below exceeds its existing deadline, the
design is amended and freshly reviewed rather than broadening a generic
allowlist.

## Authority and bounded contexts

- Canonical JSON and its exact generation descriptor remain the sole committed
  publication authority.
- The durable WAL remains recovery evidence, never a committed read view.
- Native owns deterministic serialization, temp files, hashes, fsync, rename,
  WAL retirement, and progress production.
- The Literature Hub owner owns request admission, one-writer ordering,
  progress validation, inactivity/absolute deadlines, child reap, generation
  manifest reconciliation, and client delivery.
- The index worker owns document claim/replay and may mark completion only from
  its exact durable commit receipt.
- A progress frame is liveness evidence only. It cannot mutate state, publish a
  generation, acknowledge success, renew a reader lease, or become a commit
  receipt.

## Canonical encoding

`GraphDbCanonicalJson@1` is the byte contract for progress policy,
prepared/commit evidence, existing full-request WAL v2 records, and canonical
State. Bytes
are UTF-8 without BOM, whitespace, or trailing newline. Schema object fields
use their declared order; State fields are exactly `nodes`, `edges`, `vectors`,
`passages`, `snapshots`, `checkpoints`, `generation`, `vectorBlob`, then
`commitEvidence`. Map keys sort by unsigned UTF-8 byte order. Arrays preserve
their schema order. Strings emit non-ASCII UTF-8 directly, escape quote and
backslash, and use lowercase `\u00xx` only for required U+0000..U+001F
escapes. DTO integers are base-10 JSON-safe integers without leading zeros or
negative zero.

The same rules apply recursively to every `serde_json::Value` in node refs,
vector metadata, snapshots, and checkpoints: nested object keys use unsigned
UTF-8 byte order and arrays retain order. Existing JSON integers retain their
exact signed i64 or unsigned u64 mathematical value and emit base-10 without
leading zeros. Finite `f64` values use the pinned Rust Ryu shortest round-trip
representation, including `-0.0` as distinct from integer `0`; NaN and
infinities are rejected before mutation. Production canonical JSON must
parse, canonicalize, reopen, and reproduce the same complete typed state.
Goldens include nested Unicode keys, i64 minimum, u64 maximum, JSON-safe
boundaries, `-0.0`, fractional and exponent forms, and recursive arrays/maps.

One shared typed payload produces `PreparedCommitEvidence@1` and
`CommitEvidence@1`; only the schema literal differs. `batch_commit.params` is
exactly `{expectedPreparedEvidence:<strict typed object>}`. CAS compares the
validated typed payload, while every stored/digested/handshake representation
is regenerated with `GraphDbCanonicalJson@1`. Raw policy/evidence bytes must
equal parse-then-canonicalize output, which rejects alternate whitespace, key
order, escaping, duplicate/unknown fields, and noncanonical numbers. Golden
raw-byte fixtures are split by implementation authority. Rust GraphDB
implements the full recursive State serializer and the JSON-safe DTO subset.
Literature Hub implements only policy and prepared/commit-evidence DTOs: those
objects contain JSON-safe integers and no floating-point fields. Hub never
parses and reserializes canonical State to decide authority. The repositories
share byte-identical DTO goldens; Rust-only State goldens cover i64/u64 and
Ryu floating-point cases. Neither repository defines a second serializer for
the same payload class.

## Protocol authority

GraphDB owns one canonical `NativeProgressPolicy@1` byte artifact. Its strict
ordered object is `schema`, `protocolVersion`, and `methods`; V1 contains
exactly one method policy for `batch_commit`. A method policy contains:

```text
method: "batch_commit"
phases: closed ordered phase[]
initialFrameDeadlineMs: positive safe integer
inactivityDeadlineMs: positive safe integer
phaseHardDeadlineMs: strict ordered {phase, deadlineMs}[]; `phase` follows the
  closed phase inventory and `deadlineMs` is a positive safe integer
absoluteDeadlineMs: positive safe integer
minFrameIntervalMs: positive safe integer
heartbeatIntervalMs: positive safe integer
earlyByteDelta: positive safe integer
earlyUnitDelta: positive safe integer
maxFrames: positive safe integer
maxFrameBytes: positive safe integer
```

`protocol_info` returns the exact object as `progressPolicy` and its SHA-256 of
canonical bytes as `progressPolicySha256`. The Literature Hub GraphDB lock
stores only the expected digest, not a second copy of the values. Owner accepts
the policy only when object reserialization matches the advertised digest,
that digest equals the lock, the method classification still equals `commit`,
and every value is within independent owner hard safety maxima. The maxima can
reject an unsafe policy but can never grant a longer deadline or a new method.

Executable values are derived from at least three copied-production runs and
recorded as p50/p95/max with a reviewed margin. Until those values and the
exact policy digest exist, owner integration cannot start. V1 initially admits
only `batch_commit`, after safe method/phase observability confirms that it is
the production timeout path. Corpus-wide reads and startup loading are not
silently added. The closed phases are:

```text
admitted, wal_verify, prepare_refs, vector_write, vector_sync,
vector_publish, vector_dir_sync, json_write, json_sync, json_publish,
json_dir_sync, wal_zero, wal_sync, complete
```

The owner requests progress with a reserved owner-generated
`progressProtocolVersion:1`; callers cannot supply or override it. Without that
negotiation the native emits the existing single final response only.

`NativeProgressFrame@1` is strict canonical newline-delimited JSON in this
field order:

```text
schema: "NativeProgressFrame@1"
kind: "progress"
protocolVersion: 1
id: request id
sequence: positive safe integer
method: exact admitted method
phase: exact policy phase
completedUnits: nonnegative safe integer
totalUnits: nonnegative safe integer | null
completedBytes: nonnegative safe integer
totalBytes: nonnegative safe integer | null
```

Sequence starts at 1 and increases by exactly one. The first frame is
`admitted` with zero counters. Every phase entry emits a frame even when no
counter advanced. Within a phase counters never decrease; totals, when present,
are immutable and never below completed values. After `minFrameIntervalMs`,
native may emit early when either early delta is reached. Regardless of those
deltas, if useful work advanced since the prior frame it must emit no later
than `heartbeatIntervalMs`. No-advance heartbeat frames are forbidden and
cannot hide a hung syscall. A transition advances to exactly the next phase
and may reset phase-local counters. Frames contain no free text, native wall
clock, path, or data-derived identifier.

Policy admission requires
`phases.length + ceil(absoluteDeadlineMs / heartbeatIntervalMs) <= maxFrames`
with checked arithmetic. The emitter always reserves one frame for every
remaining phase entry plus the worst-case remaining mandatory heartbeat
frames. A phase entry and due heartbeat coalesce into one frame. An optional
early-delta frame is emitted only when it leaves that reserve intact;
otherwise it is coalesced into the next mandatory frame. Reaching the optional
budget can therefore reduce frame frequency but can never suppress a required
heartbeat/transition or turn healthy work into a protocol overflow.

The owner classifies a line by the closed `schema` and `kind` pair before any
request promise can settle. A progress frame has no `ok`, `result`, `error`, or
durable-token field and is consumed only by the owner; it is not forwarded as a
client response. The final `RpcResponse` remains the only success/error
response. It is emitted once after `complete`; progress after a final response,
a second final response, wrong id/method/version, replay, skipped sequence,
phase regression, impossible counter, oversize, excess frame count, or partial
line is fatal. The native run loop owns stdout and gives `batch_commit` a
synchronous bounded progress sink; a write/flush failure is fatal. `Server`
does not retain or independently write to stdout.

For a negotiated `batch_commit`, both directions use a per-request bounded
newline framer before JSON allocation. Input and every output frame are capped
at the policy maximum plus one-byte rejection; EOF with a nonempty partial
line, invalid UTF-8, unknown/duplicate fields, noncanonical numbers, output
backpressure failure, or pipe close is fatal. A terminal token is also below
the same small frame cap. Other methods retain their existing framing contract
and cannot emit progress.

## Deadline state machine

The owner tracks four independent monotonic clocks for a progress-capable
request:

1. initial frame deadline;
2. inactivity since the last valid advancing frame or phase transition;
3. a phase-specific hard cap for bounded blocking calls such as `fsync`;
4. one non-resetting absolute request deadline.

The clocks start before the owner writes the request. Sequence-1 must be fully
framed, parsed, and validated before the initial deadline. An advancing frame
or exact next-phase transition resets inactivity; it never extends that
phase's hard cap or the absolute deadline. A phase hard cap starts on its entry
frame and resets only on the exact next-phase entry. A frame whose completed
parse timestamp is equal to or later than any applicable deadline loses the
race and is rejected. The final `RpcResponse`, not `complete` progress, settles
the request. Existing non-progress methods retain their current deadline.
Fake-monotonic-clock tests cover every event/deadline tie.

Owner hard safety validation is fixed independently of the advertised policy:
initial and inactivity at most 60,000 ms, each phase at most 300,000 ms,
absolute at most 1,800,000 ms, frame interval 100..5,000 ms, heartbeat
1,000..30,000 ms and not below frame interval, at most 4,096 frames, and at
most 2,048 UTF-8 bytes per frame. Delta fields are positive safe integers and
do not weaken the heartbeat requirement. Checked arithmetic rejects any
aggregate duration/frame overflow. These are rejection ceilings, not runtime
defaults.

On any deadline/protocol violation, the owner rejects the request, terminates
and reaps the native child, fails closed, and reconciles only after restart.
The owner never reports a timeout as a document error. Repeated owner failure is
bounded by existing systemd start limits; clients do not create an independent
30-second reload loop.

## Bounded-memory persistence algorithm

Native transaction state is the closed machine:

```text
Idle -> Active(baseGeneration, transactionNonce)
     -> Prepared(PreparedCommitEvidence@1)
     -> Idle after published generation
```

The sole method inventory adds `batch_prepare_commit` with classification
`transaction` and `wal:false`; owner requires that exact mapping. It is O(1)
over the rolling evidence and descriptor metadata and remains a non-progress
method under the existing deadline. `batch_commit` remains classification
`commit`, `wal:false`, and the only V1 progress method.

`batch_begin` creates a 32-byte random transaction nonce inside the private
owner/native boundary and returns it to the sole writer. External callers
cannot choose or override it. Existing full-request WAL v2 remains
self-contained recovery evidence; recovery before prepare therefore does not
depend on a Literature Hub journal or document identity. Literature Hub #197
associates the returned nonce with its already-durable exclusive document claim
before it may request prepare. Missing association blocks prepare/publication,
but it does not make an otherwise valid WAL unrecoverable.

Native mutations are accepted only in `Active`. `batch_prepare_commit` is accepted only after a
successful mutation, finalizes the in-memory rolling WAL evidence, checks the
held descriptor/path identity and exact byte count, freezes further mutation,
and returns the idempotent strict object:

```text
schema: "PreparedCommitEvidence@1"
transactionNonce: 64 lowercase hex
baseGeneration: nonnegative JSON-safe integer
generation: baseGeneration + 1
walSha256: 64 lowercase hex
walBytes: positive safe integer
walRecordCount: positive safe integer
```

The owner durably records these exact prepared bytes and the deterministic
expected `CommitEvidence@1` canonical bytes (the same fields with only the
schema literal changed) in its prepared journal before it may send
`batch_commit`. `batch_commit` carries the exact prepared evidence as a CAS;
missing, changed, or unprepared evidence is rejected without publication.
`CommitEvidence@1` has the same fields with only its schema name changed. It is
written inside canonical JSON at the same publication point as generation and
the blob descriptor, and is returned unchanged in `DurableGenerationToken`
and as `lastCommitEvidence` in subsequent startup/protocol handshake. The
handshake field is null for a legacy generation without evidence. Every
generation first published by this contract must contain it.

`batch_commit` then validates the prepared transaction, evidence, base
generation, WAL identity/digest/count/bytes, and all vector lengths without
changing live state. It prepares only:

- sorted borrowed keys for nodes, edges, vectors, passages, snapshots, and
  checkpoints;
- one bounded `(key, offset, len)` reference table, O(vector count);
- the next generation number and immutable descriptor metadata.

It must not clone `State`, any complete collection, individual vector values,
the complete vector blob, or the complete JSON.

### WAL evidence

The first mutation creates or opens one held no-follow, regular, single-link
WAL descriptor and retains its identity through the transaction. New appends
preserve the existing self-contained strict record and on-disk version:

```text
version: 2
baseGeneration: nonnegative JSON-safe integer
request: exact canonical RpcRequest object, including id, method, and params
```

WAL admission is the single shared rule `METHOD_SPECS.wal == true` and
`prepare_mutation` produced a self-contained canonical request. In particular,
`memory_save_file` is fully read and validated under its existing input bounds,
then transformed into canonical `memory_save` with the complete owned payload
before either counting pass. The source path is never stored in evidence. A
raw/v2 recovery record whose method is `memory_save_file` is unsafe legacy
input and remains operator fail-closed; it is not replayed, discarded, or
classified as committed residue automatically.

The request object is immutable throughout WAL admission. Append is exactly two
passes through the same canonical serializer. Pass one writes only to a
`CountingHashSink`, includes the LF, and uses checked arithmetic to reject the
request before any file write when a record would exceed
`maxWalRecordBytes=536870912`, the transaction would exceed
`maxWalRecords=1000000`, or the file would exceed
`maxWalBytes=17179869184`. The WAL is then opened
`O_RDWR|O_APPEND|O_NOFOLLOW`: an existing path must match the retained exact
regular single-link identity, while an absent path is `create_new` mode 0600.
Pass two streams the same immutable request and small envelope through a
counting SHA-256 writer into the held WAL. Its exact byte count and digest must
equal pass one. The complete record and LF require `File::sync_data()`; when
this invocation created the file, the parent directory must then be fsynced.
Finally the held/path identity, regular single-link metadata, exact resulting
size, count, and digest are revalidated. Only after all those steps succeed may
the mutation be applied and acknowledged. Reusing an already-durable
zero-length WAL does not change a directory entry and therefore does not fsync
the parent. File sync, creation-directory sync, or post-sync validation failure
is fatal with no apply and no acknowledgement. No second encoded request or
record-sized `Vec<u8>` is created. These GraphDB-owned
constants are advertised with the method inventory and pinned by its digest.
Method-specific request limits remain independently enforced before pass one.

Each complete record plus LF is synced before mutation acknowledgement and
incorporated into rolling whole-WAL SHA-256, byte, and record counters. Prepare
clones/finalizes that O(1) rolling evidence without reading the whole WAL.
Commit independently streams the held descriptor through an
allocation-bounded LF record reader and hashes exact raw bytes including LF
separators. The reader rejects at max+1 without allocating an entire record, never uses
`read_line`/`read_until`, caps envelope keys at 64 UTF-8 bytes and JSON nesting
at 128, and uses a fixed-field custom visitor for version, baseGeneration,
request.id, and the closed mutation method. It allocation-free skips only the
`request.params` value body. Duplicate/unknown envelope fields, unknown request
fields, trailing bytes, escaped content mistaken for framing, or a final record
without LF are fatal. It compares count, size, digest, descriptor metadata, and
pathname identity before and after the scan. Neither raw WAL bytes, record
payloads, parser scratch proportional to a value, nor all parsed records may
coexist in memory. Prepared state rejects further append.

The pre-generation-0 raw `RpcRequest` format remains recovery-read-only and is
accepted only beside canonical generation 0, exactly matching the existing
compatibility rule. Full-request v2 remains the only writable format and is
never rewritten into a new schema. V2/raw base equal to canonical is
`RecoveryPending`. For a legacy canonical with `commitEvidence:null`, valid v2
records all sharing one base below canonical are existing-format committed
residue and may be zeroed after bounded revalidation. For a new canonical with
commit evidence, only base exactly `G-1` whose raw digest/count/bytes equal that
evidence is committed residue; any other stale relation fails closed. Existing
v2 bytes are schema-validated without requiring their raw key order or escaping
to match the new canonical serializer, and their exact existing raw bytes are
what the evidence scanner hashes. Future/mixed base, malformed/partial JSON,
non-mutation methods, and schema confusion are fatal. Copied-production largest
records and every cap at max/max+1 are acceptance evidence; changing a constant
is a WAL-format review, not a local owner setting.

WAL retirement reuses the same held identity and streamed evidence; it never
accepts a path-only file or a caller digest. After canonical directory fsync,
native re-streams and revalidates the exact held WAL, calls `set_len(0)` on that
descriptor, requires `File::sync_all()` to succeed, then requires held size
zero and the live pathname still naming the same regular single-link inode.
Directory fsync is not substituted for this file-metadata durability step
because no directory entry changes. It does not rename, unlink, or create a
retirement sidecar, so pathname cleanup cannot delete another inode.
The zero-length WAL is the reusable idle state for the next transaction.
RecoveryPending computes evidence by bounded streaming. Path replacement,
same-inode append/truncate, partial content, record max/max+1, count/byte
overflow, malformed middle record, and changes between prepare, commit, zero,
and sync fail closed.

### Vector blob

Native creates one recognized same-directory private temp with no-follow,
single-link authority. A counting SHA-256 writer streams magic/version and
little-endian values directly from borrowed vectors in sorted-key order.
Progress is emitted from bounded writes. After flush and file sync, the final
size/hash determine the immutable generation basename. Existing destination is
accepted only after a held no-follow descriptor passes bounded streaming
size/hash/format validation plus identity recheck; otherwise the temp is
published no-replace and the directory is synced. No whole-file
materialization or `Vec<u8>` is permitted.

### Canonical JSON

A borrowing `PersistedStateView` implements serialization over the live state,
substituting next generation, the new blob descriptor, empty inline vector
values, prepared blob refs, and exact `CommitEvidence@1`. Each map is emitted
in sorted-key order from borrowed references. It streams directly through a
counting writer to the canonical JSON temp. It cannot mutate live state or
allocate output-sized buffers. After file sync, canonical rename and directory
sync are the sole publication point.

After canonical publication, native zeroes and `sync_all()`s the exact WAL. Only
then does it apply the already-prepared generation,
descriptor, commit evidence, and vector refs to live memory with operations
proven infallible, transitions to `Idle`, and returns
`DurableGenerationToken`. Any post-publication local apply failure is fatal;
restart loads canonical authority.

The old full-copy helpers are forbidden on the commit path by a structural test
and a production-shaped peak-RSS/PSS gate. The gate requires peak auxiliary
memory over pre-commit steady state to be O(total record-count borrowed
references + vector refs + bounded I/O buffers), not O(WAL bytes + JSON bytes
+ blob bytes). The persistent I/O volume remains O(WAL verification + total
JSON + blob bytes) until a separately designed storage-format change.

## Crash recovery and lost response

Startup opens the live WAL no-follow while holding the owner lock, requires a
regular single-link inode, and identity/metadata checks before and after its
evidence is derived. Upgrade/cutover preflight requires zero historical
recognized nonce-based retire temps. Their presence is operator fail-closed;
new code never creates or deletes them. The closed recovery table for canonical
generation G is:

| Canonical / live WAL | Result |
| --- | --- |
| absent or exact zero-length file | `Idle` |
| legacy canonical (`commitEvidence:null`) and every valid v2 record has one common base `< G` | bounded revalidate, `set_len(0)`, `sync_all`, then `Idle` under the existing atomic-generation contract |
| new-evidence canonical E and every valid v2 record has base exactly `G-1`, with raw digest/count/bytes exactly matching E | revalidate, `set_len(0)`, `sync_all`, then `Idle` |
| full-request v2 with every record base G, or raw request beside generation 0 | `RecoveryPending`; WAL remains self-contained replay evidence |
| new-evidence canonical with stale/mismatching v2; future/mixed base; schema confusion; partial/malformed/over-cap content | fail closed |
| any pathname/inode/content change during classification or zeroing | fail closed |

Kill/fail tests reopen a real process before/after `set_len(0)` and before/after
`sync_all()`. Full committed residue must safely re-zero, zero must stay idle,
and partial, same-inode mutation, pathname replacement, and every historical
retire-temp presence must fail closed without deleting any file.

- Before canonical rename: the prior generation remains authoritative; WAL and
  recognized temp recovery permit exact discard/replay.
- After canonical rename but before its directory sync: restart accepts only
  what the existing atomic-generation recovery contract proves durable.
- After canonical directory sync: the new generation is committed even if WAL
  zeroing or final response is lost.
- If restart sees a base-N WAL beside canonical N+1 with commit evidence,
  native treats it as committed residue only when bounded streaming reproduces
  every field of canonical N+1 `CommitEvidence@1`; it then zeroes that exact
  held WAL before ordinary service. Any mismatch remains fail-closed recovery,
  never replay. A legacy canonical without evidence follows only the separate
  legacy row above.
- On owner restart, native handshake generation/blob descriptor and canonical
  `CommitEvidence@1` are compared byte-for-byte to the owner's durable prepared
  journal's expected commit bytes. Exact transaction nonce, base/next
  generation, WAL digest/bytes/count, and descriptor generation must all agree
  before owner reconciliation. A different transaction that also produced
  N+1 cannot satisfy the evidence. Any other relation fails closed for operator
  recovery.
- Replaying the document after an unresolvable lost response must not mark or
  commit a duplicate generation. Literature Hub binds its durable job claim to
  the native-created transaction nonce after `batch_begin` and before prepare,
  and adds prepared evidence before publish; neither is supplied by document
  content.

The owner-side durable receipt journal, document association, and exact
reconciliation transition belong to Literature Hub #197. GraphDB #14 exposes
only `PreparedCommitEvidence@1`, canonical `CommitEvidence@1`, the durable
token, and handshake recovery facts needed by that contract; it does not infer
or persist a Literature Hub document identity.

## Failure and rollback

Validation and prepare failures leave live state, WAL, canonical JSON, and
generation unchanged. I/O failure before publication leaves only recognized
owned temps plus WAL. Native becomes fatal on durability ambiguity. OOM or kill
cannot produce a partial JSON protocol success. Reader generation N stays
usable until canonical N+1 publication; old immutable blobs are not removed by
this issue.

Rollback is code rollback plus service restart only before any new-format
progress-dependent behavior is required. The WAL format remains v2, avoiding a
WAL migration boundary. Once a generation containing new commit evidence is
published, rollback keeps the new native and may roll back only clients because
an old native would drop that evidence on its next publication. A prior native
is allowed only after a reviewed preflight proves WAL absent/zero and proves
that it preserves, rather than merely ignores, the required generation
evidence.

## Adversarial checkpoint tests

GraphDB #14 persistence/schema checkpoint, before owner integration:

- shared canonical raw-byte goldens for policy and prepared/commit evidence;
  Rust-only full-request WAL v2 and full-State goldens; alternate
  whitespace/key order/escape/duplicate field negatives; unknown method/phase,
  frame emission order,
  early-delta/heartbeat cadence, mandatory-budget admission, high-throughput
  early-frame coalescing, and progress output max/max+1;
- Active/Prepared transition, mutation-after-prepare rejection, idempotent
  prepare, missing/changed evidence CAS, and distinct transaction producing the
  same next generation;
- production-sized full-request WAL v2 streaming and legacy-raw
  record/aggregate caps
  at max/max+1; count/byte overflow; path replacement; same-inode mutation;
  and changes between prepare/commit/zero without full raw/record
  materialization;
- immutable two-pass append equality, mutation/divergence between passes,
  max/max+1 rejection before write, and second-pass partial I/O failure without
  acknowledging or publishing a mutation;
- first-WAL create/record-sync/parent-directory-sync/post-sync-identity kill and
  failure seams, proving every acknowledged mutation reopens with the exact WAL
  pathname and bytes; reuse of a durable zero-length inode does not create a
  directory durability dependency;
- after `memory_save_file` source deletion/replacement, the WAL scanner still
  proves the exact expanded self-contained `memory_save` payload is present;
  startup exposes only `RecoveryPending`, then Hub requeues the whole document
  from its durable source and starts a new transaction after matching recovery
  discard; unsafe raw/v2 path-dependent records fail closed without automatic
  replay or zero;
- a 512-MiB params string, oversized unknown envelope key, nesting at max and
  max+1, escaped LF, duplicate/unknown fields, trailing bytes, and a partial
  final record, with a peak auxiliary-memory gate proving no record-sized
  allocation;
- every row of the canonical/live-WAL recovery table, historical-temp preflight,
  and real-process truncate-success/sync-failure plus kill/fail before and after
  `set_len(0)` and `sync_all`; restart with old-full, zero, and partial content,
  including a second failure while re-zeroing;
- real-process reopen of a pre-contract canonical G plus valid v2 base G-1
  residue, and new-evidence canonical G plus both exact-matching and mismatching
  v2 base G-1 residue; legacy serde key order/escaping remains schema-valid and
  is hashed as existing raw bytes;
- kill/fail at every vector temp/write/sync/publish/dir-sync and JSON
  write/sync/publish/dir-sync/WAL-zero/sync boundary;
- lost final response after committed publication exposes one exact canonical
  generation/blob/commit-evidence token and handshake fact and never
  republishes it;
- restart before publication preserves generation N and whole-document replay;
- mixed valid/invalid mutation cannot be published;
- existing generation-N reader remains valid during N+1 construction;
- production-shaped run proves identical logical state/vector hashes,
  deterministic sorted JSON, one generation advance, bounded peak
  RSS/PSS/VmSwap, and duration evidence on the supported Rust runtime.

Literature Hub #197 service checkpoint, before worker cutover:

- policy raw-byte/digest mismatch, native-advertised extension, and every owner
  hard maximum at max/max+1;
- progress wrong id/version/method, replay, skip, regression, impossible totals,
  unknown/duplicate field, frame/aggregate max and max+1, partial EOF, invalid
  UTF-8, backpressure, and owner/native pipe close at every publication phase;
- dispatch before first frame, slow-but-advancing work below early deltas,
  heartbeat boundary, valid inactivity reset, phase hard cap that frames cannot
  extend, absolute deadline, and exact-deadline race under a fake monotonic
  clock;
- silence in every phase and TERM-ignore then KILL/reap without a restart loop;
- prepared journal fsync/rename failure, owner death before/after commit send,
  exact evidence reconciliation, mismatch fail-closed, durable job reclaim,
  and exactly one document completion;
- durable document claim before indexing, nonce association before prepare,
  crash with v2 WAL before prepare, missing/wrong/completed/multiple durable
  claims at prepare, and exact requeue acknowledgement before recovery
  discard.

Only after this persistence/schema checkpoint reaches fresh H0/M0 may Luna
implement the mechanical frame structs, streaming writers, and failpoint tests.
Owner/worker integration is the later Literature Hub #197 checkpoint above.

## Explicit follow-up boundary

Native startup currently reads/parses the complete canonical JSON and reads,
hashes, and decodes the complete vector blob before the owner handshake. That
work is outside an RPC and therefore cannot use `NativeProgressFrame@1`.
Startup readiness, bounded-memory loading, and its deadline/observability
contract require a separate issue and checkpoint; #14 must not solve it by
quietly extending the existing handshake timeout.
