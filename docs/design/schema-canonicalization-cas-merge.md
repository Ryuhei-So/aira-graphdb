# Bounded schema canonicalization with native CAS merge

Status: design approved for the synthetic C1-S implementation boundary. Native
implementation and the downstream consumer remain separate checkpoints. This
does not authorize production activation.

Issue: Ryuhei-So/aira-graphdb#47, supporting
Ryuhei-So/literature-hub#586 and #572.

Pinned design inputs:

- Aira GraphDB `83ef0eb309deec7e1ddc156ecda19eb4edb51010`.
- Aira Synapse `c50e0754caf207eba1a0e2e2f377c56b47c10c4b`.

## Decision

The canonicalization path will stop retrieving complete stored schemas. A
negotiated `canonicalization@1` projection returns only the scalar state that
Stage II reads, the first historical source document needed by vector
metadata, an opaque full-value compare token, and whether the current document
already contributed to the schema. The later `memory_upsert` sends a typed
create-or-merge intent. Native compares the stored full value before WAL
append and merges additions into the full stored schema, preserving all
historical and unknown fields.

Schema ontology nodes use a bounded hydration marker. Native validates the
marker against the already merged memory schema before WAL append and resolves
the full `GraphNode.ref` from memory while applying `upsert_nodes`. The full
multi-megabyte schema is not copied into that RPC or its WAL record.

The legacy full-object read and legacy `schemas` replacement lane remain
unchanged. A consumer may select this path only when one nested capability
advertises the exact projection, merge, and graph-hydration versions together.
Partial or contradictory capabilities fail closed.

Rejected alternatives:

- Raising the 8 MiB response cap retains work proportional to cumulative
  associations and moves the failure threshold.
- Paging a full schema makes canonicalization proportional to history and
  still requires a consistent multi-page view.
- Truncating arrays or casting a projection to `Schema` can erase historical
  associations on the next replacement upsert.
- Sending a hydrated full graph node in the WAL recreates the same cumulative
  payload on a second mutation.
- A client-side read/merge/write remains vulnerable to a lost update between
  read and WAL admission.

## Measured pre-code gates

A synthetic gate used the current 8 MiB response and 64 MiB indexing request
limits. It did not read or copy production data.

| Gate | Measurement | Decision |
| --- | ---: | --- |
| stored schema with 100,000 fact ids and 20,001 source documents | full object 10,660,522 bytes; old response 10,660,567 bytes | reproduces the response failure |
| one projected schema | 410-byte response | fits |
| 32 projected schemas with every bounded string represented by 6-byte JSON escapes | 4,925,260-byte complete response envelope | fits with margin |
| two-alias/two-fact current delta against that oversized stored schema | 681-byte complete request envelope; the 10,660,522-byte stored value is absent | normal use remains bounded by current work |
| current merge with 3,000 maximum-input-byte aliases | 73,983,468-byte request envelope | rejected by the existing 64 MiB frame gate before WAL |
| historical equivalence expectation model | unknown field, `createdAt`, `version`, first source owner, and full graph ref all preserved | required for native and consumer tests; this model is not implementation proof |

The measurement fixes `maxProjectedSchemas` and `maxSchemaMerges` at 32. The
64 MiB request cap stays authoritative. The oversized negative fixture does
not block the 681-byte representative current delta. A large current-document
delta can still exceed the cap; C1-S makes that failure bounded and atomic but
does not solve general cumulative mutation size.

## Authorities and bounded contexts

- Synapse owns canonicalization, the alias identity key
  `(label, language, source)`, per-document candidate pressure, schema state
  transitions, and the planned additions for the current document.
- Native owns the complete stored JSON value, preservation of omitted and
  unknown fields, the compare token, the absence check for creates, mutation
  validation, WAL admission, and atomic installation.
- Literature Hub owns job identity, whole-document retry/requeue, protocol
  capability admission, and the separate source SHA-256 evidence used to tell
  content versions apart. Its current `doc_<sha1(path)[:12]>` document id is
  path-derived, not a content hash.
- The canonical memory schema is the authority for ontology `GraphNode.ref`.
  The projected DTO is planning data and is never persisted as a schema.
- The first element of the canonical schema's `sourceDocumentIds` remains the
  vector metadata owner. The current document must never replace it merely
  because it requested the projection.

The data flow is:

1. The consumer validates the exact nested capability.
2. Stage II requests at most 32 distinct schema ids plus the current document
   id with `projection: "canonicalization@1"`.
3. Native validates each complete stored schema and returns an ordered
   projection for found ids.
4. Synapse aggregates every candidate occurrence for a schema in this
   document. Three occurrences produce `frequencyDelta: 3`; they are not
   collapsed to one.
5. Synapse constructs facts, bounded schema merge intents, graph hydration
   markers, and vector metadata from `firstSourceDocumentId` before mutation.
6. `memory_upsert` validates the complete logical post-state and all compare
   conditions before WAL append, then installs all document deltas atomically.
7. `upsert_nodes` validates a bounded ontology marker against the canonical
   memory schema before WAL append and stores a normal graph node whose `ref`
   is the exact full schema.

## Versioned wire contract

The existing `limits.indexingMemory.schema` and all existing scalar values
remain unchanged. Native adds this nested capability:

```json
{
  "limits": {
    "indexingMemory": {
      "schemaCanonicalization": {
        "schema": "native-schema-canonicalization@1",
        "projection": "canonicalization@1",
        "merge": "preserve-cas@1",
        "graphHydration": "memory-schema@1",
        "maxProjectedSchemas": 32,
        "maxSchemaMerges": 32,
        "maxGraphHydrations": 32,
        "maxAliasAdditions": 4096,
        "maxFactIdAdditions": 4096
      }
    }
  }
}
```

The addition counts are totals across one request, not per-schema multipliers.
The existing identifier, corpus, timestamp, request-byte, response-byte, and
per-section limits also apply.

### Projection

Request:

```json
{
  "corpusId": "corpus-a",
  "schemaIds": ["schema:a"],
  "projection": "canonicalization@1",
  "contributionDocumentId": "document-current"
}
```

Each found item is exactly:

```json
{
  "schemaId": "schema:a",
  "corpusId": "corpus-a",
  "headType": "drug",
  "relation": "treats",
  "tailType": "condition",
  "canonicalKey": "drug::treats::condition",
  "frequency": 7,
  "state": "stable",
  "stabilizationThreshold": 2,
  "firstSourceDocumentId": "document-historical-first",
  "contributionPresent": false,
  "mergeToken": "<64 lowercase hex characters>"
}
```

The response preserves requested-id order and omits absent schemas. It rejects
duplicate stored requested ids. It does not include aliases, fact ids, source
document ids, timestamps, version, or unknown fields. Those remain native
state. `firstSourceDocumentId` is `null` only when the stored array is empty.

`mergeToken` is SHA-256 over a domain-separated, deterministic serialization
of the complete stored schema `Value`, including unknown fields and arrays.
It is opaque to the consumer. Native recomputes it from the current value; the
consumer never reconstructs or interprets it.

Calling `memory_get_schemas_by_ids` without `projection` keeps the exact legacy
parameter and full-object behavior. Other projection names or mixed parameter
sets fail before scanning the stored section.

### Preserve/CAS merge

The projected caller omits the `schemas` key completely and sends
`schemaMerges`. Mere empty-array equivalence is not accepted: key presence is
the mode discriminator, and `schemas` plus `schemaMerges` is invalid.

Each member is one of these tagged shapes:

```json
{
  "mode": "create",
  "expectedAbsent": true,
  "schema": { "schemaId": "schema:new", "corpusId": "corpus-a", "...": "full new Schema" }
}
```

```json
{
  "mode": "merge",
  "schemaId": "schema:a",
  "expectedMergeToken": "<projection token>",
  "contributionDocumentId": "document-current",
  "frequencyDelta": 3,
  "desiredState": "stable",
  "stabilizationThreshold": 2,
  "updatedAt": "2026-09-22T00:00:00.000Z",
  "aliasAdditions": [],
  "factIdAdditions": []
}
```

Create compares against absence in the exact corpus immediately before WAL
admission. Merge compares the full-value token immediately before WAL
admission. Duplicate schema ids in the request are rejected. A create for an
existing id, a merge for an absent id, or a stale token changes neither WAL nor
state.

For merge, native:

- preserves every unknown field and all fields not explicitly owned below;
- preserves `schemaId`, `corpusId`, `headType`, `relation`, `tailType`,
  `canonicalKey`, `createdAt`, and `version` exactly;
- checked-adds `frequencyDelta` to `frequency`;
- sets only `state`, `stabilizationThreshold`, and `updatedAt` from the intent;
- appends an alias only when its identity key is absent, keeping historical
  order and the complete historical alias object;
- appends absent fact ids in request order;
- appends `contributionDocumentId` only when absent, keeping the historical
  first source owner.

Native validates the current full schema, every addition, and the complete
logical post-object before WAL. It must not clone a cumulative association
array merely to validate it. Request-frame admission remains the allocation
bound for current additions. If the stored `sourceDocumentIds` already
contains `contributionDocumentId`, native requires `frequencyDelta` to equal
zero. Apply performs no semantic check that can fail after WAL append.

### Graph schema hydration

`upsert_nodes` retains legacy `nodes`. A negotiated caller may also provide:

```json
{
  "nodes": ["non-schema GraphNode values"],
  "schemaRefHydration": "memory-schema@1",
  "schemaNodeRefs": [{
    "nodeId": "schema:schema:a",
    "corpusId": "corpus-a",
    "schemaId": "schema:a",
    "label": "drug treats condition"
  }]
}
```

Markers and `nodes` may not contain the same `(corpusId,nodeId)`. Native
requires the canonical schema to exist, requires ids/corpus and label to agree
with it, validates that the resolved full node is a valid `GraphNode`, and
finishes that preparation before WAL append. The WAL stores the bounded marker,
not the full schema. Apply resolves the same already validated memory value and
installs a normal ontology node with the exact full schema as `ref`.

`memory_upsert` and graph persistence are separate RPCs inside the same
existing whole-document native transaction. Failure between them poisons and
discards the whole transaction through the existing requeue path, so neither
RPC becomes partially committed. C1-S does not introduce an independent
cross-method commit.

## State, concurrency, CAS, and replay

For an existing schema the state transition is:

`project full value V/token T -> prepare intent -> compare current token T ->
append WAL -> apply preserved merge -> normal transaction commit`.

For a new schema it is:

`observe absent -> prepare full schema -> compare exact absence -> append WAL
-> insert -> normal transaction commit`.

The single native owner already serializes dispatch. The CAS still matters:
another document may commit between projection and this document's turn. A
stale request fails before WAL and the whole document is retried from a fresh
projection. No dirty or mixed-generation read is introduced.

Candidate pressure is counted once per schema for the first contribution from
a document id: the consumer sums all candidate occurrences and sends that sum
when `contributionPresent` is false. When it is true, including an
uncertain-commit retry or a legitimate changed-content reindex of the same
path-derived document id, it sends `frequencyDelta: 0`; set-union alias/fact
additions and the source-document append remain idempotent. This deliberately
defines C1-S `frequency` as first-document-contribution pressure, rather than
claiming that document id identifies immutable content. A changed version can
add new aliases and fact ids, but C1-S cannot subtract the old version's
pressure or add replacement pressure exactly because the stored schema has no
per-document pressure ledger. Exact frequency replacement and removal of old
associations require a later deletion/reindex contract keyed by Literature
Hub's separate source SHA-256 evidence. C1-S does not add an unbounded durable
per-version map to solve that different problem.

Native recovery does not replay this request. A crash leaves the existing WAL
recovery-pending state; the owner discards it and requeues the whole document.
Normal request-id uniqueness and commit evidence stay unchanged.

## Failure, privacy, and rollback

All malformed shapes, non-finite or unsafe numeric values, conflicting keys,
bad stored schemas, duplicate ids, cap violations, stale tokens, and missing
hydration targets are client/integrity failures before WAL. Error text may name
only the fixed method/field, expected cap, and failure class. It must not emit
schema ids, schema text, aliases, fact/source arrays, tokens, or full JSON.

Rollback means deploying a paired old consumer and runtime revision that uses
the unchanged legacy lane. A consumer must not automatically fall back after a
projection, CAS, or hydration error because that could reinterpret a failed
document inside one job. Stored JSON and graph node shapes are not migrated. A
transaction already admitted under C1-S follows the existing native WAL
discard/commit rules. There is no dual-written metadata to remove.

## Compatibility and non-goals

- Legacy `memory_get_schemas_by_ids`, legacy `memory_upsert.schemas`, ordinary
  `upsert_nodes`, WAL framing, persistence format, and generation semantics are
  unchanged.
- The projected DTO is a distinct type. It must never satisfy or be cast to
  the full `Schema` interface.
- Capability admission requires all three exact versions; old native, old
  consumer, or a partial backport fails closed.
- C1-S does not cover `memory_get_active_facts`, full-object pagination,
  truncation, response/request cap changes, storage migration, deletion
  indexing, exact changed-content frequency replacement, or
  committed-generation query reads.
- The first-document-contribution frequency definition is a user-visible
  indexing policy change for same-path changed-content reindexing and requires
  explicit review again at the production activation gate.
- C1-S does not make an over-64-MiB current document indexable. It guarantees
  rejection before WAL and no partial mutation.

## Adversarial acceptance

Before any production activation, synthetic tests must prove:

1. A full schema response over 8 MiB fails on the legacy lane while its
   projection succeeds; 32 maximum escaped projections fit the complete
   response envelope.
2. Legacy request/response shapes are unchanged byte-for-byte for fixed
   fixtures.
3. The token changes for an unknown-field or association-only change, and a
   stale token, absent merge, existing create, or duplicate id leaves WAL and
   state byte-identical.
4. Merge preserves unknown fields, `createdAt`, `version`, historical aliases,
   all fact/source ids, and first-source order while applying only declared
   scalar transitions and unique additions.
5. Three candidates for one schema produce delta three on the first document;
   uncertain-commit re-read produces delta zero. A same-path changed-content
   reindex also produces delta zero while new aliases/fact ids union safely,
   and the test names the first-contribution frequency limitation.
6. New and existing schemas in one request succeed together, while any invalid
   member rejects the entire request before WAL.
7. A request over 64 MiB is rejected at frame admission with no WAL; cumulative
   stored arrays are not cloned during prevalidation.
8. Hydration WAL contains the bounded marker and no historical sentinel text,
   while the installed graph node `ref` exactly equals the merged full memory
   schema.
9. Vector metadata keeps the historical `sourceDocumentIds[0]` for an existing
   schema and uses the new document for a new schema.
10. A failure between memory and graph RPCs poisons the same whole-document
    transaction; discard/requeue leaves no partial committed memory or graph
    state and converges without a second frequency increment.

The first implementation boundary is only native projection, preserve/CAS
merge, hydration, capability advertisement, and these synthetic tests. The
Synapse consumer is a separate PR against the pinned contract. Production
activation requires both exact revisions, their compatibility tests, and a
separate deployment decision.
