//! Test-only feasibility model for an immutable committed base plus one bounded
//! document overlay. Nothing in this module is wired into the native protocol.

#![allow(dead_code)]

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::Arc;

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct RecordKey {
    pub corpus_id: String,
    pub id: String,
}

impl RecordKey {
    pub fn new(corpus_id: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            corpus_id: corpus_id.into(),
            id: id.into(),
        }
    }

    fn heap_bytes(&self) -> usize {
        self.corpus_id.capacity() + self.id.capacity()
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct DocumentMetadata {
    pub document_id: String,
    pub title: String,
    pub section_path: Vec<String>,
}

impl RetainedBytes for DocumentMetadata {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.document_id.capacity()
            + self.title.capacity()
            + strings_retained(&self.section_path)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct VectorRecord {
    pub id: String,
    pub corpus_id: String,
    pub namespace: String,
    pub values: Vec<f64>,
    pub metadata: DocumentMetadata,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct PassageRecord {
    pub passage_id: String,
    pub corpus_id: String,
    pub text: String,
    pub normalized_text: String,
    pub metadata: DocumentMetadata,
    pub fact_ids: Vec<String>,
    pub entity_mentions: Vec<String>,
    pub quality_flags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FactRecord {
    pub fact_id: String,
    pub corpus_id: String,
    pub schema_id: String,
    pub head_entity: String,
    pub relation: String,
    pub tail_entity: String,
    pub state: String,
    pub passage_ids: Vec<String>,
    pub source_document_ids: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct SchemaRecord {
    pub schema_id: String,
    pub corpus_id: String,
    pub canonical_key: String,
    pub aliases: Vec<String>,
    pub frequency: usize,
    pub state: String,
    pub stabilization_threshold: usize,
    pub fact_ids: Vec<String>,
    pub source_document_ids: Vec<String>,
}

trait RetainedBytes {
    fn retained_bytes(&self) -> usize;
}

fn strings_retained(values: &Vec<String>) -> usize {
    values.capacity() * size_of::<String>()
        + values.iter().map(|value| value.capacity()).sum::<usize>()
}

impl RetainedBytes for VectorRecord {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.id.capacity()
            + self.corpus_id.capacity()
            + self.namespace.capacity()
            + self.values.capacity() * size_of::<f64>()
            + self.metadata.retained_bytes()
    }
}

impl RetainedBytes for PassageRecord {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.passage_id.capacity()
            + self.corpus_id.capacity()
            + self.text.capacity()
            + self.normalized_text.capacity()
            + self.metadata.retained_bytes()
            + strings_retained(&self.fact_ids)
            + strings_retained(&self.entity_mentions)
            + strings_retained(&self.quality_flags)
    }
}

impl RetainedBytes for FactRecord {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.fact_id.capacity()
            + self.corpus_id.capacity()
            + self.schema_id.capacity()
            + self.head_entity.capacity()
            + self.relation.capacity()
            + self.tail_entity.capacity()
            + self.state.capacity()
            + strings_retained(&self.passage_ids)
            + strings_retained(&self.source_document_ids)
    }
}

impl RetainedBytes for SchemaRecord {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.schema_id.capacity()
            + self.corpus_id.capacity()
            + self.canonical_key.capacity()
            + strings_retained(&self.aliases)
            + self.state.capacity()
            + strings_retained(&self.fact_ids)
            + strings_retained(&self.source_document_ids)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
enum Change<T> {
    Upsert(T),
    Deleted,
}

impl<T: RetainedBytes> RetainedBytes for Change<T> {
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + match self {
                Self::Upsert(value) => value.retained_bytes(),
                Self::Deleted => 0,
            }
    }
}

fn map_retained<T: RetainedBytes>(map: &HashMap<RecordKey, Change<T>>) -> usize {
    map.capacity() * (size_of::<RecordKey>() + size_of::<Change<T>>() + 1)
        + map
            .iter()
            .map(|(key, value)| key.heap_bytes() + value.retained_bytes())
            .sum::<usize>()
}

#[derive(Clone, Debug, Default)]
struct Overlay {
    document: Option<RecordKey>,
    vectors: HashMap<RecordKey, Change<VectorRecord>>,
    passages: HashMap<RecordKey, Change<PassageRecord>>,
    facts: HashMap<RecordKey, Change<FactRecord>>,
    schemas: HashMap<RecordKey, Change<SchemaRecord>>,
    deleted_documents: HashSet<RecordKey>,
}

impl Overlay {
    fn entry_count(&self) -> usize {
        self.vectors.len()
            + self.passages.len()
            + self.facts.len()
            + self.schemas.len()
            + self.deleted_documents.len()
    }

    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
            + self.document.as_ref().map_or(0, RecordKey::heap_bytes)
            + map_retained(&self.vectors)
            + map_retained(&self.passages)
            + map_retained(&self.facts)
            + map_retained(&self.schemas)
            + self.deleted_documents.capacity() * (size_of::<RecordKey>() + 1)
            + self
                .deleted_documents
                .iter()
                .map(RecordKey::heap_bytes)
                .sum::<usize>()
    }

    fn payload_bytes(&self) -> usize {
        self.document.as_ref().map_or(0, RecordKey::heap_bytes)
            + map_payload(&self.vectors)
            + map_payload(&self.passages)
            + map_payload(&self.facts)
            + map_payload(&self.schemas)
            + self
                .deleted_documents
                .iter()
                .map(RecordKey::heap_bytes)
                .sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        self.entry_count() == 0 && self.document.is_none()
    }
}

fn map_payload<T: RetainedBytes>(map: &HashMap<RecordKey, Change<T>>) -> usize {
    map.iter()
        .map(|(key, value)| key.heap_bytes() + value.retained_bytes())
        .sum()
}

#[derive(Debug, Serialize)]
pub struct CommittedBase {
    generation: u64,
    vectors: HashMap<RecordKey, VectorRecord>,
    passages: HashMap<RecordKey, PassageRecord>,
    facts: HashMap<RecordKey, FactRecord>,
    schemas: HashMap<RecordKey, SchemaRecord>,
}

impl CommittedBase {
    pub fn empty(generation: u64) -> Self {
        Self {
            generation,
            vectors: HashMap::new(),
            passages: HashMap::new(),
            facts: HashMap::new(),
            schemas: HashMap::new(),
        }
    }

    pub fn stable_digest(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.generation.to_le_bytes());
        digest_map(&mut hasher, b"vec", &self.vectors);
        digest_map(&mut hasher, b"pas", &self.passages);
        digest_map(&mut hasher, b"fac", &self.facts);
        digest_map(&mut hasher, b"sch", &self.schemas);
        format!("{:x}", hasher.finalize())
    }
}

fn digest_map<T: Serialize>(hasher: &mut Sha256, tag: &[u8; 3], map: &HashMap<RecordKey, T>) {
    hasher.update(tag);
    let mut keys = map.keys().collect::<Vec<_>>();
    keys.sort();
    for key in keys {
        hasher.update(key.corpus_id.as_bytes());
        hasher.update([0]);
        hasher.update(key.id.as_bytes());
        hasher.update([0]);
        hasher.update(serde_json::to_vec(&map[key]).expect("prototype record serializes"));
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OverlayLimits {
    pub max_entries: usize,
    pub max_retained_bytes: usize,
    pub max_identifier_bytes: usize,
    pub max_text_bytes: usize,
    pub max_vector_dimensions: usize,
    pub max_vector_bytes: usize,
}

impl Default for OverlayLimits {
    fn default() -> Self {
        Self {
            max_entries: 4_096,
            max_retained_bytes: 8 * 1024 * 1024,
            max_identifier_bytes: 4_096,
            max_text_bytes: 1024 * 1024,
            max_vector_dimensions: 4_096,
            max_vector_bytes: 64 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DocumentDelta {
    pub corpus_id: String,
    pub document_id: String,
    pub vectors: Vec<VectorRecord>,
    pub passages: Vec<PassageRecord>,
    pub facts: Vec<FactRecord>,
    pub schemas: Vec<SchemaRecord>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PrototypeError {
    GenerationMismatch { requested: u64, committed: u64 },
    OverlayBoundToAnotherDocument,
    MixedDeleteWithPendingChanges,
    InvalidDelta(&'static str),
    LimitExceeded(&'static str),
    ReadersActive,
    NoPendingChanges,
    GenerationExhausted,
}

pub struct CommittedLease {
    base: Arc<CommittedBase>,
}

impl CommittedLease {
    pub fn generation(&self) -> u64 {
        self.base.generation
    }

    pub fn stable_digest(&self) -> String {
        self.base.stable_digest()
    }

    pub fn vector(&self, corpus_id: &str, id: &str) -> Option<&VectorRecord> {
        self.base.vectors.get(&RecordKey::new(corpus_id, id))
    }

    pub fn passage(&self, corpus_id: &str, id: &str) -> Option<&PassageRecord> {
        self.base.passages.get(&RecordKey::new(corpus_id, id))
    }

    pub fn fact(&self, corpus_id: &str, id: &str) -> Option<&FactRecord> {
        self.base.facts.get(&RecordKey::new(corpus_id, id))
    }

    pub fn schema(&self, corpus_id: &str, id: &str) -> Option<&SchemaRecord> {
        self.base.schemas.get(&RecordKey::new(corpus_id, id))
    }
}

pub struct PrototypeEngine {
    base: Arc<CommittedBase>,
    overlay: Overlay,
    limits: OverlayLimits,
}

impl PrototypeEngine {
    pub fn new(base: CommittedBase, limits: OverlayLimits) -> Self {
        Self {
            base: Arc::new(base),
            overlay: Overlay::default(),
            limits,
        }
    }

    pub fn committed_generation(&self) -> u64 {
        self.base.generation
    }

    pub fn base_identity(&self) -> usize {
        Arc::as_ptr(&self.base) as usize
    }

    pub fn committed_lease(
        &self,
        expected_generation: u64,
    ) -> Result<CommittedLease, PrototypeError> {
        if expected_generation != self.base.generation {
            return Err(PrototypeError::GenerationMismatch {
                requested: expected_generation,
                committed: self.base.generation,
            });
        }
        Ok(CommittedLease {
            base: Arc::clone(&self.base),
        })
    }

    pub fn overlay_entry_count(&self) -> usize {
        self.overlay.entry_count()
    }

    pub fn overlay_retained_bytes(&self) -> usize {
        self.overlay.retained_bytes()
    }

    pub fn overlay_digest(&self) -> String {
        let mut hasher = Sha256::new();
        if let Some(document) = &self.overlay.document {
            hasher.update(document.corpus_id.as_bytes());
            hasher.update([0]);
            hasher.update(document.id.as_bytes());
        }
        digest_map(&mut hasher, b"vec", &self.overlay.vectors);
        digest_map(&mut hasher, b"pas", &self.overlay.passages);
        digest_map(&mut hasher, b"fac", &self.overlay.facts);
        digest_map(&mut hasher, b"sch", &self.overlay.schemas);
        let mut deleted_documents = self.overlay.deleted_documents.iter().collect::<Vec<_>>();
        deleted_documents.sort();
        for document in deleted_documents {
            hasher.update(document.corpus_id.as_bytes());
            hasher.update([0]);
            hasher.update(document.id.as_bytes());
        }
        format!("{:x}", hasher.finalize())
    }

    pub fn memory_upsert(&mut self, delta: &DocumentDelta) -> Result<(), PrototypeError> {
        if !self.overlay.deleted_documents.is_empty() {
            return Err(PrototypeError::MixedDeleteWithPendingChanges);
        }
        self.validate_delta(delta)?;
        self.preflight_delta(delta)?;
        let mut trial = self.overlay.clone();
        bind_document(&mut trial, &delta.corpus_id, &delta.document_id)?;

        for value in &delta.vectors {
            trial.vectors.insert(
                RecordKey::new(&value.corpus_id, &value.id),
                Change::Upsert(value.clone()),
            );
        }
        for value in &delta.passages {
            trial.passages.insert(
                RecordKey::new(&value.corpus_id, &value.passage_id),
                Change::Upsert(value.clone()),
            );
        }
        for value in &delta.facts {
            trial.facts.insert(
                RecordKey::new(&value.corpus_id, &value.fact_id),
                Change::Upsert(value.clone()),
            );
        }
        for value in &delta.schemas {
            trial.schemas.insert(
                RecordKey::new(&value.corpus_id, &value.schema_id),
                Change::Upsert(value.clone()),
            );
        }
        self.validate_overlay(&trial)?;
        self.overlay = trial;
        Ok(())
    }

    pub fn delete_document(
        &mut self,
        corpus_id: &str,
        document_id: &str,
    ) -> Result<(), PrototypeError> {
        if !self.overlay.is_empty() {
            return Err(PrototypeError::MixedDeleteWithPendingChanges);
        }
        self.validate_identifier(corpus_id)?;
        self.validate_identifier(document_id)?;
        self.preflight_delete(corpus_id, document_id)?;
        let mut trial = Overlay::default();
        bind_document(&mut trial, corpus_id, document_id)?;
        trial
            .deleted_documents
            .insert(RecordKey::new(corpus_id, document_id));

        for (key, value) in &self.base.passages {
            if key.corpus_id == corpus_id && value.metadata.document_id == document_id {
                trial.passages.insert(key.clone(), Change::Deleted);
            }
        }
        for (key, value) in &self.base.vectors {
            if key.corpus_id == corpus_id && value.metadata.document_id == document_id {
                trial.vectors.insert(key.clone(), Change::Deleted);
            }
        }

        for (key, value) in &self.base.facts {
            if key.corpus_id != corpus_id
                || !value.source_document_ids.iter().any(|id| id == document_id)
            {
                continue;
            }
            let mut patched = value.clone();
            patched.source_document_ids.retain(|id| id != document_id);
            patched.passage_ids.retain(|id| {
                !self.base.passages.values().any(|passage| {
                    passage.corpus_id == corpus_id
                        && passage.passage_id == *id
                        && passage.metadata.document_id == document_id
                })
            });
            if patched.source_document_ids.is_empty() {
                trial.facts.insert(key.clone(), Change::Deleted);
            } else {
                trial.facts.insert(key.clone(), Change::Upsert(patched));
            }
        }

        for (key, value) in &self.base.schemas {
            if key.corpus_id != corpus_id
                || !value.source_document_ids.iter().any(|id| id == document_id)
            {
                continue;
            }
            let mut patched = value.clone();
            patched.source_document_ids.retain(|id| id != document_id);
            patched.fact_ids.retain(|id| {
                !self.base.facts.values().any(|fact| {
                    fact.corpus_id == corpus_id
                        && fact.fact_id == *id
                        && fact
                            .source_document_ids
                            .iter()
                            .any(|doc| doc == document_id)
                        && fact
                            .source_document_ids
                            .iter()
                            .all(|doc| doc == document_id)
                })
            });
            patched.frequency = patched.frequency.saturating_sub(1);
            patched.state = if patched.frequency >= patched.stabilization_threshold {
                "stable".to_string()
            } else {
                "pending".to_string()
            };
            trial.schemas.insert(key.clone(), Change::Upsert(patched));
        }

        self.validate_overlay(&trial)?;
        self.overlay = trial;
        Ok(())
    }

    pub fn discard(&mut self) {
        self.overlay = Overlay::default();
    }

    pub fn publish(&mut self) -> Result<u64, PrototypeError> {
        if self.overlay.is_empty() {
            return Err(PrototypeError::NoPendingChanges);
        }
        if Arc::strong_count(&self.base) != 1 {
            return Err(PrototypeError::ReadersActive);
        }
        let next_generation = self
            .base
            .generation
            .checked_add(1)
            .ok_or(PrototypeError::GenerationExhausted)?;
        let base = Arc::get_mut(&mut self.base).ok_or(PrototypeError::ReadersActive)?;
        apply_changes(&mut base.vectors, &mut self.overlay.vectors);
        apply_changes(&mut base.passages, &mut self.overlay.passages);
        apply_changes(&mut base.facts, &mut self.overlay.facts);
        apply_changes(&mut base.schemas, &mut self.overlay.schemas);
        base.generation = next_generation;
        self.overlay = Overlay::default();
        Ok(base.generation)
    }

    pub fn vector(&self, corpus_id: &str, id: &str) -> Option<&VectorRecord> {
        read_change(
            &self.overlay.vectors,
            &self.base.vectors,
            &RecordKey::new(corpus_id, id),
        )
    }

    pub fn passage(&self, corpus_id: &str, id: &str) -> Option<&PassageRecord> {
        read_change(
            &self.overlay.passages,
            &self.base.passages,
            &RecordKey::new(corpus_id, id),
        )
    }

    pub fn fact(&self, corpus_id: &str, id: &str) -> Option<&FactRecord> {
        read_change(
            &self.overlay.facts,
            &self.base.facts,
            &RecordKey::new(corpus_id, id),
        )
    }

    pub fn schema(&self, corpus_id: &str, id: &str) -> Option<&SchemaRecord> {
        read_change(
            &self.overlay.schemas,
            &self.base.schemas,
            &RecordKey::new(corpus_id, id),
        )
    }

    fn validate_delta(&self, delta: &DocumentDelta) -> Result<(), PrototypeError> {
        if delta.corpus_id.is_empty() || delta.document_id.is_empty() {
            return Err(PrototypeError::InvalidDelta("empty corpus/document id"));
        }
        if delta.vectors.is_empty()
            && delta.passages.is_empty()
            && delta.facts.is_empty()
            && delta.schemas.is_empty()
        {
            return Err(PrototypeError::InvalidDelta("empty document delta"));
        }
        self.validate_identifier(&delta.corpus_id)?;
        self.validate_identifier(&delta.document_id)?;
        let submitted_entries = delta
            .vectors
            .len()
            .checked_add(delta.passages.len())
            .and_then(|count| count.checked_add(delta.facts.len()))
            .and_then(|count| count.checked_add(delta.schemas.len()))
            .ok_or(PrototypeError::LimitExceeded("overlay entries"))?;
        if submitted_entries > self.limits.max_entries {
            return Err(PrototypeError::LimitExceeded("overlay entries"));
        }
        if has_duplicate_ids(&delta.vectors, |value| &value.id)
            || has_duplicate_ids(&delta.passages, |value| &value.passage_id)
            || has_duplicate_ids(&delta.facts, |value| &value.fact_id)
            || has_duplicate_ids(&delta.schemas, |value| &value.schema_id)
        {
            return Err(PrototypeError::InvalidDelta("duplicate record id"));
        }
        for vector in &delta.vectors {
            if vector.corpus_id != delta.corpus_id
                || vector.metadata.document_id != delta.document_id
            {
                return Err(PrototypeError::InvalidDelta("vector ownership mismatch"));
            }
            self.validate_identifier(&vector.id)?;
            self.validate_identifier(&vector.corpus_id)?;
            self.validate_identifier(&vector.namespace)?;
            self.validate_text_bytes(
                vector.metadata.title.len()
                    + vector
                        .metadata
                        .section_path
                        .iter()
                        .map(String::len)
                        .sum::<usize>(),
            )?;
            if vector.values.len() > self.limits.max_vector_dimensions {
                return Err(PrototypeError::LimitExceeded("vector dimensions"));
            }
            if vector.values.iter().any(|value| !value.is_finite()) {
                return Err(PrototypeError::InvalidDelta("non-finite vector value"));
            }
            if vector.values.len().saturating_mul(size_of::<f64>()) > self.limits.max_vector_bytes {
                return Err(PrototypeError::LimitExceeded("vector bytes"));
            }
        }
        for passage in &delta.passages {
            if passage.corpus_id != delta.corpus_id
                || passage.metadata.document_id != delta.document_id
            {
                return Err(PrototypeError::InvalidDelta("passage ownership mismatch"));
            }
            self.validate_identifier(&passage.passage_id)?;
            self.validate_identifier(&passage.corpus_id)?;
            passage
                .fact_ids
                .iter()
                .try_for_each(|id| self.validate_identifier(id))?;
            self.validate_text_bytes(
                passage.text.len()
                    + passage.normalized_text.len()
                    + passage.metadata.title.len()
                    + passage
                        .metadata
                        .section_path
                        .iter()
                        .map(String::len)
                        .sum::<usize>()
                    + passage
                        .entity_mentions
                        .iter()
                        .map(String::len)
                        .sum::<usize>()
                    + passage.quality_flags.iter().map(String::len).sum::<usize>(),
            )?;
        }
        for fact in &delta.facts {
            if fact.corpus_id != delta.corpus_id
                || !fact.source_document_ids.contains(&delta.document_id)
            {
                return Err(PrototypeError::InvalidDelta("fact provenance mismatch"));
            }
            self.validate_identifier(&fact.fact_id)?;
            self.validate_identifier(&fact.corpus_id)?;
            self.validate_identifier(&fact.schema_id)?;
            fact.passage_ids
                .iter()
                .chain(&fact.source_document_ids)
                .try_for_each(|id| self.validate_identifier(id))?;
            self.validate_text_bytes(
                fact.head_entity.len()
                    + fact.relation.len()
                    + fact.tail_entity.len()
                    + fact.state.len(),
            )?;
        }
        for schema in &delta.schemas {
            if schema.corpus_id != delta.corpus_id
                || !schema.source_document_ids.contains(&delta.document_id)
            {
                return Err(PrototypeError::InvalidDelta("schema provenance mismatch"));
            }
            self.validate_identifier(&schema.schema_id)?;
            self.validate_identifier(&schema.corpus_id)?;
            schema
                .fact_ids
                .iter()
                .chain(&schema.source_document_ids)
                .try_for_each(|id| self.validate_identifier(id))?;
            self.validate_text_bytes(
                schema.canonical_key.len()
                    + schema.aliases.iter().map(String::len).sum::<usize>()
                    + schema.state.len(),
            )?;
        }
        Ok(())
    }

    fn validate_identifier(&self, value: &str) -> Result<(), PrototypeError> {
        if value.is_empty() || value.len() > self.limits.max_identifier_bytes {
            return Err(PrototypeError::LimitExceeded("identifier bytes"));
        }
        Ok(())
    }

    fn validate_text_bytes(&self, bytes: usize) -> Result<(), PrototypeError> {
        if bytes > self.limits.max_text_bytes {
            return Err(PrototypeError::LimitExceeded("text bytes"));
        }
        Ok(())
    }

    fn preflight_delta(&self, delta: &DocumentDelta) -> Result<(), PrototypeError> {
        let mut entries = self.overlay.entry_count();
        let mut payload = self.overlay.payload_bytes();
        if self.overlay.document.is_none() {
            payload = checked_add(payload, delta.corpus_id.len() + delta.document_id.len())?;
        }
        for value in &delta.vectors {
            project_record(
                &self.overlay.vectors,
                &value.corpus_id,
                &value.id,
                value,
                &mut entries,
                &mut payload,
            )?;
        }
        for value in &delta.passages {
            project_record(
                &self.overlay.passages,
                &value.corpus_id,
                &value.passage_id,
                value,
                &mut entries,
                &mut payload,
            )?;
        }
        for value in &delta.facts {
            project_record(
                &self.overlay.facts,
                &value.corpus_id,
                &value.fact_id,
                value,
                &mut entries,
                &mut payload,
            )?;
        }
        for value in &delta.schemas {
            project_record(
                &self.overlay.schemas,
                &value.corpus_id,
                &value.schema_id,
                value,
                &mut entries,
                &mut payload,
            )?;
        }
        self.validate_projected_usage(entries, payload)
    }

    fn preflight_delete(&self, corpus_id: &str, document_id: &str) -> Result<(), PrototypeError> {
        let mut entries = 1usize; // durable document tombstone metadata
        let mut payload = corpus_id
            .len()
            .checked_add(document_id.len())
            .and_then(|bytes| bytes.checked_mul(2)) // binding plus tombstone key
            .ok_or(PrototypeError::LimitExceeded("overlay retained bytes"))?;

        for (key, value) in &self.base.passages {
            if key.corpus_id == corpus_id && value.metadata.document_id == document_id {
                entries = checked_add(entries, 1)?;
                payload = checked_add(
                    payload,
                    key.heap_bytes() + size_of::<Change<PassageRecord>>(),
                )?;
            }
        }
        for (key, value) in &self.base.vectors {
            if key.corpus_id == corpus_id && value.metadata.document_id == document_id {
                entries = checked_add(entries, 1)?;
                payload = checked_add(
                    payload,
                    key.heap_bytes() + size_of::<Change<VectorRecord>>(),
                )?;
            }
        }
        for (key, value) in &self.base.facts {
            if key.corpus_id == corpus_id
                && value.source_document_ids.iter().any(|id| id == document_id)
            {
                entries = checked_add(entries, 1)?;
                // A retained shared fact is at most the original record size; an
                // exclusive fact becomes a smaller tombstone.
                payload = checked_add(
                    payload,
                    key.heap_bytes() + size_of::<Change<FactRecord>>() + value.retained_bytes(),
                )?;
            }
        }
        for (key, value) in &self.base.schemas {
            if key.corpus_id == corpus_id
                && value.source_document_ids.iter().any(|id| id == document_id)
            {
                entries = checked_add(entries, 1)?;
                payload = checked_add(
                    payload,
                    key.heap_bytes() + size_of::<Change<SchemaRecord>>() + value.retained_bytes(),
                )?;
            }
        }
        self.validate_projected_usage(entries, payload)
    }

    fn validate_projected_usage(
        &self,
        entries: usize,
        payload_bytes: usize,
    ) -> Result<(), PrototypeError> {
        if entries > self.limits.max_entries {
            return Err(PrototypeError::LimitExceeded("overlay entries"));
        }
        // HashMap/HashSet growth is implementation-specific. Three slots per
        // logical entry bounds the small-map minimum capacity and is conservative
        // for the larger maps used by this prototype.
        let largest_slot = [
            size_of::<RecordKey>() + size_of::<Change<VectorRecord>>() + 1,
            size_of::<RecordKey>() + size_of::<Change<PassageRecord>>() + 1,
            size_of::<RecordKey>() + size_of::<Change<FactRecord>>() + 1,
            size_of::<RecordKey>() + size_of::<Change<SchemaRecord>>() + 1,
            size_of::<RecordKey>() + 1,
        ]
        .into_iter()
        .max()
        .expect("nonempty slot set");
        let table_reserve = entries
            .checked_mul(3)
            .and_then(|count| count.checked_mul(largest_slot))
            .ok_or(PrototypeError::LimitExceeded("overlay retained bytes"))?;
        let projected = payload_bytes
            .checked_add(table_reserve)
            .and_then(|bytes| bytes.checked_add(size_of::<Overlay>()))
            .ok_or(PrototypeError::LimitExceeded("overlay retained bytes"))?;
        if projected > self.limits.max_retained_bytes {
            return Err(PrototypeError::LimitExceeded("overlay retained bytes"));
        }
        Ok(())
    }

    fn validate_overlay(&self, overlay: &Overlay) -> Result<(), PrototypeError> {
        if overlay.entry_count() > self.limits.max_entries {
            return Err(PrototypeError::LimitExceeded("overlay entries"));
        }
        if overlay.retained_bytes() > self.limits.max_retained_bytes {
            return Err(PrototypeError::LimitExceeded("overlay retained bytes"));
        }
        Ok(())
    }
}

fn has_duplicate_ids<T>(values: &[T], id: impl Fn(&T) -> &str) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].iter().any(|prior| id(prior) == id(value)))
}

fn checked_add(left: usize, right: usize) -> Result<usize, PrototypeError> {
    left.checked_add(right)
        .ok_or(PrototypeError::LimitExceeded("overlay retained bytes"))
}

fn project_record<T: RetainedBytes>(
    map: &HashMap<RecordKey, Change<T>>,
    corpus_id: &str,
    id: &str,
    value: &T,
    entries: &mut usize,
    payload: &mut usize,
) -> Result<(), PrototypeError> {
    let existing = map
        .iter()
        .find(|(key, _)| key.corpus_id == corpus_id && key.id == id);
    if let Some((key, change)) = existing {
        *payload = payload
            .checked_sub(key.heap_bytes() + change.retained_bytes())
            .ok_or(PrototypeError::LimitExceeded("overlay retained bytes"))?;
    } else {
        *entries = checked_add(*entries, 1)?;
    }
    *payload = checked_add(
        *payload,
        corpus_id.len() + id.len() + size_of::<Change<T>>() + value.retained_bytes(),
    )?;
    Ok(())
}

fn bind_document(
    overlay: &mut Overlay,
    corpus_id: &str,
    document_id: &str,
) -> Result<(), PrototypeError> {
    let document = RecordKey::new(corpus_id, document_id);
    match &overlay.document {
        Some(existing) if existing != &document => {
            Err(PrototypeError::OverlayBoundToAnotherDocument)
        }
        Some(_) => Ok(()),
        None => {
            overlay.document = Some(document);
            Ok(())
        }
    }
}

fn read_change<'a, T>(
    overlay: &'a HashMap<RecordKey, Change<T>>,
    base: &'a HashMap<RecordKey, T>,
    key: &RecordKey,
) -> Option<&'a T> {
    match overlay.get(key) {
        Some(Change::Upsert(value)) => Some(value),
        Some(Change::Deleted) => None,
        None => base.get(key),
    }
}

fn apply_changes<T>(base: &mut HashMap<RecordKey, T>, overlay: &mut HashMap<RecordKey, Change<T>>) {
    for (key, change) in overlay.drain() {
        match change {
            Change::Upsert(value) => {
                base.insert(key, value);
            }
            Change::Deleted => {
                base.remove(&key);
            }
        }
    }
}

pub fn representative_base(generation: u64) -> CommittedBase {
    let mut base = CommittedBase::empty(generation);
    for document_id in ["doc-a", "doc-b"] {
        let suffix = document_id.trim_start_matches("doc-");
        let passage_id = format!("passage-{suffix}");
        base.vectors.insert(
            RecordKey::new("corpus-1", format!("vector-{suffix}")),
            VectorRecord {
                id: format!("vector-{suffix}"),
                corpus_id: "corpus-1".into(),
                namespace: "passage".into(),
                values: vec![1.0, 0.5, 0.25],
                metadata: DocumentMetadata {
                    document_id: document_id.into(),
                    title: format!("Document {suffix}"),
                    section_path: vec![],
                },
            },
        );
        base.passages.insert(
            RecordKey::new("corpus-1", &passage_id),
            PassageRecord {
                passage_id: passage_id.clone(),
                corpus_id: "corpus-1".into(),
                text: format!("Evidence from document {suffix}."),
                normalized_text: format!("evidence from document {suffix}"),
                metadata: DocumentMetadata {
                    document_id: document_id.into(),
                    title: format!("Document {suffix}"),
                    section_path: vec!["Results".into()],
                },
                fact_ids: if document_id == "doc-a" {
                    vec!["fact-shared".into(), "fact-a".into()]
                } else {
                    vec!["fact-shared".into()]
                },
                entity_mentions: vec!["Alpha".into(), "Beta".into()],
                quality_flags: vec![],
            },
        );
    }
    base.facts.insert(
        RecordKey::new("corpus-1", "fact-shared"),
        FactRecord {
            fact_id: "fact-shared".into(),
            corpus_id: "corpus-1".into(),
            schema_id: "schema-shared".into(),
            head_entity: "Alpha".into(),
            relation: "relates-to".into(),
            tail_entity: "Beta".into(),
            state: "active".into(),
            passage_ids: vec!["passage-a".into(), "passage-b".into()],
            source_document_ids: vec!["doc-a".into(), "doc-b".into()],
        },
    );
    base.facts.insert(
        RecordKey::new("corpus-1", "fact-a"),
        FactRecord {
            fact_id: "fact-a".into(),
            corpus_id: "corpus-1".into(),
            schema_id: "schema-shared".into(),
            head_entity: "Only A".into(),
            relation: "supports".into(),
            tail_entity: "Alpha".into(),
            state: "active".into(),
            passage_ids: vec!["passage-a".into()],
            source_document_ids: vec!["doc-a".into()],
        },
    );
    base.schemas.insert(
        RecordKey::new("corpus-1", "schema-shared"),
        SchemaRecord {
            schema_id: "schema-shared".into(),
            corpus_id: "corpus-1".into(),
            canonical_key: "entity::relates-to::entity".into(),
            aliases: vec!["relates to".into(), "関係する".into()],
            frequency: 2,
            state: "stable".into(),
            stabilization_threshold: 2,
            fact_ids: vec!["fact-shared".into(), "fact-a".into()],
            source_document_ids: vec!["doc-a".into(), "doc-b".into()],
        },
    );
    base
}

pub fn representative_delta(document_id: &str) -> DocumentDelta {
    let suffix = document_id.trim_start_matches("doc-");
    let passage_id = format!("passage-{suffix}");
    let new_fact_id = format!("fact-{suffix}");
    DocumentDelta {
        corpus_id: "corpus-1".into(),
        document_id: document_id.into(),
        vectors: vec![VectorRecord {
            id: format!("vector-{suffix}"),
            corpus_id: "corpus-1".into(),
            namespace: "passage".into(),
            values: vec![0.2, 0.4, 0.8],
            metadata: DocumentMetadata {
                document_id: document_id.into(),
                title: format!("Document {suffix}"),
                section_path: vec![],
            },
        }],
        passages: vec![PassageRecord {
            passage_id: passage_id.clone(),
            corpus_id: "corpus-1".into(),
            text: format!("Evidence from document {suffix}."),
            normalized_text: format!("evidence from document {suffix}"),
            metadata: DocumentMetadata {
                document_id: document_id.into(),
                title: format!("Document {suffix}"),
                section_path: vec!["Results".into(), "Overlay".into()],
            },
            fact_ids: vec!["fact-shared".into(), new_fact_id.clone()],
            entity_mentions: vec!["Alpha".into(), "Gamma".into()],
            quality_flags: vec!["prototype".into()],
        }],
        facts: vec![
            FactRecord {
                fact_id: "fact-shared".into(),
                corpus_id: "corpus-1".into(),
                schema_id: "schema-shared".into(),
                head_entity: "Alpha".into(),
                relation: "relates-to".into(),
                tail_entity: "Beta".into(),
                state: "active".into(),
                passage_ids: vec!["passage-a".into(), "passage-b".into(), passage_id.clone()],
                source_document_ids: vec!["doc-a".into(), "doc-b".into(), document_id.into()],
            },
            FactRecord {
                fact_id: new_fact_id.clone(),
                corpus_id: "corpus-1".into(),
                schema_id: "schema-shared".into(),
                head_entity: "Gamma".into(),
                relation: "supports".into(),
                tail_entity: "Alpha".into(),
                state: "inactive".into(),
                passage_ids: vec![passage_id],
                source_document_ids: vec![document_id.into()],
            },
        ],
        schemas: vec![SchemaRecord {
            schema_id: "schema-shared".into(),
            corpus_id: "corpus-1".into(),
            canonical_key: "entity::relates-to::entity".into(),
            aliases: vec!["relates to".into(), "関係する".into()],
            frequency: 3,
            state: "stable".into(),
            stabilization_threshold: 2,
            fact_ids: vec!["fact-shared".into(), "fact-a".into(), new_fact_id],
            source_document_ids: vec!["doc-a".into(), "doc-b".into(), document_id.into()],
        }],
    }
}

pub fn synthetic_base(generation: u64, document_count: usize) -> CommittedBase {
    let mut base = CommittedBase::empty(generation);
    base.vectors.reserve(document_count);
    base.passages.reserve(document_count);
    for index in 0..document_count {
        let document_id = format!("scaled-doc-{index}");
        let passage_id = format!("scaled-passage-{index}");
        let vector_id = format!("scaled-vector-{index}");
        base.vectors.insert(
            RecordKey::new("scaled-corpus", &vector_id),
            VectorRecord {
                id: vector_id,
                corpus_id: "scaled-corpus".into(),
                namespace: "passage".into(),
                values: vec![0.1, 0.2, 0.3, 0.4],
                metadata: DocumentMetadata {
                    document_id: document_id.clone(),
                    title: format!("Synthetic {index}"),
                    section_path: vec![],
                },
            },
        );
        base.passages.insert(
            RecordKey::new("scaled-corpus", &passage_id),
            PassageRecord {
                passage_id,
                corpus_id: "scaled-corpus".into(),
                text: "fixed synthetic evidence text".into(),
                normalized_text: "fixed synthetic evidence text".into(),
                metadata: DocumentMetadata {
                    document_id,
                    title: format!("Synthetic {index}"),
                    section_path: vec!["Results".into()],
                },
                fact_ids: vec![],
                entity_mentions: vec![],
                quality_flags: vec![],
            },
        );
    }
    base
}

pub fn scaled_fixed_delta() -> DocumentDelta {
    let mut delta = representative_delta("scaled-doc-new");
    delta.corpus_id = "scaled-corpus".into();
    for vector in &mut delta.vectors {
        vector.corpus_id = "scaled-corpus".into();
    }
    for passage in &mut delta.passages {
        passage.corpus_id = "scaled-corpus".into();
    }
    for fact in &mut delta.facts {
        fact.corpus_id = "scaled-corpus".into();
    }
    for schema in &mut delta.schemas {
        schema.corpus_id = "scaled-corpus".into();
    }
    delta
}
