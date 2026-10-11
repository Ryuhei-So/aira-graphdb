//! `memory_delete_document` (aira-graphdb #57, literature-hub #614).
//!
//! Per-document replace delete over every store a document contributes to:
//! the memory snapshot, the graph, the vectors and the lexical passages. It
//! runs inside the caller's open batch, so it publishes together with the
//! following `memory_upsert` in one generation.
//!
//! Validation, which runs before the WAL append, rejects only malformed
//! params and a malformed stored snapshot of the requested corpus (the same
//! item-level shape `memory_upsert` already requires). Every data-shape
//! condition beyond that is handled in the apply step, which cannot fail. A
//! shared fact is pruned, never refused.

use super::*;

const DELETE_PARAMS: &[&str] = &["corpusId", "documentId", "updatedAt"];

#[derive(Default)]
struct DeleteCounts {
    removed_passages: u64,
    removed_facts: u64,
    pruned_shared_facts: u64,
    pruned_schemas: u64,
    schema_frequency_adjusted: u64,
    removed_passage_nodes: u64,
    removed_fact_nodes: u64,
    removed_entity_nodes: u64,
    removed_other_nodes: u64,
    pruned_shared_nodes: u64,
    pruned_ontology_nodes: u64,
    removed_ontology_nodes: u64,
    removed_edges: u64,
    removed_vectors: u64,
    rekeyed_vectors: u64,
    removed_lexical_passages: u64,
}

impl DeleteCounts {
    fn total(&self) -> u64 {
        [
            self.removed_passages,
            self.removed_facts,
            self.pruned_shared_facts,
            self.pruned_schemas,
            self.schema_frequency_adjusted,
            self.removed_passage_nodes,
            self.removed_fact_nodes,
            self.removed_entity_nodes,
            self.removed_other_nodes,
            self.pruned_shared_nodes,
            self.pruned_ontology_nodes,
            self.removed_ontology_nodes,
            self.removed_edges,
            self.removed_vectors,
            self.rekeyed_vectors,
            self.removed_lexical_passages,
        ]
        .iter()
        .fold(0_u64, |total, value| total.saturating_add(*value))
    }
}

/// Removes every `document_id` string from a JSON string array in place and
/// reports whether the array held it. A non-array value is left untouched.
fn prune_string(array: Option<&mut Value>, needle: &str) -> bool {
    let Some(items) = array.and_then(Value::as_array_mut) else {
        return false;
    };
    let before = items.len();
    items.retain(|item| item.as_str() != Some(needle));
    items.len() != before
}

fn string_array_contains(value: Option<&Value>, needle: &str) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|items| items.iter().any(|item| item.as_str() == Some(needle)))
}

fn first_string(value: Option<&Value>) -> Option<String> {
    value
        .and_then(Value::as_array)
        .and_then(|items| items.iter().find_map(Value::as_str))
        .map(ToString::to_string)
}

fn has_any_string(value: Option<&Value>) -> bool {
    first_string(value).is_some()
}

/// The fact id carried by an `entity-cooccur:{head}:{tail}:{factId}` edge,
/// derived from its exact endpoints rather than a suffix match.
fn cooccurrence_fact_id(edge: &GraphEdge) -> Option<&str> {
    if edge.relation != "entity_cooccurrence" {
        return None;
    }
    let head = edge.source_node_id.strip_prefix("entity:")?;
    let tail = edge.target_node_id.strip_prefix("entity:")?;
    edge.edge_id
        .strip_prefix("entity-cooccur:")?
        .strip_prefix(head)?
        .strip_prefix(':')?
        .strip_prefix(tail)?
        .strip_prefix(':')
}

fn schema_id_of_node(node: &GraphNode) -> Option<String> {
    node.r#ref
        .get("schemaId")
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            node.node_id
                .strip_prefix("schema:")
                .map(ToString::to_string)
        })
}

fn is_ontology_vector(vector: &VectorRecord) -> bool {
    vector.namespace == "schema"
        || vector.metadata.get("layer").and_then(Value::as_str) == Some("ontology")
}

/// What the read-only graph scan decided for one non-ontology node.
enum NodeFate {
    Remove,
    Prune,
}

impl Server {
    pub(super) fn validate_memory_delete_document(
        &self,
        req: &RpcRequest,
        params: &serde_json::Map<String, Value>,
    ) -> Result<(), AppError> {
        Self::validate_indexing_request_size(req)?;
        Self::require_exact_params(params, DELETE_PARAMS)?;
        let corpus_id =
            Self::bounded_required_string(params, "corpusId", MAX_INDEXING_CORPUS_ID_BYTES)?;
        Self::bounded_required_string(params, "documentId", MAX_INDEXING_DOMAIN_ID_BYTES)?;
        Self::bounded_required_string(params, "updatedAt", MAX_INDEXING_UPDATED_AT_BYTES)?;
        // The stored snapshot shape the apply step relies on: an object keyed
        // by its own corpusId, array sections, and object items with a
        // bounded id. This is exactly what a following memory_upsert needs.
        self.validate_stored_snapshot_for_upsert(corpus_id)
    }

    /// Apply step. Infallible by construction: every shape it touches was
    /// validated before the WAL append or is handled tolerantly here.
    pub(super) fn apply_memory_delete_document(
        &mut self,
        corpus_id: &str,
        document_id: &str,
        updated_at: &str,
    ) -> Value {
        let mut counts = DeleteCounts::default();

        // 1. Read-only graph and vector scan: decide non-ontology node fates,
        //    and collect the schema ids whose post-delete snapshot state the
        //    ontology rules need.
        let mut node_fates: HashMap<String, NodeFate> = HashMap::new();
        let mut ontology_keys: HashSet<String> = HashSet::new();
        let mut needed_schema_ids: HashSet<String> = HashSet::new();
        let mut removed_fact_ids: HashSet<String> = HashSet::new();
        for (key, node) in &self.state.nodes {
            if node.corpus_id != corpus_id {
                continue;
            }
            if node.layer == "ontology" {
                if Self::doc_ids_from_ref(&node.r#ref)
                    .iter()
                    .any(|id| id == document_id)
                {
                    ontology_keys.insert(key.clone());
                    if let Some(schema_id) = schema_id_of_node(node) {
                        needed_schema_ids.insert(schema_id);
                    }
                }
                continue;
            }
            if node.layer == "entity" {
                continue;
            }
            let fate = match node
                .r#ref
                .get("sourceDocumentIds")
                .and_then(Value::as_array)
            {
                Some(ids) => {
                    if !ids.iter().any(|id| id.as_str() == Some(document_id)) {
                        continue;
                    }
                    if ids
                        .iter()
                        .any(|id| id.as_str().is_some_and(|id| id != document_id))
                    {
                        NodeFate::Prune
                    } else {
                        NodeFate::Remove
                    }
                }
                None => {
                    if Self::doc_ids_from_ref(&node.r#ref)
                        .iter()
                        .any(|id| id == document_id)
                    {
                        NodeFate::Remove
                    } else {
                        continue;
                    }
                }
            };
            if matches!(fate, NodeFate::Remove) && node.layer == "fact" {
                let fact_id = node
                    .r#ref
                    .get("factId")
                    .and_then(Value::as_str)
                    .or_else(|| node.node_id.strip_prefix("fact:"))
                    .unwrap_or(&node.node_id);
                removed_fact_ids.insert(fact_id.to_string());
            }
            node_fates.insert(key.clone(), fate);
        }
        for vector in self.state.vectors.values() {
            if vector.corpus_id == corpus_id
                && is_ontology_vector(vector)
                && vector.metadata.get("documentId").and_then(Value::as_str) == Some(document_id)
                && let Some(schema_id) = vector.id.strip_prefix("schema:")
            {
                needed_schema_ids.insert(schema_id.to_string());
            }
        }

        // 2. Snapshot. Facts first so their ids prune schema factIds.
        //    `schema_post` holds the post-delete schema of every affected or
        //    needed schema; the graph mirrors it (memory-schema@1 hydration).
        let mut schema_post: HashMap<String, Value> = HashMap::new();
        if let Some(snapshot) = self.state.snapshots.get_mut(corpus_id) {
            if let Some(passages) = snapshot.get_mut("passages").and_then(Value::as_array_mut) {
                let before = passages.len();
                passages.retain(|passage| {
                    passage
                        .get("metadata")
                        .and_then(|metadata| metadata.get("documentId"))
                        .and_then(Value::as_str)
                        != Some(document_id)
                });
                counts.removed_passages = (before - passages.len()) as u64;
            }
            let mut fact_schema_ids: HashSet<String> = HashSet::new();
            if let Some(facts) = snapshot.get_mut("facts").and_then(Value::as_array_mut) {
                let before = facts.len();
                facts.retain_mut(|fact| {
                    if !prune_string(fact.get_mut("sourceDocumentIds"), document_id) {
                        return true;
                    }
                    if let Some(schema_id) = fact.get("schemaId").and_then(Value::as_str) {
                        fact_schema_ids.insert(schema_id.to_string());
                    }
                    if has_any_string(fact.get("sourceDocumentIds")) {
                        fact["updatedAt"] = json!(updated_at);
                        counts.pruned_shared_facts += 1;
                        return true;
                    }
                    if let Some(fact_id) = fact.get("factId").and_then(Value::as_str) {
                        removed_fact_ids.insert(fact_id.to_string());
                    }
                    false
                });
                counts.removed_facts = (before - facts.len()) as u64;
            }
            if let Some(schemas) = snapshot.get_mut("schemas").and_then(Value::as_array_mut) {
                for schema in schemas.iter_mut() {
                    let schema_id = schema
                        .get("schemaId")
                        .and_then(Value::as_str)
                        .expect("stored schema id validated before WAL")
                        .to_string();
                    let lists_document =
                        string_array_contains(schema.get("sourceDocumentIds"), document_id);
                    let lists_removed_fact = schema
                        .get("factIds")
                        .and_then(Value::as_array)
                        .is_some_and(|ids| {
                            ids.iter().any(|id| {
                                id.as_str().is_some_and(|id| removed_fact_ids.contains(id))
                            })
                        });
                    let affected = lists_document
                        || lists_removed_fact
                        || fact_schema_ids.contains(&schema_id);
                    if affected {
                        Self::prune_schema_value(
                            schema,
                            document_id,
                            &removed_fact_ids,
                            updated_at,
                            &mut counts,
                        );
                        counts.pruned_schemas += 1;
                    }
                    if affected || needed_schema_ids.contains(&schema_id) {
                        schema_post.insert(schema_id, schema.clone());
                    }
                }
            }
        }
        for schema_id in schema_post.keys() {
            let key = Self::key(corpus_id, &format!("schema:{schema_id}"));
            if self.state.nodes.contains_key(&key) {
                ontology_keys.insert(key);
            }
        }

        // 3. Graph nodes. `rekey` maps a surviving node id to the first
        //    remaining source document its vector should name.
        let mut removed_node_ids: HashSet<String> = HashSet::new();
        let mut rekey: HashMap<String, String> = HashMap::new();
        for (schema_id, schema) in &schema_post {
            if let Some(first) = first_string(schema.get("sourceDocumentIds")) {
                rekey.insert(format!("schema:{schema_id}"), first);
            }
        }
        for (key, fate) in node_fates {
            let node = self
                .state
                .nodes
                .get_mut(&key)
                .expect("node fate came from live state");
            match fate {
                NodeFate::Prune => {
                    prune_string(node.r#ref.get_mut("sourceDocumentIds"), document_id);
                    if let Some(first) = first_string(node.r#ref.get("sourceDocumentIds")) {
                        rekey.insert(node.node_id.clone(), first);
                    }
                    counts.pruned_shared_nodes += 1;
                }
                NodeFate::Remove => {
                    let node = self
                        .state
                        .nodes
                        .remove(&key)
                        .expect("node fate came from live state");
                    match node.layer.as_str() {
                        "passage" => counts.removed_passage_nodes += 1,
                        "fact" => counts.removed_fact_nodes += 1,
                        _ => counts.removed_other_nodes += 1,
                    }
                    removed_node_ids.insert(node.node_id);
                }
            }
        }
        for key in ontology_keys {
            let node = self
                .state
                .nodes
                .get_mut(&key)
                .expect("ontology key came from live state");
            let post = schema_id_of_node(node).and_then(|id| schema_post.get(&id));
            let support = match post {
                Some(post) => {
                    let support = first_string(post.get("sourceDocumentIds"));
                    if support.is_some() {
                        node.r#ref = post.clone();
                    }
                    support
                }
                None => {
                    prune_string(node.r#ref.get_mut("sourceDocumentIds"), document_id);
                    if let Some(fact_ids) =
                        node.r#ref.get_mut("factIds").and_then(Value::as_array_mut)
                    {
                        fact_ids.retain(|id| {
                            !id.as_str().is_some_and(|id| removed_fact_ids.contains(id))
                        });
                    }
                    first_string(node.r#ref.get("sourceDocumentIds"))
                }
            };
            match support {
                Some(first) => {
                    rekey.insert(node.node_id.clone(), first);
                    counts.pruned_ontology_nodes += 1;
                }
                None => {
                    let node = self
                        .state
                        .nodes
                        .remove(&key)
                        .expect("ontology key came from live state");
                    rekey.remove(&node.node_id);
                    removed_node_ids.insert(node.node_id);
                    counts.removed_ontology_nodes += 1;
                }
            }
        }

        // 4. Edges adjacent to removed nodes, plus co-occurrence edges that
        //    carry a removed fact id. Entity endpoints become candidates.
        let mut entity_candidates: HashSet<String> = HashSet::new();
        let before_edges = self.state.edges.len();
        self.state.edges.retain(|_, edge| {
            if edge.corpus_id != corpus_id {
                return true;
            }
            let remove = removed_node_ids.contains(&edge.source_node_id)
                || removed_node_ids.contains(&edge.target_node_id)
                || cooccurrence_fact_id(edge).is_some_and(|id| removed_fact_ids.contains(id));
            if remove {
                for endpoint in [&edge.source_node_id, &edge.target_node_id] {
                    if endpoint.starts_with("entity:") && !removed_node_ids.contains(endpoint) {
                        entity_candidates.insert(endpoint.clone());
                    }
                }
            }
            !remove
        });
        entity_candidates.retain(|node_id| {
            self.state
                .nodes
                .get(&Self::key(corpus_id, node_id))
                .is_some_and(|node| node.layer == "entity")
        });

        // 5. Entities with no remaining mention or co-occurrence edge are
        //    removed with every other edge they still have. `factCount` of a
        //    survivor is per-upsert, not a global count, and is left as is.
        if !entity_candidates.is_empty() {
            let mut supported: HashSet<&str> = HashSet::new();
            for edge in self.state.edges.values() {
                if edge.corpus_id != corpus_id
                    || !matches!(
                        edge.relation.as_str(),
                        "entity_mention" | "entity_cooccurrence"
                    )
                {
                    continue;
                }
                for endpoint in [&edge.source_node_id, &edge.target_node_id] {
                    if entity_candidates.contains(endpoint) {
                        supported.insert(endpoint.as_str());
                    }
                }
            }
            let orphans: Vec<String> = entity_candidates
                .iter()
                .filter(|node_id| !supported.contains(node_id.as_str()))
                .cloned()
                .collect();
            for node_id in orphans {
                if self
                    .state
                    .nodes
                    .remove(&Self::key(corpus_id, &node_id))
                    .is_some()
                {
                    counts.removed_entity_nodes += 1;
                    removed_node_ids.insert(node_id);
                }
            }
            self.state.edges.retain(|_, edge| {
                edge.corpus_id != corpus_id
                    || !(removed_node_ids.contains(&edge.source_node_id)
                        || removed_node_ids.contains(&edge.target_node_id))
            });
        }
        counts.removed_edges = (before_edges - self.state.edges.len()) as u64;

        // 6. Vectors: drop those of removed nodes and the document's own
        //    non-ontology vectors; re-key a survivor still naming the document.
        let mut removed_vector_keys = Vec::new();
        self.state.vectors.retain(|key, vector| {
            if vector.corpus_id != corpus_id {
                return true;
            }
            if removed_node_ids.contains(&vector.id) {
                removed_vector_keys.push(key.clone());
                return false;
            }
            if vector.metadata.get("documentId").and_then(Value::as_str) != Some(document_id) {
                return true;
            }
            match rekey.get(&vector.id) {
                Some(first) if vector.metadata.is_object() => {
                    vector.metadata["documentId"] = json!(first);
                    counts.rekeyed_vectors += 1;
                    true
                }
                _ => {
                    removed_vector_keys.push(key.clone());
                    false
                }
            }
        });
        counts.removed_vectors = removed_vector_keys.len() as u64;
        for key in removed_vector_keys {
            self.vector_values.remove(&key);
        }

        // 7. Lexical passages (the only lexical entry kind this native holds).
        let before_lexical = self.state.passages.len();
        self.state
            .passages
            .retain(|_, p| !(p.corpus_id == corpus_id && p.document_id == document_id));
        counts.removed_lexical_passages = (before_lexical - self.state.passages.len()) as u64;

        let noop = counts.total() == 0;
        if !noop {
            self.mark_cache_dirty();
        }
        json!({
            "corpusId": corpus_id,
            "documentId": document_id,
            "noop": noop,
            "removedPassages": counts.removed_passages,
            "removedFacts": counts.removed_facts,
            "prunedSharedFacts": counts.pruned_shared_facts,
            "prunedSchemas": counts.pruned_schemas,
            "schemaFrequencyAdjusted": counts.schema_frequency_adjusted,
            "removedPassageNodes": counts.removed_passage_nodes,
            "removedFactNodes": counts.removed_fact_nodes,
            "removedEntityNodes": counts.removed_entity_nodes,
            "removedOtherNodes": counts.removed_other_nodes,
            "prunedSharedNodes": counts.pruned_shared_nodes,
            "prunedOntologyNodes": counts.pruned_ontology_nodes,
            "removedOntologyNodes": counts.removed_ontology_nodes,
            "removedEdges": counts.removed_edges,
            "removedVectors": counts.removed_vectors,
            "rekeyedVectors": counts.rekeyed_vectors,
            "removedLexicalPassages": counts.removed_lexical_passages,
        })
    }

    /// The Synapse `DeleteDocumentService` rule applied to one stored schema:
    /// drop the document and its removed facts, decrement `frequency` once,
    /// then `stable` iff frequency >= stabilizationThreshold else `pending`.
    /// A schema at support 0 is kept, as that service keeps it.
    fn prune_schema_value(
        schema: &mut Value,
        document_id: &str,
        removed_fact_ids: &HashSet<String>,
        updated_at: &str,
        counts: &mut DeleteCounts,
    ) {
        prune_string(schema.get_mut("sourceDocumentIds"), document_id);
        if let Some(fact_ids) = schema.get_mut("factIds").and_then(Value::as_array_mut) {
            fact_ids.retain(|id| !id.as_str().is_some_and(|id| removed_fact_ids.contains(id)));
        }
        let safe = |value: Option<&Value>| {
            value
                .and_then(Value::as_u64)
                .filter(|value| *value <= JSON_SAFE_INTEGER_MAX)
        };
        if let Some(frequency) = safe(schema.get("frequency")) {
            let next = frequency.saturating_sub(1);
            schema["frequency"] = json!(next);
            if let Some(threshold) = safe(schema.get("stabilizationThreshold")) {
                schema["state"] = json!(if next >= threshold {
                    "stable"
                } else {
                    "pending"
                });
            }
            counts.schema_frequency_adjusted += 1;
        }
        schema["updatedAt"] = json!(updated_at);
    }
}
