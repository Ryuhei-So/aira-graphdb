use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::fs;
use std::io::{self, BufRead, Write};
use std::panic;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use aira_graphdb::graph::{InMemoryGraphStore, Properties, Value as GraphValue};
use aira_graphdb::query::{CypherDialect, execute_query_with_dialect};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct GraphNode {
    node_id: String,
    corpus_id: String,
    layer: String,
    r#ref: Value,
    label: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct GraphEdge {
    edge_id: String,
    corpus_id: String,
    source_node_id: String,
    target_node_id: String,
    relation: String,
    weight: f64,
    bridge_kind: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct VectorBlobRef {
    offset: u64,
    len: u32,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct VectorRecord {
    id: String,
    corpus_id: String,
    namespace: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    values: Vec<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    blob_ref: Option<VectorBlobRef>,
    metadata: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Passage {
    passage_id: String,
    corpus_id: String,
    document_id: String,
    text: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct State {
    nodes: HashMap<String, GraphNode>,
    edges: HashMap<String, GraphEdge>,
    vectors: HashMap<String, VectorRecord>,
    passages: HashMap<String, Passage>,
    snapshots: HashMap<String, Value>,
    checkpoints: HashMap<String, Value>,
}

/// Keep exact vector search bounded when a retrieval request is large.  The
/// native sidecar is a single owner, so using a small fixed worker cap avoids
/// turning one request into an unbounded host-wide thread fan-out.
const MAX_VECTOR_SEARCH_WORKERS: usize = 8;

#[derive(Debug)]
struct ScoredVector {
    key: String,
    score: f64,
}

impl PartialEq for ScoredVector {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.score.to_bits() == other.score.to_bits()
    }
}

impl Eq for ScoredVector {}

/// BinaryHeap keeps the worst retained candidate at its root.  This lets the
/// exact scan retain only top-k entries while preserving a deterministic
/// score-descending/key-ascending result order.
impl Ord for ScoredVector {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for ScoredVector {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcRequest {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize, Deserialize)]
struct RpcResponse {
    id: u64,
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RpcError {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure_class: Option<String>,
}

#[derive(Debug)]
struct AppError {
    code: String,
    message: String,
    failure_class: Option<String>,
}

#[derive(Clone)]
struct CrashTracker {
    audit_log_path: PathBuf,
    started_epoch_sec: u64,
    last_request_id: Arc<Mutex<Option<String>>>,
}

struct Server {
    db_path: PathBuf,
    vector_blob_path: PathBuf,
    audit_log_path: PathBuf,
    state: State,
    vector_values: HashMap<String, Vec<f64>>,
    cache_dirty: bool,
    batch_mode: bool,
    node_keys_by_corpus: HashMap<String, Vec<String>>,
    edge_keys_by_corpus: HashMap<String, Vec<String>>,
    adjacent_edge_keys_by_node: HashMap<String, Vec<String>>,
    vector_keys_by_corpus_namespace: HashMap<String, Vec<String>>,
    passage_keys_by_corpus: HashMap<String, Vec<String>>,
}

impl CrashTracker {
    fn new(audit_log_path: PathBuf) -> Self {
        let started_epoch_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_secs();
        Self {
            audit_log_path,
            started_epoch_sec,
            last_request_id: Arc::new(Mutex::new(None)),
        }
    }

    fn set_last_request_id(&self, request_id: String) {
        if let Ok(mut guard) = self.last_request_id.lock() {
            *guard = Some(request_id);
        }
    }

    fn last_request_id(&self) -> Option<String> {
        self.last_request_id
            .lock()
            .ok()
            .and_then(|guard| guard.clone())
    }

    fn uptime_sec(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_secs();
        now.saturating_sub(self.started_epoch_sec)
    }

    fn append_crash_event(
        &self,
        process_exit_code: Option<i32>,
        signal: Option<&str>,
        cause: Option<String>,
    ) {
        let payload = json!({
            "errorCode": "PROCESS_CRASH",
            "timestamp": Server::now_epoch_ms_string(),
            "processExitCode": process_exit_code,
            "signal": signal,
            "lastRequestId": self.last_request_id(),
            "uptimeSec": self.uptime_sec(),
            "cause": cause
        });
        let _ = Server::append_json_line_for_path(&self.audit_log_path, &payload);
    }
}

impl Server {
    const VECTOR_BLOB_MAGIC: &'static [u8; 4] = b"AGVB";
    const VECTOR_BLOB_VERSION: u16 = 1;

    fn open(db_path: PathBuf) -> io::Result<Self> {
        let mut state = if db_path.exists() {
            let raw = fs::read_to_string(&db_path)?;
            serde_json::from_str(&raw).unwrap_or_default()
        } else {
            State::default()
        };
        let vector_blob_path = db_path.with_extension("vblob");
        let vector_values = Self::load_vector_values(&state, &vector_blob_path)?;
        for (key, values) in &vector_values {
            if let Some(vector) = state.vectors.get_mut(key) {
                vector.values = values.clone();
            }
        }
        Ok(Self {
            audit_log_path: db_path.with_extension("native-audit.log"),
            vector_blob_path,
            db_path,
            state,
            vector_values,
            cache_dirty: true,
            batch_mode: false,
            node_keys_by_corpus: HashMap::new(),
            edge_keys_by_corpus: HashMap::new(),
            adjacent_edge_keys_by_node: HashMap::new(),
            vector_keys_by_corpus_namespace: HashMap::new(),
            passage_keys_by_corpus: HashMap::new(),
        })
    }

    fn now_epoch_ms_string() -> String {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_millis()
            .to_string()
    }

    fn append_request_audit_event_for_path(
        audit_log_path: &PathBuf,
        error_code: &str,
        failure_class: &str,
        request_id: &str,
    ) -> io::Result<()> {
        let payload = json!({
            "errorCode": error_code,
            "failureClass": failure_class,
            "requestId": request_id,
            "timestamp": Self::now_epoch_ms_string()
        });
        Self::append_json_line_for_path(audit_log_path, &payload)
    }

    fn append_json_line_for_path(audit_log_path: &PathBuf, payload: &Value) -> io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(audit_log_path)?;
        file.write_all(payload.to_string().as_bytes())?;
        file.write_all(b"\n")?;
        file.flush()
    }

    fn append_request_audit_event(&self, error_code: &str, failure_class: &str, request_id: &str) {
        let _ = Self::append_request_audit_event_for_path(
            &self.audit_log_path,
            error_code,
            failure_class,
            request_id,
        );
    }

    fn persist(&self) -> io::Result<()> {
        let start = std::time::Instant::now();
        let mut persisted_state = self.state.clone();
        let vector_blob_payload =
            Self::build_vector_blob_payload(&mut persisted_state, &self.vector_values)?;
        let raw = serde_json::to_vec(&persisted_state)
            .map_err(|err| io::Error::other(format!("serialize state failed: {err}")))?;
        let serialize_ms = start.elapsed().as_millis();
        let parent = self
            .db_path
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent)
            .map_err(|err| io::Error::other(format!("create_dir_all({:?}): {err}", parent)))?;

        Self::persist_vector_blob_atomic(&self.vector_blob_path, &vector_blob_payload)?;

        // Write to tmp file first, then rename for atomicity
        let tmp_path = self.db_path.with_extension("agdb.tmp");
        let mut file = io::BufWriter::with_capacity(
            8 * 1024 * 1024,
            fs::File::create(&tmp_path)
                .map_err(|err| io::Error::other(format!("File::create({:?}): {err}", tmp_path)))?,
        );
        file.write_all(&raw).map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("write_all({}bytes): {err}", raw.len()))
        })?;
        file.flush().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("flush: {err}"))
        })?;
        let inner = file.into_inner().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("into_inner: {err}"))
        })?;
        inner.sync_all().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("sync_all: {err}"))
        })?;
        drop(inner);

        // Atomic rename (fallback to copy+remove if rename fails)
        if let Err(_rename_err) = fs::rename(&tmp_path, &self.db_path) {
            // Fallback: copy then remove tmp (non-atomic but works across filesystems)
            fs::copy(&tmp_path, &self.db_path).map_err(|err| {
                io::Error::other(format!("copy({:?} -> {:?}): {err}", tmp_path, self.db_path))
            })?;
            let _ = fs::remove_file(&tmp_path);
        }

        let total_ms = start.elapsed().as_millis();
        eprintln!(
            "[persist] {}MB serialized in {}ms, written in {}ms",
            raw.len() / (1024 * 1024),
            serialize_ms,
            total_ms - serialize_ms
        );
        Ok(())
    }

    fn persist_if_needed(&mut self) -> io::Result<()> {
        if self.batch_mode {
            Ok(())
        } else {
            self.persist()
        }
    }

    fn key(corpus_id: &str, id: &str) -> String {
        format!("{corpus_id}:{id}")
    }

    fn load_vector_values(
        state: &State,
        blob_path: &PathBuf,
    ) -> io::Result<HashMap<String, Vec<f64>>> {
        let mut values = HashMap::new();
        let mut raw_blob: Vec<u8> = Vec::new();
        if blob_path.exists() {
            raw_blob = fs::read(blob_path)?;
            if raw_blob.len() < Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>() {
                return Err(io::Error::other("vector blob file is truncated"));
            }
            if &raw_blob[..Self::VECTOR_BLOB_MAGIC.len()] != Self::VECTOR_BLOB_MAGIC {
                return Err(io::Error::other("vector blob magic mismatch"));
            }
            let version_start = Self::VECTOR_BLOB_MAGIC.len();
            let version_end = version_start + std::mem::size_of::<u16>();
            let version = u16::from_le_bytes(
                raw_blob[version_start..version_end]
                    .try_into()
                    .expect("slice length"),
            );
            if version != Self::VECTOR_BLOB_VERSION {
                return Err(io::Error::other(format!(
                    "vector blob version mismatch: expected {}, got {}",
                    Self::VECTOR_BLOB_VERSION,
                    version
                )));
            }
        }

        let payload_offset = Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>();
        for (key, vector) in &state.vectors {
            if !vector.values.is_empty() {
                values.insert(key.clone(), vector.values.clone());
                continue;
            }
            let Some(blob_ref) = &vector.blob_ref else {
                continue;
            };
            if raw_blob.is_empty() {
                return Err(io::Error::other(
                    "vector metadata references blob but blob file is missing",
                ));
            }
            let start = payload_offset.saturating_add(blob_ref.offset as usize);
            let byte_len = (blob_ref.len as usize)
                .checked_mul(std::mem::size_of::<f64>())
                .ok_or_else(|| io::Error::other("vector blob length overflow"))?;
            let end = start
                .checked_add(byte_len)
                .ok_or_else(|| io::Error::other("vector blob offset overflow"))?;
            if end > raw_blob.len() {
                return Err(io::Error::other("vector blob reference out of bounds"));
            }
            let mut out = Vec::with_capacity(blob_ref.len as usize);
            for chunk in raw_blob[start..end].chunks_exact(std::mem::size_of::<f64>()) {
                let num = f64::from_le_bytes(chunk.try_into().expect("slice length"));
                out.push(num);
            }
            values.insert(key.clone(), out);
        }
        Ok(values)
    }

    fn build_vector_blob_payload(
        state: &mut State,
        vector_values: &HashMap<String, Vec<f64>>,
    ) -> io::Result<Vec<u8>> {
        let mut payload = Vec::new();
        payload.extend_from_slice(Self::VECTOR_BLOB_MAGIC);
        payload.extend_from_slice(&Self::VECTOR_BLOB_VERSION.to_le_bytes());
        let mut offset = 0u64;
        let mut keys: Vec<String> = state.vectors.keys().cloned().collect();
        keys.sort();
        for key in keys {
            let Some(vector) = state.vectors.get_mut(&key) else {
                continue;
            };
            let values = vector_values
                .get(&key)
                .cloned()
                .unwrap_or_else(|| vector.values.clone());
            let len = u32::try_from(values.len())
                .map_err(|_| io::Error::other("vector dimensions exceed u32"))?;
            for value in &values {
                payload.extend_from_slice(&value.to_le_bytes());
            }
            vector.values.clear();
            vector.blob_ref = Some(VectorBlobRef { offset, len });
            offset = offset
                .checked_add((len as u64) * std::mem::size_of::<f64>() as u64)
                .ok_or_else(|| io::Error::other("vector blob offset overflow"))?;
        }
        Ok(payload)
    }

    fn persist_vector_blob_atomic(path: &PathBuf, payload: &[u8]) -> io::Result<()> {
        let tmp_path = path.with_extension("vblob.tmp");
        let mut file = io::BufWriter::with_capacity(
            8 * 1024 * 1024,
            fs::File::create(&tmp_path)
                .map_err(|err| io::Error::other(format!("File::create({:?}): {err}", tmp_path)))?,
        );
        file.write_all(payload).map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("write_all(vblob): {err}"))
        })?;
        file.flush().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("flush(vblob): {err}"))
        })?;
        let inner = file.into_inner().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("into_inner(vblob): {err}"))
        })?;
        inner.sync_all().map_err(|err| {
            let _ = fs::remove_file(&tmp_path);
            io::Error::other(format!("sync_all(vblob): {err}"))
        })?;
        drop(inner);
        if let Err(_rename_err) = fs::rename(&tmp_path, path) {
            fs::copy(&tmp_path, path).map_err(|err| {
                io::Error::other(format!("copy({:?} -> {:?}): {err}", tmp_path, path))
            })?;
            let _ = fs::remove_file(&tmp_path);
        }
        Ok(())
    }

    fn node_key(corpus_id: &str, node_id: &str) -> String {
        format!("{corpus_id}:{node_id}")
    }

    fn corpus_namespace_key(corpus_id: &str, namespace: &str) -> String {
        format!("{corpus_id}:{namespace}")
    }

    fn mark_cache_dirty(&mut self) {
        self.cache_dirty = true;
    }

    fn ensure_cache(&mut self) {
        if !self.cache_dirty {
            return;
        }

        self.node_keys_by_corpus.clear();
        for (key, node) in &self.state.nodes {
            self.node_keys_by_corpus
                .entry(node.corpus_id.clone())
                .or_default()
                .push(key.clone());
        }

        self.edge_keys_by_corpus.clear();
        self.adjacent_edge_keys_by_node.clear();
        for (key, edge) in &self.state.edges {
            self.edge_keys_by_corpus
                .entry(edge.corpus_id.clone())
                .or_default()
                .push(key.clone());

            let source_key = Self::node_key(&edge.corpus_id, &edge.source_node_id);
            self.adjacent_edge_keys_by_node
                .entry(source_key)
                .or_default()
                .push(key.clone());

            if edge.source_node_id != edge.target_node_id {
                let target_key = Self::node_key(&edge.corpus_id, &edge.target_node_id);
                self.adjacent_edge_keys_by_node
                    .entry(target_key)
                    .or_default()
                    .push(key.clone());
            }
        }

        self.vector_keys_by_corpus_namespace.clear();
        for (key, vector) in &self.state.vectors {
            let corpus_namespace_key =
                Self::corpus_namespace_key(&vector.corpus_id, &vector.namespace);
            self.vector_keys_by_corpus_namespace
                .entry(corpus_namespace_key)
                .or_default()
                .push(key.clone());
        }

        self.passage_keys_by_corpus.clear();
        for (key, passage) in &self.state.passages {
            self.passage_keys_by_corpus
                .entry(passage.corpus_id.clone())
                .or_default()
                .push(key.clone());
        }

        self.cache_dirty = false;
    }

    fn doc_ids_from_ref(value: &Value) -> Vec<String> {
        if let Some(ids) = value.get("sourceDocumentIds").and_then(|v| v.as_array()) {
            return ids
                .iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect();
        }
        if let Some(document_id) = value
            .get("metadata")
            .and_then(|v| v.get("documentId"))
            .and_then(|v| v.as_str())
        {
            return vec![document_id.to_string()];
        }
        Vec::new()
    }

    #[inline]
    fn vector_norm(values: &[f64]) -> f64 {
        values.iter().map(|value| value * value).sum::<f64>().sqrt()
    }

    #[inline]
    fn cosine_with_query_norm(query: &[f64], query_norm: f64, vector: &[f64]) -> f64 {
        if query.is_empty() || vector.is_empty() || query.len() != vector.len() || query_norm == 0.0
        {
            return 0.0;
        }
        let mut dot = 0.0;
        let mut norm_b = 0.0;
        for (query_value, vector_value) in query.iter().zip(vector) {
            dot += query_value * vector_value;
            norm_b += vector_value * vector_value;
        }
        if norm_b == 0.0 {
            return 0.0;
        }
        dot / (query_norm * norm_b.sqrt())
    }

    fn retain_top_k(heap: &mut BinaryHeap<ScoredVector>, candidate: ScoredVector, top_k: usize) {
        if top_k == 0 {
            return;
        }
        if heap.len() < top_k {
            heap.push(candidate);
            return;
        }
        let Some(worst) = heap.peek() else {
            return;
        };
        let is_better = match candidate.score.total_cmp(&worst.score) {
            Ordering::Greater => true,
            Ordering::Less => false,
            _ => candidate.key < worst.key,
        };
        if is_better {
            heap.pop();
            heap.push(candidate);
        }
    }

    fn scan_vector_chunk(
        keys: &[String],
        vectors: &HashMap<String, VectorRecord>,
        vector_values: &HashMap<String, Vec<f64>>,
        query: &[f64],
        query_norm: f64,
        threshold: Option<f64>,
        top_k: usize,
    ) -> BinaryHeap<ScoredVector> {
        let mut heap = BinaryHeap::with_capacity(top_k.min(keys.len()));
        for key in keys {
            let Some(_record) = vectors.get(key) else {
                continue;
            };
            let score = vector_values
                .get(key)
                .map(|vector| Self::cosine_with_query_norm(query, query_norm, vector))
                .unwrap_or(0.0);
            if !threshold.is_none_or(|minimum| score >= minimum) {
                continue;
            }
            Self::retain_top_k(
                &mut heap,
                ScoredVector {
                    key: key.clone(),
                    score,
                },
                top_k,
            );
        }
        heap
    }

    fn vector_search_candidates(
        keys: &[String],
        vectors: &HashMap<String, VectorRecord>,
        vector_values: &HashMap<String, Vec<f64>>,
        query: &[f64],
        threshold: Option<f64>,
        top_k: usize,
    ) -> Result<Vec<ScoredVector>, AppError> {
        if keys.is_empty() || top_k == 0 {
            return Ok(Vec::new());
        }
        let query_norm = Self::vector_norm(query);
        let available = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);
        let worker_count = keys.len().min(available.min(MAX_VECTOR_SEARCH_WORKERS));
        let chunk_size = keys.len().div_ceil(worker_count);
        let partials = thread::scope(|scope| {
            let handles = keys
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        Self::scan_vector_chunk(
                            chunk,
                            vectors,
                            vector_values,
                            query,
                            query_norm,
                            threshold,
                            top_k,
                        )
                    })
                })
                .collect::<Vec<_>>();
            handles
                .into_iter()
                .map(|handle| {
                    handle.join().map_err(|_| {
                        Self::execution_io_error(
                            "vector search worker terminated unexpectedly".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })?;

        let mut heap = BinaryHeap::with_capacity(top_k.min(keys.len()));
        for partial in partials {
            for candidate in partial {
                Self::retain_top_k(&mut heap, candidate, top_k);
            }
        }
        let mut results = heap.into_vec();
        results.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        Ok(results)
    }

    fn token_score(text: &str, tokens: &[String]) -> f64 {
        let lower = text.to_lowercase();
        tokens
            .iter()
            .map(|token| lower.matches(token).count() as f64)
            .sum()
    }

    fn execution_client_error(message: String) -> AppError {
        AppError {
            code: "REQUEST_EXECUTION_FAILED".to_string(),
            message,
            failure_class: Some("CLIENT_INPUT".to_string()),
        }
    }

    fn execution_io_error(message: String) -> AppError {
        AppError {
            code: "REQUEST_EXECUTION_FAILED".to_string(),
            message,
            failure_class: Some("IO_FAILURE".to_string()),
        }
    }

    fn unsupported_method_error(method: &str) -> AppError {
        AppError {
            code: "UNSUPPORTED_FEATURE".to_string(),
            message: format!("unsupported_method:{method}"),
            failure_class: Some("CLIENT_INPUT".to_string()),
        }
    }

    /// Build an InMemoryGraphStore from State for Cypher query execution.
    /// Optionally filters by corpus_id.
    fn build_cypher_store(&self, corpus_id: Option<&str>) -> InMemoryGraphStore {
        let mut store = InMemoryGraphStore::new();
        let mut id_map: HashMap<String, String> = HashMap::new();

        for (key, node) in &self.state.nodes {
            if let Some(cid) = corpus_id {
                if node.corpus_id != cid {
                    continue;
                }
            }
            let mut props: Properties = Properties::new();
            props.insert(
                "nodeId".to_string(),
                GraphValue::String(node.node_id.clone()),
            );
            props.insert(
                "corpusId".to_string(),
                GraphValue::String(node.corpus_id.clone()),
            );
            props.insert("layer".to_string(), GraphValue::String(node.layer.clone()));
            if let Some(ref_str) = node.r#ref.as_str() {
                props.insert("ref".to_string(), GraphValue::String(ref_str.to_string()));
            } else {
                props.insert(
                    "ref".to_string(),
                    GraphValue::String(node.r#ref.to_string()),
                );
            }
            let created = store.create_node(vec![node.label.clone()], props);
            id_map.insert(key.clone(), created.id.clone());
        }

        for (_key, edge) in &self.state.edges {
            if let Some(cid) = corpus_id {
                if edge.corpus_id != cid {
                    continue;
                }
            }
            let src_key = Self::key(&edge.corpus_id, &edge.source_node_id);
            let tgt_key = Self::key(&edge.corpus_id, &edge.target_node_id);
            if let (Some(from_id), Some(to_id)) = (id_map.get(&src_key), id_map.get(&tgt_key)) {
                let mut props: Properties = Properties::new();
                props.insert("weight".to_string(), GraphValue::Float64(edge.weight));
                if let Some(ref bk) = edge.bridge_kind {
                    props.insert("bridgeKind".to_string(), GraphValue::String(bk.clone()));
                }
                props.insert(
                    "edgeId".to_string(),
                    GraphValue::String(edge.edge_id.clone()),
                );
                store.create_edge(from_id, to_id, edge.relation.clone(), props);
            }
        }

        store
    }

    fn handle(&mut self, req: RpcRequest) -> RpcResponse {
        let result: Result<Value, AppError> = (|| match req.method.as_str() {
            "ping" => Ok(json!({"pong": true})),
            "batch_begin" => {
                self.batch_mode = true;
                Ok(json!(null))
            }
            "batch_commit" => {
                self.batch_mode = false;
                self.persist().map_err(|err| {
                    Self::execution_io_error(format!("batch_commit persist failed: {err}"))
                })?;
                Ok(json!(null))
            }
            "upsert_nodes" => {
                let nodes = req
                    .params
                    .get("nodes")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                for node in nodes {
                    let parsed = serde_json::from_value::<GraphNode>(node).map_err(|err| {
                        Self::execution_client_error(format!("invalid node: {err}"))
                    })?;
                    self.state
                        .nodes
                        .insert(Self::key(&parsed.corpus_id, &parsed.node_id), parsed);
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "upsert_edges" => {
                let edges = req
                    .params
                    .get("edges")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();
                for edge in edges {
                    let parsed = serde_json::from_value::<GraphEdge>(edge).map_err(|err| {
                        Self::execution_client_error(format!("invalid edge: {err}"))
                    })?;
                    self.state
                        .edges
                        .insert(Self::key(&parsed.corpus_id, &parsed.edge_id), parsed);
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "get_node" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let node_id = req
                    .params
                    .get("nodeId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let node = self
                    .state
                    .nodes
                    .get(&Self::key(corpus_id, node_id))
                    .cloned();
                Ok(serde_json::to_value(node).unwrap_or(Value::Null))
            }
            "get_nodes" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let layer = req.params.get("layer").and_then(Value::as_str);
                self.ensure_cache();
                let mut out: Vec<GraphNode> = self
                    .node_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.nodes.get(key))
                    .filter(|n| layer.is_none_or(|l| n.layer == l))
                    .cloned()
                    .collect();
                out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                Ok(serde_json::to_value(out).unwrap_or(Value::Null))
            }
            "get_edges" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let source_node_id = req.params.get("sourceNodeId").and_then(Value::as_str);
                self.ensure_cache();
                let mut out: Vec<GraphEdge> = self
                    .edge_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.edges.get(key))
                    .filter(|e| source_node_id.is_none_or(|s| e.source_node_id == s))
                    .cloned()
                    .collect();
                out.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
                Ok(serde_json::to_value(out).unwrap_or(Value::Null))
            }
            "get_adjacent" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let node_id = req
                    .params
                    .get("nodeId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.ensure_cache();
                let node_key = Self::node_key(corpus_id, node_id);
                let mut out: Vec<GraphEdge> = self
                    .adjacent_edge_keys_by_node
                    .get(&node_key)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.edges.get(key))
                    .cloned()
                    .collect();
                out.sort_by(|a, b| a.edge_id.cmp(&b.edge_id));
                Ok(serde_json::to_value(out).unwrap_or(Value::Null))
            }
            "delete_nodes" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let node_ids = req
                    .params
                    .get("nodeIds")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut deleted = 0;
                for node_id in node_ids.iter().filter_map(Value::as_str) {
                    if self
                        .state
                        .nodes
                        .remove(&Self::key(corpus_id, node_id))
                        .is_some()
                    {
                        deleted += 1;
                    }
                    self.state.edges.retain(|_, edge| {
                        !(edge.corpus_id == corpus_id
                            && (edge.source_node_id == node_id || edge.target_node_id == node_id))
                    });
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(deleted))
            }
            "delete_edges" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let edge_ids = req
                    .params
                    .get("edgeIds")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let mut deleted = 0;
                for edge_id in edge_ids.iter().filter_map(Value::as_str) {
                    if self
                        .state
                        .edges
                        .remove(&Self::key(corpus_id, edge_id))
                        .is_some()
                    {
                        deleted += 1;
                    }
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(deleted))
            }
            "delete_by_document" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let document_id = req
                    .params
                    .get("documentId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let before_nodes = self.state.nodes.len();
                let before_edges = self.state.edges.len();
                let removable: Vec<String> = self
                    .state
                    .nodes
                    .iter()
                    .filter_map(|(k, node)| {
                        if node.corpus_id != corpus_id {
                            return None;
                        }
                        let docs = Self::doc_ids_from_ref(&node.r#ref);
                        docs.iter().any(|id| id == document_id).then_some(k.clone())
                    })
                    .collect();
                let mut removed_node_ids = Vec::new();
                for key in removable {
                    if let Some(node) = self.state.nodes.remove(&key) {
                        removed_node_ids.push(node.node_id);
                    }
                }
                self.state.edges.retain(|_, edge| {
                    !(edge.corpus_id == corpus_id
                        && (removed_node_ids.iter().any(|id| id == &edge.source_node_id)
                            || removed_node_ids.iter().any(|id| id == &edge.target_node_id)))
                });
                let mut removed_vector_keys = Vec::new();
                self.state.vectors.retain(|key, v| {
                    if v.corpus_id != corpus_id {
                        return true;
                    }
                    let doc = v
                        .metadata
                        .get("documentId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let keep = doc != document_id;
                    if !keep {
                        removed_vector_keys.push(key.clone());
                    }
                    keep
                });
                for key in removed_vector_keys {
                    self.vector_values.remove(&key);
                }
                self.state
                    .passages
                    .retain(|_, p| !(p.corpus_id == corpus_id && p.document_id == document_id));
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!({
                    "deletedNodes": before_nodes.saturating_sub(self.state.nodes.len()),
                    "deletedEdges": before_edges.saturating_sub(self.state.edges.len())
                }))
            }
            "delete_by_corpus" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let before_nodes = self.state.nodes.len();
                let before_edges = self.state.edges.len();
                self.state.nodes.retain(|_, n| n.corpus_id != corpus_id);
                self.state.edges.retain(|_, e| e.corpus_id != corpus_id);
                let mut removed_vector_keys = Vec::new();
                self.state.vectors.retain(|key, v| {
                    let keep = v.corpus_id != corpus_id;
                    if !keep {
                        removed_vector_keys.push(key.clone());
                    }
                    keep
                });
                for key in removed_vector_keys {
                    self.vector_values.remove(&key);
                }
                self.state.passages.retain(|_, p| p.corpus_id != corpus_id);
                self.state.snapshots.remove(corpus_id);
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!({
                    "deletedNodes": before_nodes.saturating_sub(self.state.nodes.len()),
                    "deletedEdges": before_edges.saturating_sub(self.state.edges.len())
                }))
            }
            "vector_upsert" => {
                let records = req
                    .params
                    .get("records")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for record in records {
                    let parsed = serde_json::from_value::<VectorRecord>(record).map_err(|err| {
                        Self::execution_client_error(format!("invalid vector record: {err}"))
                    })?;
                    let key = Self::key(&parsed.corpus_id, &parsed.id);
                    self.vector_values
                        .insert(key.clone(), parsed.values.clone());
                    let mut persisted = parsed.clone();
                    persisted.values.clear();
                    persisted.blob_ref = None;
                    self.state.vectors.insert(key, persisted);
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "vector_search" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let namespace = req
                    .params
                    .get("namespace")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let top_k = req.params.get("topK").and_then(Value::as_u64).unwrap_or(10) as usize;
                let threshold = req.params.get("threshold").and_then(Value::as_f64);
                let query_vec = req
                    .params
                    .get("queryVector")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .filter_map(Value::as_f64)
                    .collect::<Vec<_>>();
                self.ensure_cache();
                let corpus_namespace_key = Self::corpus_namespace_key(corpus_id, namespace);
                let keys = self
                    .vector_keys_by_corpus_namespace
                    .get(&corpus_namespace_key)
                    .map(Vec::as_slice)
                    .unwrap_or(&[]);
                let hits = Self::vector_search_candidates(
                    keys,
                    &self.state.vectors,
                    &self.vector_values,
                    &query_vec,
                    threshold,
                    top_k,
                )?;
                // Only the bounded final result set is converted to JSON.
                // Metadata can be large, so materializing it for every
                // threshold-passing candidate is both unnecessary and costly.
                let out = hits
                    .into_iter()
                    .filter_map(|hit| {
                        self.state.vectors.get(&hit.key).map(|vector| {
                            json!({
                                "id": vector.id,
                                "score": hit.score,
                                "metadata": vector.metadata
                            })
                        })
                    })
                    .collect::<Vec<_>>();
                Ok(json!(out))
            }
            "vector_delete_by_document" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let document_id = req
                    .params
                    .get("documentId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let mut removed_vector_keys = Vec::new();
                self.state.vectors.retain(|key, v| {
                    if v.corpus_id != corpus_id {
                        return true;
                    }
                    let doc = v
                        .metadata
                        .get("documentId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let keep = doc != document_id;
                    if !keep {
                        removed_vector_keys.push(key.clone());
                    }
                    keep
                });
                for key in removed_vector_keys {
                    self.vector_values.remove(&key);
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "memory_save" => {
                let snapshot = req
                    .params
                    .get("snapshot")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let corpus_id = snapshot
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Self::execution_client_error("missing snapshot.corpusId".to_string())
                    })?
                    .to_string();
                self.state.snapshots.insert(corpus_id, snapshot);
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "memory_save_file" => {
                let file_path = req
                    .params
                    .get("filePath")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Self::execution_client_error("missing filePath".to_string()))?;
                let file_content = std::fs::read_to_string(file_path).map_err(|err| {
                    Self::execution_io_error(format!("read file {file_path}: {err}"))
                })?;
                let snapshot: Value = serde_json::from_str(&file_content).map_err(|err| {
                    Self::execution_client_error(format!("parse JSON {file_path}: {err}"))
                })?;
                let corpus_id = snapshot
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Self::execution_client_error("missing snapshot.corpusId".to_string())
                    })?
                    .to_string();
                let facts_count = snapshot
                    .get("facts")
                    .and_then(Value::as_array)
                    .map(|a| a.len())
                    .unwrap_or(0);
                let passages_count = snapshot
                    .get("passages")
                    .and_then(Value::as_array)
                    .map(|a| a.len())
                    .unwrap_or(0);
                eprintln!(
                    "[memory_save_file] Loading {file_path}: {facts_count} facts, {passages_count} passages"
                );
                self.state.snapshots.insert(corpus_id, snapshot);
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!({ "facts": facts_count, "passages": passages_count }))
            }
            "memory_load" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let snapshot = self
                    .state
                    .snapshots
                    .get(corpus_id)
                    .cloned()
                    .unwrap_or_else(|| {
                        json!({
                            "corpusId": corpus_id,
                            "exportedAt": "",
                            "schemas": [],
                            "facts": [],
                            "passages": [],
                            "schemaVersion": 1
                        })
                    });
                Ok(snapshot)
            }
            "memory_save_checkpoint" => {
                let checkpoint = req
                    .params
                    .get("checkpoint")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let job_id = checkpoint
                    .get("jobId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Self::execution_client_error("missing checkpoint.jobId".to_string())
                    })?
                    .to_string();
                self.state.checkpoints.insert(job_id, checkpoint);
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "memory_load_checkpoint" => {
                let job_id = req
                    .params
                    .get("jobId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let checkpoint = self
                    .state
                    .checkpoints
                    .get(job_id)
                    .cloned()
                    .unwrap_or(Value::Null);
                Ok(checkpoint)
            }
            "memory_validate_integrity" => Ok(json!([])),
            "projection_get_transitions" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.ensure_cache();
                let mut out: Vec<Value> = self
                    .edge_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.edges.get(key))
                    .map(|e| {
                        json!({
                            "sourceNodeId": e.source_node_id,
                            "targetNodeId": e.target_node_id,
                            "weight": e.weight
                        })
                    })
                    .collect();
                out.sort_by(|a, b| {
                    let ak = a
                        .get("sourceNodeId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let bk = b
                        .get("sourceNodeId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    ak.cmp(bk)
                });
                Ok(json!(out))
            }
            "projection_get_dangling_nodes" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.ensure_cache();
                let mut outgoing: HashMap<String, usize> = HashMap::new();
                for edge in self
                    .edge_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.edges.get(key))
                {
                    *outgoing.entry(edge.source_node_id.clone()).or_default() += 1;
                }
                let mut dangling: Vec<String> = self
                    .node_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.nodes.get(key))
                    .filter(|n| !outgoing.contains_key(&n.node_id))
                    .map(|n| n.node_id.clone())
                    .collect();
                dangling.sort();
                Ok(json!(dangling))
            }
            "projection_get_node_count" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.ensure_cache();
                let count = self
                    .node_keys_by_corpus
                    .get(corpus_id)
                    .map(|keys| keys.len())
                    .unwrap_or(0);
                Ok(json!(count))
            }
            "lexical_index_passages" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let passages = req
                    .params
                    .get("passages")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for passage in passages {
                    let passage_id = passage
                        .get("passageId")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            Self::execution_client_error("missing passageId".to_string())
                        })?;
                    let document_id = passage
                        .get("metadata")
                        .and_then(|m| m.get("documentId"))
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            Self::execution_client_error("missing metadata.documentId".to_string())
                        })?;
                    let text = passage
                        .get("text")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let item = Passage {
                        passage_id: passage_id.to_string(),
                        corpus_id: corpus_id.to_string(),
                        document_id: document_id.to_string(),
                        text: text.to_string(),
                    };
                    self.state
                        .passages
                        .insert(Self::key(corpus_id, passage_id), item);
                }
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "lexical_search" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let query = req
                    .params
                    .get("query")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let top_k = req.params.get("topK").and_then(Value::as_u64).unwrap_or(10) as usize;
                let tokens: Vec<String> = query
                    .to_lowercase()
                    .split_whitespace()
                    .map(ToString::to_string)
                    .collect();
                self.ensure_cache();
                let mut out: Vec<Value> = self
                    .passage_keys_by_corpus
                    .get(corpus_id)
                    .into_iter()
                    .flat_map(|keys| keys.iter())
                    .filter_map(|key| self.state.passages.get(key))
                    .map(|p| {
                        json!({
                            "passageId": p.passage_id,
                            "score": Self::token_score(&p.text, &tokens)
                        })
                    })
                    .filter(|v| v.get("score").and_then(Value::as_f64).unwrap_or(0.0) > 0.0)
                    .collect();
                out.sort_by(|a, b| {
                    let sa = a.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                    let sb = b.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                    match sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal) {
                        std::cmp::Ordering::Equal => {
                            let aid = a
                                .get("passageId")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            let bid = b
                                .get("passageId")
                                .and_then(Value::as_str)
                                .unwrap_or_default();
                            aid.cmp(bid)
                        }
                        other => other,
                    }
                });
                out.truncate(top_k);
                Ok(json!(out))
            }
            "lexical_delete_by_document" => {
                let corpus_id = req
                    .params
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                let document_id = req
                    .params
                    .get("documentId")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                self.state
                    .passages
                    .retain(|_, p| !(p.corpus_id == corpus_id && p.document_id == document_id));
                self.mark_cache_dirty();
                self.persist_if_needed()
                    .map_err(|err| Self::execution_io_error(format!("persist failed: {err}")))?;
                Ok(json!(null))
            }
            "cypher_query" => {
                let query_str = req
                    .params
                    .get("query")
                    .and_then(Value::as_str)
                    .ok_or_else(|| Self::execution_client_error("missing query".to_string()))?;
                let corpus_id = req.params.get("corpusId").and_then(Value::as_str);
                let dialect_str = req
                    .params
                    .get("dialect")
                    .and_then(Value::as_str)
                    .unwrap_or("openCypher9");
                let dialect = match dialect_str {
                    "neo4jCompat" | "Neo4jCompat" => CypherDialect::Neo4jCompat,
                    _ => CypherDialect::OpenCypher9,
                };
                let mut store = self.build_cypher_store(corpus_id);
                let result =
                    execute_query_with_dialect(&mut store, query_str, dialect).map_err(|err| {
                        Self::execution_client_error(format!("cypher error: {err:?}"))
                    })?;
                Ok(serde_json::to_value(result).unwrap_or(Value::Null))
            }
            "__debug_force_panic__" => {
                if std::env::var("AGDB_ENABLE_TEST_CRASH").ok().as_deref() == Some("1") {
                    panic!("forced panic for crash audit");
                }
                Err(Self::unsupported_method_error(&req.method))
            }
            _ => Err(Self::unsupported_method_error(&req.method)),
        })();

        match result {
            Ok(value) => RpcResponse {
                id: req.id,
                ok: true,
                result: Some(value),
                error: None,
            },
            Err(err) => {
                let code = err.code.clone();
                let failure_class = err
                    .failure_class
                    .clone()
                    .unwrap_or_else(|| "INTERNAL_BUG".to_string());
                self.append_request_audit_event(&code, &failure_class, &req.id.to_string());
                RpcResponse {
                    id: req.id,
                    ok: false,
                    result: None,
                    error: Some(RpcError {
                        code: err.code,
                        message: err.message,
                        failure_class: err.failure_class,
                    }),
                }
            }
        }
    }
}

fn main() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut db_path = PathBuf::from("aira-graphdb-native.json");
    while let Some(arg) = args.next() {
        if arg == "--db" {
            if let Some(v) = args.next() {
                db_path = PathBuf::from(v);
            }
        }
    }

    let crash_tracker = CrashTracker::new(db_path.with_extension("native-audit.log"));
    let tracker_for_hook = crash_tracker.clone();
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        tracker_for_hook.append_crash_event(Some(101), None, Some(panic_info.to_string()));
        previous_hook(panic_info);
    }));

    let mut server = Server::open(db_path)?;
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    for line_result in stdin.lock().lines() {
        let line = match line_result {
            Ok(line) => line,
            Err(err) => {
                crash_tracker.append_crash_event(
                    Some(1),
                    None,
                    Some(format!("stdin read failed: {err}")),
                );
                return Err(err);
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let req = match serde_json::from_str::<RpcRequest>(&line) {
            Ok(req) => req,
            Err(err) => {
                let _ = Server::append_request_audit_event_for_path(
                    &server.audit_log_path,
                    "INVALID_REQUEST_JSON",
                    "CLIENT_INPUT",
                    "0",
                );
                let payload = serde_json::to_string(&RpcResponse {
                    id: 0,
                    ok: false,
                    result: None,
                    error: Some(RpcError {
                        code: "INVALID_REQUEST_JSON".to_string(),
                        message: format!("invalid request: {err}"),
                        failure_class: Some("CLIENT_INPUT".to_string()),
                    }),
                })
                .unwrap_or_else(|_| "{\"id\":0,\"ok\":false}".to_string());
                if let Err(write_err) = stdout
                    .write_all(payload.as_bytes())
                    .and_then(|_| stdout.write_all(b"\n"))
                    .and_then(|_| stdout.flush())
                {
                    crash_tracker.append_crash_event(
                        Some(1),
                        None,
                        Some(format!(
                            "stdout write failed after invalid request: {write_err}"
                        )),
                    );
                    return Err(write_err);
                }
                continue;
            }
        };
        crash_tracker.set_last_request_id(req.id.to_string());
        let resp = server.handle(req);
        let payload = serde_json::to_string(&resp)
            .map_err(|err| io::Error::other(format!("serialize response failed: {err}")))?;
        if let Err(write_err) = stdout
            .write_all(payload.as_bytes())
            .and_then(|_| stdout.write_all(b"\n"))
            .and_then(|_| stdout.flush())
        {
            crash_tracker.append_crash_event(
                Some(1),
                None,
                Some(format!("stdout write failed: {write_err}")),
            );
            return Err(write_err);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time ok")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{nanos}.json"))
    }

    fn cleanup(path: &PathBuf) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("vblob"));
        let _ = fs::remove_file(path.with_extension("native-audit.log"));
    }

    fn vector_record(id: &str, corpus_id: &str, namespace: &str) -> VectorRecord {
        VectorRecord {
            id: id.to_string(),
            corpus_id: corpus_id.to_string(),
            namespace: namespace.to_string(),
            values: Vec::new(),
            blob_ref: None,
            metadata: json!({"id": id}),
        }
    }

    fn reference_cosine(query: &[f64], vector: &[f64]) -> f64 {
        if query.is_empty() || vector.is_empty() || query.len() != vector.len() {
            return 0.0;
        }
        let mut dot = 0.0;
        let mut query_norm = 0.0;
        let mut vector_norm = 0.0;
        for index in 0..query.len() {
            dot += query[index] * vector[index];
            query_norm += query[index] * query[index];
            vector_norm += vector[index] * vector[index];
        }
        if query_norm == 0.0 || vector_norm == 0.0 {
            return 0.0;
        }
        dot / (query_norm.sqrt() * vector_norm.sqrt())
    }

    #[test]
    fn exact_vector_search_matches_cosine_and_has_deterministic_ties() {
        let query = [1.0, 0.0];
        let records = [
            ("tie-b", vec![0.8, 0.6]),
            ("low", vec![0.1, 0.995]),
            ("tie-a", vec![0.8, 0.6]),
            ("high", vec![1.0, 0.0]),
            ("zero", vec![0.0, 0.0]),
            ("wrong-dimension", vec![1.0, 0.0, 0.0]),
        ];
        let mut vectors = HashMap::new();
        let mut vector_values = HashMap::new();
        let mut keys = Vec::new();
        for (id, values) in records {
            let key = Server::key("corpus", id);
            keys.push(key.clone());
            vectors.insert(key.clone(), vector_record(id, "corpus", "default"));
            vector_values.insert(key, values);
        }

        let all_hits =
            Server::vector_search_candidates(&keys, &vectors, &vector_values, &query, None, 10)
                .expect("full exact scan succeeds");
        for hit in &all_hits {
            let values = vector_values.get(&hit.key).expect("vector values exist");
            assert_eq!(
                hit.score.to_bits(),
                reference_cosine(&query, values).to_bits(),
                "score changed for {}",
                hit.key
            );
        }

        let hits =
            Server::vector_search_candidates(&keys, &vectors, &vector_values, &query, None, 3)
                .expect("exact scan succeeds");
        let ids = hits.iter().map(|hit| hit.key.clone()).collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                Server::key("corpus", "high"),
                Server::key("corpus", "tie-a"),
                Server::key("corpus", "tie-b"),
            ]
        );
        let thresholded = Server::vector_search_candidates(
            &keys,
            &vectors,
            &vector_values,
            &query,
            Some(0.8),
            10,
        )
        .expect("thresholded exact scan succeeds");
        assert!(thresholded.iter().all(|hit| hit.score >= 0.8));
        assert_eq!(thresholded.len(), 3);
    }

    #[test]
    fn exact_vector_search_handles_zero_invalid_dimension_and_top_k_boundaries() {
        let query = [0.0, 0.0];
        let key_zero = Server::key("corpus", "zero");
        let key_wrong = Server::key("corpus", "wrong-dimension");
        let key_missing = Server::key("corpus", "missing-values");
        let keys = vec![key_zero.clone(), key_wrong.clone(), key_missing.clone()];
        let vectors = [
            (key_zero.clone(), vector_record("zero", "corpus", "default")),
            (
                key_wrong.clone(),
                vector_record("wrong-dimension", "corpus", "default"),
            ),
            (
                key_missing.clone(),
                vector_record("missing-values", "corpus", "default"),
            ),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();
        let vector_values = [
            (key_zero.clone(), vec![0.0, 0.0]),
            (key_wrong.clone(), vec![1.0]),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let all =
            Server::vector_search_candidates(&keys, &vectors, &vector_values, &query, None, 10)
                .expect("zero query is safe");
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|hit| hit.score == 0.0));
        assert_eq!(
            all.iter().map(|hit| hit.key.clone()).collect::<Vec<_>>(),
            vec![key_missing.clone(), key_wrong.clone(), key_zero.clone()]
        );
        assert!(
            Server::vector_search_candidates(&keys, &vectors, &vector_values, &query, None, 0)
                .expect("topK zero is safe")
                .is_empty()
        );
    }

    #[test]
    fn production_shaped_exact_scan_is_bounded_to_top_k() {
        // This is deliberately scaled down from production's ~420k x 1024
        // candidates while retaining its 1024-dimensional exact-scan shape.
        // The copied real-snapshot benchmark is run outside the unit suite.
        let dimensions = 1024;
        let candidate_count = 16_384;
        let query = (0..dimensions)
            .map(|index| if index == 0 { 1.0 } else { 0.001 })
            .collect::<Vec<_>>();
        let mut keys = Vec::with_capacity(candidate_count);
        let mut vectors = HashMap::with_capacity(candidate_count);
        let mut vector_values = HashMap::with_capacity(candidate_count);
        for index in 0..candidate_count {
            let id = format!("passage-{index:05}");
            let key = Server::key("libfull", &id);
            let values = (0..dimensions)
                .map(|dimension| {
                    if dimension == index % dimensions {
                        1.0
                    } else {
                        0.001
                    }
                })
                .collect::<Vec<_>>();
            keys.push(key.clone());
            vectors.insert(key.clone(), vector_record(&id, "libfull", "passage"));
            vector_values.insert(key, values);
        }

        let started = std::time::Instant::now();
        let hits =
            Server::vector_search_candidates(&keys, &vectors, &vector_values, &query, None, 25)
                .expect("production-shaped exact scan succeeds");
        let elapsed = started.elapsed();
        assert_eq!(hits.len(), 25);
        assert!(hits.windows(2).all(|pair| {
            pair[0].score > pair[1].score
                || (pair[0].score == pair[1].score && pair[0].key < pair[1].key)
        }));
        eprintln!(
            "production-shaped vector_search: candidates={candidate_count} dimensions={dimensions} topK=25 elapsed_ms={}",
            elapsed.as_millis()
        );
    }

    #[test]
    fn persists_vector_values_in_blob_file() {
        let path = temp_path("agdb-native-vblob");
        let mut server = Server::open(path.clone()).expect("open server");
        let req = RpcRequest {
            id: 1,
            method: "vector_upsert".to_string(),
            params: json!({
                "records": [
                    {
                        "id": "vec-1",
                        "corpusId": "c1",
                        "namespace": "default",
                        "values": [1.0, 0.0, 0.5],
                        "metadata": {"documentId":"d1"}
                    }
                ]
            }),
        };
        let resp = server.handle(req);
        assert!(resp.ok);

        let db_raw = fs::read_to_string(&path).expect("read db");
        assert!(db_raw.contains("\"blobRef\""));
        assert!(!db_raw.contains("[1.0,0.0,0.5]"));

        let blob = fs::read(path.with_extension("vblob")).expect("read blob");
        assert!(blob.starts_with(Server::VECTOR_BLOB_MAGIC));

        let mut reopened = Server::open(path.clone()).expect("reopen");
        let search = reopened.handle(RpcRequest {
            id: 2,
            method: "vector_search".to_string(),
            params: json!({
                "corpusId":"c1",
                "namespace":"default",
                "queryVector":[1.0,0.0,0.5],
                "topK": 1
            }),
        });
        assert!(search.ok);
        let result = search.result.expect("result array");
        let first_id = result
            .as_array()
            .and_then(|a| a.first())
            .and_then(|v| v.get("id"))
            .and_then(Value::as_str);
        assert_eq!(first_id, Some("vec-1"));

        cleanup(&path);
    }

    #[test]
    fn migrates_legacy_inline_vector_values_to_blob_on_persist() {
        let path = temp_path("agdb-native-vblob-legacy");
        let legacy = json!({
            "nodes": {},
            "edges": {},
            "vectors": {
                "c1:vec-legacy": {
                    "id": "vec-legacy",
                    "corpusId": "c1",
                    "namespace": "default",
                    "values": [0.5, 0.5],
                    "metadata": {"documentId":"d2"}
                }
            },
            "passages": {},
            "snapshots": {},
            "checkpoints": {}
        });
        fs::write(&path, legacy.to_string()).expect("write legacy state");

        let mut server = Server::open(path.clone()).expect("open legacy");
        let search = server.handle(RpcRequest {
            id: 1,
            method: "vector_search".to_string(),
            params: json!({
                "corpusId":"c1",
                "namespace":"default",
                "queryVector":[0.5,0.5],
                "topK": 1
            }),
        });
        assert!(search.ok);
        server.persist().expect("persist migrated");

        let db_raw = fs::read_to_string(&path).expect("read migrated db");
        assert!(db_raw.contains("\"blobRef\""));
        assert!(!db_raw.contains("\"values\":[0.5,0.5]"));
        let blob = fs::read(path.with_extension("vblob")).expect("read migrated blob");
        assert!(blob.starts_with(Server::VECTOR_BLOB_MAGIC));

        let mut reopened = Server::open(path.clone()).expect("reopen migrated");
        let search_again = reopened.handle(RpcRequest {
            id: 2,
            method: "vector_search".to_string(),
            params: json!({
                "corpusId":"c1",
                "namespace":"default",
                "queryVector":[0.5,0.5],
                "topK": 1
            }),
        });
        assert!(search_again.ok);

        cleanup(&path);
    }
}
