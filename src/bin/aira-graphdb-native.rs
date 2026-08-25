use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fs;
use std::io::{self, BufRead, BufWriter, Read, Seek, SeekFrom, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::fd::{AsRawFd, FromRawFd};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(target_os = "linux")]
use std::{ffi::CString, os::unix::ffi::OsStrExt};

use serde::de::IgnoredAny;
use serde::ser::{SerializeMap, SerializeStruct};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use aira_graphdb::graph::{InMemoryGraphStore, Properties, Value as GraphValue};
use aira_graphdb::native_persistence_contract::{
    COMMIT_EVIDENCE_SCHEMA, CommitEvidence, JSON_SAFE_INTEGER_MAX, NativeProgressFrame,
    NativeProgressPolicy, PreparedCommitEvidence, ProgressCounters,
};
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct VectorBlobDescriptor {
    basename: String,
    size: u64,
    sha256: String,
    format: u16,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredCommitEvidence {
    schema: String,
    transaction_nonce: String,
    base_generation: u64,
    generation: u64,
    wal_sha256: String,
    wal_bytes: u64,
    wal_record_count: u64,
}

impl StoredCommitEvidence {
    fn from_validated(evidence: &CommitEvidence) -> Self {
        Self {
            schema: COMMIT_EVIDENCE_SCHEMA.to_string(),
            transaction_nonce: evidence.transaction_nonce().to_string(),
            base_generation: evidence.base_generation(),
            generation: evidence.generation(),
            wal_sha256: evidence.wal_sha256().to_string(),
            wal_bytes: evidence.wal_bytes(),
            wal_record_count: evidence.wal_record_count(),
        }
    }

    fn validate(&self) -> Result<CommitEvidence, String> {
        if self.schema != COMMIT_EVIDENCE_SCHEMA {
            return Err(format!("expected schema {COMMIT_EVIDENCE_SCHEMA}"));
        }
        let evidence = PreparedCommitEvidence::new(
            self.transaction_nonce.clone(),
            self.base_generation,
            self.generation,
            self.wal_sha256.clone(),
            self.wal_bytes,
            self.wal_record_count,
        )?
        .commit_evidence();
        let encoded = serde_json::to_vec(self).map_err(|error| error.to_string())?;
        if evidence.canonical_bytes() != encoded {
            return Err("stored commit evidence is not canonical JSON".to_string());
        }
        Ok(evidence)
    }
}

impl Serialize for StoredCommitEvidence {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("CommitEvidence", 7)?;
        state.serialize_field("schema", &self.schema)?;
        state.serialize_field("transactionNonce", &self.transaction_nonce)?;
        state.serialize_field("baseGeneration", &self.base_generation)?;
        state.serialize_field("generation", &self.generation)?;
        state.serialize_field("walSha256", &self.wal_sha256)?;
        state.serialize_field("walBytes", &self.wal_bytes)?;
        state.serialize_field("walRecordCount", &self.wal_record_count)?;
        state.end()
    }
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
    #[serde(default)]
    generation: u64,
    #[serde(
        default,
        rename = "vectorBlob",
        skip_serializing_if = "Option::is_none"
    )]
    vector_blob: Option<VectorBlobDescriptor>,
    #[serde(
        default,
        rename = "commitEvidence",
        skip_serializing_if = "Option::is_none"
    )]
    commit_evidence: Option<StoredCommitEvidence>,
}

struct SortedMapView<'a, T>(&'a HashMap<String, T>);

impl<T: Serialize> Serialize for SortedMapView<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut keys: Vec<&String> = self.0.keys().collect();
        keys.sort_unstable();
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for key in keys {
            map.serialize_entry(key, &self.0[key])?;
        }
        map.end()
    }
}

struct PersistedVectorView<'a> {
    vector: &'a VectorRecord,
    blob_ref: &'a VectorBlobRef,
}

impl Serialize for PersistedVectorView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("VectorRecord", 5)?;
        state.serialize_field("id", &self.vector.id)?;
        state.serialize_field("corpusId", &self.vector.corpus_id)?;
        state.serialize_field("namespace", &self.vector.namespace)?;
        state.serialize_field("blobRef", self.blob_ref)?;
        state.serialize_field("metadata", &self.vector.metadata)?;
        state.end()
    }
}

struct PersistedVectorMapView<'a> {
    vectors: &'a HashMap<String, VectorRecord>,
    refs: &'a HashMap<String, VectorBlobRef>,
}

impl Serialize for PersistedVectorMapView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut keys: Vec<&String> = self.vectors.keys().collect();
        keys.sort_unstable();
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for key in keys {
            let blob_ref = self
                .refs
                .get(key)
                .ok_or_else(|| serde::ser::Error::custom("missing prepared vector reference"))?;
            map.serialize_entry(
                key,
                &PersistedVectorView {
                    vector: &self.vectors[key],
                    blob_ref,
                },
            )?;
        }
        map.end()
    }
}

struct PersistedStateView<'a> {
    state: &'a State,
    generation: u64,
    vector_blob: &'a VectorBlobDescriptor,
    vector_refs: &'a HashMap<String, VectorBlobRef>,
    commit_evidence: &'a StoredCommitEvidence,
}

impl Serialize for PersistedStateView<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut state = serializer.serialize_struct("State", 9)?;
        state.serialize_field("nodes", &SortedMapView(&self.state.nodes))?;
        state.serialize_field("edges", &SortedMapView(&self.state.edges))?;
        state.serialize_field(
            "vectors",
            &PersistedVectorMapView {
                vectors: &self.state.vectors,
                refs: self.vector_refs,
            },
        )?;
        state.serialize_field("passages", &SortedMapView(&self.state.passages))?;
        state.serialize_field("snapshots", &SortedMapView(&self.state.snapshots))?;
        state.serialize_field("checkpoints", &SortedMapView(&self.state.checkpoints))?;
        state.serialize_field("generation", &self.generation)?;
        state.serialize_field("vectorBlob", self.vector_blob)?;
        state.serialize_field("commitEvidence", self.commit_evidence)?;
        state.end()
    }
}

struct ArtifactHashWriter<W> {
    inner: W,
    bytes: u64,
    hasher: Sha256,
}

struct ArtifactEvidence {
    bytes: u64,
    sha256: String,
}

const STREAM_PUBLICATION_BUFFER_BYTES: usize = 1024 * 1024;
const STREAM_PUBLICATION_CACHE_WINDOW_BYTES: u64 = 64 * 1024 * 1024;

// Buffer above the hasher so tiny encoder writes are coalesced before both
// hashing and File I/O. `finish` is the only path that can return evidence.
struct BufferedArtifactWriter<W: Write> {
    inner: BufWriter<ArtifactHashWriter<W>>,
}

struct BoundedPublicationWriter<'a> {
    file: &'a mut fs::File,
    written_bytes: u64,
    released_bytes: u64,
    window_bytes: u64,
    stage: &'static str,
    release_range: fn(&fs::File, u64, u64, &str) -> io::Result<()>,
}

struct ProgressWrite<'a, W> {
    inner: W,
    progress: &'a mut dyn CommitProgress,
    bytes: u64,
    reported_bytes: u64,
}

impl<'a, W> ProgressWrite<'a, W> {
    fn new(inner: W, progress: &'a mut dyn CommitProgress) -> Self {
        Self {
            inner,
            progress,
            bytes: 0,
            reported_bytes: 0,
        }
    }

    fn finish(self) -> (W, u64) {
        (self.inner, self.bytes)
    }
}

impl<W: Write> Write for ProgressWrite<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("progress byte count overflow"))?;
        if self.bytes.saturating_sub(self.reported_bytes) >= 64 * 1024 {
            self.progress.advance(0, None, self.bytes, None)?;
            self.reported_bytes = self.bytes;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()?;
        if self.bytes != self.reported_bytes {
            self.progress.advance(0, None, self.bytes, None)?;
            self.reported_bytes = self.bytes;
        }
        Ok(())
    }
}

impl<W> ArtifactHashWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            bytes: 0,
            hasher: Sha256::new(),
        }
    }

    fn finish(mut self) -> io::Result<ArtifactEvidence>
    where
        W: Write,
    {
        self.inner.flush()?;
        Ok(self.into_evidence())
    }

    fn into_evidence(self) -> ArtifactEvidence {
        let digest = self.hasher.finalize();
        ArtifactEvidence {
            bytes: self.bytes,
            sha256: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
        }
    }
}

impl<W: Write> Write for ArtifactHashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(bytes)?;
        self.hasher.update(&bytes[..written]);
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("artifact byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> BufferedArtifactWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::with_capacity(
                STREAM_PUBLICATION_BUFFER_BYTES,
                ArtifactHashWriter::new(inner),
            ),
        }
    }

    fn finish(mut self) -> io::Result<ArtifactEvidence> {
        self.inner.flush()?;
        let (writer, buffered) = self.inner.into_parts();
        let buffered = buffered
            .map_err(|_| io::Error::other("artifact writer panicked with buffered bytes"))?;
        if !buffered.is_empty() {
            return Err(io::Error::other(
                "artifact writer retained bytes after successful flush",
            ));
        }
        Ok(writer.into_evidence())
    }
}

impl<W: Write> Write for BufferedArtifactWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn buffered_artifact_writer<W: Write>(inner: W) -> BufferedArtifactWriter<W> {
    BufferedArtifactWriter::new(inner)
}

impl<'a> BoundedPublicationWriter<'a> {
    fn new(file: &'a mut fs::File, stage: &'static str) -> Self {
        Self::with_window(file, stage, STREAM_PUBLICATION_CACHE_WINDOW_BYTES)
    }

    fn with_window(file: &'a mut fs::File, stage: &'static str, window_bytes: u64) -> Self {
        Self::with_window_and_release(file, stage, window_bytes, Server::writeback_and_evict_range)
    }

    fn with_window_and_release(
        file: &'a mut fs::File,
        stage: &'static str,
        window_bytes: u64,
        release_range: fn(&fs::File, u64, u64, &str) -> io::Result<()>,
    ) -> Self {
        assert!(
            window_bytes > 0,
            "publication cache window must be positive"
        );
        Self {
            file,
            written_bytes: 0,
            released_bytes: 0,
            window_bytes,
            stage,
            release_range,
        }
    }

    fn release_completed_windows(&mut self) -> io::Result<()> {
        while self.written_bytes.saturating_sub(self.released_bytes) >= self.window_bytes {
            (self.release_range)(
                self.file,
                self.released_bytes,
                self.window_bytes,
                self.stage,
            )?;
            self.released_bytes = self
                .released_bytes
                .checked_add(self.window_bytes)
                .ok_or_else(|| io::Error::other("publication cache offset overflow"))?;
        }
        Ok(())
    }
}

impl Write for BoundedPublicationWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let written = self.file.write(bytes)?;
        self.written_bytes = self
            .written_bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("publication byte offset overflow"))?;
        self.release_completed_windows()?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

fn bounded_publication_writer<'a>(
    file: &'a mut fs::File,
    stage: &'static str,
) -> BoundedPublicationWriter<'a> {
    BoundedPublicationWriter::new(file, stage)
}

const MAX_VECTOR_SEARCH_WORKERS: usize = 8;
const MAX_VECTOR_DIMENSIONS: usize = 4_096;
const MAX_VECTOR_SEARCH_TOP_K: usize = 10_000;
const MAX_WAL_RECORD_BYTES: u64 = 536_870_912;
const MAX_WAL_BYTES: u64 = 17_179_869_184;
const MAX_WAL_RECORDS: u64 = 1_000_000;

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

/// The heap root is the worst retained candidate: lower score first, then
/// lexicographically larger key.  This keeps the exact search bounded and
/// makes ties independent of HashMap iteration order.
impl Ord for ScoredVector {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        other
            .score
            .total_cmp(&self.score)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for ScoredVector {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RpcRequest {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IncomingRpcRequest {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
    #[serde(default)]
    progress_protocol_version: Option<u64>,
}

impl IncomingRpcRequest {
    fn into_rpc(self) -> RpcRequest {
        RpcRequest {
            id: self.id,
            method: self.method,
            params: self.params,
        }
    }
}

#[cfg(test)]
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct WalRecord {
    version: u16,
    base_generation: u64,
    request: RpcRequest,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WalRecordRef<'a> {
    version: u16,
    base_generation: u64,
    request: &'a RpcRequest,
}

struct LimitedHashWriter<W> {
    inner: W,
    bytes: u64,
    limit: u64,
    record_hasher: Sha256,
    wal_hasher: Option<Sha256>,
}

struct LimitedCountingWriter {
    bytes: u64,
    limit: u64,
}

struct WalWriteEvidence<W> {
    inner: W,
    bytes: u64,
    record_digest: [u8; 32],
    wal_hasher: Option<Sha256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalRecordReadEvidence {
    bytes: u64,
    digest: [u8; 32],
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ScannedWalEnvelope {
    #[serde(default, deserialize_with = "deserialize_present")]
    version: Present<u16>,
    #[serde(default, deserialize_with = "deserialize_present")]
    base_generation: Present<u64>,
    #[serde(default, deserialize_with = "deserialize_present")]
    request: Present<ScannedRpcRequest>,
    #[serde(default, deserialize_with = "deserialize_present")]
    id: Present<u64>,
    #[serde(default, deserialize_with = "deserialize_present")]
    method: Present<String>,
    #[serde(default, deserialize_with = "deserialize_present")]
    params: Present<IgnoredAny>,
}

#[derive(Debug, Default)]
enum Present<T> {
    #[default]
    Missing,
    Value(T),
}

fn deserialize_present<'de, D, T>(deserializer: D) -> Result<Present<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Present::Value)
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ScannedRpcRequest {
    id: u64,
    method: String,
    params: IgnoredAny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WalScanEvidence {
    identity: (u64, u64),
    bytes: u64,
    record_count: u64,
    digest: [u8; 32],
    base_generation: Option<u64>,
    legacy_raw: bool,
}

struct StrictLfRecordReader<'a, R: BufRead> {
    inner: &'a mut R,
    limit: u64,
    bytes: u64,
    payload_bytes: u64,
    record_hasher: Sha256,
    wal_hasher: &'a mut Sha256,
    finished: bool,
}

impl<'a, R: BufRead> StrictLfRecordReader<'a, R> {
    fn new(inner: &'a mut R, limit: u64, wal_hasher: &'a mut Sha256) -> Self {
        Self {
            inner,
            limit,
            bytes: 0,
            payload_bytes: 0,
            record_hasher: Sha256::new(),
            wal_hasher,
            finished: false,
        }
    }

    fn account(&mut self, bytes: &[u8], payload: bool) -> io::Result<()> {
        let added = u64::try_from(bytes.len())
            .map_err(|_| io::Error::other("WAL read length does not fit u64"))?;
        let next = self
            .bytes
            .checked_add(added)
            .ok_or_else(|| io::Error::other("WAL record byte count overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(format!(
                "WAL record exceeds {} bytes",
                self.limit
            )));
        }
        self.record_hasher.update(bytes);
        self.wal_hasher.update(bytes);
        self.bytes = next;
        if payload {
            self.payload_bytes = self
                .payload_bytes
                .checked_add(added)
                .ok_or_else(|| io::Error::other("WAL payload byte count overflow"))?;
        }
        Ok(())
    }

    fn finish(self) -> io::Result<WalRecordReadEvidence> {
        if !self.finished {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "WAL final record is missing LF",
            ));
        }
        if self.payload_bytes == 0 {
            return Err(io::Error::other("WAL contains a blank record"));
        }
        Ok(WalRecordReadEvidence {
            bytes: self.bytes,
            digest: self.record_hasher.finalize().into(),
        })
    }
}

impl<R: BufRead> Read for StrictLfRecordReader<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() || self.finished {
            return Ok(0);
        }
        let available = self.inner.fill_buf()?;
        if available.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "WAL final record is missing LF",
            ));
        }
        let before_lf = available
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap_or(available.len());
        if before_lf == 0 {
            self.account(b"\n", false)?;
            self.inner.consume(1);
            self.finished = true;
            return Ok(0);
        }
        let consumed = before_lf.min(output.len());
        output[..consumed].copy_from_slice(&available[..consumed]);
        let added = u64::try_from(consumed)
            .map_err(|_| io::Error::other("WAL read length does not fit u64"))?;
        let next = self
            .bytes
            .checked_add(added)
            .ok_or_else(|| io::Error::other("WAL record byte count overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(format!(
                "WAL record exceeds {} bytes",
                self.limit
            )));
        }
        self.record_hasher.update(&available[..consumed]);
        self.wal_hasher.update(&available[..consumed]);
        self.bytes = next;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(added)
            .ok_or_else(|| io::Error::other("WAL payload byte count overflow"))?;
        self.inner.consume(consumed);
        Ok(consumed)
    }
}

struct ProgressRead<'a, R> {
    inner: R,
    progress: &'a mut dyn CommitProgress,
    completed_units: u64,
    completed_bytes: u64,
    total_bytes: u64,
}

impl<'a, R> ProgressRead<'a, R> {
    fn new(
        inner: R,
        progress: &'a mut dyn CommitProgress,
        completed_units: u64,
        completed_bytes: u64,
        total_bytes: u64,
    ) -> Self {
        Self {
            inner,
            progress,
            completed_units,
            completed_bytes,
            total_bytes,
        }
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for ProgressRead<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(output)?;
        self.completed_bytes = self
            .completed_bytes
            .checked_add(read as u64)
            .ok_or_else(|| io::Error::other("WAL progress byte count overflow"))?;
        self.progress.advance(
            self.completed_units,
            None,
            self.completed_bytes,
            Some(self.total_bytes),
        )?;
        Ok(read)
    }
}

#[derive(Debug)]
struct JsonGuardFrame {
    object: bool,
    expect_key: bool,
    pending_method: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BoundedStringRole {
    Other,
    EnvelopeKey,
    MethodValue,
}

struct GuardedJsonReader<R> {
    inner: R,
    frames: Vec<JsonGuardFrame>,
    in_string: bool,
    escaped: bool,
    role: BoundedStringRole,
    bounded_raw: Vec<u8>,
}

impl<R> GuardedJsonReader<R> {
    const MAX_NESTING: usize = 128;
    const MAX_ENVELOPE_STRING_BYTES: usize = 64;
    const MAX_ESCAPED_ENVELOPE_BYTES: usize = Self::MAX_ENVELOPE_STRING_BYTES * 6;

    fn new(inner: R) -> Self {
        Self {
            inner,
            frames: Vec::new(),
            in_string: false,
            escaped: false,
            role: BoundedStringRole::Other,
            bounded_raw: Vec::with_capacity(Self::MAX_ESCAPED_ENVELOPE_BYTES),
        }
    }

    fn finish_bounded_string(&mut self) -> io::Result<()> {
        if self.role == BoundedStringRole::Other {
            return Ok(());
        }
        let mut encoded = Vec::with_capacity(self.bounded_raw.len() + 2);
        encoded.push(b'"');
        encoded.extend_from_slice(&self.bounded_raw);
        encoded.push(b'"');
        let decoded: String = serde_json::from_slice(&encoded)
            .map_err(|error| io::Error::other(format!("invalid bounded JSON string: {error}")))?;
        if decoded.len() > Self::MAX_ENVELOPE_STRING_BYTES {
            return Err(io::Error::other(
                "WAL envelope key or method exceeds 64 UTF-8 bytes",
            ));
        }
        if self.role == BoundedStringRole::EnvelopeKey {
            let frame = self
                .frames
                .last_mut()
                .expect("envelope key occurs inside an object");
            frame.expect_key = false;
            frame.pending_method = decoded == "method";
        }
        Ok(())
    }

    fn account(&mut self, bytes: &[u8]) -> io::Result<()> {
        for byte in bytes {
            if self.in_string {
                if self.escaped {
                    self.escaped = false;
                    if self.role != BoundedStringRole::Other {
                        self.bounded_raw.push(*byte);
                    }
                    continue;
                }
                match *byte {
                    b'\\' => {
                        self.escaped = true;
                        if self.role != BoundedStringRole::Other {
                            self.bounded_raw.push(*byte);
                        }
                    }
                    b'"' => {
                        self.finish_bounded_string()?;
                        self.in_string = false;
                        self.role = BoundedStringRole::Other;
                        self.bounded_raw.clear();
                    }
                    _ => {
                        if self.role != BoundedStringRole::Other {
                            self.bounded_raw.push(*byte);
                        }
                    }
                }
                if self.bounded_raw.len() > Self::MAX_ESCAPED_ENVELOPE_BYTES {
                    return Err(io::Error::other(
                        "WAL escaped envelope key or method exceeds bounded input",
                    ));
                }
                continue;
            }

            match *byte {
                b'"' => {
                    let depth = self.frames.len();
                    let role = self
                        .frames
                        .last()
                        .map_or(BoundedStringRole::Other, |frame| {
                            if frame.object && frame.expect_key && depth <= 2 {
                                BoundedStringRole::EnvelopeKey
                            } else if frame.object && frame.pending_method && depth <= 2 {
                                BoundedStringRole::MethodValue
                            } else {
                                BoundedStringRole::Other
                            }
                        });
                    if role != BoundedStringRole::EnvelopeKey {
                        if let Some(frame) = self.frames.last_mut() {
                            frame.pending_method = false;
                        }
                    }
                    self.in_string = true;
                    self.escaped = false;
                    self.role = role;
                    self.bounded_raw.clear();
                }
                b'{' | b'[' => {
                    if let Some(frame) = self.frames.last_mut() {
                        frame.pending_method = false;
                    }
                    if self.frames.len() >= Self::MAX_NESTING {
                        return Err(io::Error::other("WAL JSON nesting exceeds 128"));
                    }
                    self.frames.push(JsonGuardFrame {
                        object: *byte == b'{',
                        expect_key: *byte == b'{',
                        pending_method: false,
                    });
                }
                b'}' | b']' => {
                    self.frames.pop();
                }
                b',' => {
                    if let Some(frame) = self.frames.last_mut() {
                        if frame.object {
                            frame.expect_key = true;
                            frame.pending_method = false;
                        }
                    }
                }
                byte if !byte.is_ascii_whitespace() && byte != b':' => {
                    if let Some(frame) = self.frames.last_mut() {
                        frame.pending_method = false;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read> Read for GuardedJsonReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(output)?;
        self.account(&output[..read])?;
        Ok(read)
    }
}

impl<W> LimitedHashWriter<W> {
    fn new(inner: W, limit: u64, wal_hasher: Option<Sha256>) -> Self {
        Self {
            inner,
            bytes: 0,
            limit,
            record_hasher: Sha256::new(),
            wal_hasher,
        }
    }

    fn finish(self) -> WalWriteEvidence<W> {
        WalWriteEvidence {
            inner: self.inner,
            bytes: self.bytes,
            record_digest: self.record_hasher.finalize().into(),
            wal_hasher: self.wal_hasher,
        }
    }
}

impl<W: Write> Write for LimitedHashWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let requested = u64::try_from(buf.len())
            .map_err(|_| io::Error::other("WAL write length does not fit u64"))?;
        let next = self
            .bytes
            .checked_add(requested)
            .ok_or_else(|| io::Error::other("WAL record byte count overflow"))?;
        if next > self.limit {
            return Err(io::Error::other(format!(
                "WAL record exceeds {} bytes",
                self.limit
            )));
        }
        let written = self.inner.write(buf)?;
        self.record_hasher.update(&buf[..written]);
        if let Some(hasher) = self.wal_hasher.as_mut() {
            hasher.update(&buf[..written]);
        }
        self.bytes = self
            .bytes
            .checked_add(written as u64)
            .ok_or_else(|| io::Error::other("WAL record byte count overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl LimitedCountingWriter {
    fn new(limit: u64) -> Self {
        Self { bytes: 0, limit }
    }

    fn finish(self) -> u64 {
        self.bytes
    }
}

impl Write for LimitedCountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let requested = u64::try_from(buf.len())
            .map_err(|_| io::Error::other("serialized byte count does not fit u64"))?;
        let next = self
            .bytes
            .checked_add(requested)
            .ok_or_else(|| io::Error::other("serialized byte count overflow"))?;
        if next > self.limit {
            return Err(io::Error::other("serialized value exceeds its byte limit"));
        }
        self.bytes = next;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DurableGenerationToken {
    generation: u64,
    vector_blob: VectorBlobDescriptor,
    commit_evidence: StoredCommitEvidence,
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

trait CommitProgress {
    fn enter_phase(
        &mut self,
        phase: &str,
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> io::Result<()>;

    fn advance(
        &mut self,
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> io::Result<()>;
}

struct NoCommitProgress;

impl CommitProgress for NoCommitProgress {
    fn enter_phase(
        &mut self,
        _phase: &str,
        _completed_units: u64,
        _total_units: Option<u64>,
        _completed_bytes: u64,
        _total_bytes: Option<u64>,
    ) -> io::Result<()> {
        Ok(())
    }

    fn advance(
        &mut self,
        _completed_units: u64,
        _total_units: Option<u64>,
        _completed_bytes: u64,
        _total_bytes: Option<u64>,
    ) -> io::Result<()> {
        Ok(())
    }
}

struct NativeCommitProgress<'a, W: Write> {
    output: &'a mut W,
    policy: NativeProgressPolicy,
    request_id: u64,
    clock_ms: Box<dyn FnMut() -> io::Result<u64> + 'a>,
    last_emitted_ms: u64,
    phase_index: usize,
    sequence: u64,
    emitted_frames: u64,
    completed_units: u64,
    total_units: Option<u64>,
    completed_bytes: u64,
    total_bytes: Option<u64>,
}

impl<'a, W: Write> NativeCommitProgress<'a, W> {
    fn start(output: &'a mut W, policy: NativeProgressPolicy, request_id: u64) -> io::Result<Self> {
        let started = Instant::now();
        Self::start_with_clock(
            output,
            policy,
            request_id,
            Box::new(move || {
                u64::try_from(started.elapsed().as_millis())
                    .map_err(|_| io::Error::other("progress monotonic elapsed time overflow"))
            }),
        )
    }

    fn start_with_clock(
        output: &'a mut W,
        policy: NativeProgressPolicy,
        request_id: u64,
        clock_ms: Box<dyn FnMut() -> io::Result<u64> + 'a>,
    ) -> io::Result<Self> {
        let mut progress = Self {
            output,
            policy,
            request_id,
            clock_ms,
            last_emitted_ms: 0,
            phase_index: 0,
            sequence: 1,
            emitted_frames: 0,
            completed_units: 0,
            total_units: None,
            completed_bytes: 0,
            total_bytes: None,
        };
        let admitted = NativeProgressFrame::admitted(request_id).map_err(io::Error::other)?;
        progress.write_frame(admitted)?;
        Ok(progress)
    }

    fn elapsed_ms(&mut self) -> io::Result<u64> {
        (self.clock_ms)()
    }

    fn write_frame(&mut self, frame: NativeProgressFrame) -> io::Result<()> {
        let raw = frame.canonical_bytes();
        if raw.len() as u64 > self.policy.max_frame_bytes() {
            return Err(io::Error::other("progress frame exceeds policy byte limit"));
        }
        let next_count = self
            .emitted_frames
            .checked_add(1)
            .ok_or_else(|| io::Error::other("progress frame count overflow"))?;
        if next_count > self.policy.max_frames() {
            return Err(io::Error::other("progress frame count exceeds policy"));
        }
        self.output.write_all(&raw)?;
        self.output.write_all(b"\n")?;
        self.output.flush()?;
        self.emitted_frames = next_count;
        self.last_emitted_ms = self.elapsed_ms()?;
        Ok(())
    }

    fn validate_counters(
        &self,
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> io::Result<ProgressCounters> {
        if completed_units < self.completed_units || completed_bytes < self.completed_bytes {
            return Err(io::Error::other(
                "progress counters regressed within a phase",
            ));
        }
        if self.total_units.is_some() && total_units != self.total_units {
            return Err(io::Error::other(
                "progress unit total changed within a phase",
            ));
        }
        if self.total_bytes.is_some() && total_bytes != self.total_bytes {
            return Err(io::Error::other(
                "progress byte total changed within a phase",
            ));
        }
        ProgressCounters::new(completed_units, total_units, completed_bytes, total_bytes)
            .map_err(io::Error::other)
    }

    fn remaining_reserve(&self, elapsed_ms: u64) -> io::Result<u64> {
        let remaining_phases = self
            .policy
            .phases()
            .len()
            .saturating_sub(self.phase_index + 1) as u64;
        let remaining_ms = self
            .policy
            .absolute_deadline_ms()
            .saturating_sub(elapsed_ms);
        let heartbeat = self.policy.heartbeat_interval_ms();
        let remaining_heartbeats = remaining_ms
            .checked_add(heartbeat - 1)
            .and_then(|value| value.checked_div(heartbeat))
            .ok_or_else(|| io::Error::other("progress reserve arithmetic overflow"))?;
        remaining_phases
            .checked_add(remaining_heartbeats)
            .ok_or_else(|| io::Error::other("progress reserve arithmetic overflow"))
    }
}

impl<W: Write> CommitProgress for NativeCommitProgress<'_, W> {
    fn enter_phase(
        &mut self,
        phase: &str,
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> io::Result<()> {
        let next_index = self
            .phase_index
            .checked_add(1)
            .ok_or_else(|| io::Error::other("progress phase index overflow"))?;
        if self.policy.phases().get(next_index).copied() != Some(phase) {
            return Err(io::Error::other("progress phase skipped or regressed"));
        }
        let counters =
            ProgressCounters::new(completed_units, total_units, completed_bytes, total_bytes)
                .map_err(io::Error::other)?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("progress sequence overflow"))?;
        let frame = NativeProgressFrame::progress(self.request_id, self.sequence, phase, counters)
            .map_err(io::Error::other)?;
        self.phase_index = next_index;
        self.completed_units = completed_units;
        self.total_units = total_units;
        self.completed_bytes = completed_bytes;
        self.total_bytes = total_bytes;
        self.write_frame(frame)
    }

    fn advance(
        &mut self,
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> io::Result<()> {
        let counters =
            self.validate_counters(completed_units, total_units, completed_bytes, total_bytes)?;
        let unit_delta = completed_units.saturating_sub(self.completed_units);
        let byte_delta = completed_bytes.saturating_sub(self.completed_bytes);
        if unit_delta == 0 && byte_delta == 0 {
            return Ok(());
        }
        let elapsed = self.elapsed_ms()?;
        let since_last = elapsed.saturating_sub(self.last_emitted_ms);
        let due_heartbeat = since_last >= self.policy.heartbeat_interval_ms();
        let due_early = since_last >= self.policy.min_frame_interval_ms()
            && (unit_delta >= self.policy.early_unit_delta()
                || byte_delta >= self.policy.early_byte_delta());
        if !due_heartbeat && !due_early {
            return Ok(());
        }
        let reserve = self.remaining_reserve(elapsed)?;
        if self
            .emitted_frames
            .checked_add(1)
            .and_then(|count| count.checked_add(reserve))
            .is_none_or(|required| required > self.policy.max_frames())
        {
            return Ok(());
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| io::Error::other("progress sequence overflow"))?;
        let phase = self.policy.phases()[self.phase_index];
        let frame = NativeProgressFrame::progress(self.request_id, self.sequence, phase, counters)
            .map_err(io::Error::other)?;
        self.completed_units = completed_units;
        self.total_units = total_units;
        self.completed_bytes = completed_bytes;
        self.total_bytes = total_bytes;
        self.write_frame(frame)
    }
}

#[derive(Clone)]
struct CrashTracker {
    audit_log_path: PathBuf,
    started_epoch_sec: u64,
    last_request_id: Arc<Mutex<Option<String>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TransactionState {
    Idle,
    Active {
        base_generation: u64,
        transaction_nonce: String,
        mutation_seen: bool,
    },
    Prepared {
        evidence: PreparedCommitEvidence,
        wal_identity: (u64, u64),
    },
    RecoveryPending {
        base_generation: u64,
        wal_digest: [u8; 32],
        record_count: usize,
        wal_device: u64,
        wal_inode: u64,
    },
}

struct Server {
    /// Filesystem paths are present only for the normal owner-facing mode.
    /// Descriptor mode deliberately has no path or parent-directory authority.
    db_path: Option<PathBuf>,
    audit_log_path: Option<PathBuf>,
    state: State,
    vector_values: HashMap<String, Vec<f64>>,
    cache_dirty: bool,
    transaction: TransactionState,
    wal_path: Option<PathBuf>,
    wal_bytes: u64,
    wal_record_count: u64,
    wal_hasher: Sha256,
    wal_file: Option<fs::File>,
    wal_identity: Option<(u64, u64)>,
    active_mutation_request_ids: HashSet<u64>,
    wal_replaying: bool,
    last_persist_bytes: u64,
    fatal: bool,
    node_keys_by_corpus: HashMap<String, Vec<String>>,
    edge_keys_by_corpus: HashMap<String, Vec<String>>,
    adjacent_edge_keys_by_node: HashMap<String, Vec<String>>,
    vector_keys_by_corpus_namespace: HashMap<String, Vec<String>>,
    passage_keys_by_corpus: HashMap<String, Vec<String>>,
    access_mode: AccessMode,
}

#[derive(Debug, Clone)]
enum AccessMode {
    Normal,
    DescriptorReadOnly(DescriptorReadHandshake),
}

#[derive(Debug, Clone)]
struct DescriptorReadHandshake {
    canonical_sha256: String,
    vector_blob_sha256: String,
    vector_blob_size: u64,
    legacy_generation0: bool,
    legacy_binding_sha256: Option<String>,
    method_inventory_sha256: String,
}

#[derive(Debug)]
struct DescriptorReadConfig {
    canonical_fd: i32,
    vector_blob_fd: i32,
    expected_generation: u64,
    legacy_generation0: bool,
    legacy_binding_sha256: Option<String>,
}

const ACCESS_MODE_DESCRIPTOR_READ_ONLY: &str = "descriptor-read-only";
const DESCRIPTOR_READ_ONLY_METHOD_CODE: &str = "DESCRIPTOR_READ_ONLY_METHOD";
const DESCRIPTOR_READ_ONLY_METHOD_MESSAGE: &str =
    "method is unavailable in descriptor read-only mode";
// The descriptor protocol must never turn an inherited file into an
// unbounded allocation. These caps are intentionally independent from the
// normal --db persistence path and are checked from fstat before reading.
const MAX_DESCRIPTOR_CANONICAL_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_DESCRIPTOR_VECTOR_BLOB_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES: usize = 2 * 1024 * 1024;
const MAX_DESCRIPTOR_PROTOCOL_BYTES: u64 = 64 * 1024 * 1024;
const MAX_DESCRIPTOR_PROTOCOL_FRAMES: u64 = 4096;
const INDEXING_MEMORY_PROTOCOL_SCHEMA: &str = "native-indexing-memory@1";
// The normal protocol retains the existing full-snapshot compatibility lane,
// while still preventing an unterminated stdin frame from allocating without
// a ceiling. Indexing-memory requests have the much smaller independent cap
// below and are rejected before WAL append or dispatch.
const MAX_NORMAL_REQUEST_FRAME_BYTES: usize = MAX_WAL_RECORD_BYTES as usize;
const MAX_INDEXING_REQUEST_BYTES: u64 = 64 * 1024 * 1024;
const MAX_INDEXING_RESPONSE_BYTES: u64 = 8 * 1024 * 1024;
const MAX_INDEXING_SCHEMA_IDS: usize = 4096;
const MAX_INDEXING_ACTIVE_FACTS: usize = 100;
const MAX_INDEXING_DELTA_ITEMS_PER_SECTION: usize = 4096;
const MAX_INDEXING_DOMAIN_ID_BYTES: usize = 4096;
const MAX_INDEXING_CORPUS_ID_BYTES: usize = 1024;
const MAX_INDEXING_UPDATED_AT_BYTES: usize = 128;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MethodWireProfile {
    Normal,
    BoundedIndexing,
}

#[derive(Debug, Clone, Copy)]
struct MethodSpec {
    name: &'static str,
    classification: &'static str,
    wal: bool,
    wire_profile: MethodWireProfile,
}

// This table is the one policy authority used by protocol_info, WAL admission,
// and method-specific wire limits. Unknown methods deliberately have no read
// classification or bounded-indexing profile.
const METHOD_SPECS: &[MethodSpec] = &[
    MethodSpec {
        name: "ping",
        classification: "health",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "protocol_info",
        classification: "health",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "batch_begin",
        classification: "transaction",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "batch_prepare_commit",
        classification: "transaction",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "batch_commit",
        classification: "commit",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "recovery_discard",
        classification: "recovery",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "upsert_nodes",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "upsert_edges",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "get_node",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "get_nodes",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "get_edges",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "get_adjacent",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "delete_nodes",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "delete_edges",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "delete_by_document",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "delete_by_corpus",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "vector_upsert",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "vector_search",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "vector_delete_by_document",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_upsert",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::BoundedIndexing,
    },
    MethodSpec {
        name: "memory_save",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_save_file",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_load",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_get_schemas_by_ids",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::BoundedIndexing,
    },
    MethodSpec {
        name: "memory_get_active_facts",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::BoundedIndexing,
    },
    // This exact method name is durable WAL intent. A pre-capability binary
    // cannot scan it, so any admitted record must be resolved or quarantined
    // with the current binary before rolling back to that older binary.
    MethodSpec {
        name: "memory_activate_facts_by_schema_ids",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::BoundedIndexing,
    },
    MethodSpec {
        name: "memory_save_checkpoint",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_load_checkpoint",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "memory_validate_integrity",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "projection_get_transitions",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "projection_get_dangling_nodes",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "projection_get_node_count",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "lexical_index_passages",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "lexical_search",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "lexical_delete_by_document",
        classification: "mutation",
        wal: true,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "cypher_query",
        classification: "read",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
    MethodSpec {
        name: "__debug_force_panic__",
        classification: "debug",
        wal: false,
        wire_profile: MethodWireProfile::Normal,
    },
];

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
    const WAL_VERSION: u16 = 2;

    fn metadata_link_count(metadata: &fs::Metadata) -> u64 {
        #[cfg(unix)]
        {
            metadata.nlink()
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            1
        }
    }

    fn metadata_identity(metadata: &fs::Metadata) -> (u64, u64) {
        #[cfg(unix)]
        {
            (metadata.dev(), metadata.ino())
        }
        #[cfg(not(unix))]
        {
            let _ = metadata;
            (0, 0)
        }
    }

    fn require_regular_single_link(path: &Path, metadata: &fs::Metadata) -> io::Result<()> {
        if !metadata.file_type().is_file() {
            return Err(io::Error::other(format!(
                "{} is not a regular file",
                path.display()
            )));
        }
        if Self::metadata_link_count(metadata) != 1 {
            return Err(io::Error::other(format!(
                "{} must have exactly one hard link",
                path.display()
            )));
        }
        Ok(())
    }

    fn resolve_db_path(input: PathBuf) -> io::Result<PathBuf> {
        let absolute = if input.is_absolute() {
            input
        } else {
            std::env::current_dir()?.join(input)
        };
        if let Ok(metadata) = fs::symlink_metadata(&absolute) {
            Self::require_regular_single_link(&absolute, &metadata)?;
            return fs::canonicalize(&absolute);
        }
        let parent = Self::parent_dir(&absolute);
        let canonical_parent = fs::canonicalize(&parent).map_err(|err| {
            io::Error::other(format!(
                "canonical database parent {} failed: {err}",
                parent.display()
            ))
        })?;
        let file_name = absolute
            .file_name()
            .ok_or_else(|| io::Error::other("database path must name a file"))?;
        Ok(canonical_parent.join(file_name))
    }

    fn open_regular_nofollow(path: &Path) -> io::Result<fs::File> {
        let mut options = fs::OpenOptions::new();
        options.read(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        Self::require_regular_single_link(path, &metadata)?;
        Ok(file)
    }

    fn open_wal_readwrite_nofollow(path: &Path) -> io::Result<fs::File> {
        let mut options = fs::OpenOptions::new();
        options.read(true).write(true).append(true);
        #[cfg(unix)]
        options.custom_flags(libc::O_NOFOLLOW);
        let file = options.open(path)?;
        let metadata = file.metadata()?;
        Self::require_regular_single_link(path, &metadata)?;
        Ok(file)
    }

    fn read_regular_nofollow(path: &Path) -> io::Result<Vec<u8>> {
        let mut file = Self::open_regular_nofollow(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        Ok(raw)
    }

    fn validate_regular_path_identity(path: &Path, expected: (u64, u64)) -> io::Result<()> {
        let path_metadata = fs::symlink_metadata(path)?;
        Self::require_regular_single_link(path, &path_metadata)?;
        if Self::metadata_identity(&path_metadata) != expected {
            return Err(io::Error::other(format!(
                "{} no longer names the expected inode",
                path.display()
            )));
        }
        let file = Self::open_regular_nofollow(path)?;
        if Self::metadata_identity(&file.metadata()?) != expected {
            return Err(io::Error::other(format!(
                "{} changed during identity validation",
                path.display()
            )));
        }
        Ok(())
    }

    #[allow(dead_code)]
    fn open(db_path: PathBuf) -> io::Result<Self> {
        Self::open_resolved(Self::resolve_db_path(db_path)?)
    }

    fn open_resolved(db_path: PathBuf) -> io::Result<Self> {
        Self::cleanup_rename_probe_artifacts(&db_path)?;
        Self::probe_noreplace_support(&db_path)?;
        let mut state = if let Ok(metadata) = fs::symlink_metadata(&db_path) {
            Self::require_regular_single_link(&db_path, &metadata)?;
            let raw = Self::read_regular_nofollow(&db_path)?;
            serde_json::from_slice::<State>(&raw)
                .map_err(|err| io::Error::other(format!("parse canonical state failed: {err}")))?
        } else {
            State::default()
        };
        Self::validate_state_commit_evidence(&state)?;
        let legacy_vector_blob_path = db_path.with_extension("vblob");
        let vector_values = Self::load_vector_values(&state, &db_path, &legacy_vector_blob_path)?;
        for (key, values) in &vector_values {
            if let Some(vector) = state.vectors.get_mut(key) {
                vector.values = values.clone();
            }
        }
        Self::cleanup_recognized_temps(
            &Self::parent_dir(&db_path),
            db_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("aira-graphdb-native.json"),
            state
                .vector_blob
                .as_ref()
                .map(|descriptor| descriptor.basename.as_str()),
            state.generation,
        )?;
        Ok(Self {
            audit_log_path: Some(db_path.with_extension("native-audit.log")),
            wal_path: Some(db_path.with_extension("agdb.wal")),
            wal_bytes: fs::metadata(db_path.with_extension("agdb.wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            wal_record_count: 0,
            wal_hasher: Sha256::new(),
            wal_file: None,
            wal_identity: None,
            active_mutation_request_ids: HashSet::new(),
            wal_replaying: false,
            last_persist_bytes: fs::metadata(&db_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            db_path: Some(db_path),
            state,
            vector_values,
            cache_dirty: true,
            transaction: TransactionState::Idle,
            fatal: false,
            node_keys_by_corpus: HashMap::new(),
            edge_keys_by_corpus: HashMap::new(),
            adjacent_edge_keys_by_node: HashMap::new(),
            vector_keys_by_corpus_namespace: HashMap::new(),
            passage_keys_by_corpus: HashMap::new(),
            access_mode: AccessMode::Normal,
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
        // Descriptor mode has no audit-file authority. Its only successful
        // writes are the protocol bytes sent to stdout by the caller.
        if let Some(path) = self.audit_log_path.as_ref() {
            let _ = Self::append_request_audit_event_for_path(
                path,
                error_code,
                failure_class,
                request_id,
            );
        }
    }

    fn parent_dir(path: &Path) -> PathBuf {
        path.parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."))
    }

    fn sync_parent_dir(path: &Path) -> io::Result<()> {
        fs::File::open(path)?.sync_all()
    }

    fn crypto_nonce() -> io::Result<String> {
        let mut bytes = [0u8; 16];
        let mut source = fs::File::open("/dev/urandom")?;
        source.read_exact(&mut bytes)?;
        Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn crypto_transaction_nonce() -> io::Result<String> {
        let mut bytes = [0u8; 32];
        let mut source = fs::File::open("/dev/urandom")?;
        source.read_exact(&mut bytes)?;
        Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
    }

    fn is_durable_temp_stage(stage: &str) -> bool {
        matches!(
            stage,
            "blob_temp_sync"
                | "json_temp_sync"
                | "wal_compact_sync"
                | "wal_retire"
                | "rename_probe"
        )
    }

    const PROBE_MOVE_PAYLOAD: &'static [u8] = b"move-source";
    const PROBE_COLLISION_PAYLOAD: &'static [u8] = b"collision-source";
    const PROBE_MAX_PAYLOAD_LEN: u64 = 16;

    fn temporary_path(target: &Path, stage: &str) -> io::Result<PathBuf> {
        if !Self::is_durable_temp_stage(stage) {
            return Err(io::Error::other(format!(
                "unrecognized durable temporary stage {stage}"
            )));
        }
        let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
        let file_name = target
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("aira-graphdb");
        let nonce = Self::crypto_nonce()?;
        Ok(Self::parent_dir(target).join(format!(".{file_name}.{stage}.{nonce}.{id}.tmp")))
    }

    fn recognized_durable_temp(name: &str) -> Option<(&str, &str)> {
        let Some(name) = name.strip_prefix('.') else {
            return None;
        };
        let Some(name) = name.strip_suffix(".tmp") else {
            return None;
        };
        let mut parts = name.rsplitn(4, '.');
        let Some(id) = parts.next() else {
            return None;
        };
        let Some(nonce) = parts.next() else {
            return None;
        };
        let Some(stage) = parts.next() else {
            return None;
        };
        let Some(target) = parts.next() else {
            return None;
        };
        if target.is_empty()
            || id.is_empty()
            || !id.chars().all(|character| character.is_ascii_digit())
            || nonce.len() != 32
            || !nonce.chars().all(|character| character.is_ascii_hexdigit())
            || !Self::is_durable_temp_stage(stage)
        {
            return None;
        }
        Some((target, stage))
    }

    fn cleanup_recognized_temps(
        parent: &Path,
        db_file_name: &str,
        referenced_blob: Option<&str>,
        current_generation: u64,
    ) -> io::Result<()> {
        let db_stem = Path::new(db_file_name)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .unwrap_or("aira-graphdb");
        let wal_path = Path::new(db_file_name).with_extension("agdb.wal");
        let wal_file_name = wal_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("aira-graphdb.agdb.wal");
        let mut removed = false;
        let entries = match fs::read_dir(parent) {
            Ok(entries) => entries,
            Err(err) => return Err(err),
        };
        let mut retired_wals = Vec::new();
        for entry in entries {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some((target, stage)) = Self::recognized_durable_temp(&name) else {
                continue;
            };
            if stage == "rename_probe" {
                continue;
            }
            if stage == "wal_retire" {
                if target == wal_file_name {
                    retired_wals.push(entry.path());
                }
                continue;
            }
            let target_is_blob =
                target.starts_with(&format!("{db_stem}.g")) && target.ends_with(".vblob");
            if referenced_blob.is_some_and(|referenced| referenced == target)
                || (target != db_file_name && target != wal_file_name && !target_is_blob)
            {
                continue;
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            if metadata.file_type().is_file() && Self::metadata_link_count(&metadata) == 1 {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        for retired in retired_wals {
            if Self::cleanup_retired_wal_exact(
                &retired,
                &parent.join(&wal_path),
                current_generation,
            )? {
                removed = true;
            }
        }
        if removed {
            Self::sync_parent_dir(parent)?;
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn rename_noreplace(source: &Path, destination: &Path) -> io::Result<()> {
        let source = CString::new(source.as_os_str().as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "source path contains NUL"))?;
        let destination = CString::new(destination.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "destination path contains NUL")
        })?;
        // SAFETY: both C strings remain alive for the duration of renameat2.
        let result = unsafe {
            libc::renameat2(
                libc::AT_FDCWD,
                source.as_ptr(),
                libc::AT_FDCWD,
                destination.as_ptr(),
                libc::RENAME_NOREPLACE,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }

    #[cfg(not(target_os = "linux"))]
    fn rename_noreplace(_source: &Path, _destination: &Path) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "exact WAL retirement requires atomic no-replace rename support",
        ))
    }

    fn probe_noreplace_support(db_path: &Path) -> io::Result<()> {
        let parent = Self::parent_dir(db_path);
        let source = Self::temporary_path(db_path, "rename_probe")?;
        let destination = Self::temporary_path(db_path, "rename_probe")?;
        let create_probe = |path: &Path, bytes: &[u8]| -> io::Result<(u64, u64)> {
            let mut options = fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
            let mut file = options.open(path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(Self::metadata_identity(&file.metadata()?))
        };
        let mut source_authority: Option<((u64, u64), Vec<u8>)> = None;
        let mut destination_authority: Option<((u64, u64), Vec<u8>)> = None;
        let result = (|| -> io::Result<()> {
            let source_identity = create_probe(&source, Self::PROBE_MOVE_PAYLOAD)?;
            source_authority = Some((source_identity, Self::PROBE_MOVE_PAYLOAD.to_vec()));
            Self::durability_failpoint("after_noreplace_probe_source_create")?;
            Self::sync_parent_dir(&parent)?;
            Self::durability_failpoint("after_noreplace_probe_source_dir_fsync")?;
            let move_result =
                if Self::failpoint_matches("AGDB_NATIVE_FAIL_POINT", "noreplace_unsupported") {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "injected no-replace capability failure",
                    ))
                } else {
                    Self::rename_noreplace(&source, &destination)
                };
            move_result.map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Unsupported,
                    format!(
                        "native persistence requires Linux renameat2 RENAME_NOREPLACE support in {}: {err}",
                        parent.display()
                    ),
                )
            })?;
            source_authority = None;
            destination_authority = Some((source_identity, Self::PROBE_MOVE_PAYLOAD.to_vec()));
            Self::durability_failpoint("after_noreplace_probe_move")?;
            if source.exists() {
                return Err(io::Error::other(
                    "atomic no-replace move probe left the source path present",
                ));
            }
            Self::validate_owned_probe_path(
                &destination,
                source_identity,
                Self::PROBE_MOVE_PAYLOAD,
            )?;

            let collision_identity = create_probe(&source, Self::PROBE_COLLISION_PAYLOAD)?;
            source_authority = Some((collision_identity, Self::PROBE_COLLISION_PAYLOAD.to_vec()));
            Self::durability_failpoint("after_noreplace_probe_collision_source_create")?;
            Self::sync_parent_dir(&parent)?;
            Self::durability_failpoint("after_noreplace_probe_collision_dir_fsync")?;
            match Self::rename_noreplace(&source, &destination) {
                Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {}
                Ok(()) => {
                    return Err(io::Error::other(
                        "atomic no-replace probe replaced an existing destination",
                    ));
                }
                Err(err) => return Err(err),
            }
            Self::validate_owned_probe_path(
                &source,
                collision_identity,
                Self::PROBE_COLLISION_PAYLOAD,
            )?;
            Self::validate_owned_probe_path(
                &destination,
                source_identity,
                Self::PROBE_MOVE_PAYLOAD,
            )?;
            Ok(())
        })();
        Self::durability_pausepoint("before_noreplace_probe_cleanup")?;
        let mut cleanup_error = None;
        if let Some((identity, raw)) = source_authority {
            if let Err(err) =
                Self::cleanup_owned_rename_probe_exact(&source, db_path, identity, &raw)
            {
                cleanup_error = Some(err);
            }
        }
        if let Some((identity, raw)) = destination_authority {
            if let Err(err) =
                Self::cleanup_owned_rename_probe_exact(&destination, db_path, identity, &raw)
            {
                cleanup_error.get_or_insert(err);
            }
        }
        Self::durability_failpoint("after_noreplace_probe_cleanup_unlink")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_noreplace_probe_cleanup_dir_fsync")?;
        if let Some(err) = cleanup_error {
            return Err(err);
        }
        result
    }

    fn cleanup_rename_probe_artifacts(db_path: &Path) -> io::Result<()> {
        let parent = Self::parent_dir(db_path);
        let db_file_name = db_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("aira-graphdb-native.json");
        let mut candidates = Vec::new();
        for entry in fs::read_dir(&parent)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if matches!(
                Self::recognized_durable_temp(&name),
                Some((target, "rename_probe")) if target == db_file_name
            ) {
                candidates.push(entry.path());
            }
        }
        for candidate in candidates {
            Self::cleanup_rename_probe_exact(&candidate, db_path)?;
        }
        Ok(())
    }

    fn read_bounded_probe_payload(file: &mut fs::File) -> io::Result<Vec<u8>> {
        if file.metadata()?.len() > Self::PROBE_MAX_PAYLOAD_LEN {
            return Err(io::Error::other("rename probe artifact exceeds size limit"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut raw = Vec::with_capacity(Self::PROBE_MAX_PAYLOAD_LEN as usize);
        file.take(Self::PROBE_MAX_PAYLOAD_LEN + 1)
            .read_to_end(&mut raw)?;
        if raw.len() as u64 > Self::PROBE_MAX_PAYLOAD_LEN
            || (!Self::PROBE_MOVE_PAYLOAD.starts_with(&raw)
                && !Self::PROBE_COLLISION_PAYLOAD.starts_with(&raw))
        {
            return Err(io::Error::other(
                "rename probe artifact has invalid bounded payload",
            ));
        }
        Ok(raw)
    }

    fn validate_owned_probe_path(
        path: &Path,
        expected_identity: (u64, u64),
        expected_raw: &[u8],
    ) -> io::Result<()> {
        let mut held = Self::open_regular_nofollow(path)?;
        if Self::metadata_identity(&held.metadata()?) != expected_identity
            || Self::read_bounded_probe_payload(&mut held)? != expected_raw
        {
            return Err(io::Error::other(
                "atomic no-replace probe changed an owned inode",
            ));
        }
        Ok(())
    }

    fn claim_and_remove_probe_exact(
        path: &Path,
        db_path: &Path,
        mut held: fs::File,
        identity: (u64, u64),
        raw: &[u8],
    ) -> io::Result<()> {
        let claimed = Self::temporary_path(db_path, "rename_probe")?;
        Self::rename_noreplace(path, &claimed)?;
        Self::durability_failpoint("after_rename_probe_cleanup_claim")?;
        Self::validate_regular_path_identity(&claimed, identity)?;
        if Self::read_bounded_probe_payload(&mut held)? != raw {
            return Err(io::Error::other(
                "rename probe artifact changed during cleanup",
            ));
        }
        let parent = Self::parent_dir(db_path);
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_rename_probe_cleanup_dir_fsync")?;
        fs::remove_file(&claimed)?;
        Self::durability_failpoint("after_rename_probe_cleanup_unlink")?;
        Self::sync_parent_dir(&parent)
    }

    fn cleanup_owned_rename_probe_exact(
        path: &Path,
        db_path: &Path,
        expected_identity: (u64, u64),
        expected_raw: &[u8],
    ) -> io::Result<()> {
        let mut held = match Self::open_regular_nofollow(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err),
        };
        if Self::metadata_identity(&held.metadata()?) != expected_identity
            || Self::read_bounded_probe_payload(&mut held)? != expected_raw
        {
            return Err(io::Error::other(
                "rename probe cleanup refused an unowned inode",
            ));
        }
        Self::claim_and_remove_probe_exact(path, db_path, held, expected_identity, expected_raw)
    }

    fn cleanup_rename_probe_exact(path: &Path, db_path: &Path) -> io::Result<()> {
        let metadata = fs::symlink_metadata(path)?;
        if !metadata.file_type().is_file() || Self::metadata_link_count(&metadata) != 1 {
            return Ok(());
        }
        let mut held = Self::open_regular_nofollow(path)?;
        let identity = Self::metadata_identity(&held.metadata()?);
        let raw = Self::read_bounded_probe_payload(&mut held)?;
        Self::claim_and_remove_probe_exact(path, db_path, held, identity, &raw)
    }

    fn cleanup_retired_wal_exact(
        path: &Path,
        wal_path: &Path,
        current_generation: u64,
    ) -> io::Result<bool> {
        let path_metadata = fs::symlink_metadata(path)?;
        if !path_metadata.file_type().is_file() || Self::metadata_link_count(&path_metadata) != 1 {
            return Ok(false);
        }
        let mut held = Self::open_regular_nofollow(path)?;
        let identity = Self::metadata_identity(&held.metadata()?);
        let evidence = Self::scan_wal_file(&mut held, false)?;
        if evidence.identity != identity
            || evidence
                .base_generation
                .is_some_and(|base| base >= current_generation)
        {
            return Err(io::Error::other(
                "retired WAL contains a current or future generation",
            ));
        }
        let claimed = Self::temporary_path(wal_path, "wal_retire")?;
        Self::durability_pausepoint("before_startup_wal_retire_claim")?;
        Self::rename_noreplace(path, &claimed)?;
        Self::durability_pausepoint("after_startup_wal_retire_claim")?;
        Self::durability_failpoint("after_startup_wal_retire_claim")?;
        Self::validate_regular_path_identity(&claimed, identity)?;
        if Self::scan_wal_file(&mut held, false)? != evidence {
            return Err(io::Error::other(
                "retired WAL content changed during startup cleanup",
            ));
        }
        let parent = Self::parent_dir(wal_path);
        Self::durability_failpoint("before_startup_wal_retire_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_startup_wal_retire_dir_fsync")?;
        Self::durability_failpoint("before_startup_wal_retire_unlink")?;
        fs::remove_file(&claimed)?;
        Self::durability_failpoint("after_startup_wal_retire_unlink")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_startup_wal_retire_final_dir_fsync")?;
        Ok(true)
    }

    fn failpoint_matches(var_name: &str, stage: &str) -> bool {
        std::env::var(var_name)
            .ok()
            .map(|value| {
                value
                    .split(',')
                    .map(str::trim)
                    .any(|candidate| candidate == stage)
            })
            .unwrap_or(false)
    }

    /// Test-only, process-local durability probes. The kill variant exits
    /// without a response so the caller can exercise the real reopen path.
    fn durability_failpoint(stage: &str) -> io::Result<()> {
        for variable in [
            "AGDB_NATIVE_FAIL_POINT",
            "AGDB_NATIVE_FAILPOINT",
            "AGDB_FAILPOINT",
        ] {
            if Self::failpoint_matches(variable, stage) {
                return Err(io::Error::other(format!(
                    "injected durability failure at {stage}"
                )));
            }
        }
        for variable in ["AGDB_NATIVE_KILL_POINT", "AGDB_NATIVE_KILLPOINT"] {
            if Self::failpoint_matches(variable, stage) {
                #[cfg(unix)]
                unsafe {
                    libc::kill(libc::getpid(), libc::SIGKILL);
                    std::process::abort();
                }
                #[cfg(not(unix))]
                std::process::exit(137);
            }
        }
        Ok(())
    }

    fn durability_pausepoint(stage: &str) -> io::Result<()> {
        if !Self::failpoint_matches("AGDB_NATIVE_TEST_PAUSE_POINT", stage) {
            return Ok(());
        }
        if let Ok(marker) = std::env::var("AGDB_NATIVE_TEST_PAUSE_MARKER") {
            let marker = PathBuf::from(marker);
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&marker)?;
            file.write_all(stage.as_bytes())?;
            file.sync_all()?;
            Self::sync_parent_dir(&Self::parent_dir(&marker))?;
        }
        let milliseconds = std::env::var("AGDB_NATIVE_TEST_PAUSE_MS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(100)
            .min(5_000);
        std::thread::sleep(std::time::Duration::from_millis(milliseconds));
        Ok(())
    }

    fn create_streaming_temp(target: &Path, sync_stage: &str) -> io::Result<(PathBuf, fs::File)> {
        let tmp_path = Self::temporary_path(target, sync_stage)?;
        Self::durability_failpoint(&format!("before_{sync_stage}_create"))?;
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        let file = options
            .open(&tmp_path)
            .map_err(|err| io::Error::other(format!("create temp file failed: {err}")))?;
        Self::durability_failpoint(&format!("after_{sync_stage}_create"))?;
        Ok((tmp_path, file))
    }

    fn sync_streaming_temp(
        file: &mut fs::File,
        tmp_path: &Path,
        sync_stage: &str,
    ) -> io::Result<()> {
        let result = (|| {
            file.flush()
                .map_err(|err| io::Error::other(format!("flush temp file failed: {err}")))?;
            Self::durability_failpoint(&format!("after_{sync_stage}_write"))?;
            Self::durability_failpoint(&format!("before_{sync_stage}_fsync"))?;
            file.sync_all()
                .map_err(|err| io::Error::other(format!("sync temp file failed: {err}")))?;
            Self::durability_failpoint(&format!("after_{sync_stage}_fsync"))?;
            let bytes = file.metadata()?.len();
            Self::evict_clean_range(file, 0, bytes, sync_stage)
        })();
        if result.is_err() {
            let _ = fs::remove_file(tmp_path);
        }
        result
    }

    #[cfg(target_os = "linux")]
    fn checked_cache_range(offset: u64, bytes: u64) -> io::Result<(libc::off64_t, libc::off64_t)> {
        let end = offset
            .checked_add(bytes)
            .ok_or_else(|| io::Error::other("publication cache range overflow"))?;
        if end > i64::MAX as u64 {
            return Err(io::Error::other("publication cache range exceeds off64_t"));
        }
        let offset = libc::off64_t::try_from(offset)
            .map_err(|_| io::Error::other("publication cache offset exceeds off64_t"))?;
        let bytes = libc::off64_t::try_from(bytes)
            .map_err(|_| io::Error::other("publication cache length exceeds off64_t"))?;
        Ok((offset, bytes))
    }

    #[cfg(target_os = "linux")]
    fn writeback_and_evict_range(
        file: &fs::File,
        offset: u64,
        bytes: u64,
        stage: &str,
    ) -> io::Result<()> {
        let (offset, bytes) = Self::checked_cache_range(offset, bytes)?;
        Self::durability_failpoint(&format!("before_{stage}_cache_writeback"))?;
        let flags = libc::SYNC_FILE_RANGE_WAIT_BEFORE
            | libc::SYNC_FILE_RANGE_WRITE
            | libc::SYNC_FILE_RANGE_WAIT_AFTER;
        let result = unsafe { libc::sync_file_range(file.as_raw_fd(), offset, bytes, flags) };
        if result != 0 {
            return Err(io::Error::other(format!(
                "write back publication cache range failed: {}",
                io::Error::last_os_error()
            )));
        }
        Self::durability_failpoint(&format!("after_{stage}_cache_writeback"))?;
        Self::evict_clean_range(file, offset as u64, bytes as u64, stage)
    }

    #[cfg(not(target_os = "linux"))]
    fn writeback_and_evict_range(
        _file: &fs::File,
        _offset: u64,
        _bytes: u64,
        _stage: &str,
    ) -> io::Result<()> {
        // Correctness and final fsync ordering are portable. The production
        // cgroup cache bound is Linux-only and must not be inferred here.
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn evict_clean_range(file: &fs::File, offset: u64, bytes: u64, stage: &str) -> io::Result<()> {
        if bytes == 0 {
            return Ok(());
        }
        let (offset, bytes) = Self::checked_cache_range(offset, bytes)?;
        Self::durability_failpoint(&format!("before_{stage}_cache_evict"))?;
        let result = unsafe {
            libc::posix_fadvise(file.as_raw_fd(), offset, bytes, libc::POSIX_FADV_DONTNEED)
        };
        if result != 0 {
            return Err(io::Error::other(format!(
                "evict publication cache range failed: {}",
                io::Error::from_raw_os_error(result)
            )));
        }
        Self::durability_failpoint(&format!("after_{stage}_cache_evict"))
    }

    #[cfg(not(target_os = "linux"))]
    fn evict_clean_range(
        _file: &fs::File,
        _offset: u64,
        _bytes: u64,
        _stage: &str,
    ) -> io::Result<()> {
        Ok(())
    }

    fn validate_blob_streaming_progress(
        path: &Path,
        descriptor: &VectorBlobDescriptor,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<fs::File> {
        let mut file = Self::open_regular_nofollow(path)
            .map_err(|err| io::Error::other(format!("open vector blob failed: {err}")))?;
        let identity = Self::metadata_identity(&file.metadata()?);
        let mut writer = ArtifactHashWriter::new(io::sink());
        let mut prefix = [0u8; 10];
        let mut prefix_used = 0usize;
        let mut buffer = [0u8; 64 * 1024];
        let mut released_bytes = 0u64;
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            let copy = (prefix.len() - prefix_used).min(count);
            prefix[prefix_used..prefix_used + copy].copy_from_slice(&buffer[..copy]);
            prefix_used += copy;
            writer.write_all(&buffer[..count])?;
            progress.advance(0, None, writer.bytes, Some(descriptor.size))?;
            while writer.bytes.saturating_sub(released_bytes)
                >= STREAM_PUBLICATION_CACHE_WINDOW_BYTES
            {
                Self::evict_clean_range(
                    &file,
                    released_bytes,
                    STREAM_PUBLICATION_CACHE_WINDOW_BYTES,
                    "blob_validation",
                )?;
                released_bytes = released_bytes
                    .checked_add(STREAM_PUBLICATION_CACHE_WINDOW_BYTES)
                    .ok_or_else(|| io::Error::other("validation cache offset overflow"))?;
            }
        }
        let evidence = writer.finish()?;
        Self::evict_clean_range(
            &file,
            released_bytes,
            evidence.bytes.saturating_sub(released_bytes),
            "blob_validation",
        )?;
        if prefix_used < Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>() {
            return Err(io::Error::other("vector blob file is truncated"));
        }
        if &prefix[..Self::VECTOR_BLOB_MAGIC.len()] != Self::VECTOR_BLOB_MAGIC {
            return Err(io::Error::other("vector blob magic mismatch"));
        }
        let version_start = Self::VECTOR_BLOB_MAGIC.len();
        let version = u16::from_le_bytes(
            prefix[version_start..version_start + 2]
                .try_into()
                .expect("validated version prefix"),
        );
        if descriptor.format != version
            || descriptor.size != evidence.bytes
            || descriptor.sha256 != evidence.sha256
        {
            return Err(io::Error::other(
                "vector blob does not match its committed descriptor",
            ));
        }
        Self::validate_regular_path_identity(path, identity)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(file)
    }

    fn stream_vector_blob_temp(
        db_path: &Path,
        vectors: &HashMap<String, VectorRecord>,
        vector_values: &HashMap<String, Vec<f64>>,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<(PathBuf, ArtifactEvidence, HashMap<String, VectorBlobRef>)> {
        let total_units = u64::try_from(vectors.len())
            .map_err(|_| io::Error::other("vector count does not fit progress counter"))?;
        progress.enter_phase("prepare_refs", 0, Some(total_units), 0, None)?;
        let mut keys: Vec<&String> = vectors.keys().collect();
        keys.sort_unstable();
        let mut offset = 0u64;
        let mut refs = HashMap::with_capacity(vectors.len());
        for (index, key) in keys.iter().enumerate() {
            let vector = &vectors[*key];
            let values = vector_values.get(*key).unwrap_or(&vector.values);
            let len = u32::try_from(values.len())
                .map_err(|_| io::Error::other("vector dimensions exceed u32"))?;
            refs.insert((*key).clone(), VectorBlobRef { offset, len });
            offset = offset
                .checked_add((len as u64) * std::mem::size_of::<f64>() as u64)
                .ok_or_else(|| io::Error::other("vector blob offset overflow"))?;
            progress.advance(
                u64::try_from(index + 1)
                    .map_err(|_| io::Error::other("vector progress count overflow"))?,
                Some(total_units),
                offset,
                None,
            )?;
        }
        let header_bytes = u64::try_from(Self::VECTOR_BLOB_MAGIC.len() + 2)
            .map_err(|_| io::Error::other("vector blob header size overflow"))?;
        let total_bytes = header_bytes
            .checked_add(offset)
            .ok_or_else(|| io::Error::other("vector blob size overflow"))?;
        let (tmp_path, mut file) = Self::create_streaming_temp(db_path, "blob_temp_sync")?;
        let result = (|| {
            progress.enter_phase("vector_write", 0, Some(total_units), 0, Some(total_bytes))?;
            Self::durability_failpoint("before_blob_temp_sync_write")?;
            let bounded = bounded_publication_writer(&mut file, "blob_publication");
            let mut writer = buffered_artifact_writer(bounded);
            writer.write_all(Self::VECTOR_BLOB_MAGIC)?;
            writer.write_all(&Self::VECTOR_BLOB_VERSION.to_le_bytes())?;
            let mut completed_bytes = header_bytes;
            for (index, key) in keys.iter().enumerate() {
                let vector = &vectors[*key];
                let values = vector_values.get(*key).unwrap_or(&vector.values);
                for value in values {
                    writer.write_all(&value.to_le_bytes())?;
                }
                completed_bytes = completed_bytes
                    .checked_add((values.len() as u64) * std::mem::size_of::<f64>() as u64)
                    .ok_or_else(|| io::Error::other("vector progress byte count overflow"))?;
                progress.advance(
                    u64::try_from(index + 1)
                        .map_err(|_| io::Error::other("vector progress count overflow"))?,
                    Some(total_units),
                    completed_bytes,
                    Some(total_bytes),
                )?;
            }
            let evidence = writer.finish()?;
            progress.enter_phase(
                "vector_sync",
                total_units,
                Some(total_units),
                evidence.bytes,
                Some(total_bytes),
            )?;
            Self::sync_streaming_temp(&mut file, &tmp_path, "blob_temp_sync")?;
            Ok::<_, io::Error>((evidence, refs))
        })();
        match result {
            Ok((evidence, refs)) => Ok((tmp_path, evidence, refs)),
            Err(err) => {
                let _ = fs::remove_file(&tmp_path);
                Err(err)
            }
        }
    }

    fn stream_canonical_temp(
        db_path: &Path,
        view: &PersistedStateView<'_>,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<(PathBuf, u64)> {
        let (tmp_path, mut file) = Self::create_streaming_temp(db_path, "json_temp_sync")?;
        let result = (|| {
            progress.enter_phase("json_write", 0, None, 0, None)?;
            Self::durability_failpoint("before_json_temp_sync_write")?;
            let bounded = bounded_publication_writer(&mut file, "json_publication");
            let writer = buffered_artifact_writer(bounded);
            let mut observed = ProgressWrite::new(writer, progress);
            serde_json::to_writer(&mut observed, view)
                .map_err(|err| io::Error::other(format!("serialize state failed: {err}")))?;
            let (writer, observed_bytes) = observed.finish();
            let evidence = writer.finish()?;
            if evidence.bytes != observed_bytes {
                return Err(io::Error::other("JSON progress byte accounting diverged"));
            }
            progress.enter_phase("json_sync", 0, None, evidence.bytes, Some(evidence.bytes))?;
            Self::sync_streaming_temp(&mut file, &tmp_path, "json_temp_sync")?;
            Ok::<_, io::Error>(evidence.bytes)
        })();
        match result {
            Ok(bytes) => Ok((tmp_path, bytes)),
            Err(err) => {
                let _ = fs::remove_file(&tmp_path);
                Err(err)
            }
        }
    }

    fn validate_blob_basename(basename: &str) -> io::Result<()> {
        let path = Path::new(basename);
        if basename.is_empty()
            || path.is_absolute()
            || basename.contains('/')
            || basename.contains('\\')
            || basename == "."
            || basename == ".."
            || path.components().count() != 1
        {
            return Err(io::Error::other(
                "vector blob basename is not a safe basename",
            ));
        }
        Ok(())
    }

    fn sha256_hex(bytes: &[u8]) -> String {
        let digest = Sha256::digest(bytes);
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn open_validated_blob(
        path: &Path,
        descriptor: Option<&VectorBlobDescriptor>,
    ) -> io::Result<(fs::File, Vec<u8>)> {
        let mut file = Self::open_regular_nofollow(path)
            .map_err(|err| io::Error::other(format!("open vector blob failed: {err}")))?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        if raw.len() < Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>() {
            return Err(io::Error::other("vector blob file is truncated"));
        }
        if &raw[..Self::VECTOR_BLOB_MAGIC.len()] != Self::VECTOR_BLOB_MAGIC {
            return Err(io::Error::other("vector blob magic mismatch"));
        }
        let version_start = Self::VECTOR_BLOB_MAGIC.len();
        let version_end = version_start + std::mem::size_of::<u16>();
        let version = u16::from_le_bytes(
            raw[version_start..version_end]
                .try_into()
                .expect("validated slice length"),
        );
        if version != Self::VECTOR_BLOB_VERSION {
            return Err(io::Error::other(format!(
                "vector blob version mismatch: expected {}, got {version}",
                Self::VECTOR_BLOB_VERSION
            )));
        }
        if let Some(descriptor) = descriptor {
            if descriptor.format != version {
                return Err(io::Error::other("vector blob descriptor format mismatch"));
            }
            if descriptor.size != raw.len() as u64 {
                return Err(io::Error::other(
                    "vector blob size does not match descriptor",
                ));
            }
            if descriptor.sha256 != Self::sha256_hex(&raw) {
                return Err(io::Error::other(
                    "vector blob hash does not match descriptor",
                ));
            }
        }
        Ok((file, raw))
    }

    fn read_validated_blob(
        path: &Path,
        descriptor: Option<&VectorBlobDescriptor>,
    ) -> io::Result<Vec<u8>> {
        let (_, raw) = Self::open_validated_blob(path, descriptor)?;
        Ok(raw)
    }

    fn load_vector_values(
        state: &State,
        db_path: &Path,
        legacy_blob_path: &Path,
    ) -> io::Result<HashMap<String, Vec<f64>>> {
        if state.generation == 0 && state.vector_blob.is_some() {
            return Err(io::Error::other(
                "legacy generation zero must not contain a vector blob descriptor",
            ));
        }
        if state.generation > 0 && state.vector_blob.is_none() {
            return Err(io::Error::other(
                "committed generation is missing its vector blob descriptor",
            ));
        }
        let raw_blob = if let Some(descriptor) = state.vector_blob.as_ref() {
            Self::validate_blob_basename(&descriptor.basename)?;
            let blob_path = Self::parent_dir(db_path).join(&descriptor.basename);
            Some(Self::read_validated_blob(&blob_path, Some(descriptor))?)
        } else {
            match fs::symlink_metadata(legacy_blob_path) {
                Ok(_) => Some(Self::read_validated_blob(legacy_blob_path, None)?),
                Err(err) if err.kind() == io::ErrorKind::NotFound => None,
                Err(err) => return Err(err),
            }
        };
        Self::decode_vector_values(state, raw_blob.as_deref())
    }

    fn validate_sha256(value: &str, field: &str) -> io::Result<()> {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(io::Error::other(format!(
                "{field} must be a lowercase SHA-256 hex digest"
            )));
        }
        if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
            return Err(io::Error::other(format!(
                "{field} must be a lowercase SHA-256 hex digest"
            )));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn validate_inherited_readonly_file(
        file: fs::File,
        name: &str,
        max_bytes: u64,
    ) -> io::Result<fs::File> {
        // The caller has already taken ownership of every inherited
        // descriptor, so failure while validating either file closes both.
        // F_GETFL observes the open file description, not merely inode mode.
        let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(io::Error::other(format!(
                "{name} fcntl(F_GETFL) failed: {}",
                io::Error::last_os_error()
            )));
        }
        if flags & libc::O_ACCMODE != libc::O_RDONLY {
            return Err(io::Error::other(format!("{name} must be opened O_RDONLY")));
        }
        #[cfg(target_os = "linux")]
        if flags & libc::O_PATH != 0 {
            return Err(io::Error::other(format!(
                "{name} must be readable, not O_PATH"
            )));
        }
        let metadata = file.metadata()?;
        if !metadata.file_type().is_file() {
            return Err(io::Error::other(format!(
                "{name} must refer to a regular file"
            )));
        }
        if metadata.len() > max_bytes {
            return Err(io::Error::other(format!("{name} exceeds its bounded size")));
        }
        Ok(file)
    }

    #[cfg(not(unix))]
    fn validate_inherited_readonly_file(
        _file: fs::File,
        name: &str,
        _max_bytes: u64,
    ) -> io::Result<fs::File> {
        Err(io::Error::other(format!(
            "descriptor mode is unsupported on this platform ({name})"
        )))
    }

    fn read_bounded_descriptor_file(
        mut file: fs::File,
        max_bytes: u64,
        name: &str,
    ) -> io::Result<Vec<u8>> {
        let size = file.metadata()?.len();
        if size > max_bytes {
            return Err(io::Error::other(format!("{name} exceeds its bounded size")));
        }
        let capacity = usize::try_from(size)
            .map_err(|_| io::Error::other(format!("{name} size does not fit usize")))?;
        let mut raw = Vec::new();
        raw.try_reserve_exact(capacity)
            .map_err(|_| io::Error::other(format!("{name} allocation exceeds bounded capacity")))?;
        file.seek(SeekFrom::Start(0))?;
        file.take(max_bytes.saturating_add(1))
            .read_to_end(&mut raw)?;
        if raw.len() as u64 != size || raw.len() as u64 > max_bytes {
            return Err(io::Error::other(format!(
                "{name} changed or exceeded its bounded size"
            )));
        }
        Ok(raw)
    }

    fn validate_descriptor_blob_bytes(raw: &[u8]) -> io::Result<()> {
        let header_len = Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>();
        if raw.len() < header_len {
            return Err(io::Error::other("vector blob file is truncated"));
        }
        if &raw[..Self::VECTOR_BLOB_MAGIC.len()] != Self::VECTOR_BLOB_MAGIC {
            return Err(io::Error::other("vector blob magic mismatch"));
        }
        let version_start = Self::VECTOR_BLOB_MAGIC.len();
        let version_end = version_start + std::mem::size_of::<u16>();
        let version = u16::from_le_bytes(
            raw[version_start..version_end]
                .try_into()
                .expect("validated slice length"),
        );
        if version != Self::VECTOR_BLOB_VERSION {
            return Err(io::Error::other(format!(
                "vector blob version mismatch: expected {}, got {version}",
                Self::VECTOR_BLOB_VERSION
            )));
        }
        Ok(())
    }

    #[cfg(unix)]
    fn open_descriptor_read(config: DescriptorReadConfig) -> io::Result<Self> {
        if config.canonical_fd < 0 || config.vector_blob_fd < 0 {
            // A valid peer descriptor is still owned by this consumed config.
            for fd in [config.canonical_fd, config.vector_blob_fd] {
                if fd >= 0 {
                    // SAFETY: each non-negative descriptor occurs once in
                    // this branch because the equal case is handled below.
                    drop(unsafe { fs::File::from_raw_fd(fd) });
                }
            }
            return Err(io::Error::other(
                "descriptor FDs must be non-negative integers",
            ));
        }
        if config.canonical_fd == config.vector_blob_fd {
            // SAFETY: config is consumed and the one descriptor is converted
            // exactly once, solely to close it on this rejection path.
            drop(unsafe { fs::File::from_raw_fd(config.canonical_fd) });
            return Err(io::Error::other(
                "canonical and vector blob FDs must be distinct",
            ));
        }
        // SAFETY: config is consumed and the descriptors are distinct. Take
        // ownership of both before either validation can fail.
        let canonical_file = unsafe { fs::File::from_raw_fd(config.canonical_fd) };
        let blob_file = unsafe { fs::File::from_raw_fd(config.vector_blob_fd) };
        let canonical_file = Self::validate_inherited_readonly_file(
            canonical_file,
            "canonical FD",
            MAX_DESCRIPTOR_CANONICAL_BYTES,
        )?;
        let blob_file = Self::validate_inherited_readonly_file(
            blob_file,
            "vector blob FD",
            MAX_DESCRIPTOR_VECTOR_BLOB_BYTES,
        )?;
        if Self::metadata_identity(&canonical_file.metadata()?)
            == Self::metadata_identity(&blob_file.metadata()?)
        {
            return Err(io::Error::other(
                "canonical and vector blob descriptors must name distinct files",
            ));
        }
        let canonical_raw = Self::read_bounded_descriptor_file(
            canonical_file,
            MAX_DESCRIPTOR_CANONICAL_BYTES,
            "canonical FD",
        )?;
        let blob_raw = Self::read_bounded_descriptor_file(
            blob_file,
            MAX_DESCRIPTOR_VECTOR_BLOB_BYTES,
            "vector blob FD",
        )?;
        Self::validate_descriptor_blob_bytes(&blob_raw)?;
        let canonical_sha256 = Self::sha256_hex(&canonical_raw);
        let vector_blob_sha256 = Self::sha256_hex(&blob_raw);
        let state: State = serde_json::from_slice(&canonical_raw)
            .map_err(|err| io::Error::other(format!("parse canonical descriptor failed: {err}")))?;
        Self::validate_state_commit_evidence(&state)?;
        if state.generation > JSON_SAFE_INTEGER_MAX {
            return Err(io::Error::other(
                "canonical generation exceeds MAX_SAFE_INTEGER",
            ));
        }
        if state.generation != config.expected_generation {
            return Err(io::Error::other(
                "canonical generation does not match expectedGeneration",
            ));
        }
        if config.expected_generation == 0 {
            if !config.legacy_generation0 {
                return Err(io::Error::other(
                    "generation zero requires explicit legacyGeneration0",
                ));
            }
            let binding = config
                .legacy_binding_sha256
                .as_deref()
                .ok_or_else(|| io::Error::other("generation zero requires legacy binding hash"))?;
            Self::validate_sha256(binding, "legacy binding hash")?;
            if state.vector_blob.is_some() {
                return Err(io::Error::other(
                    "generation zero must not contain a vectorBlob descriptor",
                ));
            }
        } else {
            if config.legacy_generation0 || config.legacy_binding_sha256.is_some() {
                return Err(io::Error::other(
                    "legacy generation zero metadata is forbidden for positive generations",
                ));
            }
            let descriptor = state.vector_blob.as_ref().ok_or_else(|| {
                io::Error::other("positive generation requires a vectorBlob descriptor")
            })?;
            Self::validate_blob_basename(&descriptor.basename)?;
            if descriptor.size != blob_raw.len() as u64 {
                return Err(io::Error::other(
                    "vector blob size does not match canonical descriptor",
                ));
            }
            Self::validate_sha256(&descriptor.sha256, "vectorBlob.sha256")?;
            if descriptor.sha256 != vector_blob_sha256 {
                return Err(io::Error::other(
                    "vector blob hash does not match canonical descriptor",
                ));
            }
            if descriptor.format != Self::VECTOR_BLOB_VERSION {
                return Err(io::Error::other("vector blob descriptor format mismatch"));
            }
        }
        let vector_values = Self::decode_vector_values(&state, Some(&blob_raw))?;
        let handshake = DescriptorReadHandshake {
            canonical_sha256,
            vector_blob_sha256,
            vector_blob_size: blob_raw.len() as u64,
            legacy_generation0: config.legacy_generation0,
            legacy_binding_sha256: config.legacy_binding_sha256.clone(),
            method_inventory_sha256: Self::descriptor_method_inventory_sha256(),
        };
        Ok(Self {
            db_path: None,
            audit_log_path: None,
            state,
            vector_values,
            cache_dirty: true,
            transaction: TransactionState::Idle,
            wal_path: None,
            wal_bytes: 0,
            wal_record_count: 0,
            wal_hasher: Sha256::new(),
            wal_file: None,
            wal_identity: None,
            active_mutation_request_ids: HashSet::new(),
            wal_replaying: false,
            last_persist_bytes: canonical_raw.len() as u64,
            fatal: false,
            node_keys_by_corpus: HashMap::new(),
            edge_keys_by_corpus: HashMap::new(),
            adjacent_edge_keys_by_node: HashMap::new(),
            vector_keys_by_corpus_namespace: HashMap::new(),
            passage_keys_by_corpus: HashMap::new(),
            access_mode: AccessMode::DescriptorReadOnly(handshake),
        })
    }

    fn decode_vector_values(
        state: &State,
        raw_blob: Option<&[u8]>,
    ) -> io::Result<HashMap<String, Vec<f64>>> {
        let mut values = HashMap::new();
        let payload_offset = Self::VECTOR_BLOB_MAGIC.len() + std::mem::size_of::<u16>();
        for (key, vector) in &state.vectors {
            if !vector.values.is_empty() {
                values.insert(key.clone(), vector.values.clone());
                continue;
            }
            let Some(blob_ref) = &vector.blob_ref else {
                continue;
            };
            let raw_blob = raw_blob.ok_or_else(|| {
                io::Error::other("vector metadata references blob but blob file is missing")
            })?;
            let start = payload_offset
                .checked_add(blob_ref.offset as usize)
                .ok_or_else(|| io::Error::other("vector blob offset overflow"))?;
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
                out.push(f64::from_le_bytes(
                    chunk.try_into().expect("validated f64 chunk length"),
                ));
            }
            values.insert(key.clone(), out);
        }
        Ok(values)
    }

    fn validate_state_commit_evidence(state: &State) -> io::Result<Option<CommitEvidence>> {
        let Some(stored) = state.commit_evidence.as_ref() else {
            return Ok(None);
        };
        if state.generation == 0 {
            return Err(io::Error::other(
                "legacy generation zero must not contain commit evidence",
            ));
        }
        let evidence = stored
            .validate()
            .map_err(|error| io::Error::other(format!("invalid commit evidence: {error}")))?;
        if evidence.generation() != state.generation {
            return Err(io::Error::other(
                "commit evidence generation does not match canonical generation",
            ));
        }
        Ok(Some(evidence))
    }

    fn prepared_evidence_value(evidence: &PreparedCommitEvidence) -> Value {
        serde_json::from_slice(&evidence.canonical_bytes())
            .expect("validated prepared evidence is canonical JSON")
    }

    fn prepared_evidence_from_params(params: &Value) -> Result<PreparedCommitEvidence, AppError> {
        let params = params.as_object().ok_or_else(|| {
            Self::execution_client_error("batch_commit params must be an object".to_string())
        })?;
        if params.len() != 1 {
            return Err(Self::execution_client_error(
                "batch_commit requires only preparedCommitEvidence".to_string(),
            ));
        }
        let raw = params
            .get("preparedCommitEvidence")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                Self::execution_client_error(
                    "batch_commit requires preparedCommitEvidence".to_string(),
                )
            })?;
        let required = [
            "schema",
            "transactionNonce",
            "baseGeneration",
            "generation",
            "walSha256",
            "walBytes",
            "walRecordCount",
        ];
        if raw.len() != required.len() || required.iter().any(|field| !raw.contains_key(*field)) {
            return Err(Self::execution_client_error(
                "preparedCommitEvidence has an invalid field set".to_string(),
            ));
        }
        if raw.get("schema").and_then(Value::as_str) != Some("PreparedCommitEvidence@1") {
            return Err(Self::execution_client_error(
                "preparedCommitEvidence schema is invalid".to_string(),
            ));
        }
        PreparedCommitEvidence::new(
            raw.get("transactionNonce")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            raw.get("baseGeneration")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX),
            raw.get("generation")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX),
            raw.get("walSha256")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string(),
            raw.get("walBytes")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX),
            raw.get("walRecordCount")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX),
        )
        .map_err(Self::execution_client_error)
    }

    fn prepare_commit(&mut self) -> io::Result<PreparedCommitEvidence> {
        if let TransactionState::Prepared { evidence, .. } = &self.transaction {
            return Ok(evidence.clone());
        }
        let (base_generation, transaction_nonce, mutation_seen) = match &self.transaction {
            TransactionState::Active {
                base_generation,
                transaction_nonce,
                mutation_seen,
            } => (*base_generation, transaction_nonce.clone(), *mutation_seen),
            _ => {
                return Err(io::Error::other("prepare requires an active mutated batch"));
            }
        };
        if !mutation_seen {
            return Err(io::Error::other(
                "prepare requires a successful durable mutation",
            ));
        }
        if base_generation != self.state.generation {
            return Err(io::Error::other(
                "active batch base generation does not match canonical generation",
            ));
        }
        let wal_path = self.require_wal_path()?;
        let file = self
            .wal_file
            .as_ref()
            .ok_or_else(|| io::Error::other("durable WAL descriptor is missing"))?;
        let metadata = file.metadata()?;
        let identity = Self::metadata_identity(&metadata);
        Self::validate_regular_path_identity(wal_path, identity)?;
        if self.wal_identity != Some(identity) || self.wal_bytes != metadata.len() {
            return Err(io::Error::other(
                "durable WAL identity or byte count changed before prepare",
            ));
        }
        if self.wal_bytes == 0 || self.wal_record_count == 0 {
            return Err(io::Error::other(
                "prepare requires one non-empty canonical WAL generation",
            ));
        }
        let digest: [u8; 32] = self.wal_hasher.clone().finalize().into();
        let evidence = PreparedCommitEvidence::new(
            transaction_nonce,
            base_generation,
            base_generation
                .checked_add(1)
                .ok_or_else(|| io::Error::other("generation overflow"))?,
            Self::digest_hex(&digest),
            self.wal_bytes,
            self.wal_record_count,
        )
        .map_err(io::Error::other)?;
        self.transaction = TransactionState::Prepared {
            evidence: evidence.clone(),
            wal_identity: identity,
        };
        Ok(evidence)
    }

    fn persist(
        &mut self,
        supplied_evidence: &PreparedCommitEvidence,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<DurableGenerationToken> {
        let start = std::time::Instant::now();
        let current_generation = self.state.generation;
        let (prepared_evidence, prepared_identity) = match &self.transaction {
            TransactionState::Prepared {
                evidence,
                wal_identity,
            } => (evidence.clone(), *wal_identity),
            _ => return Err(io::Error::other("generation publication requires prepare")),
        };
        if &prepared_evidence != supplied_evidence {
            return Err(io::Error::other(
                "batch_commit evidence does not match the prepared transaction",
            ));
        }
        let base_generation = prepared_evidence.base_generation();
        if base_generation != current_generation {
            return Err(io::Error::other(
                "prepared base generation does not match canonical generation",
            ));
        }
        let next_generation = current_generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("generation overflow"))?;
        progress.enter_phase(
            "wal_verify",
            0,
            None,
            0,
            Some(prepared_evidence.wal_bytes()),
        )?;
        let wal_evidence = self
            .scan_wal_with_identity_progress(false, progress)?
            .ok_or_else(|| io::Error::other("durable WAL disappeared before publication"))?;
        if wal_evidence.base_generation != Some(current_generation) {
            return Err(io::Error::other(
                "cannot commit with WAL records from a different generation",
            ));
        }
        if wal_evidence.record_count == 0 {
            return Err(io::Error::other(
                "cannot publish a generation without a durable WAL mutation",
            ));
        }
        if wal_evidence.identity != prepared_identity
            || wal_evidence.bytes != prepared_evidence.wal_bytes()
            || wal_evidence.record_count != prepared_evidence.wal_record_count()
            || Self::digest_hex(&wal_evidence.digest) != prepared_evidence.wal_sha256()
        {
            return Err(io::Error::other(
                "durable WAL does not match prepared commit evidence",
            ));
        }
        let commit_evidence = prepared_evidence.commit_evidence();
        let stored_commit_evidence = StoredCommitEvidence::from_validated(&commit_evidence);

        let db_path = self.require_db_path()?;
        let parent = Self::parent_dir(db_path);
        fs::create_dir_all(&parent)
            .map_err(|err| io::Error::other(format!("create database directory failed: {err}")))?;

        let (blob_tmp, blob_evidence, prepared_vector_refs) = Self::stream_vector_blob_temp(
            db_path,
            &self.state.vectors,
            &self.vector_values,
            progress,
        )?;
        let blob_basename = format!(
            "{}.g{next_generation:020}.{}.vblob",
            db_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("vectors"),
            blob_evidence.sha256,
        );
        let blob_descriptor = VectorBlobDescriptor {
            basename: blob_basename,
            size: blob_evidence.bytes,
            sha256: blob_evidence.sha256,
            format: Self::VECTOR_BLOB_VERSION,
        };
        let blob_path = parent.join(&blob_descriptor.basename);

        progress.enter_phase("vector_publish", 0, None, 0, Some(blob_evidence.bytes))?;
        match fs::symlink_metadata(&blob_path) {
            Ok(metadata) => {
                Self::require_regular_single_link(&blob_path, &metadata)?;
                let file =
                    Self::validate_blob_streaming_progress(&blob_path, &blob_descriptor, progress)?;
                Self::durability_failpoint("blob_existing_sync")?;
                file.sync_all().map_err(|err| {
                    io::Error::other(format!("sync existing vector blob failed: {err}"))
                })?;
                fs::remove_file(&blob_tmp)?;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                if let Err(err) = Self::durability_failpoint("before_blob_rename") {
                    let _ = fs::remove_file(&blob_tmp);
                    return Err(err);
                }
                if let Err(err) = Self::rename_noreplace(&blob_tmp, &blob_path) {
                    let _ = fs::remove_file(&blob_tmp);
                    return Err(io::Error::other(format!(
                        "publish vector blob failed: {err}"
                    )));
                }
                Self::durability_failpoint("after_blob_rename")?;
                Self::validate_blob_streaming_progress(&blob_path, &blob_descriptor, progress)?;
            }
            Err(err) => {
                let _ = fs::remove_file(&blob_tmp);
                return Err(err);
            }
        }
        progress.enter_phase(
            "vector_dir_sync",
            0,
            None,
            blob_evidence.bytes,
            Some(blob_evidence.bytes),
        )?;
        Self::durability_failpoint("before_blob_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_blob_dir_fsync")?;

        let persisted_state = PersistedStateView {
            state: &self.state,
            generation: next_generation,
            vector_blob: &blob_descriptor,
            vector_refs: &prepared_vector_refs,
            commit_evidence: &stored_commit_evidence,
        };
        match fs::symlink_metadata(db_path) {
            Ok(metadata) => Self::require_regular_single_link(db_path, &metadata)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let (json_tmp, json_bytes) =
            Self::stream_canonical_temp(db_path, &persisted_state, progress)?;
        progress.enter_phase("json_publish", 0, None, json_bytes, Some(json_bytes))?;
        if let Err(err) = Self::durability_failpoint("before_json_rename") {
            let _ = fs::remove_file(&json_tmp);
            return Err(err);
        }
        if let Err(err) = fs::rename(&json_tmp, db_path) {
            let _ = fs::remove_file(&json_tmp);
            return Err(io::Error::other(format!(
                "publish canonical state failed: {err}"
            )));
        }
        Self::durability_failpoint("after_json_rename")?;
        progress.enter_phase("json_dir_sync", 0, None, json_bytes, Some(json_bytes))?;
        Self::durability_failpoint("before_json_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_json_dir_fsync")?;

        Self::durability_failpoint("before_wal_retire")?;
        self.retire_wal_exact_progress(wal_evidence, progress)
            .map_err(|err| io::Error::other(format!("retire WAL failed: {err}")))?;
        Self::durability_failpoint("after_wal_retire")?;

        self.state.generation = next_generation;
        self.state.vector_blob = Some(blob_descriptor.clone());
        self.state.commit_evidence = Some(stored_commit_evidence.clone());
        for (key, blob_ref) in prepared_vector_refs {
            let vector = self
                .state
                .vectors
                .get_mut(&key)
                .expect("prepared vector key came from live state");
            vector.values.clear();
            vector.blob_ref = Some(blob_ref);
        }
        self.last_persist_bytes = json_bytes;
        self.wal_bytes = 0;
        self.wal_record_count = 0;
        self.wal_hasher = Sha256::new();
        progress.enter_phase("complete", 0, None, 0, None)?;
        let total_ms = start.elapsed().as_millis();
        eprintln!(
            "[persist] generation={} blobBytes={} blobSha256={} jsonBytes={} elapsedMs={total_ms}",
            next_generation, blob_descriptor.size, blob_descriptor.sha256, json_bytes,
        );
        Ok(DurableGenerationToken {
            generation: next_generation,
            vector_blob: blob_descriptor,
            commit_evidence: stored_commit_evidence,
        })
    }

    fn persist_if_needed(&mut self) -> io::Result<()> {
        // Mutations are published only by the explicit batch_commit path.
        // Keeping this helper as a no-op makes it impossible for a collection
        // handler or an EOF path to become an accidental publication entrypoint.
        Ok(())
    }

    fn method_spec(method: &str) -> Option<&'static MethodSpec> {
        METHOD_SPECS.iter().find(|spec| spec.name == method)
    }

    fn is_indexing_memory_method(method: &str) -> bool {
        Self::method_spec(method)
            .is_some_and(|spec| spec.wire_profile == MethodWireProfile::BoundedIndexing)
    }

    fn require_db_path(&self) -> io::Result<&Path> {
        self.db_path.as_deref().ok_or_else(|| {
            io::Error::other("filesystem database path is unavailable in descriptor mode")
        })
    }

    fn require_wal_path(&self) -> io::Result<&Path> {
        self.wal_path
            .as_deref()
            .ok_or_else(|| io::Error::other("WAL path is unavailable in descriptor mode"))
    }

    fn descriptor_method_specs() -> impl Iterator<Item = &'static MethodSpec> {
        METHOD_SPECS
            .iter()
            // The descriptor checkpoint exposes only health. The three
            // bounded retrieval methods are added here when their contract
            // implementation lands; no legacy read is implicitly admitted.
            .filter(|spec| matches!(spec.name, "ping" | "protocol_info"))
    }

    fn descriptor_method_inventory() -> Vec<Value> {
        Self::descriptor_method_specs()
            .map(|spec| {
                json!({
                    "name": spec.name,
                    "classification": spec.classification,
                    "wal": spec.wal,
                })
            })
            .collect()
    }

    fn descriptor_method_inventory_sha256() -> String {
        let encoded = Self::descriptor_method_specs()
            .map(|spec| format!("{}\t{}\t{}\n", spec.name, spec.classification, spec.wal))
            .collect::<String>();
        Self::sha256_hex(encoded.as_bytes())
    }

    /// WAL admission and protocol_info both use METHOD_SPECS. Unknown
    /// methods are not treated as reads and therefore never enter the WAL.
    fn is_mutating_method(method: &str) -> bool {
        Self::method_spec(method).is_some_and(|spec| spec.wal)
    }

    fn validate_new_mutation_request_id(&self, request_id: u64) -> Result<(), AppError> {
        if self.active_mutation_request_ids.contains(&request_id) {
            return Err(Self::execution_client_error(
                "mutation request id is already present in the active transaction".to_string(),
            ));
        }
        Ok(())
    }

    fn scan_wal_file(
        file: &mut fs::File,
        allow_legacy_generation0: bool,
    ) -> io::Result<WalScanEvidence> {
        let mut progress = NoCommitProgress;
        Self::scan_wal_file_with_limits_and_progress(
            file,
            allow_legacy_generation0,
            MAX_WAL_RECORD_BYTES,
            MAX_WAL_BYTES,
            MAX_WAL_RECORDS,
            &mut progress,
        )
    }

    #[cfg(test)]
    fn scan_wal_file_with_limits(
        file: &mut fs::File,
        allow_legacy_generation0: bool,
        record_limit: u64,
        wal_limit: u64,
        record_count_limit: u64,
    ) -> io::Result<WalScanEvidence> {
        let mut progress = NoCommitProgress;
        Self::scan_wal_file_with_limits_and_progress(
            file,
            allow_legacy_generation0,
            record_limit,
            wal_limit,
            record_count_limit,
            &mut progress,
        )
    }

    fn scan_wal_file_with_limits_and_progress(
        file: &mut fs::File,
        allow_legacy_generation0: bool,
        record_limit: u64,
        wal_limit: u64,
        record_count_limit: u64,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<WalScanEvidence> {
        let before = file.metadata()?;
        let identity = Self::metadata_identity(&before);
        if before.len() > wal_limit {
            return Err(io::Error::other("WAL exceeds aggregate byte limit"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut source = io::BufReader::new(file);
        let mut wal_hasher = Sha256::new();
        let mut bytes = 0u64;
        let mut record_count = 0u64;
        let mut base_generation = None;
        let mut legacy_raw = false;
        while !source.fill_buf()?.is_empty() {
            if record_count >= record_count_limit {
                return Err(io::Error::other("WAL exceeds aggregate record limit"));
            }
            let record_reader =
                StrictLfRecordReader::new(&mut source, record_limit, &mut wal_hasher);
            let guarded_record = GuardedJsonReader::new(record_reader);
            let observed_record =
                ProgressRead::new(guarded_record, progress, record_count, bytes, before.len());
            let mut buffered_record = io::BufReader::with_capacity(64 * 1024, observed_record);
            let mut deserializer = serde_json::Deserializer::from_reader(&mut buffered_record);
            let envelope = ScannedWalEnvelope::deserialize(&mut deserializer)
                .map_err(|error| io::Error::other(format!("parse WAL record failed: {error}")))?;
            deserializer
                .end()
                .map_err(|error| io::Error::other(format!("parse WAL record failed: {error}")))?;
            drop(deserializer);
            let record_reader = buffered_record.into_inner().into_inner().into_inner();
            let record_evidence = record_reader.finish()?;
            let (record_base_generation, request) = match envelope {
                ScannedWalEnvelope {
                    version: Present::Value(version),
                    base_generation: Present::Value(base_generation),
                    request: Present::Value(request),
                    id: Present::Missing,
                    method: Present::Missing,
                    params: Present::Missing,
                } => {
                    if version != Self::WAL_VERSION {
                        return Err(io::Error::other(format!(
                            "unsupported WAL version {version}"
                        )));
                    }
                    if legacy_raw {
                        return Err(io::Error::other("WAL mixes legacy and v2 records"));
                    }
                    (base_generation, request)
                }
                ScannedWalEnvelope {
                    version: Present::Missing,
                    base_generation: Present::Missing,
                    request: Present::Missing,
                    id: Present::Value(id),
                    method: Present::Value(method),
                    params: Present::Value(params),
                } if allow_legacy_generation0
                    && base_generation.is_none_or(|base| base == 0)
                    && (record_count == 0 || legacy_raw) =>
                {
                    legacy_raw = true;
                    (0, ScannedRpcRequest { id, method, params })
                }
                _ => return Err(io::Error::other("WAL record has an invalid envelope shape")),
            };
            if !Self::is_mutating_method(&request.method) {
                return Err(io::Error::other(
                    "WAL record contains a non-mutating method",
                ));
            }
            if request.method == "memory_save_file" {
                return Err(io::Error::other(
                    "WAL record contains a non-canonical memory_save_file request",
                ));
            }
            if base_generation.is_some_and(|base| base != record_base_generation) {
                return Err(io::Error::other("WAL contains mixed base generations"));
            }
            base_generation = Some(record_base_generation);
            let _ = (request.id, request.params);
            bytes = bytes
                .checked_add(record_evidence.bytes)
                .ok_or_else(|| io::Error::other("WAL aggregate byte count overflow"))?;
            if bytes > wal_limit {
                return Err(io::Error::other("WAL exceeds aggregate byte limit"));
            }
            record_count = record_count
                .checked_add(1)
                .ok_or_else(|| io::Error::other("WAL aggregate record count overflow"))?;
            progress.advance(record_count, None, bytes, Some(before.len()))?;
        }
        let after = source.get_ref().metadata()?;
        if Self::metadata_identity(&after) != identity
            || after.len() != before.len()
            || bytes != before.len()
        {
            return Err(io::Error::other("WAL changed during bounded scan"));
        }
        Ok(WalScanEvidence {
            identity,
            bytes,
            record_count,
            digest: wal_hasher.finalize().into(),
            base_generation,
            legacy_raw,
        })
    }

    fn scan_wal_with_identity(
        &mut self,
        allow_legacy_generation0: bool,
    ) -> io::Result<Option<WalScanEvidence>> {
        let mut progress = NoCommitProgress;
        self.scan_wal_with_identity_progress(allow_legacy_generation0, &mut progress)
    }

    fn scan_wal_with_identity_progress(
        &mut self,
        allow_legacy_generation0: bool,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<Option<WalScanEvidence>> {
        let wal_path = self.require_wal_path()?.to_path_buf();
        let mut file = match self.wal_file.take() {
            Some(file) => file,
            None => match Self::open_wal_readwrite_nofollow(&wal_path) {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
                Err(error) => return Err(io::Error::other(format!("open WAL failed: {error}"))),
            },
        };
        let evidence = Self::scan_wal_file_with_limits_and_progress(
            &mut file,
            allow_legacy_generation0,
            MAX_WAL_RECORD_BYTES,
            MAX_WAL_BYTES,
            MAX_WAL_RECORDS,
            progress,
        )?;
        Self::validate_regular_path_identity(&wal_path, evidence.identity)?;
        self.wal_file = Some(file);
        Ok(Some(evidence))
    }

    fn hash_wal_file_progress(
        file: &mut fs::File,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<((u64, u64), u64, [u8; 32])> {
        let before = file.metadata()?;
        let identity = Self::metadata_identity(&before);
        if before.len() > MAX_WAL_BYTES {
            return Err(io::Error::other("WAL exceeds aggregate byte limit"));
        }
        file.seek(SeekFrom::Start(0))?;
        let mut hasher = Sha256::new();
        let mut bytes = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            bytes = bytes
                .checked_add(read as u64)
                .ok_or_else(|| io::Error::other("WAL aggregate byte count overflow"))?;
            if bytes > MAX_WAL_BYTES {
                return Err(io::Error::other("WAL exceeds aggregate byte limit"));
            }
            hasher.update(&buffer[..read]);
            progress.advance(0, None, bytes, Some(before.len()))?;
        }
        let after = file.metadata()?;
        if Self::metadata_identity(&after) != identity
            || after.len() != before.len()
            || bytes != before.len()
        {
            return Err(io::Error::other("WAL changed during bounded hash"));
        }
        Ok((identity, bytes, hasher.finalize().into()))
    }

    fn retire_wal_exact(&mut self, expected: WalScanEvidence) -> io::Result<()> {
        let mut progress = NoCommitProgress;
        self.retire_wal_exact_progress(expected, &mut progress)
    }

    fn retire_wal_exact_progress(
        &mut self,
        expected: WalScanEvidence,
        progress: &mut dyn CommitProgress,
    ) -> io::Result<()> {
        let wal_path = self.require_wal_path()?.to_path_buf();
        let mut held_wal = self
            .wal_file
            .take()
            .ok_or_else(|| io::Error::other("held WAL descriptor is missing"))?;
        if Self::metadata_identity(&held_wal.metadata()?) != expected.identity {
            return Err(io::Error::other("WAL identity changed before retirement"));
        }
        progress.enter_phase("wal_zero", 0, None, 0, Some(expected.bytes))?;
        if Self::hash_wal_file_progress(&mut held_wal, progress)?
            != (expected.identity, expected.bytes, expected.digest)
        {
            return Err(io::Error::other("WAL content changed before retirement"));
        }
        Self::durability_failpoint("before_wal_zero")?;
        held_wal.set_len(0)?;
        Self::durability_failpoint("after_wal_zero")?;
        progress.enter_phase("wal_sync", 0, None, 0, Some(0))?;
        Self::durability_failpoint("before_wal_zero_sync")?;
        held_wal.sync_all()?;
        Self::durability_failpoint("after_wal_zero_sync")?;
        Self::durability_failpoint("before_wal_zero_validate")?;
        let metadata = held_wal.metadata()?;
        if Self::metadata_identity(&metadata) != expected.identity || metadata.len() != 0 {
            return Err(io::Error::other(
                "held WAL did not reach the exact durable zero state",
            ));
        }
        Self::validate_regular_path_identity(&wal_path, expected.identity)?;
        Self::durability_failpoint("after_wal_zero_validate")?;
        self.wal_identity = Some(expected.identity);
        self.wal_file = Some(held_wal);
        Ok(())
    }

    #[cfg(test)]
    fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn digest_hex(digest: &[u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn stream_wal_record<W: Write>(
        writer: W,
        base_generation: u64,
        request: &RpcRequest,
        wal_hasher: Option<Sha256>,
    ) -> io::Result<WalWriteEvidence<W>> {
        Self::stream_wal_record_with_limit(
            writer,
            base_generation,
            request,
            wal_hasher,
            MAX_WAL_RECORD_BYTES,
        )
    }

    #[allow(dead_code)] // Test seam for max/max+1 without allocating 512 MiB.
    fn stream_wal_record_with_limit<W: Write>(
        writer: W,
        base_generation: u64,
        request: &RpcRequest,
        wal_hasher: Option<Sha256>,
        record_limit: u64,
    ) -> io::Result<WalWriteEvidence<W>> {
        let record = WalRecordRef {
            version: Self::WAL_VERSION,
            base_generation,
            request,
        };
        let mut writer = LimitedHashWriter::new(writer, record_limit, wal_hasher);
        serde_json::to_writer(&mut writer, &record)
            .map_err(|err| io::Error::other(format!("serialize WAL record failed: {err}")))?;
        writer.write_all(b"\n")?;
        Ok(writer.finish())
    }

    #[allow(dead_code)] // Used by the atomic writer switch after reader review.
    fn require_same_wal_record<A, B>(
        measured: &WalWriteEvidence<A>,
        written: &WalWriteEvidence<B>,
    ) -> io::Result<()> {
        if measured.bytes != written.bytes || measured.record_digest != written.record_digest {
            return Err(io::Error::other(
                "WAL request changed between counting and append passes",
            ));
        }
        Ok(())
    }

    fn open_wal_for_append(&self) -> io::Result<(fs::File, bool, (u64, u64))> {
        let wal_path = self.require_wal_path()?;
        for _ in 0..2 {
            match fs::symlink_metadata(wal_path) {
                Ok(path_metadata) => {
                    Self::require_regular_single_link(wal_path, &path_metadata)?;
                    let mut options = fs::OpenOptions::new();
                    options.read(true).write(true).append(true);
                    #[cfg(unix)]
                    options.custom_flags(libc::O_NOFOLLOW);
                    let file = options.open(wal_path)?;
                    let file_metadata = file.metadata()?;
                    Self::require_regular_single_link(wal_path, &file_metadata)?;
                    if Self::metadata_identity(&path_metadata)
                        != Self::metadata_identity(&file_metadata)
                    {
                        return Err(io::Error::other(
                            "WAL changed while being opened for append",
                        ));
                    }
                    let identity = Self::metadata_identity(&file_metadata);
                    if self.wal_identity != Some(identity) {
                        return Err(io::Error::other("unexpected WAL inode before append"));
                    }
                    return Ok((file, false, identity));
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {
                    if self.wal_identity.is_some() {
                        return Err(io::Error::other("active WAL disappeared before append"));
                    }
                    let mut options = fs::OpenOptions::new();
                    options.read(true).write(true).append(true).create_new(true);
                    #[cfg(unix)]
                    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
                    match options.open(wal_path) {
                        Ok(file) => {
                            let metadata = file.metadata()?;
                            Self::require_regular_single_link(wal_path, &metadata)?;
                            let identity = Self::metadata_identity(&metadata);
                            return Ok((file, true, identity));
                        }
                        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
                        Err(err) => return Err(err),
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Err(io::Error::other(
            "WAL path changed repeatedly while being opened for append",
        ))
    }

    fn wal_append(&mut self, request: &RpcRequest) -> io::Result<()> {
        if self.active_mutation_request_ids.contains(&request.id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "duplicate mutation request id reached WAL append",
            ));
        }
        let measured = Self::stream_wal_record(io::sink(), self.state.generation, request, None)?;
        let next_bytes = self
            .wal_bytes
            .checked_add(measured.bytes)
            .ok_or_else(|| io::Error::other("WAL aggregate byte count overflow"))?;
        if next_bytes > MAX_WAL_BYTES {
            return Err(io::Error::other("WAL exceeds aggregate byte limit"));
        }
        let next_record_count = self
            .wal_record_count
            .checked_add(1)
            .ok_or_else(|| io::Error::other("WAL aggregate record count overflow"))?;
        if next_record_count > MAX_WAL_RECORDS {
            return Err(io::Error::other("WAL exceeds aggregate record limit"));
        }
        let wal_path = self.require_wal_path()?.to_path_buf();
        let parent = Self::parent_dir(&wal_path);
        fs::create_dir_all(&parent)?;
        let (file, wal_created, wal_identity) = match self.wal_file.take() {
            Some(file) => {
                let identity = Self::metadata_identity(&file.metadata()?);
                if self.wal_identity != Some(identity) {
                    return Err(io::Error::other("held WAL identity changed before append"));
                }
                Self::validate_regular_path_identity(&wal_path, identity)?;
                (file, false, identity)
            }
            None => self.open_wal_for_append()?,
        };
        if file.metadata()?.len() != self.wal_bytes {
            return Err(io::Error::other("WAL byte count changed before append"));
        }
        Self::durability_failpoint("before_wal_write")?;
        let buffered_file = io::BufWriter::with_capacity(64 * 1024, file);
        let written = Self::stream_wal_record(
            buffered_file,
            self.state.generation,
            request,
            Some(self.wal_hasher.clone()),
        )?;
        Self::require_same_wal_record(&measured, &written)?;
        let WalWriteEvidence {
            inner: mut buffered_file,
            bytes: _,
            record_digest: _,
            wal_hasher,
        } = written;
        buffered_file.flush()?;
        let file = buffered_file
            .into_inner()
            .map_err(|error| error.into_error())?;
        Self::durability_failpoint("after_wal_write")?;
        Self::durability_failpoint("before_wal_sync")?;
        file.sync_data()?;
        Self::durability_failpoint("after_wal_sync")?;
        if wal_created {
            Self::durability_failpoint("before_wal_dir_fsync")?;
            Self::sync_parent_dir(&parent)?;
            Self::durability_failpoint("after_wal_dir_fsync")?;
        }
        Self::durability_pausepoint("after_wal_sync_before_identity_check")?;
        Self::validate_regular_path_identity(&wal_path, wal_identity)?;
        if file.metadata()?.len() != next_bytes {
            return Err(io::Error::other("WAL byte count changed after append"));
        }
        self.wal_identity = Some(wal_identity);
        self.wal_bytes = next_bytes;
        self.wal_record_count = next_record_count;
        self.wal_hasher = wal_hasher.expect("WAL append always carries rolling hash state");
        self.wal_file = Some(file);
        let inserted = self.active_mutation_request_ids.insert(request.id);
        debug_assert!(
            inserted,
            "mutation request id was checked before WAL append"
        );
        Ok(())
    }

    fn replay_wal(&mut self) -> io::Result<usize> {
        let generation = self.state.generation;
        let wal_evidence = self.scan_wal_with_identity(generation == 0)?;
        let Some(wal_evidence) = wal_evidence else {
            self.wal_bytes = 0;
            self.wal_record_count = 0;
            self.wal_hasher = Sha256::new();
            self.wal_identity = None;
            self.transaction = TransactionState::Idle;
            eprintln!("[wal] recoveryPending=false skipped=0 bytes=0");
            return Ok(0);
        };
        if wal_evidence.record_count == 0 {
            if wal_evidence.bytes != 0 {
                return Err(io::Error::other(
                    "zero-record WAL has nonzero durable bytes",
                ));
            }
            self.wal_bytes = 0;
            self.wal_record_count = 0;
            self.wal_hasher = Sha256::new();
            self.wal_identity = Some(wal_evidence.identity);
            self.transaction = TransactionState::Idle;
            eprintln!("[wal] recoveryPending=false skipped=0 bytes=0");
            return Ok(0);
        }
        let wal_base_generation = wal_evidence
            .base_generation
            .expect("non-empty scanned WAL has a base generation");
        if wal_base_generation > generation {
            return Err(io::Error::other(format!(
                "WAL base generation {wal_base_generation} is newer than canonical generation {generation}",
            )));
        }
        if wal_base_generation < generation {
            if let Some(stored) = self.state.commit_evidence.as_ref() {
                let evidence = stored.validate().map_err(|error| {
                    io::Error::other(format!("invalid canonical commit evidence: {error}"))
                })?;
                if wal_base_generation != evidence.base_generation()
                    || generation != evidence.generation()
                    || wal_evidence.bytes != evidence.wal_bytes()
                    || wal_evidence.record_count != evidence.wal_record_count()
                    || Self::digest_hex(&wal_evidence.digest) != evidence.wal_sha256()
                {
                    return Err(io::Error::other(
                        "committed WAL residue does not match canonical commit evidence",
                    ));
                }
            }
            Self::durability_failpoint("before_wal_rewrite_retire")?;
            self.retire_wal_exact(wal_evidence)?;
            Self::durability_failpoint("after_wal_rewrite_retire")?;
            self.wal_bytes = 0;
            self.wal_record_count = 0;
            self.wal_hasher = Sha256::new();
            self.transaction = TransactionState::Idle;
            eprintln!(
                "[wal] recoveryPending=false skipped={} bytes=0",
                wal_evidence.record_count
            );
            return Ok(0);
        }
        self.wal_bytes = wal_evidence.bytes;
        self.wal_record_count = wal_evidence.record_count;
        let digest = wal_evidence.digest;
        let (wal_device, wal_inode) = wal_evidence.identity;
        self.transaction = TransactionState::RecoveryPending {
            base_generation: generation,
            wal_digest: digest,
            record_count: usize::try_from(wal_evidence.record_count)
                .map_err(|_| io::Error::other("WAL record count does not fit usize"))?,
            wal_device,
            wal_inode,
        };
        eprintln!(
            "[wal] recoveryPending=true generation={} records={} skipped={} bytes={} digest={}",
            generation,
            wal_evidence.record_count,
            0,
            self.wal_bytes,
            Self::digest_hex(&digest),
        );
        usize::try_from(wal_evidence.record_count)
            .map_err(|_| io::Error::other("WAL record count does not fit usize"))
    }

    fn discard_recovery(&mut self, params: &Value) -> Result<Value, AppError> {
        let TransactionState::RecoveryPending {
            base_generation,
            wal_digest,
            record_count,
            wal_device,
            wal_inode,
        } = self.transaction
        else {
            return Err(Self::execution_client_error(
                "recovery_discard requires recovery pending".to_string(),
            ));
        };
        let object = params.as_object().ok_or_else(|| {
            Self::execution_client_error("recovery params must be an object".to_string())
        })?;
        let expected_generation = object
            .get("baseGeneration")
            .and_then(Value::as_u64)
            .ok_or_else(|| Self::execution_client_error("missing baseGeneration".to_string()))?;
        let expected_digest = object
            .get("walDigest")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::execution_client_error("missing walDigest".to_string()))?;
        let actual_digest = Self::digest_hex(&wal_digest);
        if expected_generation != base_generation || expected_digest != actual_digest {
            return Err(Self::execution_client_error(
                "recovery compare-and-swap failed".to_string(),
            ));
        }
        let current = self
            .scan_wal_with_identity(base_generation == 0)
            .map_err(|err| Self::execution_io_error(format!("revalidate recovery WAL: {err}")))?;
        let Some(current) = current else {
            return Err(Self::execution_io_error(
                "recovery WAL disappeared before quarantine".to_string(),
            ));
        };
        if current.identity != (wal_device, wal_inode)
            || current.digest != wal_digest
            || current.record_count != record_count as u64
            || current.base_generation != Some(base_generation)
        {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine".to_string(),
            ));
        }
        let wal_path = self
            .require_wal_path()
            .map_err(|err| Self::execution_io_error(err.to_string()))?
            .to_path_buf();
        let mut held_wal = self.wal_file.take().ok_or_else(|| {
            Self::execution_io_error("held recovery WAL descriptor is missing".to_string())
        })?;
        let held_metadata = held_wal
            .metadata()
            .map_err(|err| Self::execution_io_error(format!("inspect held recovery WAL: {err}")))?;
        if Self::metadata_identity(&held_metadata) != (wal_device, wal_inode) {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine hold".to_string(),
            ));
        }
        if Self::scan_wal_file(&mut held_wal, current.legacy_raw)
            .map_err(|err| Self::execution_io_error(format!("scan held recovery WAL: {err}")))?
            != current
        {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine rename".to_string(),
            ));
        }
        let parent = Self::parent_dir(&wal_path);
        let nonce = Self::crypto_nonce().map_err(|err| {
            Self::execution_io_error(format!("create recovery nonce failed: {err}"))
        })?;
        let file_name = wal_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("aira-graphdb.agdb.wal");
        let quarantine = parent.join(format!(
            ".{file_name}.recovery-{actual_digest}-{nonce}.quarantine"
        ));
        Self::durability_pausepoint("before_recovery_quarantine_rename").map_err(|err| {
            Self::execution_io_error(format!("pause recovery quarantine failed: {err}"))
        })?;
        Self::durability_failpoint("before_recovery_quarantine_rename").map_err(|err| {
            Self::execution_io_error(format!("quarantine recovery WAL failed: {err}"))
        })?;
        Self::rename_noreplace(&wal_path, &quarantine).map_err(|err| {
            Self::execution_io_error(format!("quarantine recovery WAL failed: {err}"))
        })?;
        let quarantine_metadata = Self::open_regular_nofollow(&quarantine)
            .and_then(|file| file.metadata())
            .map_err(|err| {
                Self::execution_io_error(format!("validate quarantined recovery WAL: {err}"))
            })?;
        if Self::metadata_identity(&quarantine_metadata) != (wal_device, wal_inode) {
            return Err(Self::execution_io_error(
                "recovery WAL changed during quarantine".to_string(),
            ));
        }
        if Self::scan_wal_file(&mut held_wal, current.legacy_raw).map_err(|err| {
            Self::execution_io_error(format!("re-scan quarantined recovery WAL: {err}"))
        })? != current
        {
            return Err(Self::execution_io_error(
                "recovery WAL changed during quarantine".to_string(),
            ));
        }
        Self::durability_failpoint("after_recovery_quarantine_rename").map_err(|err| {
            Self::execution_io_error(format!("quarantine recovery WAL failed: {err}"))
        })?;
        Self::durability_failpoint("before_recovery_quarantine_dir_fsync").map_err(|err| {
            Self::execution_io_error(format!("sync recovery quarantine failed: {err}"))
        })?;
        Self::sync_parent_dir(&parent).map_err(|err| {
            Self::execution_io_error(format!("sync recovery quarantine failed: {err}"))
        })?;
        Self::durability_failpoint("after_recovery_quarantine_dir_fsync").map_err(|err| {
            Self::execution_io_error(format!("sync recovery quarantine failed: {err}"))
        })?;
        self.wal_bytes = 0;
        self.wal_record_count = 0;
        self.wal_hasher = Sha256::new();
        self.wal_identity = None;
        self.active_mutation_request_ids.clear();
        self.transaction = TransactionState::Idle;
        Ok(json!({
            "baseGeneration": base_generation,
            "walDigest": actual_digest,
            "recordCount": record_count,
            "quarantined": true,
        }))
    }

    fn key(corpus_id: &str, id: &str) -> String {
        format!("{corpus_id}:{id}")
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
    fn stable_cosine(a: &[f64], b: &[f64]) -> f64 {
        let scale_a = a.iter().map(|value| value.abs()).fold(0.0, f64::max);
        let scale_b = b.iter().map(|value| value.abs()).fold(0.0, f64::max);
        if scale_a == 0.0 || scale_b == 0.0 {
            return 0.0;
        }
        let mut dot = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;
        for (a_value, b_value) in a.iter().zip(b) {
            let scaled_a = a_value / scale_a;
            let scaled_b = b_value / scale_b;
            dot += scaled_a * scaled_b;
            norm_a += scaled_a * scaled_a;
            norm_b += scaled_b * scaled_b;
        }
        let denominator = norm_a.sqrt() * norm_b.sqrt();
        if denominator == 0.0 || !denominator.is_finite() {
            return 0.0;
        }
        let score = dot / denominator;
        if score.is_finite() { score } else { 0.0 }
    }

    #[inline]
    fn safe_norm(values: &[f64]) -> Option<f64> {
        if values.iter().any(|value| !value.is_finite()) {
            return None;
        }
        let norm_squared = values.iter().map(|value| value * value).sum::<f64>();
        if norm_squared.is_finite() {
            return Some(norm_squared.sqrt());
        }
        let scale = values.iter().map(|value| value.abs()).fold(0.0, f64::max);
        if scale == 0.0 {
            return Some(0.0);
        }
        Some(
            values
                .iter()
                .map(|value| {
                    let scaled = value / scale;
                    scaled * scaled
                })
                .sum::<f64>()
                .sqrt()
                * scale,
        )
    }

    #[inline]
    fn cosine_with_query_norm(query: &[f64], query_norm: f64, vector: &[f64]) -> f64 {
        if query.is_empty()
            || vector.is_empty()
            || query.len() != vector.len()
            || query_norm == 0.0
            || vector.iter().any(|value| !value.is_finite())
        {
            return 0.0;
        }
        if !query_norm.is_finite() {
            return Self::stable_cosine(query, vector);
        }
        let mut dot = 0.0;
        let mut norm_b = 0.0;
        for (query_value, vector_value) in query.iter().zip(vector) {
            dot += query_value * vector_value;
            norm_b += vector_value * vector_value;
        }
        let denominator = query_norm * norm_b.sqrt();
        if dot.is_finite() && norm_b.is_finite() && denominator.is_finite() && denominator != 0.0 {
            let score = dot / denominator;
            if score.is_finite() {
                return score;
            }
        }
        Self::stable_cosine(query, vector)
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
            CmpOrdering::Greater => true,
            CmpOrdering::Less => false,
            CmpOrdering::Equal => candidate.key < worst.key,
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
            let Some(record) = vectors.get(key) else {
                continue;
            };
            let canonical_key = Self::key(&record.corpus_id, &record.id);
            let score = vector_values
                .get(&canonical_key)
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
        let query_norm = Self::safe_norm(query).unwrap_or(0.0);
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

    fn params_object<'a>(
        params: &'a Value,
    ) -> Result<&'a serde_json::Map<String, Value>, AppError> {
        params
            .as_object()
            .ok_or_else(|| Self::execution_client_error("params must be an object".to_string()))
    }

    fn mutation_params_object<'a>(
        params: &'a Value,
    ) -> Result<&'a serde_json::Map<String, Value>, AppError> {
        params.as_object().ok_or_else(|| {
            Self::execution_client_error("mutation params must be an object".to_string())
        })
    }

    fn optional_string<'a>(
        params: &'a serde_json::Map<String, Value>,
        name: &str,
    ) -> Result<Option<&'a str>, AppError> {
        match params.get(name) {
            None => Ok(None),
            Some(value) => value
                .as_str()
                .map(Some)
                .ok_or_else(|| Self::execution_client_error(format!("{name} must be a string"))),
        }
    }

    fn required_string<'a>(
        params: &'a serde_json::Map<String, Value>,
        name: &str,
    ) -> Result<&'a str, AppError> {
        Self::optional_string(params, name)?
            .ok_or_else(|| Self::execution_client_error(format!("missing {name}")))
    }

    fn optional_array<'a>(
        params: &'a serde_json::Map<String, Value>,
        name: &str,
    ) -> Result<Option<&'a Vec<Value>>, AppError> {
        match params.get(name) {
            None => Ok(None),
            Some(value) => value
                .as_array()
                .map(Some)
                .ok_or_else(|| Self::execution_client_error(format!("{name} must be an array"))),
        }
    }

    fn validate_string_array(
        params: &serde_json::Map<String, Value>,
        name: &str,
    ) -> Result<(), AppError> {
        if let Some(items) = Self::optional_array(params, name)? {
            for item in items {
                if item.as_str().is_none() {
                    return Err(Self::execution_client_error(format!(
                        "{name} must contain only strings"
                    )));
                }
            }
        }
        Ok(())
    }

    fn require_exact_params(
        params: &serde_json::Map<String, Value>,
        allowed: &[&str],
    ) -> Result<(), AppError> {
        if params.len() != allowed.len()
            || params.keys().any(|key| !allowed.contains(&key.as_str()))
        {
            return Err(Self::execution_client_error(format!(
                "params must contain exactly {}",
                allowed.join(",")
            )));
        }
        Ok(())
    }

    fn reject_unknown_params(
        params: &serde_json::Map<String, Value>,
        allowed: &[&str],
    ) -> Result<(), AppError> {
        if params.keys().any(|key| !allowed.contains(&key.as_str())) {
            // Do not reflect an attacker-controlled key into the bounded
            // response or audit stream.
            return Err(Self::execution_client_error(
                "params contain an unknown field".to_string(),
            ));
        }
        Ok(())
    }

    fn bounded_unique_ids<'a>(
        params: &'a serde_json::Map<String, Value>,
        name: &str,
        maximum: usize,
        allow_empty: bool,
    ) -> Result<Vec<&'a str>, AppError> {
        let items = Self::optional_array(params, name)?
            .ok_or_else(|| Self::execution_client_error(format!("missing {name}")))?;
        if (!allow_empty && items.is_empty()) || items.len() > maximum {
            let lower = usize::from(!allow_empty);
            return Err(Self::execution_client_error(format!(
                "{name} length must be in [{lower}, {maximum}]"
            )));
        }
        let mut seen = HashSet::with_capacity(items.len());
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            let id = item
                .as_str()
                .filter(|value| {
                    !value.is_empty() && value.len() <= MAX_INDEXING_DOMAIN_ID_BYTES
                })
                .ok_or_else(|| {
                    Self::execution_client_error(format!(
                        "{name} must contain only non-empty strings of at most {MAX_INDEXING_DOMAIN_ID_BYTES} bytes"
                    ))
                })?;
            if !seen.insert(id) {
                return Err(Self::execution_client_error(format!(
                    "{name} must not contain duplicate IDs"
                )));
            }
            ids.push(id);
        }
        Ok(ids)
    }

    fn bounded_required_string<'a>(
        params: &'a serde_json::Map<String, Value>,
        name: &str,
        maximum_bytes: usize,
    ) -> Result<&'a str, AppError> {
        let value = Self::required_string(params, name)?;
        if value.is_empty() || value.len() > maximum_bytes {
            return Err(Self::execution_client_error(format!(
                "{name} must contain between 1 and {maximum_bytes} bytes"
            )));
        }
        Ok(value)
    }

    fn bounded_serialized_bytes<T: Serialize>(
        value: &T,
        limit: u64,
        error_message: &'static str,
    ) -> Result<u64, AppError> {
        let writer = LimitedCountingWriter::new(limit);
        let mut serializer = serde_json::Serializer::new(writer);
        value
            .serialize(&mut serializer)
            .map_err(|_| Self::execution_client_error(error_message.to_string()))?;
        Ok(serializer.into_inner().finish())
    }

    fn validate_indexing_request_size(req: &RpcRequest) -> Result<(), AppError> {
        Self::bounded_serialized_bytes(
            req,
            MAX_INDEXING_REQUEST_BYTES,
            "bounded indexing request exceeds its byte limit",
        )?;
        Ok(())
    }

    fn indexing_result_array_limit(request_id: u64) -> Result<u64, AppError> {
        let empty_response = RpcResponse {
            id: request_id,
            ok: true,
            result: Some(Value::Array(Vec::new())),
            error: None,
        };
        let empty_bytes = Self::bounded_serialized_bytes(
            &empty_response,
            MAX_INDEXING_RESPONSE_BYTES,
            "bounded indexing response exceeds its byte limit",
        )?;
        let envelope_bytes = empty_bytes.checked_sub(2).ok_or_else(|| {
            Self::execution_client_error(
                "bounded indexing response envelope accounting failed".to_string(),
            )
        })?;
        MAX_INDEXING_RESPONSE_BYTES
            .checked_sub(envelope_bytes)
            .ok_or_else(|| {
                Self::execution_client_error(
                    "bounded indexing response envelope exceeds its byte limit".to_string(),
                )
            })
    }

    fn add_bounded_indexing_response_item(
        accumulated: u64,
        item: &Value,
        has_previous_item: bool,
        array_limit: u64,
    ) -> Result<u64, AppError> {
        // `accumulated` starts with the two result-array brackets. The caller
        // derives `array_limit` from the exact response envelope for this
        // request ID, so no fixed-size envelope estimate can drift.
        let accumulated = accumulated
            .checked_add(u64::from(has_previous_item))
            .ok_or_else(|| {
                Self::execution_client_error(
                    "bounded indexing response byte count overflow".to_string(),
                )
            })?;
        let remaining = array_limit.checked_sub(accumulated).ok_or_else(|| {
            Self::execution_client_error(
                "bounded indexing response exceeds its byte limit".to_string(),
            )
        })?;
        let writer = LimitedCountingWriter::new(remaining);
        let mut writer = serde_json::Serializer::new(writer);
        item.serialize(&mut writer).map_err(|_| {
            Self::execution_client_error(
                "bounded indexing response exceeds its byte limit".to_string(),
            )
        })?;
        let measured = writer.into_inner().finish();
        accumulated.checked_add(measured).ok_or_else(|| {
            Self::execution_client_error(
                "bounded indexing response byte count overflow".to_string(),
            )
        })
    }

    fn validate_indexing_response_size(response: &RpcResponse) -> Result<(), AppError> {
        Self::bounded_serialized_bytes(
            response,
            MAX_INDEXING_RESPONSE_BYTES,
            "bounded indexing response exceeds its byte limit",
        )?;
        Ok(())
    }

    fn validate_embedded_corpus(
        item: &serde_json::Map<String, Value>,
        corpus_id: &str,
        label: &str,
    ) -> Result<(), AppError> {
        match item.get("corpusId") {
            None => {}
            Some(Value::String(embedded)) if embedded == corpus_id => {}
            Some(Value::String(_)) => {
                return Err(Self::execution_client_error(format!(
                    "{label}.corpusId does not match corpusId"
                )));
            }
            Some(_) => {
                return Err(Self::execution_client_error(format!(
                    "{label}.corpusId must be a string"
                )));
            }
        }
        Ok(())
    }

    fn stored_snapshot_section<'a>(
        &'a self,
        corpus_id: &str,
        section: &str,
    ) -> Result<Option<&'a Vec<Value>>, AppError> {
        let Some(snapshot) = self.state.snapshots.get(corpus_id) else {
            return Ok(None);
        };
        let object = snapshot.as_object().ok_or_else(|| {
            Self::execution_client_error("stored memory snapshot must be an object".to_string())
        })?;
        match object.get("corpusId") {
            Some(Value::String(stored)) if stored == corpus_id => {}
            Some(Value::String(_)) => {
                return Err(Self::execution_client_error(
                    "stored memory snapshot corpusId does not match its key".to_string(),
                ));
            }
            _ => {
                return Err(Self::execution_client_error(
                    "stored memory snapshot has no string corpusId".to_string(),
                ));
            }
        }
        match object.get(section) {
            None => Ok(None),
            Some(Value::Array(items)) => Ok(Some(items)),
            Some(_) => Err(Self::execution_client_error(format!(
                "stored {section} must be an array"
            ))),
        }
    }

    fn validate_stored_section_item(
        item: &Value,
        corpus_id: &str,
        section: &str,
        id_key: &str,
    ) -> Result<(), AppError> {
        let object = item.as_object().ok_or_else(|| {
            Self::execution_client_error(format!("stored {section} must contain only objects"))
        })?;
        Self::validate_embedded_corpus(object, corpus_id, &format!("stored {section}"))?;
        object
            .get(id_key)
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty() && id.len() <= MAX_INDEXING_DOMAIN_ID_BYTES)
            .ok_or_else(|| {
                Self::execution_client_error(format!(
                    "stored {section} has no bounded string {id_key}"
                ))
            })?;
        Ok(())
    }

    fn validate_stored_snapshot_for_upsert(&self, corpus_id: &str) -> Result<(), AppError> {
        for (section, id_key) in [
            ("passages", "passageId"),
            ("facts", "factId"),
            ("schemas", "schemaId"),
        ] {
            if let Some(items) = self.stored_snapshot_section(corpus_id, section)? {
                for item in items {
                    Self::validate_stored_section_item(item, corpus_id, section, id_key)?;
                }
            }
        }
        Ok(())
    }

    fn validate_stored_fact_item<'a>(
        fact: &'a Value,
        corpus_id: &str,
    ) -> Result<&'a serde_json::Map<String, Value>, AppError> {
        Self::validate_stored_section_item(fact, corpus_id, "facts", "factId")?;
        let object = fact
            .as_object()
            .expect("stored fact object validated above");
        Self::validate_fact_schema_and_state(object, "stored fact")?;
        Ok(object)
    }

    fn validate_fact_schema_and_state(
        object: &serde_json::Map<String, Value>,
        label: &str,
    ) -> Result<(), AppError> {
        for field in ["schemaId", "state"] {
            if !object
                .get(field)
                .and_then(Value::as_str)
                .is_some_and(|value| {
                    !value.is_empty() && value.len() <= MAX_INDEXING_DOMAIN_ID_BYTES
                })
            {
                return Err(Self::execution_client_error(format!(
                    "{label} has no bounded string {field}"
                )));
            }
        }
        Ok(())
    }

    fn validate_stored_facts_for_activation(&self, corpus_id: &str) -> Result<(), AppError> {
        let facts = self
            .stored_snapshot_section(corpus_id, "facts")?
            .into_iter()
            .flatten();
        for fact in facts {
            Self::validate_stored_fact_item(fact, corpus_id)?;
        }
        Ok(())
    }

    fn merge_snapshot_section(existing: &mut Vec<Value>, incoming: Vec<Value>, id_key: &str) {
        // Index only the request delta. Scanning existing in reverse preserves
        // the legacy behavior of replacing the last pre-existing duplicate ID
        // without allocating an O(corpus) lookup table.
        let mut incoming = incoming.into_iter().map(Some).collect::<Vec<_>>();
        let incoming_index = incoming
            .iter()
            .enumerate()
            .map(|(index, item)| {
                let id = item
                    .as_ref()
                    .and_then(|value| value.get(id_key))
                    .and_then(Value::as_str)
                    .expect("validated memory upsert item has an ID");
                (id.to_string(), index)
            })
            .collect::<HashMap<_, _>>();
        for item in existing.iter_mut().rev() {
            let Some(index) = item
                .get(id_key)
                .and_then(Value::as_str)
                .and_then(|id| incoming_index.get(id))
            else {
                continue;
            };
            if let Some(replacement) = incoming[*index].take() {
                *item = replacement;
            }
        }
        existing.extend(incoming.into_iter().flatten());
    }

    fn validate_mutation_params(&self, req: &RpcRequest) -> Result<(), AppError> {
        let params = Self::mutation_params_object(&req.params)?;
        match req.method.as_str() {
            "upsert_nodes" => {
                if let Some(items) = Self::optional_array(params, "nodes")? {
                    for item in items {
                        serde_json::from_value::<GraphNode>(item.clone()).map_err(|err| {
                            Self::execution_client_error(format!("invalid node: {err}"))
                        })?;
                    }
                }
            }
            "upsert_edges" => {
                if let Some(items) = Self::optional_array(params, "edges")? {
                    for item in items {
                        serde_json::from_value::<GraphEdge>(item.clone()).map_err(|err| {
                            Self::execution_client_error(format!("invalid edge: {err}"))
                        })?;
                    }
                }
            }
            "delete_nodes" => {
                let _ = Self::optional_string(params, "corpusId")?;
                Self::validate_string_array(params, "nodeIds")?;
            }
            "delete_edges" => {
                let _ = Self::optional_string(params, "corpusId")?;
                Self::validate_string_array(params, "edgeIds")?;
            }
            "delete_by_document" => {
                let _ = Self::optional_string(params, "corpusId")?;
                let _ = Self::optional_string(params, "documentId")?;
            }
            "delete_by_corpus" => {
                let _ = Self::optional_string(params, "corpusId")?;
            }
            "vector_upsert" => {
                if let Some(items) = Self::optional_array(params, "records")? {
                    for item in items {
                        let record = serde_json::from_value::<VectorRecord>(item.clone()).map_err(
                            |err| {
                                Self::execution_client_error(format!(
                                    "invalid vector record: {err}"
                                ))
                            },
                        )?;
                        if record.values.len() > MAX_VECTOR_DIMENSIONS {
                            return Err(Self::execution_client_error(format!(
                                "vector dimensions must not exceed {MAX_VECTOR_DIMENSIONS}"
                            )));
                        }
                        if record.values.iter().any(|value| !value.is_finite()) {
                            return Err(Self::execution_client_error(
                                "vector values must be finite numbers".to_string(),
                            ));
                        }
                    }
                }
            }
            "vector_delete_by_document" => {
                let _ = Self::optional_string(params, "corpusId")?;
                let _ = Self::optional_string(params, "documentId")?;
            }
            "memory_upsert" => {
                Self::validate_indexing_request_size(req)?;
                Self::reject_unknown_params(
                    params,
                    &["corpusId", "passages", "facts", "schemas", "exportedAt"],
                )?;
                let corpus_id = Self::bounded_required_string(
                    params,
                    "corpusId",
                    MAX_INDEXING_CORPUS_ID_BYTES,
                )?;
                for (section, id_key) in [
                    ("passages", "passageId"),
                    ("facts", "factId"),
                    ("schemas", "schemaId"),
                ] {
                    let Some(items) = Self::optional_array(params, section)? else {
                        continue;
                    };
                    if items.len() > MAX_INDEXING_DELTA_ITEMS_PER_SECTION {
                        return Err(Self::execution_client_error(format!(
                            "{section} length must not exceed {MAX_INDEXING_DELTA_ITEMS_PER_SECTION}"
                        )));
                    }
                    let mut seen = HashSet::with_capacity(items.len());
                    for item in items {
                        let object = item.as_object().ok_or_else(|| {
                            Self::execution_client_error(format!("{section} items must be objects"))
                        })?;
                        let id = object.get(id_key).and_then(Value::as_str).ok_or_else(|| {
                            Self::execution_client_error(format!("missing {section}.{id_key}"))
                        })?;
                        if id.is_empty() || id.len() > MAX_INDEXING_DOMAIN_ID_BYTES {
                            return Err(Self::execution_client_error(format!(
                                "{section}.{id_key} must contain between 1 and {MAX_INDEXING_DOMAIN_ID_BYTES} bytes"
                            )));
                        }
                        if !seen.insert(id) {
                            return Err(Self::execution_client_error(format!(
                                "{section} must not contain duplicate {id_key} values"
                            )));
                        }
                        Self::validate_embedded_corpus(object, corpus_id, section)?;
                        if section == "facts" {
                            Self::validate_fact_schema_and_state(object, "facts item")?;
                        }
                    }
                }
                if let Some(exported_at) = params.get("exportedAt") {
                    if !exported_at
                        .as_str()
                        .is_some_and(|value| value.len() <= MAX_INDEXING_UPDATED_AT_BYTES)
                    {
                        return Err(Self::execution_client_error(format!(
                            "exportedAt must contain at most {MAX_INDEXING_UPDATED_AT_BYTES} bytes"
                        )));
                    }
                }
                // Validate the complete stored merge target before WAL append.
                // The later reverse scan may then mutate without discovering a
                // deterministic shape/corpus error after durability evidence
                // has already been written.
                self.validate_stored_snapshot_for_upsert(corpus_id)?;
            }
            "memory_activate_facts_by_schema_ids" => {
                Self::validate_indexing_request_size(req)?;
                Self::require_exact_params(params, &["corpusId", "schemaIds", "updatedAt"])?;
                let corpus_id = Self::bounded_required_string(
                    params,
                    "corpusId",
                    MAX_INDEXING_CORPUS_ID_BYTES,
                )?;
                let _ =
                    Self::bounded_unique_ids(params, "schemaIds", MAX_INDEXING_SCHEMA_IDS, false)?;
                let _ = Self::bounded_required_string(
                    params,
                    "updatedAt",
                    MAX_INDEXING_UPDATED_AT_BYTES,
                )?;
                // This validation runs before WAL append. A deterministic
                // compatibility/corpus error must never create an
                // unreplayable recovery record.
                self.validate_stored_facts_for_activation(corpus_id)?;
            }
            "memory_save" => {
                let snapshot = params
                    .get("snapshot")
                    .ok_or_else(|| Self::execution_client_error("missing snapshot".to_string()))?;
                let snapshot = snapshot.as_object().ok_or_else(|| {
                    Self::execution_client_error("snapshot must be an object".to_string())
                })?;
                let corpus_id = snapshot
                    .get("corpusId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Self::execution_client_error("missing snapshot.corpusId".to_string())
                    })?;
                if corpus_id.is_empty() {
                    return Err(Self::execution_client_error(
                        "snapshot.corpusId must not be empty".to_string(),
                    ));
                }
            }
            "memory_save_checkpoint" => {
                let checkpoint = params.get("checkpoint").ok_or_else(|| {
                    Self::execution_client_error("missing checkpoint".to_string())
                })?;
                let checkpoint = checkpoint.as_object().ok_or_else(|| {
                    Self::execution_client_error("checkpoint must be an object".to_string())
                })?;
                let job_id = checkpoint
                    .get("jobId")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        Self::execution_client_error("missing checkpoint.jobId".to_string())
                    })?;
                if job_id.is_empty() {
                    return Err(Self::execution_client_error(
                        "checkpoint.jobId must not be empty".to_string(),
                    ));
                }
            }
            "lexical_index_passages" => {
                let corpus_id = Self::optional_string(params, "corpusId")?.unwrap_or_default();
                if corpus_id.is_empty() {
                    return Err(Self::execution_client_error("missing corpusId".to_string()));
                }
                if let Some(items) = Self::optional_array(params, "passages")? {
                    for item in items {
                        let object = item.as_object().ok_or_else(|| {
                            Self::execution_client_error(
                                "passages must contain objects".to_string(),
                            )
                        })?;
                        let passage_id = object
                            .get("passageId")
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                Self::execution_client_error("missing passageId".to_string())
                            })?;
                        if passage_id.is_empty() {
                            return Err(Self::execution_client_error(
                                "passageId must not be empty".to_string(),
                            ));
                        }
                        let document_id = object
                            .get("metadata")
                            .and_then(Value::as_object)
                            .and_then(|metadata| metadata.get("documentId"))
                            .and_then(Value::as_str)
                            .ok_or_else(|| {
                                Self::execution_client_error(
                                    "missing metadata.documentId".to_string(),
                                )
                            })?;
                        if document_id.is_empty() {
                            return Err(Self::execution_client_error(
                                "metadata.documentId must not be empty".to_string(),
                            ));
                        }
                        if let Some(text) = object.get("text") {
                            if !text.is_string() {
                                return Err(Self::execution_client_error(
                                    "passage text must be a string".to_string(),
                                ));
                            }
                        }
                    }
                }
            }
            "lexical_delete_by_document" => {
                let _ = Self::optional_string(params, "corpusId")?;
                let _ = Self::optional_string(params, "documentId")?;
            }
            "memory_save_file" => {
                return Err(Self::execution_client_error(
                    "memory_save_file must be canonicalized before validation".to_string(),
                ));
            }
            method => {
                return Err(Self::execution_client_error(format!(
                    "mutation method has no validator: {method}"
                )));
            }
        }
        Ok(())
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

    fn canonicalize_request(&self, req: RpcRequest) -> Result<RpcRequest, AppError> {
        if req.method != "memory_save_file" {
            return Ok(req);
        }
        let file_path = req
            .params
            .get("filePath")
            .and_then(Value::as_str)
            .ok_or_else(|| Self::execution_client_error("missing filePath".to_string()))?;
        let file_content = fs::read_to_string(file_path)
            .map_err(|err| Self::execution_io_error(format!("read snapshot failed: {err}")))?;
        let snapshot: Value = serde_json::from_str(&file_content)
            .map_err(|err| Self::execution_client_error(format!("parse snapshot failed: {err}")))?;
        if snapshot.get("corpusId").and_then(Value::as_str).is_none() {
            return Err(Self::execution_client_error(
                "missing snapshot.corpusId".to_string(),
            ));
        }
        Ok(RpcRequest {
            id: req.id,
            method: "memory_save".to_string(),
            params: json!({"snapshot": snapshot}),
        })
    }

    fn response_for_result(&self, id: u64, result: Result<Value, AppError>) -> RpcResponse {
        match result {
            Ok(value) => RpcResponse {
                id,
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
                self.append_request_audit_event(&code, &failure_class, &id.to_string());
                RpcResponse {
                    id,
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

    #[allow(dead_code)]
    fn handle(&mut self, req: RpcRequest) -> RpcResponse {
        let id = req.id;
        match self.canonicalize_request(req) {
            Ok(req) => self.handle_prepared(req),
            Err(err) => {
                self.fatal = true;
                self.response_for_result(id, Err(err))
            }
        }
    }

    fn handle_prepared(&mut self, req: RpcRequest) -> RpcResponse {
        let mut progress = NoCommitProgress;
        self.handle_prepared_with_progress(req, &mut progress)
    }

    fn handle_prepared_with_progress(
        &mut self,
        req: RpcRequest,
        progress: &mut dyn CommitProgress,
    ) -> RpcResponse {
        let is_mutation = Self::is_mutating_method(&req.method);
        let Some(spec) = Self::method_spec(&req.method) else {
            return self
                .response_for_result(req.id, Err(Self::unsupported_method_error(&req.method)));
        };
        if matches!(self.access_mode, AccessMode::DescriptorReadOnly(_))
            && !Self::descriptor_method_specs().any(|allowed| allowed.name == spec.name)
        {
            return self.response_for_result(
                req.id,
                Err(AppError {
                    code: DESCRIPTOR_READ_ONLY_METHOD_CODE.to_string(),
                    message: DESCRIPTOR_READ_ONLY_METHOD_MESSAGE.to_string(),
                    failure_class: Some("CLIENT_INPUT".to_string()),
                }),
            );
        }
        if matches!(&self.transaction, TransactionState::RecoveryPending { .. })
            && !matches!(spec.classification, "health" | "recovery")
        {
            return self.response_for_result(
                req.id,
                Err(Self::execution_client_error(
                    "recovery pending; ordinary requests are unavailable".to_string(),
                )),
            );
        }
        if is_mutation
            && !self.wal_replaying
            && !matches!(&self.transaction, TransactionState::Active { .. })
        {
            self.fatal = true;
            return self.response_for_result(
                req.id,
                Err(Self::execution_client_error(
                    "mutation requires an active batch".to_string(),
                )),
            );
        }
        let result: Result<Value, AppError> = (|| {
            if is_mutation {
                self.validate_mutation_params(&req)?;
            } else if Self::is_indexing_memory_method(&req.method) {
                Self::validate_indexing_request_size(&req)?;
            }
            match req.method.as_str() {
                "ping" => Ok(json!({"pong": true})),
                "protocol_info" => {
                    let descriptor_handshake = match &self.access_mode {
                        AccessMode::DescriptorReadOnly(handshake) => Some(handshake),
                        AccessMode::Normal => None,
                    };
                    let methods = descriptor_handshake
                        .is_some()
                        .then(Self::descriptor_method_inventory)
                        .unwrap_or_else(|| {
                            METHOD_SPECS
                                .iter()
                                .map(|spec| {
                                    json!({
                                        "name": spec.name,
                                        "classification": spec.classification,
                                        "wal": spec.wal,
                                    })
                                })
                                .collect()
                        });
                    let mut protocol = json!({
                        "protocolVersion": "native-method-policy@1",
                        "generation": self.state.generation,
                        // This is the exact descriptor selected by the
                        // already-validated canonical generation.  It is a
                        // recovery fact only: callers cannot supply it or use
                        // it to publish a generation.  Legacy generation zero
                        // intentionally has no descriptor.
                        "vectorBlob": self.state.vector_blob.as_ref(),
                        "state": match &self.transaction {
                            TransactionState::RecoveryPending { .. } => "recoveryPending",
                            TransactionState::Active { .. } => "active",
                            TransactionState::Prepared { .. } => "prepared",
                            TransactionState::Idle => "idle",
                        },
                        "recovery": match &self.transaction {
                            TransactionState::RecoveryPending { base_generation, wal_digest, record_count, .. } => Some(json!({
                                "baseGeneration": base_generation,
                                "walDigest": Self::digest_hex(&wal_digest),
                                "recordCount": record_count,
                            })),
                            _ => None,
                        },
                        "lastCommitEvidence": self.state.commit_evidence.as_ref(),
                        "limits": {
                            "wire": {
                                "maxNormalRequestFrameBytes": MAX_NORMAL_REQUEST_FRAME_BYTES,
                            },
                            "wal": {
                                "maxRecordBytes": MAX_WAL_RECORD_BYTES,
                                "maxBytes": MAX_WAL_BYTES,
                                "maxRecords": MAX_WAL_RECORDS,
                                "mutationRequestIdUniqueness": "activeTransaction",
                            },
                            "vector": {
                                "maxDimensions": MAX_VECTOR_DIMENSIONS,
                                "maxSearchTopK": MAX_VECTOR_SEARCH_TOP_K,
                            },
                            "indexingMemory": {
                                "schema": INDEXING_MEMORY_PROTOCOL_SCHEMA,
                                "maxRequestBytes": MAX_INDEXING_REQUEST_BYTES,
                                "maxResponseBytes": MAX_INDEXING_RESPONSE_BYTES,
                                "maxSchemaIds": MAX_INDEXING_SCHEMA_IDS,
                                "maxActiveFacts": MAX_INDEXING_ACTIVE_FACTS,
                                "maxDeltaItemsPerSection": MAX_INDEXING_DELTA_ITEMS_PER_SECTION,
                                "maxDomainIdBytes": MAX_INDEXING_DOMAIN_ID_BYTES,
                                "maxCorpusIdBytes": MAX_INDEXING_CORPUS_ID_BYTES,
                                "maxUpdatedAtBytes": MAX_INDEXING_UPDATED_AT_BYTES,
                            }
                        },
                        "methods": methods,
                    });
                    if let Some(handshake) = descriptor_handshake {
                        protocol["accessMode"] = json!(ACCESS_MODE_DESCRIPTOR_READ_ONLY);
                        protocol["readOnly"] = json!(true);
                        protocol["canonicalSha256"] = json!(handshake.canonical_sha256);
                        protocol["vectorBlobSha256"] = json!(handshake.vector_blob_sha256);
                        protocol["vectorBlobSize"] = json!(handshake.vector_blob_size);
                        protocol["legacyGeneration0"] = json!(handshake.legacy_generation0);
                        protocol["methodInventorySha256"] =
                            json!(handshake.method_inventory_sha256);
                        if let Some(binding) = &handshake.legacy_binding_sha256 {
                            protocol["legacyBindingSha256"] = json!(binding);
                        }
                    } else {
                        let progress_policy = NativeProgressPolicy::checked_in_candidate()
                            .map_err(|error| {
                                Self::execution_io_error(format!(
                                    "load native progress candidate failed: {error}"
                                ))
                            })?;
                        let progress_policy_value: Value = serde_json::from_slice(
                            &progress_policy.canonical_bytes(),
                        )
                        .map_err(|error| {
                            Self::execution_io_error(format!(
                                "parse native progress candidate failed: {error}"
                            ))
                        })?;
                        protocol["progressPolicy"] = progress_policy_value;
                        protocol["progressPolicySha256"] = json!(progress_policy.sha256());
                    }
                    Ok(protocol)
                }
                "batch_begin" => {
                    if !matches!(&self.transaction, TransactionState::Idle) {
                        Err(Self::execution_client_error(
                            "batch_begin requires an idle transaction".to_string(),
                        ))
                    } else {
                        if self.wal_bytes != 0 || self.wal_record_count != 0 {
                            self.fatal = true;
                            return Err(Self::execution_io_error(
                                "idle transaction retains WAL evidence".to_string(),
                            ));
                        }
                        match (self.wal_file.as_ref(), self.wal_identity) {
                            (None, None) => {}
                            (Some(file), Some(identity)) => {
                                let metadata = file.metadata().map_err(|error| {
                                    self.fatal = true;
                                    Self::execution_io_error(format!(
                                        "inspect idle WAL descriptor failed: {error}"
                                    ))
                                })?;
                                if Self::metadata_identity(&metadata) != identity
                                    || metadata.len() != 0
                                    || Self::validate_regular_path_identity(
                                        self.require_wal_path().map_err(|error| {
                                            Self::execution_io_error(error.to_string())
                                        })?,
                                        identity,
                                    )
                                    .is_err()
                                {
                                    self.fatal = true;
                                    return Err(Self::execution_io_error(
                                        "idle WAL descriptor is not the exact zero-length pathname authority"
                                            .to_string(),
                                    ));
                                }
                            }
                            _ => {
                                self.fatal = true;
                                return Err(Self::execution_io_error(
                                    "idle WAL descriptor and identity disagree".to_string(),
                                ));
                            }
                        }
                        if !self.active_mutation_request_ids.is_empty() {
                            self.fatal = true;
                            return Err(Self::execution_io_error(
                                "idle transaction retains mutation request IDs".to_string(),
                            ));
                        }
                        self.wal_hasher = Sha256::new();
                        let transaction_nonce =
                            Self::crypto_transaction_nonce().map_err(|error| {
                                self.fatal = true;
                                Self::execution_io_error(format!(
                                    "generate transaction nonce failed: {error}"
                                ))
                            })?;
                        self.transaction = TransactionState::Active {
                            base_generation: self.state.generation,
                            transaction_nonce: transaction_nonce.clone(),
                            mutation_seen: false,
                        };
                        Ok(json!({"transactionNonce": transaction_nonce}))
                    }
                }
                "batch_prepare_commit" => {
                    match &self.transaction {
                        TransactionState::Active {
                            mutation_seen: false,
                            ..
                        }
                        | TransactionState::Idle => {
                            return Err(Self::execution_client_error(
                                "batch_prepare_commit requires an active mutated batch".to_string(),
                            ));
                        }
                        TransactionState::RecoveryPending { .. } => {
                            return Err(Self::execution_client_error(
                                "batch_prepare_commit is unavailable during recovery".to_string(),
                            ));
                        }
                        TransactionState::Active {
                            mutation_seen: true,
                            ..
                        }
                        | TransactionState::Prepared { .. } => {}
                    }
                    let evidence = self.prepare_commit().map_err(|error| {
                        self.fatal = true;
                        Self::execution_io_error(format!("batch prepare failed: {error}"))
                    })?;
                    Ok(Self::prepared_evidence_value(&evidence))
                }
                "batch_commit" => {
                    match &self.transaction {
                        TransactionState::Prepared { .. } => {}
                        TransactionState::Idle | TransactionState::Active { .. } => {
                            return Err(Self::execution_client_error(
                                "batch_commit requires a prepared transaction".to_string(),
                            ));
                        }
                        TransactionState::RecoveryPending { .. } => {
                            return Err(Self::execution_client_error(
                                "batch_commit is unavailable during recovery".to_string(),
                            ));
                        }
                    }
                    let supplied_evidence = Self::prepared_evidence_from_params(&req.params)?;
                    let prepared_evidence = match &self.transaction {
                        TransactionState::Prepared { evidence, .. } => evidence,
                        _ => unreachable!("prepared state checked above"),
                    };
                    if &supplied_evidence != prepared_evidence {
                        return Err(Self::execution_client_error(
                            "batch_commit evidence does not match the prepared transaction"
                                .to_string(),
                        ));
                    }
                    let token = self.persist(&supplied_evidence, progress).map_err(|err| {
                        self.fatal = true;
                        Self::execution_io_error(format!("batch_commit persist failed: {err}"))
                    })?;
                    self.active_mutation_request_ids.clear();
                    self.transaction = TransactionState::Idle;
                    Ok(serde_json::to_value(token).unwrap_or(Value::Null))
                }
                "recovery_discard" => {
                    let outcome = self.discard_recovery(&req.params);
                    if outcome
                        .as_ref()
                        .err()
                        .is_some_and(|err| err.failure_class.as_deref() != Some("CLIENT_INPUT"))
                    {
                        self.fatal = true;
                    }
                    outcome
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                                && (edge.source_node_id == node_id
                                    || edge.target_node_id == node_id))
                        });
                    }
                    self.mark_cache_dirty();
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                        let parsed =
                            serde_json::from_value::<VectorRecord>(record).map_err(|err| {
                                Self::execution_client_error(format!(
                                    "invalid vector record: {err}"
                                ))
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    let top_k_u64 = match req.params.get("topK") {
                        None => 10,
                        Some(value) => value.as_u64().ok_or_else(|| {
                            Self::execution_client_error(
                                "topK must be a positive integer".to_string(),
                            )
                        })?,
                    };
                    let top_k = usize::try_from(top_k_u64).map_err(|_| {
                        Self::execution_client_error(format!(
                            "topK must not exceed {MAX_VECTOR_SEARCH_TOP_K}"
                        ))
                    })?;
                    if top_k == 0 || top_k > MAX_VECTOR_SEARCH_TOP_K {
                        return Err(Self::execution_client_error(format!(
                            "topK must be in [1, {MAX_VECTOR_SEARCH_TOP_K}]"
                        )));
                    }
                    let threshold = req.params.get("threshold").and_then(Value::as_f64);
                    let query_values = req
                        .params
                        .get("queryVector")
                        .and_then(Value::as_array)
                        .ok_or_else(|| {
                            Self::execution_client_error("queryVector must be an array".to_string())
                        })?;
                    if query_values.is_empty() || query_values.len() > MAX_VECTOR_DIMENSIONS {
                        return Err(Self::execution_client_error(format!(
                            "queryVector dimensions must be in [1, {MAX_VECTOR_DIMENSIONS}]"
                        )));
                    }
                    let query_vec = query_values
                        .iter()
                        .map(|value| {
                            value
                                .as_f64()
                                .filter(|number| number.is_finite())
                                .ok_or_else(|| {
                                    Self::execution_client_error(
                                        "queryVector must contain only finite numbers".to_string(),
                                    )
                                })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
                    Ok(json!(null))
                }
                "memory_upsert" => {
                    // Delta merge into the stored snapshot: callers send only the
                    // new document's passages/facts (+schema updates), so RPC
                    // payload and WAL growth stay O(delta) instead of O(corpus).
                    let corpus_id = req
                        .params
                        .get("corpusId")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            Self::execution_client_error("missing corpusId".to_string())
                        })?
                        .to_string();
                    let snapshot = self
                        .state
                        .snapshots
                        .entry(corpus_id.clone())
                        .or_insert_with(|| {
                            json!({
                                "corpusId": corpus_id,
                                "schemaVersion": 1,
                                "passages": [],
                                "facts": [],
                                "schemas": []
                            })
                        });
                    for (section, id_key) in [
                        ("passages", "passageId"),
                        ("facts", "factId"),
                        ("schemas", "schemaId"),
                    ] {
                        let incoming = match req.params.get(section).and_then(Value::as_array) {
                            Some(items) if !items.is_empty() => items.clone(),
                            _ => continue,
                        };
                        let existing = snapshot.get_mut(section).and_then(Value::as_array_mut);
                        if let Some(existing) = existing {
                            Self::merge_snapshot_section(existing, incoming, id_key);
                        } else {
                            snapshot[section] = Value::Array(incoming);
                        }
                    }
                    if let Some(exported) = req.params.get("exportedAt").cloned() {
                        snapshot["exportedAt"] = exported;
                    }
                    self.mark_cache_dirty();
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
                    Ok(json!(null))
                }
                "memory_save_file" => Err(Self::execution_client_error(
                    "memory_save_file must be normalized before dispatch".to_string(),
                )),
                "memory_load" => {
                    let corpus_id = req
                        .params
                        .get("corpusId")
                        .and_then(Value::as_str)
                        .unwrap_or_default();
                    let snapshot =
                        self.state
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
                "memory_get_schemas_by_ids" => {
                    let params = Self::params_object(&req.params)?;
                    Self::require_exact_params(params, &["corpusId", "schemaIds"])?;
                    let corpus_id = Self::bounded_required_string(
                        params,
                        "corpusId",
                        MAX_INDEXING_CORPUS_ID_BYTES,
                    )?;
                    let schema_ids = Self::bounded_unique_ids(
                        params,
                        "schemaIds",
                        MAX_INDEXING_SCHEMA_IDS,
                        true,
                    )?;
                    if schema_ids.is_empty() {
                        return Ok(json!([]));
                    }
                    let requested = schema_ids.iter().copied().collect::<HashSet<_>>();
                    let mut found = HashMap::with_capacity(schema_ids.len());
                    let mut response_bytes = 2_u64;
                    let response_limit = Self::indexing_result_array_limit(req.id)?;
                    let schemas = self
                        .stored_snapshot_section(corpus_id, "schemas")?
                        .into_iter()
                        .flatten();
                    for schema in schemas {
                        Self::validate_stored_section_item(
                            schema, corpus_id, "schemas", "schemaId",
                        )?;
                        let object = schema.as_object().ok_or_else(|| {
                            Self::execution_client_error(
                                "stored schemas must contain only objects".to_string(),
                            )
                        })?;
                        let schema_id = object
                            .get("schemaId")
                            .and_then(Value::as_str)
                            .filter(|id| !id.is_empty())
                            .ok_or_else(|| {
                                Self::execution_client_error(
                                    "stored schema has no non-empty schemaId".to_string(),
                                )
                            })?;
                        if requested.contains(schema_id) {
                            if found.contains_key(schema_id) {
                                return Err(Self::execution_client_error(
                                    "stored schemas contain a duplicate requested schemaId"
                                        .to_string(),
                                ));
                            }
                            response_bytes = Self::add_bounded_indexing_response_item(
                                response_bytes,
                                schema,
                                !found.is_empty(),
                                response_limit,
                            )?;
                            found.insert(schema_id.to_string(), schema.clone());
                        }
                    }
                    Ok(Value::Array(
                        schema_ids
                            .iter()
                            .filter_map(|schema_id| found.remove(*schema_id))
                            .collect(),
                    ))
                }
                "memory_get_active_facts" => {
                    let params = Self::params_object(&req.params)?;
                    Self::require_exact_params(params, &["corpusId", "limit"])?;
                    let corpus_id = Self::bounded_required_string(
                        params,
                        "corpusId",
                        MAX_INDEXING_CORPUS_ID_BYTES,
                    )?;
                    let limit_u64 =
                        params.get("limit").and_then(Value::as_u64).ok_or_else(|| {
                            Self::execution_client_error(
                                "limit must be a nonnegative integer".to_string(),
                            )
                        })?;
                    let limit = usize::try_from(limit_u64).map_err(|_| {
                        Self::execution_client_error(format!(
                            "limit must not exceed {MAX_INDEXING_ACTIVE_FACTS}"
                        ))
                    })?;
                    if limit > MAX_INDEXING_ACTIVE_FACTS {
                        return Err(Self::execution_client_error(format!(
                            "limit must not exceed {MAX_INDEXING_ACTIVE_FACTS}"
                        )));
                    }
                    if limit == 0 {
                        return Ok(json!([]));
                    }
                    let mut active = Vec::with_capacity(limit);
                    let mut response_bytes = 2_u64;
                    let response_limit = Self::indexing_result_array_limit(req.id)?;
                    let facts = self
                        .stored_snapshot_section(corpus_id, "facts")?
                        .into_iter()
                        .flatten();
                    for fact in facts {
                        let object = Self::validate_stored_fact_item(fact, corpus_id)?;
                        if object.get("state").and_then(Value::as_str) == Some("active") {
                            response_bytes = Self::add_bounded_indexing_response_item(
                                response_bytes,
                                fact,
                                !active.is_empty(),
                                response_limit,
                            )?;
                            active.push(fact.clone());
                            if active.len() == limit {
                                break;
                            }
                        }
                    }
                    Ok(Value::Array(active))
                }
                "memory_activate_facts_by_schema_ids" => {
                    let params = Self::params_object(&req.params)?;
                    let corpus_id = Self::required_string(params, "corpusId")?;
                    let schema_ids = Self::bounded_unique_ids(
                        params,
                        "schemaIds",
                        MAX_INDEXING_SCHEMA_IDS,
                        false,
                    )?;
                    let schema_ids = schema_ids.iter().copied().collect::<HashSet<_>>();
                    let updated_at = Self::required_string(params, "updatedAt")?.to_string();
                    self.validate_stored_facts_for_activation(corpus_id)?;
                    let facts = self
                        .state
                        .snapshots
                        .get_mut(corpus_id)
                        .and_then(|snapshot| snapshot.get_mut("facts"))
                        .and_then(Value::as_array_mut);
                    let Some(facts) = facts else {
                        return Ok(json!({"activated": 0}));
                    };
                    let mut activated = 0_u64;
                    for fact in facts.iter_mut() {
                        let object = fact
                            .as_object_mut()
                            .expect("stored facts validated before mutation");
                        let should_activate = object.get("state").and_then(Value::as_str)
                            == Some("inactive")
                            && object
                                .get("schemaId")
                                .and_then(Value::as_str)
                                .is_some_and(|schema_id| schema_ids.contains(schema_id));
                        if should_activate {
                            object.insert("state".to_string(), json!("active"));
                            object.insert("updatedAt".to_string(), json!(updated_at));
                            activated = activated.checked_add(1).ok_or_else(|| {
                                Self::execution_client_error(
                                    "activated fact count overflow".to_string(),
                                )
                            })?;
                        }
                    }
                    self.mark_cache_dirty();
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
                    Ok(json!({"activated": activated}))
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                                Self::execution_client_error(
                                    "missing metadata.documentId".to_string(),
                                )
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    let top_k =
                        req.params.get("topK").and_then(Value::as_u64).unwrap_or(10) as usize;
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
                    self.persist_if_needed().map_err(|err| {
                        Self::execution_io_error(format!("persist failed: {err}"))
                    })?;
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
                    let result = execute_query_with_dialect(&mut store, query_str, dialect)
                        .map_err(|err| {
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
            }
        })();

        if result.is_err() && is_mutation {
            self.fatal = true;
        } else if result.is_ok() && is_mutation && !self.wal_replaying {
            if let TransactionState::Active { mutation_seen, .. } = &mut self.transaction {
                *mutation_seen = true;
            }
        }
        self.response_for_result(req.id, result)
    }
}

enum CliMode {
    Normal { db_path: PathBuf },
    DescriptorRead(DescriptorReadConfig),
}

fn descriptor_cli_flag(arg: &str) -> bool {
    matches!(
        arg,
        "--descriptor-read"
            | "--canonical-fd"
            | "--canonical-json-fd"
            | "--vector-blob-fd"
            | "--vector-fd"
            | "--expected-generation"
            | "--legacy-generation0"
            | "--legacy-binding-hash"
            | "--legacy-binding-sha256"
    )
}

fn descriptor_like_cli_flag(arg: &str) -> bool {
    descriptor_cli_flag(arg)
        || [
            "--descriptor",
            "--canonical",
            "--vector",
            "--expected-generation",
            "--legacy-generation",
            "--legacy-binding",
        ]
        .iter()
        .any(|prefix| arg.starts_with(prefix))
}

fn parse_descriptor_i32(value: Option<&String>, name: &str) -> io::Result<i32> {
    let value = value.ok_or_else(|| io::Error::other(format!("{name} requires a value")))?;
    value
        .parse::<i32>()
        .map_err(|_| io::Error::other(format!("{name} must be an integer FD")))
}

fn parse_descriptor_generation(value: Option<&String>) -> io::Result<u64> {
    let value = value.ok_or_else(|| io::Error::other("--expected-generation requires a value"))?;
    let generation = value
        .parse::<u64>()
        .map_err(|_| io::Error::other("--expected-generation must be an unsigned integer"))?;
    if generation > JSON_SAFE_INTEGER_MAX {
        return Err(io::Error::other(
            "--expected-generation exceeds MAX_SAFE_INTEGER",
        ));
    }
    Ok(generation)
}

fn parse_cli() -> io::Result<CliMode> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let descriptor_requested = args.iter().any(|arg| descriptor_like_cli_flag(arg));
    if !descriptor_requested {
        // Keep the historical normal-mode parser deliberately unchanged:
        // unknown options are ignored and --db without a following value is
        // harmless, as it was before descriptor mode existed.
        let mut db_path = PathBuf::from("aira-graphdb-native.json");
        let mut index = 0;
        while index < args.len() {
            if args[index] == "--db" {
                if let Some(value) = args.get(index + 1) {
                    db_path = PathBuf::from(value);
                    index += 1;
                }
            }
            index += 1;
        }
        return Ok(CliMode::Normal { db_path });
    }

    let mut descriptor_mode = false;
    let mut canonical_fd = None;
    let mut vector_blob_fd = None;
    let mut expected_generation = None;
    let mut legacy_generation0 = false;
    let mut legacy_binding_sha256 = None;
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        match arg {
            "--descriptor-read" => {
                if descriptor_mode {
                    return Err(io::Error::other("duplicate --descriptor-read"));
                }
                descriptor_mode = true;
            }
            "--db" => {
                return Err(io::Error::other(
                    "--db/path is mutually exclusive with descriptor read mode",
                ));
            }
            "--canonical-fd" | "--canonical-json-fd" => {
                if canonical_fd.is_some() {
                    return Err(io::Error::other("duplicate canonical descriptor FD"));
                }
                canonical_fd = Some(parse_descriptor_i32(args.get(index + 1), arg)?);
                index += 1;
            }
            "--vector-blob-fd" | "--vector-fd" => {
                if vector_blob_fd.is_some() {
                    return Err(io::Error::other("duplicate vector blob descriptor FD"));
                }
                vector_blob_fd = Some(parse_descriptor_i32(args.get(index + 1), arg)?);
                index += 1;
            }
            "--expected-generation" => {
                if expected_generation.is_some() {
                    return Err(io::Error::other("duplicate --expected-generation"));
                }
                expected_generation = Some(parse_descriptor_generation(args.get(index + 1))?);
                index += 1;
            }
            "--legacy-generation0" => {
                if legacy_generation0 {
                    return Err(io::Error::other("duplicate --legacy-generation0"));
                }
                legacy_generation0 = true;
            }
            "--legacy-binding-hash" | "--legacy-binding-sha256" => {
                if legacy_binding_sha256.is_some() {
                    return Err(io::Error::other("duplicate legacy binding hash"));
                }
                let value = args
                    .get(index + 1)
                    .ok_or_else(|| io::Error::other(format!("{arg} requires a value")))?;
                Server::validate_sha256(value, "legacy binding hash")?;
                legacy_binding_sha256 = Some(value.clone());
                index += 1;
            }
            other => {
                return Err(io::Error::other(format!(
                    "unknown descriptor read option {other}"
                )));
            }
        }
        index += 1;
    }
    if !descriptor_mode {
        return Err(io::Error::other(
            "descriptor read options require --descriptor-read",
        ));
    }
    let expected_generation = expected_generation
        .ok_or_else(|| io::Error::other("descriptor mode requires --expected-generation"))?;
    let canonical_fd =
        canonical_fd.ok_or_else(|| io::Error::other("descriptor mode requires --canonical-fd"))?;
    let vector_blob_fd = vector_blob_fd
        .ok_or_else(|| io::Error::other("descriptor mode requires --vector-blob-fd"))?;
    if expected_generation == 0 {
        if !legacy_generation0 {
            return Err(io::Error::other(
                "generation zero requires --legacy-generation0",
            ));
        }
        if legacy_binding_sha256.is_none() {
            return Err(io::Error::other(
                "generation zero requires --legacy-binding-hash",
            ));
        }
    } else if legacy_generation0 || legacy_binding_sha256.is_some() {
        return Err(io::Error::other(
            "legacy generation zero metadata is forbidden for positive generations",
        ));
    }
    Ok(CliMode::DescriptorRead(DescriptorReadConfig {
        canonical_fd,
        vector_blob_fd,
        expected_generation,
        legacy_generation0,
        legacy_binding_sha256,
    }))
}

fn read_bounded_normal_frame<R: BufRead>(
    reader: &mut R,
    maximum_bytes: usize,
) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            // Preserve the legacy normal-mode behavior: unlike descriptor
            // mode, a final JSON request does not require a trailing LF.
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let payload_bytes = newline.unwrap_or(buffered.len());
        let next = frame
            .len()
            .checked_add(payload_bytes)
            .ok_or_else(|| io::Error::other("normal protocol frame byte count overflow"))?;
        if next > maximum_bytes {
            return Err(io::Error::other(format!(
                "normal protocol input frame exceeds {maximum_bytes} bytes"
            )));
        }
        frame.extend_from_slice(&buffered[..payload_bytes]);
        reader.consume(payload_bytes + usize::from(newline.is_some()));
        if newline.is_some() {
            if frame.last() == Some(&b'\r') {
                frame.pop();
            }
            return Ok(Some(frame));
        }
    }
}

fn serialize_normal_response(
    response: &RpcResponse,
    bounded_indexing_response: bool,
) -> io::Result<String> {
    if bounded_indexing_response {
        Server::validate_indexing_response_size(response).map_err(|error| {
            io::Error::other(format!(
                "bounded indexing response validation failed: {}",
                error.message
            ))
        })?;
    }
    serde_json::to_string(response)
        .map_err(|err| io::Error::other(format!("serialize response failed: {err}")))
}

fn run_normal(db_path: PathBuf) -> io::Result<()> {
    let db_path = Server::resolve_db_path(db_path)?;
    let crash_tracker = CrashTracker::new(db_path.with_extension("native-audit.log"));
    let tracker_for_hook = crash_tracker.clone();
    let previous_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        tracker_for_hook.append_crash_event(Some(101), None, Some(panic_info.to_string()));
        previous_hook(panic_info);
    }));

    let mut server = Server::open_resolved(db_path)?;
    server.replay_wal()?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut stdout = io::stdout();
    loop {
        let line = match read_bounded_normal_frame(&mut input, MAX_NORMAL_REQUEST_FRAME_BYTES) {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(err) => {
                crash_tracker.append_crash_event(
                    Some(1),
                    None,
                    Some(format!("stdin read failed: {err}")),
                );
                return Err(err);
            }
        };
        let request_wire_bytes = u64::try_from(line.len())
            .map_err(|_| io::Error::other("normal request length does not fit u64"))?;
        let line = match String::from_utf8(line) {
            Ok(line) => line,
            Err(_) => {
                let error = io::Error::other("normal protocol input frame is not valid UTF-8");
                crash_tracker.append_crash_event(Some(1), None, Some(error.to_string()));
                return Err(error);
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let incoming = match serde_json::from_str::<IncomingRpcRequest>(&line) {
            Ok(req) => req,
            Err(err) => {
                let _ = Server::append_request_audit_event_for_path(
                    server
                        .audit_log_path
                        .as_ref()
                        .expect("normal mode has an audit path"),
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
        drop(line);
        let progress_protocol_version = incoming.progress_protocol_version;
        let req = incoming.into_rpc();
        crash_tracker.set_last_request_id(req.id.to_string());
        let method_for_wal = Server::is_mutating_method(&req.method);
        let bounded_indexing_response = Server::is_indexing_memory_method(&req.method);
        let indexing_wire_overflow =
            bounded_indexing_response && request_wire_bytes > MAX_INDEXING_REQUEST_BYTES;
        let resp = if indexing_wire_overflow {
            if method_for_wal {
                server.fatal = true;
            }
            server.response_for_result(
                req.id,
                Err(Server::execution_client_error(
                    "bounded indexing request exceeds its byte limit".to_string(),
                )),
            )
        } else if let Some(version) = progress_protocol_version {
            if version != 1 || req.method != "batch_commit" {
                server.fatal = true;
                server.response_for_result(
                    req.id,
                    Err(Server::execution_client_error(
                        "progressProtocolVersion 1 is reserved for batch_commit".to_string(),
                    )),
                )
            } else {
                let policy = NativeProgressPolicy::checked_in_candidate().map_err(|error| {
                    io::Error::other(format!("load native progress candidate failed: {error}"))
                })?;
                let mut progress = NativeCommitProgress::start(&mut stdout, policy, req.id)?;
                server.handle_prepared_with_progress(req, &mut progress)
            }
        } else if method_for_wal && !matches!(&server.transaction, TransactionState::Active { .. })
        {
            // Admission must precede canonicalization: memory_save_file reads
            // an external path and must do no I/O while idle or recovering.
            server.handle_prepared(req)
        } else if method_for_wal {
            let request_id = req.id;
            match server.canonicalize_request(req) {
                Err(err) => {
                    server.fatal = true;
                    server.response_for_result(request_id, Err(err))
                }
                Ok(canonical) => {
                    let validation = server
                        .validate_new_mutation_request_id(canonical.id)
                        .and_then(|()| server.validate_mutation_params(&canonical));
                    if let Err(err) = validation {
                        server.fatal = true;
                        server.response_for_result(request_id, Err(err))
                    } else if let Err(err) = server.wal_append(&canonical) {
                        server.fatal = true;
                        server.response_for_result(
                            request_id,
                            Err(Server::execution_io_error(format!(
                                "durability failure: {err}"
                            ))),
                        )
                    } else {
                        server.handle_prepared(canonical)
                    }
                }
            }
        } else {
            server.handle_prepared(req)
        };
        let payload = serialize_normal_response(&resp, bounded_indexing_response)?;
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
        if server.fatal {
            return Err(io::Error::other(
                "native durability boundary entered fail-closed state",
            ));
        }
    }
    // EOF is not a publication event. An active batch remains as a
    // RecoveryPending WAL; the next exclusive owner quarantines it with an
    // exact token and requeues the whole document from the durable source.
    Ok(())
}

fn run_descriptor_read(config: DescriptorReadConfig) -> io::Result<()> {
    let mut server = Server::open_descriptor_read(config)?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut stdout = io::stdout();
    let mut input_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut frames = 0u64;
    loop {
        let line = read_bounded_descriptor_frame(&mut input)?;
        let Some(line) = line else { break };
        input_bytes = input_bytes
            .checked_add(line.len() as u64)
            .ok_or_else(|| io::Error::other("descriptor protocol input byte counter overflow"))?;
        if input_bytes > MAX_DESCRIPTOR_PROTOCOL_BYTES {
            return Err(io::Error::other(
                "descriptor protocol cumulative input exceeds 64MiB",
            ));
        }
        frames = frames
            .checked_add(1)
            .ok_or_else(|| io::Error::other("descriptor protocol frame counter overflow"))?;
        if frames > MAX_DESCRIPTOR_PROTOCOL_FRAMES {
            return Err(io::Error::other(
                "descriptor protocol frame count exceeds 4096",
            ));
        }
        let line = String::from_utf8(line)
            .map_err(|_| io::Error::other("descriptor protocol frame is not UTF-8"))?;
        if line.trim().is_empty() {
            continue;
        }
        let incoming = match serde_json::from_str::<IncomingRpcRequest>(&line) {
            Ok(req) => req,
            Err(err) => {
                let response = RpcResponse {
                    id: 0,
                    ok: false,
                    result: None,
                    error: Some(RpcError {
                        code: "INVALID_REQUEST_JSON".to_string(),
                        message: format!("invalid request: {err}"),
                        failure_class: Some("CLIENT_INPUT".to_string()),
                    }),
                };
                let payload = serde_json::to_string(&response)
                    .map_err(|serialize_err| io::Error::other(serialize_err.to_string()))?;
                output_bytes = account_descriptor_output(output_bytes, payload.len())?;
                stdout.write_all(payload.as_bytes())?;
                stdout.write_all(b"\n")?;
                stdout.flush()?;
                continue;
            }
        };
        let response = if incoming.progress_protocol_version.is_some() {
            server.fatal = true;
            server.response_for_result(
                incoming.id,
                Err(Server::execution_client_error(
                    "progress is unavailable in descriptor read-only mode".to_string(),
                )),
            )
        } else {
            server.handle_prepared(incoming.into_rpc())
        };
        let payload = serde_json::to_string(&response)
            .map_err(|err| io::Error::other(format!("serialize response failed: {err}")))?;
        output_bytes = account_descriptor_output(output_bytes, payload.len())?;
        stdout.write_all(payload.as_bytes())?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
        if server.fatal {
            return Err(io::Error::other(
                "descriptor read-only boundary entered fail-closed state",
            ));
        }
    }
    // Descriptor mode has no transaction, WAL, audit, cache, or persistence
    // authority. EOF therefore only closes inherited read descriptors.
    Ok(())
}

fn account_descriptor_output(current: u64, payload_len: usize) -> io::Result<u64> {
    let frame_bytes = u64::try_from(payload_len)
        .map_err(|_| io::Error::other("descriptor protocol output length does not fit u64"))?
        .checked_add(1)
        .ok_or_else(|| io::Error::other("descriptor protocol output frame counter overflow"))?;
    if frame_bytes > MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES as u64 {
        return Err(io::Error::other(
            "descriptor protocol output frame exceeds 2MiB",
        ));
    }
    let total = current
        .checked_add(frame_bytes)
        .ok_or_else(|| io::Error::other("descriptor protocol output byte counter overflow"))?;
    if total > MAX_DESCRIPTOR_PROTOCOL_BYTES {
        return Err(io::Error::other(
            "descriptor protocol cumulative output exceeds 64MiB",
        ));
    }
    Ok(total)
}

fn read_bounded_descriptor_frame<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut frame = Vec::new();
    loop {
        let buffered = reader.fill_buf()?;
        if buffered.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::other(
                    "descriptor protocol ended with a partial frame",
                ))
            };
        }
        let newline = buffered.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(buffered.len(), |position| position + 1);
        if frame.len().saturating_add(take) > MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES {
            return Err(io::Error::other(
                "descriptor protocol input frame exceeds 2MiB",
            ));
        }
        frame.extend_from_slice(&buffered[..take]);
        reader.consume(take);
        if newline.is_some() {
            return Ok(Some(frame));
        }
    }
}

fn main() -> io::Result<()> {
    match parse_cli()? {
        CliMode::Normal { db_path } => run_normal(db_path),
        CliMode::DescriptorRead(config) => run_descriptor_read(config),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::collections::BTreeSet;
    use std::io::BufReader;
    use std::rc::Rc;
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
        let _ = fs::remove_file(path.with_extension("agdb.wal"));
        let _ = fs::remove_file(path.with_extension("native-audit.log"));
        if let Some(parent) = path.parent() {
            let stem = path
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or_default();
            if let Ok(entries) = fs::read_dir(parent) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if name.starts_with(&format!("{stem}.g")) && name.ends_with(".vblob") {
                        let _ = fs::remove_file(entry.path());
                    }
                }
            }
        }
    }

    #[test]
    fn snapshot_delta_merge_indexes_only_incoming_ids_and_preserves_order() {
        let mut existing = (0..100_000)
            .map(|index| json!({"factId": format!("f{index}"), "value": index}))
            .collect::<Vec<_>>();
        Server::merge_snapshot_section(
            &mut existing,
            vec![
                json!({"factId":"f99999","value":"replaced"}),
                json!({"factId":"new","value":"appended"}),
            ],
            "factId",
        );
        assert_eq!(existing.len(), 100_001);
        assert_eq!(existing[99_999]["value"], json!("replaced"));
        assert_eq!(existing[100_000]["factId"], json!("new"));

        let source = std::fs::read_to_string(file!()).expect("read native source");
        let start = source
            .find("fn merge_snapshot_section")
            .expect("delta merge helper");
        let end = source[start..]
            .find("fn validate_mutation_params")
            .map(|offset| start + offset)
            .expect("next helper");
        let helper = &source[start..end];
        assert!(helper.contains("incoming_index"));
        assert!(!helper.contains("existing.iter().enumerate()"));
        assert!(!helper.contains("HashMap<String, usize> = HashMap::new()"));
    }

    #[test]
    fn indexing_wire_accounting_is_exact_at_max_and_max_plus_one() {
        let empty_response = RpcResponse {
            id: u64::MAX,
            ok: true,
            result: Some(json!([])),
            error: None,
        };
        let empty_bytes = serde_json::to_vec(&empty_response).unwrap().len() as u64;
        let array_limit = Server::indexing_result_array_limit(u64::MAX).unwrap();
        assert_eq!(array_limit + empty_bytes - 2, MAX_INDEXING_RESPONSE_BYTES);

        let first = json!({"factId":"f1","text":"ok"});
        let unbounded_first =
            Server::add_bounded_indexing_response_item(2, &first, false, u64::MAX)
                .expect("measure first bounded item");
        let first_bytes =
            Server::add_bounded_indexing_response_item(2, &first, false, unbounded_first)
                .expect("exact array maximum");
        assert_eq!(first_bytes, unbounded_first);
        assert!(
            Server::add_bounded_indexing_response_item(2, &first, false, unbounded_first - 1)
                .is_err(),
            "array max+1 must fail"
        );
        let second = json!({"factId":"f2","text":"ok"});
        let second_bytes =
            Server::add_bounded_indexing_response_item(first_bytes, &second, true, u64::MAX)
                .expect("second bounded item");
        assert_eq!(
            second_bytes,
            2 + serde_json::to_vec(&first).unwrap().len() as u64
                + 1
                + serde_json::to_vec(&second).unwrap().len() as u64
        );

        let empty_string_response = RpcResponse {
            id: u64::MAX,
            ok: true,
            result: Some(Value::String(String::new())),
            error: None,
        };
        let base_bytes = serde_json::to_vec(&empty_string_response).unwrap().len();
        let payload_bytes = MAX_INDEXING_RESPONSE_BYTES as usize - base_bytes;
        let mut exact_response = RpcResponse {
            id: u64::MAX,
            ok: true,
            result: Some(Value::String("x".repeat(payload_bytes))),
            error: None,
        };
        assert_eq!(
            serde_json::to_vec(&exact_response).unwrap().len() as u64,
            MAX_INDEXING_RESPONSE_BYTES
        );
        Server::validate_indexing_response_size(&exact_response).unwrap();
        if let Some(Value::String(value)) = exact_response.result.as_mut() {
            value.push('x');
        }
        assert!(Server::validate_indexing_response_size(&exact_response).is_err());

        let request = RpcRequest {
            id: 1,
            method: "memory_get_active_facts".to_string(),
            params: json!({"corpusId":"c1","limit":1}),
        };
        let request_bytes = serde_json::to_vec(&request).unwrap().len() as u64;
        assert_eq!(
            Server::bounded_serialized_bytes(&request, request_bytes, "too large").unwrap(),
            request_bytes
        );
        assert!(
            Server::bounded_serialized_bytes(&request, request_bytes - 1, "too large").is_err()
        );
    }

    #[test]
    fn normal_frame_reader_bounds_input_and_preserves_legacy_eof() {
        let mut exact = BufReader::new(&b"abc\r\nrest"[..]);
        assert_eq!(
            read_bounded_normal_frame(&mut exact, 4).unwrap(),
            Some(b"abc".to_vec())
        );
        assert_eq!(
            read_bounded_normal_frame(&mut exact, 4).unwrap(),
            Some(b"rest".to_vec())
        );
        assert_eq!(read_bounded_normal_frame(&mut exact, 4).unwrap(), None);

        let mut exact_max = BufReader::new(&b"abcd\n"[..]);
        assert_eq!(
            read_bounded_normal_frame(&mut exact_max, 4).unwrap(),
            Some(b"abcd".to_vec())
        );
        let mut max_plus_one = BufReader::new(&b"abcde\n"[..]);
        assert!(read_bounded_normal_frame(&mut max_plus_one, 4).is_err());
    }

    #[derive(Default)]
    struct SharedSinkState {
        bytes: Vec<u8>,
        writes: usize,
        flushes: usize,
        fail_flush: bool,
    }

    struct SharedSink(Rc<RefCell<SharedSinkState>>);

    impl Write for SharedSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let mut state = self.0.borrow_mut();
            state.writes += 1;
            state.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            let mut state = self.0.borrow_mut();
            state.flushes += 1;
            if state.fail_flush {
                return Err(io::Error::other("injected artifact flush failure"));
            }
            Ok(())
        }
    }

    #[test]
    fn buffered_artifact_writer_preserves_bytes_hash_and_bounds_inner_writes() {
        let state = Rc::new(RefCell::new(SharedSinkState::default()));
        let mut writer = buffered_artifact_writer(SharedSink(Rc::clone(&state)));
        let payload_len = STREAM_PUBLICATION_BUFFER_BYTES * 2 + 137;
        let payload: Vec<u8> = (0..payload_len).map(|index| (index % 251) as u8).collect();
        for chunk in payload.chunks(8) {
            writer.write_all(chunk).unwrap();
        }
        let evidence = writer.finish().unwrap();

        let state = state.borrow();
        assert_eq!(state.bytes, payload);
        assert_eq!(evidence.bytes, payload_len as u64);
        assert_eq!(evidence.sha256, Server::sha256_hex(&payload));
        assert!(
            state.writes <= 3,
            "fixed-capacity buffering emitted {} inner writes for {} eight-byte chunks",
            state.writes,
            payload_len.div_ceil(8)
        );
        assert_eq!(state.flushes, 1);
    }

    #[test]
    fn artifact_evidence_is_not_returned_when_final_flush_fails() {
        let state = Rc::new(RefCell::new(SharedSinkState {
            fail_flush: true,
            ..SharedSinkState::default()
        }));
        let mut writer = buffered_artifact_writer(SharedSink(Rc::clone(&state)));
        writer.write_all(b"canonical bytes pending flush").unwrap();
        let error = match writer.finish() {
            Ok(_) => panic!("flush failure returned artifact evidence"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("injected artifact flush failure")
        );
        let state = state.borrow();
        assert_eq!(state.bytes, b"canonical bytes pending flush");
        assert_eq!(state.flushes, 1);
    }

    #[test]
    fn bounded_publication_writer_preserves_bytes_across_multiple_cache_windows() {
        let path = temp_path("bounded-publication-cache-windows");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let payload = vec![0x5au8; 10 * 1024 + 37];
        {
            let mut writer =
                BoundedPublicationWriter::with_window(&mut file, "unit_publication", 4 * 1024);
            for chunk in payload.chunks(137) {
                writer.write_all(chunk).unwrap();
            }
            writer.flush().unwrap();
            assert_eq!(writer.written_bytes, payload.len() as u64);
            assert_eq!(writer.released_bytes, 8 * 1024);
        }
        file.sync_all().unwrap();
        drop(file);
        assert_eq!(fs::read(&path).unwrap(), payload);
        let _ = fs::remove_file(path);
    }

    #[test]
    fn bounded_publication_writer_propagates_cache_release_failure() {
        fn reject_release(
            _file: &fs::File,
            _offset: u64,
            _bytes: u64,
            _stage: &str,
        ) -> io::Result<()> {
            Err(io::Error::other("injected cache release failure"))
        }

        let path = temp_path("bounded-publication-cache-failure");
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let error = {
            let mut writer = BoundedPublicationWriter::with_window_and_release(
                &mut file,
                "unit_publication",
                4 * 1024,
                reject_release,
            );
            writer.write_all(&vec![0x5au8; 8 * 1024]).unwrap_err()
        };
        assert_eq!(error.to_string(), "injected cache release failure");
        let _ = fs::remove_file(path);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn publication_cache_ranges_reject_off64_overflow() {
        assert!(Server::checked_cache_range(i64::MAX as u64 - 1, 1).is_ok());
        assert!(Server::checked_cache_range(i64::MAX as u64, 1).is_err());
        assert!(Server::checked_cache_range(i64::MAX as u64 + 1, 1).is_err());
        assert!(Server::checked_cache_range(0, i64::MAX as u64 + 1).is_err());
        assert!(Server::checked_cache_range(u64::MAX, 1).is_err());
    }

    #[test]
    fn progress_emitter_is_monotonic_cadenced_and_never_heartbeats_without_advance() {
        let now = Rc::new(Cell::new(0u64));
        let clock = Rc::clone(&now);
        let policy = NativeProgressPolicy::checked_in_candidate().unwrap();
        let mut output = Vec::new();
        let mut progress = NativeCommitProgress::start_with_clock(
            &mut output,
            policy,
            42,
            Box::new(move || Ok(clock.get())),
        )
        .unwrap();
        progress
            .enter_phase("wal_verify", 0, None, 0, Some(200_000_000))
            .unwrap();
        let after_entries = progress.emitted_frames;

        now.set(500);
        progress
            .advance(0, None, 67_108_863, Some(200_000_000))
            .unwrap();
        assert_eq!(progress.emitted_frames, after_entries);
        progress
            .advance(0, None, 67_108_864, Some(200_000_000))
            .unwrap();
        assert_eq!(progress.emitted_frames, after_entries + 1);

        now.set(5_499);
        progress
            .advance(0, None, 67_108_865, Some(200_000_000))
            .unwrap();
        assert_eq!(progress.emitted_frames, after_entries + 1);
        now.set(5_500);
        progress
            .advance(0, None, 67_108_866, Some(200_000_000))
            .unwrap();
        assert_eq!(progress.emitted_frames, after_entries + 2);

        now.set(10_500);
        progress
            .advance(0, None, 67_108_866, Some(200_000_000))
            .unwrap();
        assert_eq!(progress.emitted_frames, after_entries + 2);
        assert!(
            progress
                .advance(0, None, 67_108_865, Some(200_000_000))
                .is_err()
        );
    }

    struct FailAfterBytes {
        remaining: usize,
    }

    impl Write for FailAfterBytes {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "closed progress pipe",
                ));
            }
            let written = bytes.len().min(self.remaining);
            self.remaining -= written;
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn progress_output_failure_is_fatal_to_the_emitter() {
        let policy = NativeProgressPolicy::checked_in_candidate().unwrap();
        let admitted_bytes = NativeProgressFrame::admitted(7)
            .unwrap()
            .canonical_bytes()
            .len()
            + 1;
        let mut output = FailAfterBytes {
            remaining: admitted_bytes,
        };
        let mut progress =
            NativeCommitProgress::start_with_clock(&mut output, policy, 7, Box::new(|| Ok(0)))
                .unwrap();
        let error = progress
            .enter_phase("wal_verify", 0, None, 0, Some(1))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn progress_negotiation_rejects_duplicates_without_tightening_legacy_unknown_fields() {
        assert!(
            serde_json::from_str::<IncomingRpcRequest>(
                r#"{"id":1,"method":"batch_commit","params":{},"progressProtocolVersion":1,"progressProtocolVersion":1}"#,
            )
            .is_err()
        );
        let legacy = serde_json::from_str::<IncomingRpcRequest>(
            r#"{"id":1,"method":"ping","params":{},"legacyIgnoredField":true}"#,
        )
        .unwrap();
        assert_eq!(legacy.method, "ping");
        assert_eq!(legacy.progress_protocol_version, None);
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

    #[test]
    fn wal_record_two_pass_stream_matches_v2_bytes_and_enforces_limit() {
        let request = RpcRequest {
            id: 17,
            method: "memory_save".to_string(),
            params: json!({"snapshot":{"corpusId":"corpus","facts":[]}}),
        };
        let mut expected = serde_json::to_vec(&WalRecord {
            version: Server::WAL_VERSION,
            base_generation: 7,
            request: request.clone(),
        })
        .unwrap();
        expected.push(b'\n');

        let counted = Server::stream_wal_record(io::sink(), 7, &request, None).unwrap();
        assert_eq!(counted.bytes, expected.len() as u64);
        assert_eq!(counted.record_digest, Server::digest_bytes(&expected));

        let prefix = b"existing WAL\n";
        let mut seeded = Sha256::new();
        seeded.update(prefix);
        let written = Server::stream_wal_record(Vec::new(), 7, &request, Some(seeded)).unwrap();
        assert_eq!(written.inner, expected);
        assert_eq!(written.bytes, counted.bytes);
        assert_eq!(written.record_digest, counted.record_digest);
        Server::require_same_wal_record(&counted, &written).unwrap();
        let mut complete = prefix.to_vec();
        complete.extend_from_slice(&expected);
        assert_eq!(
            written.wal_hasher.unwrap().finalize().as_slice(),
            Sha256::digest(&complete).as_slice()
        );

        assert!(
            Server::stream_wal_record_with_limit(
                io::sink(),
                7,
                &request,
                None,
                expected.len() as u64,
            )
            .is_ok()
        );
        assert!(
            Server::stream_wal_record_with_limit(
                io::sink(),
                7,
                &request,
                None,
                expected.len() as u64 - 1,
            )
            .is_err()
        );

        let changed = RpcRequest {
            id: request.id,
            method: request.method.clone(),
            params: json!({"snapshot":{"corpusId":"edited","facts":[]}}),
        };
        let changed = Server::stream_wal_record(io::sink(), 7, &changed, None).unwrap();
        assert_eq!(counted.bytes, changed.bytes);
        assert_ne!(counted.record_digest, changed.record_digest);
        assert!(Server::require_same_wal_record(&counted, &changed).is_err());

        let mut production_limit = LimitedHashWriter::new(io::sink(), MAX_WAL_RECORD_BYTES, None);
        production_limit.bytes = MAX_WAL_RECORD_BYTES - 1;
        assert_eq!(production_limit.write(b"x").unwrap(), 1);
        assert_eq!(production_limit.bytes, MAX_WAL_RECORD_BYTES);
        assert!(production_limit.write(b"y").is_err());

        let mut overflow = LimitedHashWriter::new(io::sink(), u64::MAX, None);
        overflow.bytes = u64::MAX;
        assert!(overflow.write(b"x").is_err());
    }

    #[test]
    fn wal_record_stream_propagates_partial_writer_failure_without_evidence() {
        struct FailAfter {
            remaining: usize,
        }

        impl Write for FailAfter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.remaining == 0 {
                    return Err(io::Error::other("injected partial write"));
                }
                let written = bytes.len().min(self.remaining);
                self.remaining -= written;
                Ok(written)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let request = RpcRequest {
            id: 19,
            method: "memory_save".to_string(),
            params: json!({"snapshot":{"corpusId":"corpus","facts":[]}}),
        };
        assert!(
            Server::stream_wal_record(
                FailAfter { remaining: 17 },
                7,
                &request,
                Some(Sha256::new()),
            )
            .is_err()
        );
    }

    #[test]
    fn strict_lf_framer_hashes_exact_frames_without_confusing_escaped_lf() {
        let raw = b"{\"value\":\"line\\ninside\"}\n{\"value\":2}\n";
        let expected = [
            b"{\"value\":\"line\\ninside\"}".as_slice(),
            b"{\"value\":2}".as_slice(),
        ];
        let mut source = BufReader::new(raw.as_slice());
        let mut wal_hasher = Sha256::new();
        let mut total = 0_u64;
        for expected_payload in expected {
            let mut record = StrictLfRecordReader::new(&mut source, 128, &mut wal_hasher);
            let mut payload = Vec::new();
            record.read_to_end(&mut payload).unwrap();
            let evidence = record.finish().unwrap();
            assert_eq!(payload, expected_payload);
            let mut framed = expected_payload.to_vec();
            framed.push(b'\n');
            assert_eq!(evidence.bytes, framed.len() as u64);
            assert_eq!(evidence.digest, Server::digest_bytes(&framed));
            total += evidence.bytes;
        }
        assert!(source.fill_buf().unwrap().is_empty());
        assert_eq!(total, raw.len() as u64);
        assert_eq!(
            wal_hasher.finalize().as_slice(),
            Sha256::digest(raw).as_slice()
        );
    }

    #[test]
    fn strict_lf_framer_rejects_partial_blank_and_record_limit_plus_one() {
        fn frame(raw: &[u8], limit: u64) -> io::Result<(Vec<u8>, WalRecordReadEvidence)> {
            let mut source = BufReader::new(raw);
            let mut wal_hasher = Sha256::new();
            let mut record = StrictLfRecordReader::new(&mut source, limit, &mut wal_hasher);
            let mut payload = Vec::new();
            record.read_to_end(&mut payload)?;
            Ok((payload, record.finish()?))
        }

        let (payload, evidence) = frame(b"{}\n", 3).unwrap();
        assert_eq!(payload, b"{}");
        assert_eq!(evidence.bytes, 3);
        assert!(frame(b"{}\n", 2).is_err(), "record max+1");
        assert!(frame(b"{}", 3).is_err(), "missing final LF");
        assert!(frame(b"\n", 1).is_err(), "blank record");
    }

    #[test]
    fn semantic_wal_scanner_is_strict_for_v2_and_generation_zero_legacy() {
        fn scan(raw: &[u8], allow_legacy: bool) -> io::Result<WalScanEvidence> {
            let path = temp_path("semantic-wal-scan");
            fs::write(&path, raw)?;
            let mut file = Server::open_regular_nofollow(&path)?;
            let result = Server::scan_wal_file(&mut file, allow_legacy);
            fs::remove_file(path)?;
            result
        }

        fn scan_limits(
            raw: &[u8],
            record_limit: u64,
            wal_limit: u64,
            record_count_limit: u64,
        ) -> io::Result<WalScanEvidence> {
            let path = temp_path("semantic-wal-scan-limits");
            fs::write(&path, raw)?;
            let mut file = Server::open_regular_nofollow(&path)?;
            let result = Server::scan_wal_file_with_limits(
                &mut file,
                false,
                record_limit,
                wal_limit,
                record_count_limit,
            );
            fs::remove_file(path)?;
            result
        }

        let request = RpcRequest {
            id: 23,
            method: "memory_save".to_string(),
            params: json!({"snapshot":{"corpusId":"corpus","facts":[]}}),
        };
        let v2 = Server::stream_wal_record(Vec::new(), 9, &request, None)
            .unwrap()
            .inner;
        let evidence = scan(&v2, false).unwrap();
        assert_eq!(evidence.bytes, v2.len() as u64);
        assert_eq!(evidence.record_count, 1);
        assert_eq!(evidence.base_generation, Some(9));
        assert!(!evidence.legacy_raw);
        assert!(scan_limits(&v2, v2.len() as u64, v2.len() as u64, 1).is_ok());
        assert!(scan_limits(&v2, v2.len() as u64 - 1, v2.len() as u64, 1).is_err());
        assert!(scan_limits(&v2, v2.len() as u64, v2.len() as u64 - 1, 1).is_err());
        let mut two_records = v2.clone();
        two_records.extend_from_slice(&v2);
        assert!(scan_limits(&two_records, v2.len() as u64, two_records.len() as u64, 1,).is_err());

        let legacy = format!(
            "{}\n{}\n",
            serde_json::to_string(&request).unwrap(),
            serde_json::to_string(&request).unwrap()
        );
        let evidence = scan(legacy.as_bytes(), true).unwrap();
        assert_eq!(evidence.record_count, 2);
        assert_eq!(evidence.base_generation, Some(0));
        assert!(evidence.legacy_raw);
        assert!(scan(legacy.as_bytes(), false).is_err());

        let invalid: [&[u8]; 4] = [
            b"{\"version\":null,\"baseGeneration\":0,\"request\":{\"id\":1,\"method\":\"memory_save\",\"params\":{}}}\n",
            b"{\"version\":2,\"baseGeneration\":0,\"request\":{\"id\":1,\"method\":\"memory_save\",\"params\":{}},\"id\":1,\"method\":\"memory_save\",\"params\":{}}\n",
            b"{\"version\":2,\"baseGeneration\":0,\"request\":{\"id\":1,\"method\":\"memory_save\",\"params\":{}},\"unknown\":1}\n",
            b"{\"version\":2,\"baseGeneration\":0,\"request\":{\"id\":1,\"method\":\"memory_save\",\"params\":{}}}",
        ];
        for raw in invalid {
            assert!(
                scan(raw, true).is_err(),
                "accepted {}",
                String::from_utf8_lossy(raw)
            );
        }

        let oversized_key = format!(
            "{{\"{}\":1,\"version\":2,\"baseGeneration\":0,\"request\":{{\"id\":1,\"method\":\"memory_save\",\"params\":{{}}}}}}\n",
            "k".repeat(65)
        );
        assert!(scan(oversized_key.as_bytes(), false).is_err());
        let oversized_method = format!(
            "{{\"version\":2,\"baseGeneration\":0,\"request\":{{\"id\":1,\"method\":\"{}\",\"params\":{{}}}}}}\n",
            "m".repeat(65)
        );
        assert!(scan(oversized_method.as_bytes(), false).is_err());
        let large_param = format!(
            "{{\"version\":2,\"baseGeneration\":0,\"request\":{{\"id\":1,\"method\":\"memory_save\",\"params\":{{\"payload\":\"{}\"}}}}}}\n",
            "p".repeat(1_048_576)
        );
        assert!(scan(large_param.as_bytes(), false).is_ok());
        let nested = format!(
            "{{\"version\":2,\"baseGeneration\":0,\"request\":{{\"id\":1,\"method\":\"memory_save\",\"params\":{}}}}}\n",
            "[".repeat(127) + &"]".repeat(127)
        );
        assert!(scan(nested.as_bytes(), false).is_err());
    }

    fn reference_cosine(query: &[f64], vector: &[f64]) -> f64 {
        let mut dot = 0.0;
        let mut query_norm = 0.0;
        let mut vector_norm = 0.0;
        for index in 0..query.len() {
            dot += query[index] * vector[index];
            query_norm += query[index] * query[index];
            vector_norm += vector[index] * vector[index];
        }
        dot / (query_norm.sqrt() * vector_norm.sqrt())
    }

    #[test]
    fn exact_vector_search_preserves_finite_scores_and_deterministic_ties() {
        let query = [1.0, 0.0];
        let records = [
            ("tie-b", vec![0.8, 0.6]),
            ("low", vec![0.1, 0.995]),
            ("tie-a", vec![0.8, 0.6]),
            ("high", vec![1.0, 0.0]),
            ("zero", vec![0.0, 0.0]),
            ("huge", vec![1e308, 1e308]),
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

        let hits = Server::vector_search_candidates(
            &keys,
            &vectors,
            &vector_values,
            &query,
            Some(-1.0),
            10,
        )
        .expect("exact scan succeeds");
        assert!(hits.iter().all(|hit| hit.score.is_finite()));
        assert_eq!(
            hits.iter()
                .take(3)
                .map(|hit| hit.key.clone())
                .collect::<Vec<_>>(),
            vec![
                Server::key("corpus", "high"),
                Server::key("corpus", "tie-a"),
                Server::key("corpus", "tie-b"),
            ]
        );
        let huge_score = hits
            .iter()
            .find(|hit| hit.key.ends_with(":huge"))
            .expect("huge vector is retained")
            .score;
        assert!(huge_score.is_finite());
        let tie_scores = hits
            .iter()
            .filter(|hit| hit.key.ends_with(":tie-a") || hit.key.ends_with(":tie-b"))
            .map(|hit| hit.score.to_bits())
            .collect::<Vec<_>>();
        assert_eq!(tie_scores.len(), 2);
        assert_eq!(tie_scores[0], tie_scores[1]);
        let huge_self_score = Server::stable_cosine(&[1e308, 1e308], &[1e308, 1e308]);
        assert!(huge_self_score.is_finite());
        assert!(huge_self_score > 0.999999);
        for hit in &hits {
            if let Some(values) = vector_values.get(&hit.key) {
                if values.iter().all(|value| value.abs() < 1e300)
                    && values.iter().any(|value| *value != 0.0)
                {
                    assert_eq!(
                        hit.score.to_bits(),
                        reference_cosine(&query, values).to_bits(),
                        "finite score changed for {}",
                        hit.key
                    );
                }
            }
        }
    }

    #[test]
    fn exact_vector_search_handles_negative_threshold_and_top_k_zero() {
        let keys = ["a", "b", "c"]
            .into_iter()
            .map(|id| Server::key("corpus", id))
            .collect::<Vec<_>>();
        let vectors = keys
            .iter()
            .map(|key| {
                (
                    key.clone(),
                    vector_record(key.rsplit(':').next().unwrap(), "corpus", "default"),
                )
            })
            .collect::<HashMap<_, _>>();
        let vector_values = [
            (keys[0].clone(), vec![1.0, 0.0]),
            (keys[1].clone(), vec![-1.0, 0.0]),
            (keys[2].clone(), vec![0.0, 0.0]),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        let negative = Server::vector_search_candidates(
            &keys,
            &vectors,
            &vector_values,
            &[1.0, 0.0],
            Some(-1.0),
            10,
        )
        .expect("negative threshold succeeds");
        assert_eq!(negative.len(), 3);
        assert!(negative.iter().all(|hit| hit.score.is_finite()));
        assert!(
            Server::vector_search_candidates(
                &keys,
                &vectors,
                &vector_values,
                &[1.0, 0.0],
                Some(-1.0),
                0
            )
            .expect("topK zero succeeds")
            .is_empty()
        );
    }

    #[test]
    fn production_shaped_exact_scan_is_bounded_to_top_k() {
        let dimensions = 1024;
        let candidate_count = 8192;
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
                .expect("production-shaped scan succeeds");
        assert_eq!(hits.len(), 25);
        assert!(hits.windows(2).all(|pair| {
            pair[0].score > pair[1].score
                || (pair[0].score == pair[1].score && pair[0].key < pair[1].key)
        }));
        eprintln!(
            "production-shaped vector_search candidates={candidate_count} dimensions={dimensions} topK=25 elapsed_ms={}",
            started.elapsed().as_millis()
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn no_replace_rename_preserves_an_existing_destination() {
        let source = temp_path("agdb-native-rename-source");
        let destination = temp_path("agdb-native-rename-destination");
        fs::write(&source, b"source").expect("write rename source");
        fs::write(&destination, b"destination").expect("write rename destination");

        let error = Server::rename_noreplace(&source, &destination)
            .expect_err("no-replace rename must reject a destination collision");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&source).unwrap(), b"source");
        assert_eq!(fs::read(&destination).unwrap(), b"destination");
        let _ = fs::remove_file(source);
        let _ = fs::remove_file(destination);
    }

    fn begin_batch(server: &mut Server) {
        let response = server.handle(RpcRequest {
            id: 90,
            method: "batch_begin".to_string(),
            params: json!({}),
        });
        assert!(response.ok, "batch_begin failed: {response:?}");
    }

    fn apply_mutation(server: &mut Server, request: RpcRequest) {
        let canonical = server
            .canonicalize_request(request)
            .expect("canonical mutation request");
        server
            .validate_mutation_params(&canonical)
            .expect("mutation preflight failed");
        server.wal_append(&canonical).expect("append mutation WAL");
        let response = server.handle_prepared(canonical);
        assert!(response.ok, "mutation failed: {response:?}");
    }

    fn commit_batch(server: &mut Server) -> RpcResponse {
        let prepared = server.handle(RpcRequest {
            id: 91,
            method: "batch_prepare_commit".to_string(),
            params: json!({}),
        });
        assert!(prepared.ok, "batch_prepare_commit failed: {prepared:?}");
        let response = server.handle(RpcRequest {
            id: 92,
            method: "batch_commit".to_string(),
            params: json!({
                "preparedCommitEvidence": prepared.result.expect("prepared evidence")
            }),
        });
        assert!(response.ok, "batch_commit failed: {response:?}");
        response
    }

    #[test]
    fn vector_rpc_limits_are_advertised_and_fail_closed_before_work() {
        let path = temp_path("agdb-native-vector-limits");
        let mut server = Server::open(path.clone()).expect("open server");

        let protocol = server.handle(RpcRequest {
            id: 1,
            method: "protocol_info".to_string(),
            params: json!({}),
        });
        assert!(protocol.ok);
        assert_eq!(
            protocol.result.as_ref().unwrap()["limits"]["vector"]["maxDimensions"],
            json!(MAX_VECTOR_DIMENSIONS)
        );
        assert_eq!(
            protocol.result.as_ref().unwrap()["limits"]["vector"]["maxSearchTopK"],
            json!(MAX_VECTOR_SEARCH_TOP_K)
        );
        assert_eq!(
            protocol.result.as_ref().unwrap()["limits"]["wal"],
            json!({
                "maxRecordBytes": MAX_WAL_RECORD_BYTES,
                "maxBytes": MAX_WAL_BYTES,
                "maxRecords": MAX_WAL_RECORDS,
                "mutationRequestIdUniqueness": "activeTransaction",
            })
        );

        let boundary = server.handle(RpcRequest {
            id: 2,
            method: "vector_search".to_string(),
            params: json!({
                "corpusId": "c1",
                "namespace": "default",
                "queryVector": vec![0.0; MAX_VECTOR_DIMENSIONS],
                "topK": MAX_VECTOR_SEARCH_TOP_K,
            }),
        });
        assert!(boundary.ok, "declared vector limits must be accepted");

        for (id, params) in [
            (
                3,
                json!({
                    "corpusId": "c1",
                    "namespace": "default",
                    "queryVector": vec![0.0; MAX_VECTOR_DIMENSIONS + 1],
                    "topK": 1,
                }),
            ),
            (
                4,
                json!({
                    "corpusId": "c1",
                    "namespace": "default",
                    "queryVector": [1.0],
                    "topK": MAX_VECTOR_SEARCH_TOP_K + 1,
                }),
            ),
            (
                5,
                json!({
                    "corpusId": "c1",
                    "namespace": "default",
                    "queryVector": [1.0],
                    "topK": 0,
                }),
            ),
            (
                6,
                json!({
                    "corpusId": "c1",
                    "namespace": "default",
                    "queryVector": [1.0, "not-a-number"],
                    "topK": 1,
                }),
            ),
        ] {
            let response = server.handle(RpcRequest {
                id,
                method: "vector_search".to_string(),
                params,
            });
            assert!(!response.ok, "out-of-contract vector search must fail");
            let error = response.error.expect("structured client error");
            assert_eq!(error.code, "REQUEST_EXECUTION_FAILED");
            assert_eq!(error.failure_class.as_deref(), Some("CLIENT_INPUT"));
        }

        begin_batch(&mut server);
        let oversized_upsert = server.handle(RpcRequest {
            id: 7,
            method: "vector_upsert".to_string(),
            params: json!({
                "records": [{
                    "id": "too-wide",
                    "corpusId": "c1",
                    "namespace": "default",
                    "values": vec![0.0; MAX_VECTOR_DIMENSIONS + 1],
                    "metadata": {"documentId": "d1"},
                }]
            }),
        });
        assert!(!oversized_upsert.ok);
        let error = oversized_upsert.error.expect("structured upsert error");
        assert_eq!(error.failure_class.as_deref(), Some("CLIENT_INPUT"));
        assert!(server.state.vectors.is_empty());
        assert!(matches!(
            server.transaction,
            TransactionState::Active {
                mutation_seen: false,
                ..
            }
        ));

        cleanup(&path);
    }

    #[test]
    fn persists_vector_values_in_blob_file() {
        let path = temp_path("agdb-native-vblob");
        let mut server = Server::open(path.clone()).expect("open server");
        begin_batch(&mut server);
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
        apply_mutation(&mut server, req);
        let token_response = commit_batch(&mut server);
        let token: DurableGenerationToken =
            serde_json::from_value(token_response.result.unwrap()).expect("generation token");

        let db_raw = fs::read_to_string(&path).expect("read db");
        assert!(db_raw.contains("\"blobRef\""));
        assert!(!db_raw.contains("[1.0,0.0,0.5]"));

        let blob_path = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(&token.vector_blob.basename);
        let blob = fs::read(blob_path).expect("read blob");
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
        begin_batch(&mut server);
        apply_mutation(
            &mut server,
            RpcRequest {
                id: 2,
                method: "vector_upsert".to_string(),
                params: json!({
                    "records": [{
                        "id": "vec-legacy",
                        "corpusId": "c1",
                        "namespace": "default",
                        "values": [0.5, 0.5],
                        "metadata": {"documentId":"d2"}
                    }]
                }),
            },
        );
        commit_batch(&mut server);

        let db_raw = fs::read_to_string(&path).expect("read migrated db");
        assert!(db_raw.contains("\"blobRef\""));
        assert!(!db_raw.contains("\"values\":[0.5,0.5]"));
        let descriptor = serde_json::from_str::<State>(&db_raw)
            .expect("parse migrated state")
            .vector_blob
            .expect("migrated descriptor");
        let blob_path = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(descriptor.basename);
        let blob = fs::read(blob_path).expect("read migrated blob");
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

    #[test]
    fn reads_legacy_adjacent_vblob_without_a_generation_descriptor() {
        let path = temp_path("agdb-native-vblob-adjacent-legacy");
        let legacy = json!({
            "nodes": {},
            "edges": {},
            "vectors": {
                "c1:vec-legacy": {
                    "id": "vec-legacy",
                    "corpusId": "c1",
                    "namespace": "default",
                    "blobRef": {"offset": 0, "len": 2},
                    "metadata": {"documentId":"d3"}
                }
            },
            "passages": {},
            "snapshots": {},
            "checkpoints": {}
        });
        fs::write(&path, legacy.to_string()).expect("write legacy state");
        let mut blob = Vec::new();
        blob.extend_from_slice(Server::VECTOR_BLOB_MAGIC);
        blob.extend_from_slice(&Server::VECTOR_BLOB_VERSION.to_le_bytes());
        blob.extend_from_slice(&1.0f64.to_le_bytes());
        blob.extend_from_slice(&0.0f64.to_le_bytes());
        fs::write(path.with_extension("vblob"), blob).expect("write legacy adjacent blob");

        let mut server = Server::open(path.clone()).expect("open legacy adjacent pair");
        let search = server.handle(RpcRequest {
            id: 1,
            method: "vector_search".to_string(),
            params: json!({
                "corpusId":"c1",
                "namespace":"default",
                "queryVector":[1.0,0.0],
                "topK": 1
            }),
        });
        assert!(search.ok);
        assert_eq!(search.result.unwrap()[0]["id"].as_str(), Some("vec-legacy"));
        cleanup(&path);
    }

    #[test]
    fn rejects_invalid_committed_blob_descriptor_without_rewriting_json() {
        let path = temp_path("agdb-native-descriptor-rejection");
        let mut server = Server::open(path.clone()).expect("open server");
        begin_batch(&mut server);
        let request = RpcRequest {
            id: 1,
            method: "vector_upsert".to_string(),
            params: json!({
                "records": [{
                    "id": "vec-1",
                    "corpusId": "c1",
                    "namespace": "default",
                    "values": [1.0, 0.0],
                    "metadata": {"documentId":"d4"}
                }]
            }),
        };
        apply_mutation(&mut server, request);
        commit_batch(&mut server);
        let canonical = fs::read(&path).expect("read canonical state");
        let state: State = serde_json::from_slice(&canonical).expect("parse state");
        let descriptor = state.vector_blob.clone().expect("descriptor");

        let cases: &[(&str, fn(&mut VectorBlobDescriptor))] = &[
            ("missing", |descriptor: &mut VectorBlobDescriptor| {
                descriptor.basename = "missing.vblob".to_string();
            }),
            ("hash", |descriptor: &mut VectorBlobDescriptor| {
                descriptor.sha256 = "00".repeat(32);
            }),
            ("size", |descriptor: &mut VectorBlobDescriptor| {
                descriptor.size += 1;
            }),
            ("format", |descriptor: &mut VectorBlobDescriptor| {
                descriptor.format = 99;
            }),
            ("path", |descriptor: &mut VectorBlobDescriptor| {
                descriptor.basename = "../escape.vblob".to_string();
            }),
        ];
        for (label, mutate) in cases {
            let mut tampered = state.clone();
            let mut tampered_descriptor = descriptor.clone();
            mutate(&mut tampered_descriptor);
            tampered.vector_blob = Some(tampered_descriptor);
            fs::write(&path, serde_json::to_vec(&tampered).unwrap())
                .unwrap_or_else(|_| panic!("write {label} descriptor"));
            assert!(
                Server::open(path.clone()).is_err(),
                "{label} must fail closed"
            );
            assert_eq!(
                fs::read(&path).expect("tampered canonical JSON remains"),
                serde_json::to_vec(&tampered).expect("tampered JSON serialization")
            );
        }
        fs::write(&path, canonical).expect("restore canonical state");
        cleanup(&path);
    }

    #[test]
    fn method_policy_inventory_exercises_every_manual_dispatch_arm() {
        let expected = [
            "ping",
            "protocol_info",
            "batch_begin",
            "batch_prepare_commit",
            "batch_commit",
            "recovery_discard",
            "upsert_nodes",
            "upsert_edges",
            "get_node",
            "get_nodes",
            "get_edges",
            "get_adjacent",
            "delete_nodes",
            "delete_edges",
            "delete_by_document",
            "delete_by_corpus",
            "vector_upsert",
            "vector_search",
            "vector_delete_by_document",
            "memory_upsert",
            "memory_save",
            "memory_save_file",
            "memory_load",
            "memory_get_schemas_by_ids",
            "memory_get_active_facts",
            "memory_activate_facts_by_schema_ids",
            "memory_save_checkpoint",
            "memory_load_checkpoint",
            "memory_validate_integrity",
            "projection_get_transitions",
            "projection_get_dangling_nodes",
            "projection_get_node_count",
            "lexical_index_passages",
            "lexical_search",
            "lexical_delete_by_document",
            "cypher_query",
            "__debug_force_panic__",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let actual = METHOD_SPECS
            .iter()
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual, expected);

        let expected_bounded_indexing = [
            "memory_upsert",
            "memory_get_schemas_by_ids",
            "memory_get_active_facts",
            "memory_activate_facts_by_schema_ids",
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();
        let actual_bounded_indexing = METHOD_SPECS
            .iter()
            .filter(|spec| spec.wire_profile == MethodWireProfile::BoundedIndexing)
            .map(|spec| spec.name)
            .collect::<BTreeSet<_>>();
        assert_eq!(actual_bounded_indexing, expected_bounded_indexing);
        assert!(!Server::is_indexing_memory_method(
            "unknown_dispatch_method"
        ));

        for spec in METHOD_SPECS {
            let path = temp_path("agdb-native-dispatch");
            let mut server = Server::open(path.clone()).expect("open dispatch server");
            if spec.wal {
                server.transaction = TransactionState::Active {
                    base_generation: 0,
                    transaction_nonce: "11".repeat(32),
                    mutation_seen: false,
                };
            }
            let response = server.handle(RpcRequest {
                id: 1,
                method: spec.name.to_string(),
                params: json!({}),
            });
            if spec.name != "__debug_force_panic__" {
                assert_ne!(
                    response.error.as_ref().map(|error| error.code.as_str()),
                    Some("UNSUPPORTED_FEATURE"),
                    "policy method {} did not reach a dispatch arm",
                    spec.name
                );
            }
            cleanup(&path);
        }
        let path = temp_path("agdb-native-dispatch-unknown");
        let mut server = Server::open(path.clone()).expect("open unknown dispatch server");
        let response = server.handle(RpcRequest {
            id: 1,
            method: "unknown_dispatch_method".to_string(),
            params: json!({}),
        });
        assert_eq!(
            response.error.as_ref().map(|error| error.code.as_str()),
            Some("UNSUPPORTED_FEATURE")
        );
        cleanup(&path);
    }

    #[cfg(unix)]
    fn descriptor_fixture(
        generation: u64,
        descriptor: bool,
    ) -> (PathBuf, PathBuf, DescriptorReadConfig, String) {
        use std::os::fd::IntoRawFd;

        let canonical_path = temp_path("agdb-native-descriptor-canonical");
        let blob_path = temp_path("agdb-native-descriptor-blob");
        let mut blob = Vec::new();
        blob.extend_from_slice(Server::VECTOR_BLOB_MAGIC);
        blob.extend_from_slice(&Server::VECTOR_BLOB_VERSION.to_le_bytes());
        blob.extend_from_slice(&1.0f64.to_le_bytes());
        blob.extend_from_slice(&0.0f64.to_le_bytes());
        let blob_sha256 = Server::sha256_hex(&blob);
        let canonical = if descriptor {
            json!({
                "nodes": {},
                "edges": {},
                "generation": generation,
                "vectorBlob": {
                    "basename": "copied.vblob",
                    "size": blob.len(),
                    "sha256": blob_sha256,
                    "format": Server::VECTOR_BLOB_VERSION,
                },
                "vectors": {
                    "c1:v1": {
                        "id": "v1",
                        "corpusId": "c1",
                        "namespace": "default",
                        "blobRef": {"offset": 0, "len": 2},
                        "metadata": {"documentId": "d1"}
                    }
                },
                "passages": {},
                "snapshots": {},
                "checkpoints": {}
            })
        } else {
            json!({
                "nodes": {},
                "edges": {},
                "generation": generation,
                "vectors": {},
                "passages": {},
                "snapshots": {},
                "checkpoints": {}
            })
        };
        let canonical_raw = serde_json::to_vec(&canonical).expect("serialize descriptor fixture");
        fs::write(&canonical_path, canonical_raw).expect("write canonical fixture");
        fs::write(&blob_path, &blob).expect("write blob fixture");
        let canonical_fd = fs::OpenOptions::new()
            .read(true)
            .open(&canonical_path)
            .expect("open canonical fixture")
            .into_raw_fd();
        let vector_blob_fd = fs::OpenOptions::new()
            .read(true)
            .open(&blob_path)
            .expect("open blob fixture")
            .into_raw_fd();
        let config = DescriptorReadConfig {
            canonical_fd,
            vector_blob_fd,
            expected_generation: generation,
            legacy_generation0: generation == 0 && !descriptor,
            legacy_binding_sha256: (generation == 0 && !descriptor).then(|| "ab".repeat(32)),
        };
        (canonical_path, blob_path, config, blob_sha256)
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_read_handshake_is_authoritative_and_mutations_are_rejected() {
        let (canonical_path, blob_path, config, blob_sha256) = descriptor_fixture(1, true);
        let mut server = Server::open_descriptor_read(config).expect("descriptor mode opens");
        let sidecars = [
            canonical_path.clone(),
            blob_path.clone(),
            canonical_path.with_extension("native-audit.log"),
            canonical_path.with_extension("agdb.wal"),
        ];
        let before = sidecars
            .iter()
            .map(|path| path.exists())
            .collect::<Vec<_>>();
        let info = server.handle_prepared(RpcRequest {
            id: 1,
            method: "protocol_info".to_string(),
            params: json!({}),
        });
        assert!(info.ok, "protocol_info failed: {info:?}");
        let result = info.result.expect("protocol result");
        assert_eq!(
            result["accessMode"],
            json!(ACCESS_MODE_DESCRIPTOR_READ_ONLY)
        );
        assert_eq!(result["generation"], json!(1));
        assert_eq!(result["vectorBlobSha256"], json!(blob_sha256));
        assert_eq!(result["readOnly"], json!(true));
        let methods = result["methods"].as_array().expect("method inventory");
        assert!(methods.iter().all(|method| {
            matches!(
                method["classification"].as_str(),
                Some("health") | Some("read")
            ) && method["wal"] == json!(false)
        }));
        assert_eq!(
            result["methodInventorySha256"],
            json!("126080ca61644282fd7ed09b10d5fd0571eba49c6dc19132d4cc6168d07eaf1f")
        );
        let legacy_read = server.handle_prepared(RpcRequest {
            id: 2,
            method: "get_nodes".to_string(),
            params: json!({"corpusId": "c1"}),
        });
        assert_eq!(
            legacy_read.error.as_ref().map(|error| error.code.as_str()),
            Some(DESCRIPTOR_READ_ONLY_METHOD_CODE)
        );
        for method in [
            "batch_begin",
            "batch_commit",
            "recovery_discard",
            "upsert_nodes",
            "memory_save",
            "memory_save_file",
        ] {
            let response = server.handle_prepared(RpcRequest {
                id: 3,
                method: method.to_string(),
                params: json!({}),
            });
            assert!(!response.ok, "{method} must be rejected");
            assert_eq!(
                response.error.as_ref().map(|error| error.code.as_str()),
                Some(DESCRIPTOR_READ_ONLY_METHOD_CODE),
                "{method} must use stable descriptor rejection"
            );
        }
        assert!(server.audit_log_path.is_none());
        let after = sidecars
            .iter()
            .map(|path| path.exists())
            .collect::<Vec<_>>();
        assert_eq!(before, after, "descriptor reads must not create sidecars");
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_generation_zero_requires_explicit_legacy_binding_and_no_descriptor() {
        let (canonical_path, blob_path, config, blob_sha256) = descriptor_fixture(0, false);
        let mut server = Server::open_descriptor_read(config).expect("legacy descriptor opens");
        let info = server.handle_prepared(RpcRequest {
            id: 1,
            method: "protocol_info".to_string(),
            params: json!({}),
        });
        let result = info.result.expect("legacy protocol result");
        assert_eq!(result["generation"], json!(0));
        assert_eq!(result["legacyGeneration0"], json!(true));
        assert_eq!(result["vectorBlobSha256"], json!(blob_sha256));
        assert_eq!(result["legacyBindingSha256"], json!("ab".repeat(32)));
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);

        let (canonical_path, blob_path, mut bad, _) = descriptor_fixture(0, false);
        bad.legacy_generation0 = false;
        assert!(Server::open_descriptor_read(bad).is_err());
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_mode_rejects_generation_and_descriptor_mismatches() {
        let (canonical_path, blob_path, mut wrong_generation, _) = descriptor_fixture(1, true);
        wrong_generation.expected_generation = 2;
        assert!(Server::open_descriptor_read(wrong_generation).is_err());
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, mut missing_descriptor, _) = descriptor_fixture(1, true);
        missing_descriptor.expected_generation = 0;
        missing_descriptor.legacy_generation0 = true;
        missing_descriptor.legacy_binding_sha256 = Some("cd".repeat(32));
        assert!(Server::open_descriptor_read(missing_descriptor).is_err());
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_mode_rejects_blob_authority_and_fd_alias_mismatches() {
        use std::os::fd::IntoRawFd;

        let (canonical_path, blob_path, config, _) = descriptor_fixture(1, true);
        let mut blob = fs::read(&blob_path).expect("read blob fixture");
        *blob.last_mut().expect("blob payload") ^= 0x01;
        fs::write(&blob_path, blob).expect("rewrite blob fixture");
        assert!(
            Server::open_descriptor_read(config).is_err(),
            "hash mismatch"
        );
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, config, _) = descriptor_fixture(1, true);
        let mut blob = fs::read(&blob_path).expect("read blob fixture");
        blob.extend_from_slice(&0.0f64.to_le_bytes());
        fs::write(&blob_path, blob).expect("extend blob fixture");
        assert!(
            Server::open_descriptor_read(config).is_err(),
            "size mismatch"
        );
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, config, _) = descriptor_fixture(1, true);
        let mut canonical: Value =
            serde_json::from_slice(&fs::read(&canonical_path).expect("read canonical fixture"))
                .expect("parse canonical fixture");
        canonical["vectorBlob"]["format"] = json!(2);
        fs::write(
            &canonical_path,
            serde_json::to_vec(&canonical).expect("serialize canonical fixture"),
        )
        .expect("rewrite canonical fixture");
        assert!(
            Server::open_descriptor_read(config).is_err(),
            "format mismatch"
        );
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, mut config, _) = descriptor_fixture(1, true);
        unsafe {
            libc::close(config.vector_blob_fd);
        }
        config.vector_blob_fd = config.canonical_fd;
        assert!(Server::open_descriptor_read(config).is_err(), "same raw FD");
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, mut config, _) = descriptor_fixture(1, true);
        unsafe {
            libc::close(config.vector_blob_fd);
        }
        config.vector_blob_fd = fs::OpenOptions::new()
            .read(true)
            .open(&canonical_path)
            .expect("open canonical alias")
            .into_raw_fd();
        assert!(
            Server::open_descriptor_read(config).is_err(),
            "distinct FDs for same inode"
        );
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);
    }

    #[cfg(unix)]
    #[test]
    fn descriptor_mode_rejects_writable_and_nonregular_fds() {
        use std::os::fd::{IntoRawFd, RawFd};

        let (canonical_path, blob_path, mut config, _) = descriptor_fixture(1, true);
        let writable = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&canonical_path)
            .expect("open writable canonical")
            .into_raw_fd();
        unsafe {
            libc::close(config.canonical_fd);
        }
        config.canonical_fd = writable;
        assert!(Server::open_descriptor_read(config).is_err());
        cleanup(&canonical_path);
        let _ = fs::remove_file(&blob_path);

        let (canonical_path, blob_path, mut nonregular, _) = descriptor_fixture(1, true);
        let mut pipe_fds = [0 as RawFd; 2];
        let result = unsafe { libc::pipe(pipe_fds.as_mut_ptr()) };
        assert_eq!(result, 0, "pipe setup");
        unsafe {
            libc::close(nonregular.canonical_fd);
        }
        nonregular.canonical_fd = pipe_fds[0];
        assert!(Server::open_descriptor_read(nonregular).is_err());
        unsafe {
            libc::close(pipe_fds[1]);
        }
        cleanup(&canonical_path);
        let _ = fs::remove_file(blob_path);
    }

    #[test]
    fn descriptor_output_accounting_accepts_exact_limits_and_rejects_plus_one() {
        assert_eq!(
            account_descriptor_output(0, MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES - 1)
                .expect("exact frame limit"),
            MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES as u64
        );
        assert!(account_descriptor_output(0, MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES).is_err());

        let before_last_frame =
            MAX_DESCRIPTOR_PROTOCOL_BYTES - MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES as u64;
        assert_eq!(
            account_descriptor_output(before_last_frame, MAX_DESCRIPTOR_PROTOCOL_FRAME_BYTES - 1)
                .expect("exact cumulative limit"),
            MAX_DESCRIPTOR_PROTOCOL_BYTES
        );
        assert!(account_descriptor_output(MAX_DESCRIPTOR_PROTOCOL_BYTES, 0).is_err());
    }
}
