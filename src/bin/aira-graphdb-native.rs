use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::panic;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct VectorBlobDescriptor {
    basename: String,
    size: u64,
    sha256: String,
    format: u16,
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
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RpcRequest {
    id: u64,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct WalRecord {
    version: u16,
    base_generation: u64,
    request: RpcRequest,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct DurableGenerationToken {
    generation: u64,
    vector_blob: VectorBlobDescriptor,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransactionState {
    Idle,
    Active {
        base_generation: u64,
        mutation_seen: bool,
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
    db_path: PathBuf,
    audit_log_path: PathBuf,
    state: State,
    vector_values: HashMap<String, Vec<f64>>,
    cache_dirty: bool,
    transaction: TransactionState,
    wal_path: PathBuf,
    wal_bytes: u64,
    wal_identity: Option<(u64, u64)>,
    wal_replaying: bool,
    last_persist_bytes: u64,
    fatal: bool,
    node_keys_by_corpus: HashMap<String, Vec<String>>,
    edge_keys_by_corpus: HashMap<String, Vec<String>>,
    adjacent_edge_keys_by_node: HashMap<String, Vec<String>>,
    vector_keys_by_corpus_namespace: HashMap<String, Vec<String>>,
    passage_keys_by_corpus: HashMap<String, Vec<String>>,
}

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy)]
struct MethodSpec {
    name: &'static str,
    classification: &'static str,
    wal: bool,
}

// This table is the one policy authority used both by protocol_info and WAL
// admission. Unknown methods deliberately have no read classification.
const METHOD_SPECS: &[MethodSpec] = &[
    MethodSpec {
        name: "ping",
        classification: "health",
        wal: false,
    },
    MethodSpec {
        name: "protocol_info",
        classification: "health",
        wal: false,
    },
    MethodSpec {
        name: "batch_begin",
        classification: "transaction",
        wal: false,
    },
    MethodSpec {
        name: "batch_commit",
        classification: "commit",
        wal: false,
    },
    MethodSpec {
        name: "recovery_discard",
        classification: "recovery",
        wal: false,
    },
    MethodSpec {
        name: "upsert_nodes",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "upsert_edges",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "get_node",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "get_nodes",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "get_edges",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "get_adjacent",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "delete_nodes",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "delete_edges",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "delete_by_document",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "delete_by_corpus",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "vector_upsert",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "vector_search",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "vector_delete_by_document",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "memory_upsert",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "memory_save",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "memory_save_file",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "memory_load",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "memory_save_checkpoint",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "memory_load_checkpoint",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "memory_validate_integrity",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "projection_get_transitions",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "projection_get_dangling_nodes",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "projection_get_node_count",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "lexical_index_passages",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "lexical_search",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "lexical_delete_by_document",
        classification: "mutation",
        wal: true,
    },
    MethodSpec {
        name: "cypher_query",
        classification: "read",
        wal: false,
    },
    MethodSpec {
        name: "__debug_force_panic__",
        classification: "debug",
        wal: false,
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

    fn read_regular_nofollow(path: &Path) -> io::Result<Vec<u8>> {
        let mut file = Self::open_regular_nofollow(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        Ok(raw)
    }

    fn read_regular_nofollow_with_identity(path: &Path) -> io::Result<(Vec<u8>, (u64, u64))> {
        let path_metadata = fs::symlink_metadata(path)?;
        Self::require_regular_single_link(path, &path_metadata)?;
        let mut file = Self::open_regular_nofollow(path)?;
        let file_metadata = file.metadata()?;
        let path_identity = Self::metadata_identity(&path_metadata);
        let file_identity = Self::metadata_identity(&file_metadata);
        if path_identity != file_identity {
            return Err(io::Error::other(format!(
                "{} changed while being opened",
                path.display()
            )));
        }
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;
        Ok((raw, file_identity))
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
        let mut state = if let Ok(metadata) = fs::symlink_metadata(&db_path) {
            Self::require_regular_single_link(&db_path, &metadata)?;
            let raw = Self::read_regular_nofollow(&db_path)?;
            serde_json::from_slice::<State>(&raw)
                .map_err(|err| io::Error::other(format!("parse canonical state failed: {err}")))?
        } else {
            State::default()
        };
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
            audit_log_path: db_path.with_extension("native-audit.log"),
            wal_path: db_path.with_extension("agdb.wal"),
            wal_bytes: fs::metadata(db_path.with_extension("agdb.wal"))
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            wal_identity: None,
            wal_replaying: false,
            last_persist_bytes: fs::metadata(&db_path)
                .map(|metadata| metadata.len())
                .unwrap_or(0),
            db_path,
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

    fn is_durable_temp_stage(stage: &str) -> bool {
        matches!(
            stage,
            "blob_temp_sync" | "json_temp_sync" | "wal_compact_sync" | "wal_retire"
        )
    }

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
            if stage == "wal_retire" {
                if target == wal_file_name
                    && Self::retired_wal_is_removable(&entry.path(), current_generation)?
                {
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
            if Self::retired_wal_is_removable(&retired, current_generation)? {
                fs::remove_file(retired)?;
                removed = true;
            }
        }
        if removed {
            Self::sync_parent_dir(parent)?;
        }
        Ok(())
    }

    fn retired_wal_is_removable(path: &Path, current_generation: u64) -> io::Result<bool> {
        let path_metadata = fs::symlink_metadata(path)?;
        if !path_metadata.file_type().is_file() || Self::metadata_link_count(&path_metadata) != 1 {
            return Ok(false);
        }
        let (raw, identity) = Self::read_regular_nofollow_with_identity(path)?;
        let records = Self::parse_wal_records(&raw, current_generation)?;
        if records
            .iter()
            .any(|record| record.base_generation >= current_generation)
        {
            return Err(io::Error::other(
                "retired WAL contains a current or future generation",
            ));
        }
        Self::validate_regular_path_identity(path, identity)?;
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

    fn write_durable_temp(target: &Path, bytes: &[u8], sync_stage: &str) -> io::Result<PathBuf> {
        let tmp_path = Self::temporary_path(target, sync_stage)?;
        let result = (|| {
            Self::durability_failpoint(&format!("before_{sync_stage}_create"))?;
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp_path)
                .map_err(|err| io::Error::other(format!("create temp file failed: {err}")))?;
            Self::durability_failpoint(&format!("after_{sync_stage}_create"))?;
            Self::durability_failpoint(&format!("before_{sync_stage}_write"))?;
            file.write_all(bytes)
                .map_err(|err| io::Error::other(format!("write temp file failed: {err}")))?;
            file.flush()
                .map_err(|err| io::Error::other(format!("flush temp file failed: {err}")))?;
            Self::durability_failpoint(&format!("after_{sync_stage}_write"))?;
            Self::durability_failpoint(&format!("before_{sync_stage}_fsync"))?;
            file.sync_all()
                .map_err(|err| io::Error::other(format!("sync temp file failed: {err}")))?;
            Self::durability_failpoint(&format!("after_{sync_stage}_fsync"))?;
            Ok::<(), io::Error>(())
        })();
        if let Err(err) = result {
            let _ = fs::remove_file(&tmp_path);
            return Err(err);
        }
        Ok(tmp_path)
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

    fn persist(&mut self) -> io::Result<DurableGenerationToken> {
        let start = std::time::Instant::now();
        let current_generation = self.state.generation;
        let transaction = self.transaction;
        let TransactionState::Active {
            base_generation,
            mutation_seen: true,
        } = transaction
        else {
            return Err(io::Error::other(
                "generation publication requires an active mutated batch",
            ));
        };
        if base_generation != current_generation {
            return Err(io::Error::other(
                "active batch base generation does not match canonical generation",
            ));
        }
        let next_generation = current_generation
            .checked_add(1)
            .ok_or_else(|| io::Error::other("generation overflow"))?;
        let (wal_records, wal_identity, wal_raw) = self.read_wal_records_with_identity()?;
        if wal_records
            .iter()
            .any(|record| record.base_generation != current_generation)
        {
            return Err(io::Error::other(
                "cannot commit with WAL records from a different generation",
            ));
        }
        if wal_records.is_empty() {
            return Err(io::Error::other(
                "cannot publish a generation without a durable WAL mutation",
            ));
        }
        let wal_identity = wal_identity
            .ok_or_else(|| io::Error::other("durable WAL disappeared before publication"))?;

        let parent = Self::parent_dir(&self.db_path);
        fs::create_dir_all(&parent)
            .map_err(|err| io::Error::other(format!("create database directory failed: {err}")))?;

        let mut persisted_state = self.state.clone();
        let vector_blob_payload =
            Self::build_vector_blob_payload(&mut persisted_state, &self.vector_values)?;
        let blob_sha256 = Self::sha256_hex(&vector_blob_payload);
        let blob_basename = format!(
            "{}.g{next_generation:020}.{blob_sha256}.vblob",
            self.db_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .unwrap_or("vectors")
        );
        let blob_descriptor = VectorBlobDescriptor {
            basename: blob_basename,
            size: vector_blob_payload.len() as u64,
            sha256: blob_sha256,
            format: Self::VECTOR_BLOB_VERSION,
        };
        let blob_path = parent.join(&blob_descriptor.basename);

        match fs::symlink_metadata(&blob_path) {
            Ok(metadata) => {
                Self::require_regular_single_link(&blob_path, &metadata)?;
                let (file, _) = Self::open_validated_blob(&blob_path, Some(&blob_descriptor))?;
                Self::durability_failpoint("blob_existing_sync")?;
                file.sync_all().map_err(|err| {
                    io::Error::other(format!("sync existing vector blob failed: {err}"))
                })?;
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let blob_tmp =
                    Self::write_durable_temp(&blob_path, &vector_blob_payload, "blob_temp_sync")?;
                if let Err(err) = Self::durability_failpoint("before_blob_rename") {
                    let _ = fs::remove_file(&blob_tmp);
                    return Err(err);
                }
                if let Err(err) = fs::rename(&blob_tmp, &blob_path) {
                    let _ = fs::remove_file(&blob_tmp);
                    return Err(io::Error::other(format!(
                        "publish vector blob failed: {err}"
                    )));
                }
                Self::durability_failpoint("after_blob_rename")?;
                Self::read_validated_blob(&blob_path, Some(&blob_descriptor))?;
            }
            Err(err) => return Err(err),
        }
        Self::durability_failpoint("before_blob_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_blob_dir_fsync")?;

        persisted_state.generation = next_generation;
        persisted_state.vector_blob = Some(blob_descriptor.clone());
        let raw = serde_json::to_vec(&persisted_state)
            .map_err(|err| io::Error::other(format!("serialize state failed: {err}")))?;
        match fs::symlink_metadata(&self.db_path) {
            Ok(metadata) => Self::require_regular_single_link(&self.db_path, &metadata)?,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => return Err(err),
        }
        let json_tmp = Self::write_durable_temp(&self.db_path, &raw, "json_temp_sync")?;
        if let Err(err) = Self::durability_failpoint("before_json_rename") {
            let _ = fs::remove_file(&json_tmp);
            return Err(err);
        }
        if let Err(err) = fs::rename(&json_tmp, &self.db_path) {
            let _ = fs::remove_file(&json_tmp);
            return Err(io::Error::other(format!(
                "publish canonical state failed: {err}"
            )));
        }
        Self::durability_failpoint("after_json_rename")?;
        Self::durability_failpoint("before_json_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_json_dir_fsync")?;

        Self::durability_failpoint("before_wal_retire")?;
        self.retire_wal_exact(wal_identity, &wal_raw)
            .map_err(|err| io::Error::other(format!("retire WAL failed: {err}")))?;
        Self::durability_failpoint("after_wal_retire")?;
        Self::durability_failpoint("before_final_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_final_dir_fsync")?;

        self.state = persisted_state;
        self.last_persist_bytes = raw.len() as u64;
        self.wal_bytes = 0;
        self.wal_identity = None;
        let total_ms = start.elapsed().as_millis();
        eprintln!(
            "[persist] generation={} blobBytes={} blobSha256={} jsonBytes={} elapsedMs={total_ms}",
            next_generation,
            vector_blob_payload.len(),
            blob_descriptor.sha256,
            raw.len(),
        );
        Ok(DurableGenerationToken {
            generation: next_generation,
            vector_blob: blob_descriptor,
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

    /// WAL admission and protocol_info both use METHOD_SPECS. Unknown
    /// methods are not treated as reads and therefore never enter the WAL.
    fn is_mutating_method(method: &str) -> bool {
        Self::method_spec(method).is_some_and(|spec| spec.wal)
    }

    fn parse_wal_records(raw: &[u8], generation: u64) -> io::Result<Vec<WalRecord>> {
        let content = std::str::from_utf8(raw)
            .map_err(|err| io::Error::other(format!("WAL is not UTF-8: {err}")))?;
        let mut records = Vec::new();
        for (line_number, line) in content.lines().enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            let record = match serde_json::from_str::<WalRecord>(line) {
                Ok(record) => record,
                Err(versioned_error) if generation == 0 => {
                    // The pre-generation WAL was a raw RpcRequest. It is only
                    // safe to interpret it while the canonical state is still
                    // the legacy generation zero snapshot.
                    let legacy_value = serde_json::from_str::<Value>(line).map_err(|_| {
                        io::Error::other(format!(
                            "malformed WAL record at line {}: {versioned_error}",
                            line_number + 1
                        ))
                    })?;
                    if legacy_value.as_object().is_none_or(|object| {
                        object.contains_key("version")
                            || object.contains_key("baseGeneration")
                            || object.contains_key("request")
                    }) {
                        return Err(io::Error::other(format!(
                            "malformed WAL record at line {}: {versioned_error}",
                            line_number + 1
                        )));
                    }
                    let request = serde_json::from_str::<RpcRequest>(line).map_err(|_| {
                        io::Error::other(format!(
                            "malformed WAL record at line {}: {versioned_error}",
                            line_number + 1
                        ))
                    })?;
                    WalRecord {
                        version: Self::WAL_VERSION,
                        base_generation: 0,
                        request,
                    }
                }
                Err(err) => {
                    return Err(io::Error::other(format!(
                        "malformed WAL record at line {}: {err}",
                        line_number + 1
                    )));
                }
            };
            if record.version != Self::WAL_VERSION {
                return Err(io::Error::other(format!(
                    "unsupported WAL version {}",
                    record.version
                )));
            }
            if !Self::is_mutating_method(&record.request.method) {
                return Err(io::Error::other(
                    "WAL record contains a non-mutating method",
                ));
            }
            if record.request.method == "memory_save_file" {
                return Err(io::Error::other(
                    "WAL record contains a non-canonical memory_save_file request",
                ));
            }
            records.push(record);
        }
        Ok(records)
    }

    fn read_wal_records_with_identity(
        &self,
    ) -> io::Result<(Vec<WalRecord>, Option<(u64, u64)>, Vec<u8>)> {
        let (raw, identity) = match Self::read_regular_nofollow_with_identity(&self.wal_path) {
            Ok(result) => result,
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Ok((Vec::new(), None, Vec::new()));
            }
            Err(err) => return Err(io::Error::other(format!("read WAL failed: {err}"))),
        };
        let records = Self::parse_wal_records(&raw, self.state.generation)?;
        Ok((records, Some(identity), raw))
    }

    fn retire_wal_exact(
        &self,
        expected_identity: (u64, u64),
        expected_raw: &[u8],
    ) -> io::Result<()> {
        let parent = Self::parent_dir(&self.wal_path);
        let mut held_wal = Self::open_regular_nofollow(&self.wal_path)?;
        if Self::metadata_identity(&held_wal.metadata()?) != expected_identity {
            return Err(io::Error::other("WAL identity changed before retirement"));
        }
        let mut held_before = Vec::new();
        held_wal.read_to_end(&mut held_before)?;
        if held_before != expected_raw {
            return Err(io::Error::other("WAL content changed before retirement"));
        }
        let retired = Self::temporary_path(&self.wal_path, "wal_retire")?;
        fs::rename(&self.wal_path, &retired)?;
        Self::validate_regular_path_identity(&retired, expected_identity)?;
        held_wal.seek(SeekFrom::Start(0))?;
        let mut held_after = Vec::new();
        held_wal.read_to_end(&mut held_after)?;
        if held_after != expected_raw {
            return Err(io::Error::other("WAL content changed during retirement"));
        }
        Self::durability_failpoint("after_wal_retire_rename")?;
        Self::durability_failpoint("before_wal_retire_dir_fsync")?;
        Self::sync_parent_dir(&parent)?;
        Self::durability_failpoint("after_wal_retire_dir_fsync")?;
        Self::durability_failpoint("before_wal_retire_unlink")?;
        fs::remove_file(&retired)?;
        Self::durability_failpoint("after_wal_retire_unlink")?;
        Self::sync_parent_dir(&parent)
    }

    fn rewrite_wal_records(&self, records: &[WalRecord]) -> io::Result<()> {
        if records.is_empty() {
            return Err(io::Error::other(
                "empty WAL rewrite requires exact retirement identity",
            ));
        }
        let mut bytes = Vec::new();
        for record in records {
            let encoded = serde_json::to_vec(record)
                .map_err(|err| io::Error::other(format!("serialize WAL failed: {err}")))?;
            bytes.extend_from_slice(&encoded);
            bytes.push(b'\n');
        }
        let tmp = Self::write_durable_temp(&self.wal_path, &bytes, "wal_compact_sync")?;
        Self::durability_failpoint("before_wal_rewrite_rename")?;
        if let Err(err) = fs::rename(&tmp, &self.wal_path) {
            let _ = fs::remove_file(&tmp);
            return Err(io::Error::other(format!("replace WAL failed: {err}")));
        }
        Self::durability_failpoint("after_wal_rewrite_rename")?;
        Self::durability_failpoint("before_wal_rewrite_dir_fsync")?;
        Self::sync_parent_dir(&Self::parent_dir(&self.wal_path))?;
        Self::durability_failpoint("after_wal_rewrite_dir_fsync")
    }

    fn encoded_wal_records(records: &[WalRecord]) -> io::Result<Vec<u8>> {
        let mut bytes = Vec::new();
        for record in records {
            let encoded = serde_json::to_vec(record)
                .map_err(|err| io::Error::other(format!("serialize WAL failed: {err}")))?;
            bytes.extend_from_slice(&encoded);
            bytes.push(b'\n');
        }
        Ok(bytes)
    }

    fn digest_bytes(bytes: &[u8]) -> [u8; 32] {
        Sha256::digest(bytes).into()
    }

    fn digest_hex(digest: &[u8; 32]) -> String {
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn open_wal_for_append(&self) -> io::Result<(fs::File, bool, (u64, u64))> {
        for _ in 0..2 {
            match fs::symlink_metadata(&self.wal_path) {
                Ok(path_metadata) => {
                    Self::require_regular_single_link(&self.wal_path, &path_metadata)?;
                    let mut options = fs::OpenOptions::new();
                    options.write(true).append(true);
                    #[cfg(unix)]
                    options.custom_flags(libc::O_NOFOLLOW);
                    let file = options.open(&self.wal_path)?;
                    let file_metadata = file.metadata()?;
                    Self::require_regular_single_link(&self.wal_path, &file_metadata)?;
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
                    options.write(true).append(true).create_new(true);
                    #[cfg(unix)]
                    options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
                    match options.open(&self.wal_path) {
                        Ok(file) => {
                            let metadata = file.metadata()?;
                            Self::require_regular_single_link(&self.wal_path, &metadata)?;
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
        let record = WalRecord {
            version: Self::WAL_VERSION,
            base_generation: self.state.generation,
            request: request.clone(),
        };
        let encoded = serde_json::to_vec(&record)
            .map_err(|err| io::Error::other(format!("serialize WAL record failed: {err}")))?;
        let parent = Self::parent_dir(&self.wal_path);
        fs::create_dir_all(&parent)?;
        let (mut file, wal_created, wal_identity) = self.open_wal_for_append()?;
        Self::durability_failpoint("before_wal_write")?;
        file.write_all(&encoded)?;
        file.write_all(b"\n")?;
        file.flush()?;
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
        Self::validate_regular_path_identity(&self.wal_path, wal_identity)?;
        self.wal_identity = Some(wal_identity);
        self.wal_bytes += encoded.len() as u64 + 1;
        Ok(())
    }

    fn replay_wal(&mut self) -> io::Result<usize> {
        let (records, mut wal_identity, mut wal_raw) = self.read_wal_records_with_identity()?;
        let generation = self.state.generation;
        let mut replayable = Vec::new();
        let mut skipped = 0usize;
        for record in records {
            match record.base_generation.cmp(&generation) {
                std::cmp::Ordering::Less => skipped += 1,
                std::cmp::Ordering::Equal => replayable.push(record),
                std::cmp::Ordering::Greater => {
                    return Err(io::Error::other(format!(
                        "WAL base generation {} is newer than canonical generation {generation}",
                        record.base_generation
                    )));
                }
            }
        }
        if replayable.is_empty() {
            if let Some(identity) = wal_identity {
                Self::durability_failpoint("before_wal_rewrite_retire")?;
                self.retire_wal_exact(identity, &wal_raw)?;
                Self::durability_failpoint("after_wal_rewrite_retire")?;
            }
            self.wal_bytes = 0;
            self.wal_identity = None;
            self.transaction = TransactionState::Idle;
            eprintln!("[wal] recoveryPending=false skipped={skipped} bytes=0");
            return Ok(0);
        }
        if skipped > 0 {
            self.rewrite_wal_records(&replayable)?;
            let snapshot = self.read_wal_records_with_identity()?;
            wal_identity = snapshot.1;
            wal_raw = snapshot.2;
        }
        self.wal_bytes = wal_raw.len() as u64;
        let encoded = Self::encoded_wal_records(&replayable)?;
        let digest = Self::digest_bytes(&encoded);
        let (wal_device, wal_inode) = wal_identity.ok_or_else(|| {
            io::Error::other("recovery WAL disappeared before entering recovery pending")
        })?;
        self.transaction = TransactionState::RecoveryPending {
            base_generation: generation,
            wal_digest: digest,
            record_count: replayable.len(),
            wal_device,
            wal_inode,
        };
        eprintln!(
            "[wal] recoveryPending=true generation={} records={} skipped={} bytes={} digest={}",
            generation,
            replayable.len(),
            skipped,
            self.wal_bytes,
            Self::digest_hex(&digest),
        );
        Ok(replayable.len())
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
        let (current_records, current_identity, current_raw) = self
            .read_wal_records_with_identity()
            .map_err(|err| Self::execution_io_error(format!("revalidate recovery WAL: {err}")))?;
        let Some((current_device, current_inode)) = current_identity else {
            return Err(Self::execution_io_error(
                "recovery WAL disappeared before quarantine".to_string(),
            ));
        };
        let current_encoded = Self::encoded_wal_records(&current_records)
            .map_err(|err| Self::execution_io_error(format!("canonicalize recovery WAL: {err}")))?;
        let current_digest = Self::digest_bytes(&current_encoded);
        if current_device != wal_device
            || current_inode != wal_inode
            || current_digest != wal_digest
            || current_records.len() != record_count
            || current_records
                .iter()
                .any(|record| record.base_generation != base_generation)
        {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine".to_string(),
            ));
        }
        let mut held_wal = Self::open_regular_nofollow(&self.wal_path).map_err(|err| {
            Self::execution_io_error(format!("hold recovery WAL for quarantine: {err}"))
        })?;
        let held_metadata = held_wal
            .metadata()
            .map_err(|err| Self::execution_io_error(format!("inspect held recovery WAL: {err}")))?;
        if Self::metadata_identity(&held_metadata) != (wal_device, wal_inode) {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine hold".to_string(),
            ));
        }
        let mut held_before = Vec::new();
        held_wal
            .read_to_end(&mut held_before)
            .map_err(|err| Self::execution_io_error(format!("read held recovery WAL: {err}")))?;
        if held_before != current_raw {
            return Err(Self::execution_io_error(
                "recovery WAL changed before quarantine rename".to_string(),
            ));
        }
        let parent = Self::parent_dir(&self.wal_path);
        let nonce = Self::crypto_nonce().map_err(|err| {
            Self::execution_io_error(format!("create recovery nonce failed: {err}"))
        })?;
        let file_name = self
            .wal_path
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
        fs::rename(&self.wal_path, &quarantine).map_err(|err| {
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
        held_wal.seek(SeekFrom::Start(0)).map_err(|err| {
            Self::execution_io_error(format!("rewind quarantined recovery WAL: {err}"))
        })?;
        let mut held_after = Vec::new();
        held_wal.read_to_end(&mut held_after).map_err(|err| {
            Self::execution_io_error(format!("re-read quarantined recovery WAL: {err}"))
        })?;
        if held_after != current_raw {
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
        self.wal_identity = None;
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

    fn cosine(a: &[f64], b: &[f64]) -> f64 {
        if a.is_empty() || b.is_empty() || a.len() != b.len() {
            return 0.0;
        }
        let mut dot = 0.0;
        let mut norm_a = 0.0;
        let mut norm_b = 0.0;
        for i in 0..a.len() {
            dot += a[i] * b[i];
            norm_a += a[i] * a[i];
            norm_b += b[i] * b[i];
        }
        if norm_a == 0.0 || norm_b == 0.0 {
            return 0.0;
        }
        dot / (norm_a.sqrt() * norm_b.sqrt())
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
                        serde_json::from_value::<VectorRecord>(item.clone()).map_err(|err| {
                            Self::execution_client_error(format!("invalid vector record: {err}"))
                        })?;
                    }
                }
            }
            "vector_delete_by_document" => {
                let _ = Self::optional_string(params, "corpusId")?;
                let _ = Self::optional_string(params, "documentId")?;
            }
            "memory_upsert" => {
                Self::required_string(params, "corpusId")?;
                for (section, id_key) in [
                    ("passages", "passageId"),
                    ("facts", "factId"),
                    ("schemas", "schemaId"),
                ] {
                    let Some(items) = Self::optional_array(params, section)? else {
                        continue;
                    };
                    for item in items {
                        let object = item.as_object().ok_or_else(|| {
                            Self::execution_client_error(format!("{section} items must be objects"))
                        })?;
                        let id = object.get(id_key).and_then(Value::as_str).ok_or_else(|| {
                            Self::execution_client_error(format!("missing {section}.{id_key}"))
                        })?;
                        if id.is_empty() {
                            return Err(Self::execution_client_error(format!(
                                "{section}.{id_key} must not be empty"
                            )));
                        }
                    }
                }
                if let Some(exported_at) = params.get("exportedAt") {
                    if !exported_at.is_string() {
                        return Err(Self::execution_client_error(
                            "exportedAt must be a string".to_string(),
                        ));
                    }
                }
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
        let is_mutation = Self::is_mutating_method(&req.method);
        let Some(spec) = Self::method_spec(&req.method) else {
            return self
                .response_for_result(req.id, Err(Self::unsupported_method_error(&req.method)));
        };
        if matches!(self.transaction, TransactionState::RecoveryPending { .. })
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
            && !matches!(self.transaction, TransactionState::Active { .. })
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
            }
            match req.method.as_str() {
                "ping" => Ok(json!({"pong": true})),
                "protocol_info" => {
                    let methods: Vec<Value> = METHOD_SPECS
                        .iter()
                        .map(|spec| {
                            json!({
                                "name": spec.name,
                                "classification": spec.classification,
                                "wal": spec.wal,
                            })
                        })
                        .collect();
                    Ok(json!({
                        "protocolVersion": "native-method-policy@1",
                        "generation": self.state.generation,
                        "state": match self.transaction {
                            TransactionState::RecoveryPending { .. } => "recoveryPending",
                            TransactionState::Active { .. } => "active",
                            TransactionState::Idle => "idle",
                        },
                        "recovery": match self.transaction {
                            TransactionState::RecoveryPending { base_generation, wal_digest, record_count, .. } => Some(json!({
                                "baseGeneration": base_generation,
                                "walDigest": Self::digest_hex(&wal_digest),
                                "recordCount": record_count,
                            })),
                            _ => None,
                        },
                        "methods": methods,
                    }))
                }
                "batch_begin" => {
                    if !matches!(self.transaction, TransactionState::Idle) {
                        Err(Self::execution_client_error(
                            "batch_begin requires an idle transaction".to_string(),
                        ))
                    } else {
                        self.transaction = TransactionState::Active {
                            base_generation: self.state.generation,
                            mutation_seen: false,
                        };
                        Ok(json!(null))
                    }
                }
                "batch_commit" => {
                    let mutation_seen = match self.transaction {
                        TransactionState::Active { mutation_seen, .. } => mutation_seen,
                        TransactionState::Idle => {
                            return Err(Self::execution_client_error(
                                "batch_commit requires an active mutated batch".to_string(),
                            ));
                        }
                        TransactionState::RecoveryPending { .. } => {
                            return Err(Self::execution_client_error(
                                "batch_commit is unavailable during recovery".to_string(),
                            ));
                        }
                    };
                    if !mutation_seen {
                        return Err(Self::execution_client_error(
                            "batch_commit requires a successful mutation".to_string(),
                        ));
                    }
                    let token = self.persist().map_err(|err| {
                        self.fatal = true;
                        Self::execution_io_error(format!("batch_commit persist failed: {err}"))
                    })?;
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
                    let top_k =
                        req.params.get("topK").and_then(Value::as_u64).unwrap_or(10) as usize;
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
                    let mut out: Vec<Value> = self
                        .vector_keys_by_corpus_namespace
                        .get(&corpus_namespace_key)
                        .into_iter()
                        .flat_map(|keys| keys.iter())
                        .filter_map(|key| self.state.vectors.get(key))
                        .map(|v| {
                            let key = Self::key(&v.corpus_id, &v.id);
                            let values = self.vector_values.get(&key);
                            let score = values
                                .map(|vector| Self::cosine(&query_vec, vector))
                                .unwrap_or(0.0);
                            (v, score)
                        })
                        .filter(|(_, score)| threshold.is_none_or(|th| *score >= th))
                        .map(|(v, score)| {
                            json!({
                                "id": v.id,
                                "score": score,
                                "metadata": v.metadata
                            })
                        })
                        .collect();
                    out.sort_by(|a, b| {
                        let sa = a.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                        let sb = b.get("score").and_then(Value::as_f64).unwrap_or(0.0);
                        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    out.truncate(top_k);
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
                            let mut index: HashMap<String, usize> = HashMap::new();
                            for (i, item) in existing.iter().enumerate() {
                                if let Some(id) = item.get(id_key).and_then(Value::as_str) {
                                    index.insert(id.to_string(), i);
                                }
                            }
                            for item in incoming {
                                let id = item
                                    .get(id_key)
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_string();
                                if let Some(&i) = index.get(&id) {
                                    existing[i] = item;
                                } else {
                                    index.insert(id, existing.len());
                                    existing.push(item);
                                }
                            }
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
        let method_for_wal = Server::is_mutating_method(&req.method);
        let resp =
            if method_for_wal && !matches!(server.transaction, TransactionState::Active { .. }) {
                // Admission must precede canonicalization: memory_save_file reads
                // an external path and must do no I/O while idle or recovering.
                server.handle_prepared(req)
            } else if method_for_wal {
                match server.canonicalize_request(req.clone()) {
                    Err(err) => {
                        server.fatal = true;
                        server.response_for_result(req.id, Err(err))
                    }
                    Ok(canonical) => {
                        let validation = server.validate_mutation_params(&canonical);
                        if let Err(err) = validation {
                            server.fatal = true;
                            server.response_for_result(req.id, Err(err))
                        } else if let Err(err) = server.wal_append(&canonical) {
                            server.fatal = true;
                            server.response_for_result(
                                req.id,
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
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

    fn commit_batch(server: &mut Server) {
        let response = server.handle(RpcRequest {
            id: 91,
            method: "batch_commit".to_string(),
            params: json!({}),
        });
        assert!(response.ok, "batch_commit failed: {response:?}");
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
        let token_response = server.handle(RpcRequest {
            id: 2,
            method: "batch_commit".to_string(),
            params: json!({}),
        });
        assert!(token_response.ok);
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

        for spec in METHOD_SPECS {
            let path = temp_path("agdb-native-dispatch");
            let mut server = Server::open(path.clone()).expect("open dispatch server");
            if spec.wal {
                server.transaction = TransactionState::Active {
                    base_generation: 0,
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
}
