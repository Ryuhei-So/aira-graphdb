use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
#[cfg(target_os = "linux")]
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(target_os = "linux")]
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::fs::hard_link;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
#[cfg(unix)]
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use aira_graphdb::native_persistence_contract::BATCH_COMMIT_PHASES;

struct TempDb {
    dir: PathBuf,
    path: PathBuf,
}

impl TempDb {
    fn new(label: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("aira-graphdb-atomic-{label}-{nonce}"));
        std::fs::create_dir(&dir).expect("create temporary database directory");
        let path = dir.join("state.json");
        Self { dir, path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct NativeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl NativeProcess {
    fn spawn(path: &Path, envs: &[(&str, &str)]) -> Self {
        Self::spawn_inner(Some(path), None, envs)
    }

    fn spawn_in_dir(path: &Path, cwd: &Path, envs: &[(&str, &str)]) -> Self {
        Self::spawn_inner(Some(path), Some(cwd), envs)
    }

    fn spawn_default_in_dir(cwd: &Path) -> Self {
        Self::spawn_inner(None, Some(cwd), &[])
    }

    fn spawn_inner(path: Option<&Path>, cwd: Option<&Path>, envs: &[(&str, &str)]) -> Self {
        let binary = std::env::var("CARGO_BIN_EXE_aira-graphdb-native").unwrap_or_else(|_| {
            let exe = std::env::current_exe().expect("test executable path");
            exe.parent()
                .and_then(Path::parent)
                .expect("target debug directory")
                .join("aira-graphdb-native")
                .to_string_lossy()
                .into_owned()
        });
        let mut command = Command::new(binary);
        if let Some(path) = path {
            command.arg("--db").arg(path);
        }
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        if let Some(cwd) = cwd {
            command.current_dir(cwd);
        }
        for (name, value) in envs {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("native binary starts");
        Self {
            stdin: Some(child.stdin.take().expect("native stdin")),
            stdout: BufReader::new(child.stdout.take().expect("native stdout")),
            child,
        }
    }

    fn send(&mut self, request: Value) -> Value {
        let line = request.to_string();
        let stdin = self.stdin.as_mut().expect("native stdin is open");
        stdin.write_all(line.as_bytes()).expect("write request");
        stdin.write_all(b"\n").expect("write request newline");
        stdin.flush().expect("flush request");
        let mut response = String::new();
        self.stdout
            .read_line(&mut response)
            .expect("read native response");
        serde_json::from_str(response.trim()).expect("native response is JSON")
    }

    fn send_without_read(&mut self, request: Value) {
        let line = request.to_string();
        let stdin = self.stdin.as_mut().expect("native stdin is open");
        stdin.write_all(line.as_bytes()).expect("write request");
        stdin.write_all(b"\n").expect("write request newline");
        stdin.flush().expect("flush request");
    }

    fn send_with_progress(&mut self, request: Value) -> (Vec<Value>, Value) {
        let line = request.to_string();
        let stdin = self.stdin.as_mut().expect("native stdin is open");
        stdin.write_all(line.as_bytes()).expect("write request");
        stdin.write_all(b"\n").expect("write request newline");
        stdin.flush().expect("flush request");

        let mut progress = Vec::new();
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read native progress or terminal response");
            assert_ne!(read, 0, "native stdout closed before terminal response");
            assert!(
                line.ends_with('\n'),
                "native emitted a partial output frame"
            );
            let value: Value =
                serde_json::from_str(line.trim_end()).expect("native output is JSON");
            if value.get("ok").is_some() {
                return (progress, value);
            }
            assert_eq!(value["schema"], json!("NativeProgressFrame@1"));
            assert_eq!(value["kind"], json!("progress"));
            progress.push(value);
        }
    }

    fn prepare_commit(&mut self, id: u64) -> Value {
        let response = self.send(json!({
            "id": id,
            "method": "batch_prepare_commit",
            "params": {}
        }));
        assert_eq!(response["ok"], json!(true), "prepare failed: {response}");
        response["result"].clone()
    }

    fn commit(&mut self, id: u64) -> Value {
        let evidence = self.prepare_commit(id);
        self.send(batch_commit(id, evidence))
    }

    fn send_commit_without_read(&mut self, id: u64) {
        let evidence = self.prepare_commit(id);
        self.send_without_read(batch_commit(id, evidence));
    }

    fn finish(mut self) -> ExitStatus {
        self.stdin.take();
        self.child.wait().expect("native exits")
    }

    fn finish_with_stdout_tail(mut self) -> (ExitStatus, String) {
        self.stdin.take();
        let mut tail = String::new();
        self.stdout
            .read_to_string(&mut tail)
            .expect("drain native stdout");
        (self.child.wait().expect("native exits"), tail)
    }

    #[cfg(target_os = "linux")]
    fn rss_bytes(&self) -> u64 {
        Self::rss_bytes_for_pid(self.child.id())
    }

    #[cfg(target_os = "linux")]
    fn rss_bytes_for_pid(pid: u32) -> u64 {
        let status =
            std::fs::read_to_string(format!("/proc/{pid}/status")).expect("read native VmRSS");
        status
            .lines()
            .find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some("VmRSS:")).then(|| {
                    fields
                        .next()
                        .expect("VmRSS value")
                        .parse::<u64>()
                        .expect("VmRSS is numeric")
                        * 1024
                })
            })
            .expect("native VmRSS entry")
    }

    #[cfg(target_os = "linux")]
    fn send_with_peak_rss(&mut self, request: Value) -> (Value, u64, u64) {
        let baseline = self.rss_bytes();
        let peak = Arc::new(AtomicU64::new(baseline));
        let running = Arc::new(AtomicBool::new(true));
        let sampler_peak = Arc::clone(&peak);
        let sampler_running = Arc::clone(&running);
        let pid = self.child.id();
        let sampler = std::thread::spawn(move || {
            while sampler_running.load(Ordering::Relaxed) {
                sampler_peak.fetch_max(Self::rss_bytes_for_pid(pid), Ordering::Relaxed);
                std::thread::sleep(Duration::from_micros(100));
            }
            sampler_peak.fetch_max(Self::rss_bytes_for_pid(pid), Ordering::Relaxed);
        });
        let response = self.send(request);
        running.store(false, Ordering::Relaxed);
        sampler.join().expect("RSS sampler exits");
        (response, baseline, peak.load(Ordering::Relaxed))
    }
}

impl Drop for NativeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn vector_upsert(id: u64, values: [f64; 2], generation: &str) -> Value {
    vector_upsert_for_document(id, "v1", "d1", values, generation)
}

fn vector_upsert_for_document(
    id: u64,
    vector_id: &str,
    document_id: &str,
    values: [f64; 2],
    generation: &str,
) -> Value {
    json!({
        "id": id,
        "method": "vector_upsert",
        "params": {
            "records": [{
                "id": vector_id,
                "corpusId": "c1",
                "namespace": "default",
                "values": values,
                "metadata": {"documentId": document_id, "generation": generation}
            }]
        }
    })
}

fn vector_search(id: u64, query_vector: [f64; 2]) -> Value {
    json!({
        "id": id,
        "method": "vector_search",
        "params": {
            "corpusId":"c1",
            "namespace":"default",
            "queryVector": query_vector,
            "threshold": 0.9,
            "topK": 10
        }
    })
}

fn bulk_vector_upsert(id: u64, count: usize) -> Value {
    let records = (0..count)
        .map(|index| {
            json!({
                "id": format!("bulk-v-{index}"),
                "corpusId": "c1",
                "namespace": "default",
                "values": [1.0, 0.0],
                "metadata": {
                    "documentId": format!("bulk-document-{index}"),
                    "generation": "representative"
                }
            })
        })
        .collect::<Vec<_>>();
    json!({
        "id": id,
        "method": "vector_upsert",
        "params": {"records": records}
    })
}

fn batch_commit(id: u64, evidence: Value) -> Value {
    json!({
        "id": id,
        "method": "batch_commit",
        "params": {"preparedCommitEvidence": evidence}
    })
}

fn batch_commit_without_evidence(id: u64) -> Value {
    json!({"id": id, "method": "batch_commit", "params": {}})
}

fn batch_begin(id: u64) -> Value {
    json!({"id": id, "method": "batch_begin", "params": {}})
}

fn memory_save_file(id: u64, path: &Path) -> Value {
    json!({
        "id": id,
        "method": "memory_save_file",
        "params": {"filePath": path}
    })
}

fn generation(path: &Path) -> u64 {
    std::fs::read(path)
        .ok()
        .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
        .and_then(|state| state["generation"].as_u64())
        .unwrap_or(0)
}

fn encoded_wal_record(base_generation: u64, request: &Value) -> Vec<u8> {
    let id = request["id"].as_u64().expect("WAL request id");
    let method = request["method"].as_str().expect("WAL request method");
    let params = request.get("params").unwrap_or(&Value::Null);
    let method = serde_json::to_string(method).expect("encode WAL method");
    let params = serde_json::to_string(params).expect("encode WAL params");
    let mut encoded = String::from("{\"version\":2,\"baseGeneration\":");
    encoded.push_str(&base_generation.to_string());
    encoded.push_str(",\"request\":{\"id\":");
    encoded.push_str(&id.to_string());
    encoded.push_str(",\"method\":");
    encoded.push_str(&method);
    encoded.push_str(",\"params\":");
    encoded.push_str(&params);
    encoded.push_str("}}\n");
    encoded.into_bytes()
}

fn write_wal_record(path: &Path, base_generation: u64, request: &Value) -> Vec<u8> {
    let bytes = encoded_wal_record(base_generation, request);
    std::fs::write(path.with_extension("agdb.wal"), &bytes).expect("write WAL record");
    bytes
}

fn digest_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn recovery_discard(id: u64, base_generation: u64, wal_digest: &str) -> Value {
    json!({
        "id": id,
        "method": "recovery_discard",
        "params": {
            "baseGeneration": base_generation,
            "walDigest": wal_digest
        }
    })
}

fn open_recovery_pending(path: &Path, request: &Value) -> (NativeProcess, Vec<u8>, String) {
    open_recovery_pending_with_env(path, request, &[])
}

fn open_recovery_pending_with_env(
    path: &Path,
    request: &Value,
    envs: &[(&str, &str)],
) -> (NativeProcess, Vec<u8>, String) {
    let wal_bytes = write_wal_record(path, 0, request);
    let wal_digest = digest_hex(&wal_bytes);
    let mut native = NativeProcess::spawn(path, envs);
    let info = native.send(json!({
        "id": 900,
        "method": "protocol_info",
        "params": {}
    }));
    assert_eq!(info["ok"], json!(true));
    assert_eq!(info["result"]["state"], json!("recoveryPending"));
    assert_eq!(info["result"]["recovery"]["baseGeneration"], json!(0));
    assert_eq!(info["result"]["recovery"]["walDigest"], json!(wal_digest));
    (native, wal_bytes, wal_digest)
}

fn discard_pending_recovery(path: &Path) {
    let mut native = NativeProcess::spawn(path, &[]);
    let info = native.send(json!({
        "id": 900,
        "method": "protocol_info",
        "params": {}
    }));
    if info["result"]["state"] == json!("recoveryPending") {
        let recovery = &info["result"]["recovery"];
        let response = native.send(json!({
            "id": 901,
            "method": "recovery_discard",
            "params": {
                "baseGeneration": recovery["baseGeneration"],
                "walDigest": recovery["walDigest"],
            }
        }));
        assert_eq!(response["ok"], json!(true), "recovery discard: {response}");
    }
    assert_eq!(native.finish().code(), Some(0));
}

fn quarantined_wal_paths(dir: &Path, wal_bytes: &[u8]) -> Vec<PathBuf> {
    std::fs::read_dir(dir)
        .expect("read database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".quarantine"))
        })
        .filter(|path| std::fs::read(path).ok().as_deref() == Some(wal_bytes))
        .collect()
}

fn recognized_wal_retire_path(db: &TempDb, id: u64) -> PathBuf {
    let wal_path = db.path.with_extension("agdb.wal");
    let wal_name = wal_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("WAL file name");
    db.dir.join(format!(
        ".{wal_name}.wal_retire.0123456789abcdef0123456789abcdef.{id}.tmp"
    ))
}

fn recognized_wal_retire_paths(db: &TempDb) -> Vec<PathBuf> {
    let wal_path = db.path.with_extension("agdb.wal");
    let wal_name = wal_path
        .file_name()
        .and_then(|name| name.to_str())
        .expect("WAL file name");
    let prefix = format!(".{wal_name}.wal_retire.");
    std::fs::read_dir(&db.dir)
        .expect("read database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(&prefix) && name.ends_with(".tmp"))
        })
        .collect()
}

fn assert_zero_wal(path: &Path, context: &str) {
    let wal_path = path.with_extension("agdb.wal");
    let metadata = std::fs::metadata(&wal_path)
        .unwrap_or_else(|error| panic!("{context}: zero WAL is missing: {error}"));
    assert_eq!(metadata.len(), 0, "{context}: WAL is not zero-length");
}

fn rename_probe_paths(db: &TempDb) -> Vec<PathBuf> {
    std::fs::read_dir(&db.dir)
        .expect("read database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(".rename_probe.") && name.ends_with(".tmp"))
        })
        .collect()
}

fn generation_blob_paths(db: &TempDb) -> Vec<PathBuf> {
    let mut paths = std::fs::read_dir(&db.dir)
        .expect("read database directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "vblob")
        })
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn seed_vector(path: &Path) {
    let mut native = NativeProcess::spawn(path, &[]);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, [1.0, 0.0], "old"))["ok"],
        json!(true)
    );
    let commit = native.commit(2);
    assert_eq!(commit["ok"], json!(true), "seed commit failed: {commit}");
    assert_eq!(commit["result"]["generation"], json!(1));
    assert_eq!(native.finish().code(), Some(0));
}

fn probe_vector_pair(path: &Path) {
    discard_pending_recovery(path);
    let mut native = NativeProcess::spawn(path, &[]);
    let old = native.send(json!({
        "id": 10,
        "method": "vector_search",
        "params": {"corpusId":"c1", "namespace":"default", "queryVector":[1.0,0.0], "threshold":0.9, "topK":1}
    }));
    let new = native.send(json!({
        "id": 11,
        "method": "vector_search",
        "params": {"corpusId":"c1", "namespace":"default", "queryVector":[0.0,1.0], "threshold":0.9, "topK":1}
    }));
    let old_items = old["result"].as_array().expect("old search array");
    let new_items = new["result"].as_array().expect("new search array");
    assert_eq!(
        old_items.len() + new_items.len(),
        1,
        "one complete vector generation must match"
    );
    if let Some(item) = old_items.first() {
        assert_eq!(item["metadata"]["generation"], json!("old"));
    }
    if let Some(item) = new_items.first() {
        assert_eq!(item["metadata"]["generation"], json!("new"));
    }
    assert_eq!(native.finish().code(), Some(0));
}

fn assert_native_killed(status: ExitStatus, stage: &str) {
    #[cfg(unix)]
    assert_eq!(status.signal(), Some(9), "kill point {stage}");
    #[cfg(not(unix))]
    assert_eq!(status.code(), Some(137), "kill point {stage}");
}

#[test]
fn committed_generation_has_sole_pointer_and_durable_blob_descriptor() {
    let db = TempDb::new("descriptor");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, [1.0, 0.0], "old"))["ok"],
        json!(true)
    );
    let commit = native.commit(2);
    assert_eq!(commit["result"]["generation"], json!(1));
    let descriptor = &commit["result"]["vectorBlob"];
    let basename = descriptor["basename"].as_str().expect("blob basename");
    assert!(!Path::new(basename).is_absolute());
    assert!(!basename.contains('/'));
    assert_eq!(descriptor["format"], json!(1));

    let state_raw = std::fs::read_to_string(&db.path).expect("canonical JSON");
    let state: Value = serde_json::from_str(&state_raw).expect("canonical JSON parses");
    assert_eq!(state["generation"], json!(1));
    assert_eq!(state["vectorBlob"], *descriptor);
    assert!(!state_raw.contains("\"values\":[1.0,0.0]"));
    let blob_path = db.dir.join(basename);
    let blob = std::fs::read(&blob_path).expect("immutable vector blob");
    let digest = Sha256::digest(&blob);
    let digest_hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
    assert_eq!(descriptor["size"], json!(blob.len()));
    assert_eq!(descriptor["sha256"], json!(digest_hex));
    assert!(!db.dir.join("state.vblob").exists());

    let protocol = native.send(json!({
        "id": 3,
        "method": "protocol_info",
        "params": {}
    }));
    assert_eq!(protocol["result"]["generation"], json!(1));
    assert_eq!(protocol["result"]["vectorBlob"], *descriptor);
    assert_eq!(native.finish().code(), Some(0));
    probe_vector_pair(&db.path);
}

#[test]
fn protocol_info_reports_the_canonical_descriptor_across_runtime_states() {
    let db = TempDb::new("protocol-descriptor-states");
    let mut legacy = NativeProcess::spawn(&db.path, &[]);
    let legacy_info = legacy.send(json!({
        "id": 1,
        "method": "protocol_info",
        "params": {}
    }));
    assert_eq!(legacy_info["result"]["generation"], json!(0));
    assert_eq!(legacy_info["result"]["vectorBlob"], Value::Null);
    assert_eq!(legacy.finish().code(), Some(0));

    seed_vector(&db.path);
    let canonical: Value =
        serde_json::from_slice(&std::fs::read(&db.path).expect("read canonical generation"))
            .expect("parse canonical generation");
    let descriptor = canonical["vectorBlob"].clone();

    let mut active = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(active.send(batch_begin(10))["ok"], json!(true));
    let active_info = active.send(json!({
        "id": 11,
        "method": "protocol_info",
        "params": {}
    }));
    assert_eq!(active_info["result"]["state"], json!("active"));
    assert_eq!(active_info["result"]["generation"], json!(1));
    assert_eq!(active_info["result"]["vectorBlob"], descriptor);
    assert_eq!(active.finish().code(), Some(0));

    let request = vector_upsert_for_document(
        12,
        "pending-vector",
        "pending-document",
        [0.0, 1.0],
        "pending",
    );
    write_wal_record(&db.path, 1, &request);
    let mut recovery = NativeProcess::spawn(&db.path, &[]);
    let recovery_info = recovery.send(json!({
        "id": 13,
        "method": "protocol_info",
        "params": {}
    }));
    assert_eq!(recovery_info["result"]["state"], json!("recoveryPending"));
    assert_eq!(recovery_info["result"]["generation"], json!(1));
    assert_eq!(recovery_info["result"]["vectorBlob"], descriptor);
    assert_eq!(recovery.finish().code(), Some(0));
    let after_recovery: Value =
        serde_json::from_slice(&std::fs::read(&db.path).expect("read canonical after recovery"))
            .expect("parse canonical after recovery");
    assert_eq!(after_recovery["generation"], json!(1));
    assert_eq!(after_recovery["vectorBlob"], descriptor);
    assert!(
        db.path.with_extension("agdb.wal").exists(),
        "observing recovery metadata must not retire its WAL"
    );
}

#[test]
fn normal_startup_rejects_a_generation_zero_descriptor() {
    let db = TempDb::new("generation-zero-descriptor");
    seed_vector(&db.path);
    let mut canonical: Value = serde_json::from_slice(
        &std::fs::read(&db.path).expect("read positive canonical generation"),
    )
    .expect("parse positive canonical generation");
    canonical["generation"] = json!(0);
    let invalid = serde_json::to_vec(&canonical).expect("encode invalid generation zero");
    std::fs::write(&db.path, &invalid).expect("write invalid generation zero");

    let native = NativeProcess::spawn(&db.path, &[]);
    assert_ne!(native.finish().code(), Some(0));
    assert_eq!(
        std::fs::read(&db.path).expect("canonical remains after rejection"),
        invalid
    );
}

#[test]
fn partial_blob_is_rejected_without_rewriting_canonical_json() {
    let db = TempDb::new("partial");
    seed_vector(&db.path);
    let before = std::fs::read(&db.path).expect("canonical JSON before corruption");
    let state: Value = serde_json::from_slice(&before).expect("state JSON");
    let blob_path = db
        .dir
        .join(state["vectorBlob"]["basename"].as_str().unwrap());
    let mut blob = std::fs::read(&blob_path).expect("blob before corruption");
    blob.truncate(blob.len().saturating_sub(1));
    std::fs::write(&blob_path, blob).expect("truncate blob");

    let native = NativeProcess::spawn(&db.path, &[]);
    let status = native.finish();
    assert_ne!(status.code(), Some(0));
    assert_eq!(
        std::fs::read(&db.path).expect("canonical JSON remains"),
        before
    );
}

#[test]
fn every_native_commit_kill_point_reopens_a_complete_vector_generation() {
    for stage in [
        "after_blob_temp_sync_fsync",
        "after_blob_rename",
        "after_blob_dir_fsync",
        "after_json_temp_sync_fsync",
        "after_json_rename",
        "after_json_dir_fsync",
        "after_wal_zero",
        "after_wal_zero_sync",
        "after_wal_zero_validate",
        "after_wal_retire",
    ] {
        let db = TempDb::new(stage);
        seed_vector(&db.path);
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
        assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(3, [0.0, 1.0], "new"))["ok"],
            json!(true)
        );
        native.send_commit_without_read(4);
        assert_native_killed(native.finish(), stage);
        if stage == "after_json_dir_fsync" {
            assert!(
                db.path.with_extension("agdb.wal").exists(),
                "base-generation WAL must remain after JSON publication"
            );
        }
        probe_vector_pair(&db.path);
        if stage == "after_json_dir_fsync" {
            assert_zero_wal(&db.path, "reopen must zero WAL already included by JSON");
        }
    }
}

#[test]
fn injected_write_sync_rename_and_directory_failures_never_return_a_token() {
    for stage in [
        "blob_temp_sync_create",
        "blob_temp_sync_write",
        "blob_temp_sync_fsync",
        "blob_temp_sync_cache_evict",
        "blob_rename",
        "blob_dir_fsync",
        "json_temp_sync_create",
        "json_temp_sync_write",
        "json_temp_sync_fsync",
        "json_temp_sync_cache_evict",
        "json_rename",
        "json_dir_fsync",
        "wal_zero",
        "wal_zero_sync",
        "wal_zero_validate",
        "wal_retire",
    ] {
        for phase in ["before", "after"] {
            let failpoint = format!("{phase}_{stage}");
            let db = TempDb::new(&format!("{phase}-{stage}"));
            seed_vector(&db.path);
            let before = std::fs::read(&db.path).expect("base canonical JSON");
            let mut native =
                NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", failpoint.as_str())]);
            assert_eq!(native.send(batch_begin(5))["ok"], json!(true));
            assert_eq!(
                native.send(vector_upsert(6, [0.0, 1.0], "new"))["ok"],
                json!(true)
            );
            let commit = native.commit(7);
            assert_eq!(commit["ok"], json!(false), "failpoint {failpoint}");
            assert!(
                commit.get("result").is_none(),
                "failpoint {failpoint} returned token"
            );
            assert_ne!(native.finish().code(), Some(0));

            let canonical_after = std::fs::read(&db.path).expect("canonical JSON remains readable");
            let after_generation = generation(&db.path);
            assert!(after_generation == 1 || after_generation == 2);
            if after_generation == 1 {
                assert_eq!(
                    canonical_after, before,
                    "failpoint {failpoint} advanced JSON"
                );
            }
            probe_vector_pair(&db.path);
        }
    }
}

#[test]
fn wal_zero_substages_reopen_one_complete_generation() {
    for stage in [
        "after_wal_zero",
        "after_wal_zero_sync",
        "after_wal_zero_validate",
    ] {
        let db = TempDb::new(stage);
        seed_vector(&db.path);
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
        assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(3, [0.0, 1.0], "new"))["ok"],
            json!(true)
        );
        native.send_commit_without_read(4);
        assert_native_killed(native.finish(), stage);
        assert_eq!(generation(&db.path), 2, "{stage} must publish N+1 first");

        assert_zero_wal(&db.path, stage);
        assert!(recognized_wal_retire_paths(&db).is_empty());
        probe_vector_pair(&db.path);
        assert_eq!(
            generation(&db.path),
            2,
            "{stage} changed canonical generation"
        );
        assert!(
            recognized_wal_retire_paths(&db).is_empty(),
            "{stage} left a retired WAL artifact after restart"
        );
    }
}

#[test]
fn injected_wal_zero_substage_failures_never_return_token() {
    for stage in [
        "after_wal_zero",
        "after_wal_zero_sync",
        "after_wal_zero_validate",
    ] {
        let db = TempDb::new(&format!("fail-{stage}"));
        seed_vector(&db.path);
        let canonical_before = std::fs::read(&db.path).expect("canonical state before failure");
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", stage)]);
        assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(3, [0.0, 1.0], "new"))["ok"],
            json!(true)
        );
        let commit = native.commit(4);
        assert_eq!(commit["ok"], json!(false), "{stage}: {commit}");
        assert!(commit.get("result").is_none(), "{stage}: {commit}");
        assert_ne!(native.finish().code(), Some(0), "{stage} stayed alive");
        assert!(matches!(generation(&db.path), 1 | 2));
        if generation(&db.path) == 1 {
            assert_eq!(
                std::fs::read(&db.path).expect("canonical state after failed retire"),
                canonical_before,
                "{stage} advanced canonical JSON"
            );
        }
        probe_vector_pair(&db.path);
        assert!(
            recognized_wal_retire_paths(&db).is_empty(),
            "{stage} left a retired WAL artifact after recovery"
        );
    }
}

#[test]
fn startup_retires_only_empty_or_already_published_wal_artifacts() {
    let request = vector_upsert_for_document(1, "old-v", "old-document", [1.0, 0.0], "old");
    for (label, wal_bytes) in [
        ("empty", Vec::new()),
        ("old", encoded_wal_record(0, &request)),
    ] {
        let db = TempDb::new(&format!("retired-orphan-{label}"));
        seed_vector(&db.path);
        let retired = recognized_wal_retire_path(&db, 7);
        std::fs::write(&retired, &wal_bytes).expect("write recognized retired orphan");
        assert_zero_wal(&db.path, "seeded canonical idle WAL");

        let mut native = NativeProcess::spawn(&db.path, &[]);
        let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
        assert_eq!(info["ok"], json!(true), "{label}: {info}");
        assert_eq!(info["result"]["state"], json!("idle"));
        assert_eq!(info["result"]["generation"], json!(1));
        assert_eq!(native.finish().code(), Some(0));
        assert!(!retired.exists(), "{label} orphan was not retired");

        let mut reopened = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(
            reopened.send(json!({"id":2,"method":"ping","params":{}}))["ok"],
            json!(true)
        );
        assert_eq!(reopened.finish().code(), Some(0));
        assert!(!retired.exists(), "{label} orphan reappeared after restart");
        assert_eq!(
            generation(&db.path),
            1,
            "{label} advanced canonical generation"
        );
    }
}

#[test]
fn unsupported_noreplace_fails_before_loading_or_mutating_canonical_state() {
    let db = TempDb::new("noreplace-preflight");
    seed_vector(&db.path);
    let canonical_before = std::fs::read(&db.path).expect("canonical before preflight failure");
    let blobs_before = generation_blob_paths(&db);
    let native = NativeProcess::spawn(
        &db.path,
        &[("AGDB_NATIVE_FAIL_POINT", "noreplace_unsupported")],
    );
    assert_ne!(native.finish().code(), Some(0));
    assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);
    assert_eq!(generation_blob_paths(&db), blobs_before);
    assert_zero_wal(&db.path, "noreplace preflight preserves idle WAL");
    assert!(rename_probe_paths(&db).is_empty());
}

#[test]
fn oversized_or_corrupt_probe_artifacts_fail_bounded_without_deletion() {
    for label in ["oversized", "corrupt"] {
        let db = TempDb::new(&format!("probe-bounded-{label}"));
        seed_vector(&db.path);
        let canonical_before = std::fs::read(&db.path).expect("canonical before bad probe");
        let artifact = db.dir.join(format!(
            ".state.json.rename_probe.0123456789abcdef0123456789abcdef.{}.tmp",
            if label == "oversized" { 21 } else { 22 }
        ));
        if label == "oversized" {
            let file = std::fs::File::create(&artifact).expect("create sparse probe artifact");
            file.set_len(1_u64 << 40)
                .expect("size sparse probe artifact");
        } else {
            std::fs::write(&artifact, b"not-a-probe").expect("write corrupt probe artifact");
        }
        let started = std::time::Instant::now();
        let native = NativeProcess::spawn(&db.path, &[]);
        assert_ne!(native.finish().code(), Some(0), "{label} startup succeeded");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "{label} probe rejection was not bounded"
        );
        assert!(artifact.exists(), "{label} artifact was deleted");
        if label == "oversized" {
            assert_eq!(std::fs::metadata(&artifact).unwrap().len(), 1_u64 << 40);
        } else {
            assert_eq!(std::fs::read(&artifact).unwrap(), b"not-a-probe");
        }
        assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);
        assert_eq!(generation(&db.path), 1);
    }
}

#[test]
fn normal_probe_cleanup_rejects_post_validation_path_swap_without_unlinking() {
    let db = TempDb::new("probe-owned-cleanup-swap");
    seed_vector(&db.path);
    let canonical_before = std::fs::read(&db.path).expect("canonical before probe swap");
    let marker = db.dir.join("before-probe-cleanup.marker");
    let marker_value = marker.to_string_lossy().into_owned();
    let replacement = db.dir.join("probe-cleanup-replacement");
    let replacement_raw = b"replacement-must-survive".to_vec();
    std::fs::write(&replacement, &replacement_raw).expect("write replacement sentinel");
    let saved = db.dir.join("saved-owned-probe");
    let swap_dir = db.dir.clone();
    let swap_marker = marker.clone();
    let swap_replacement = replacement.clone();
    let swap_saved = saved.clone();
    let (path_sender, path_receiver) = std::sync::mpsc::channel();
    let swapper = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !swap_marker.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "native did not reach normal probe cleanup pausepoint"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let owned = std::fs::read_dir(&swap_dir)
            .expect("read probe directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| std::fs::read(path).is_ok_and(|raw| raw == b"collision-source"))
            .expect("owned collision probe path");
        std::fs::rename(&owned, &swap_saved).expect("save owned probe inode");
        std::fs::rename(&swap_replacement, &owned).expect("install replacement probe inode");
        path_sender.send(owned).expect("report swapped probe path");
    });
    let native = NativeProcess::spawn(
        &db.path,
        &[
            (
                "AGDB_NATIVE_TEST_PAUSE_POINT",
                "before_noreplace_probe_cleanup",
            ),
            ("AGDB_NATIVE_TEST_PAUSE_MS", "250"),
            ("AGDB_NATIVE_TEST_PAUSE_MARKER", marker_value.as_str()),
        ],
    );
    swapper.join().expect("normal probe cleanup swapper exits");
    let swapped = path_receiver.recv().expect("receive swapped probe path");
    assert_ne!(native.finish().code(), Some(0));
    assert_eq!(std::fs::read(&swapped).unwrap(), replacement_raw);
    assert_eq!(std::fs::read(&saved).unwrap(), b"collision-source");
    assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);
}

#[test]
fn repeated_probe_kill_and_failure_seams_keep_artifacts_bounded_and_recover() {
    for stage in [
        "after_noreplace_probe_source_create",
        "after_noreplace_probe_source_dir_fsync",
        "after_noreplace_probe_move",
        "after_noreplace_probe_collision_source_create",
        "after_noreplace_probe_collision_dir_fsync",
        "after_noreplace_probe_cleanup_unlink",
    ] {
        let db = TempDb::new(&format!("probe-kill-{stage}"));
        seed_vector(&db.path);
        let canonical_before = std::fs::read(&db.path).expect("canonical before probe kill");
        for iteration in 0..2 {
            let native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
            assert_native_killed(native.finish(), stage);
            assert!(
                rename_probe_paths(&db).len() <= 2,
                "{stage} iteration {iteration} accumulated probe artifacts"
            );
            assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);
        }
        let mut reopened = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(
            reopened.send(json!({"id":1,"method":"ping","params":{}}))["ok"],
            json!(true),
            "{stage} did not recover"
        );
        assert_eq!(reopened.finish().code(), Some(0));
        assert!(rename_probe_paths(&db).is_empty(), "{stage} left artifacts");
        assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);

        let failure_db = TempDb::new(&format!("probe-failure-{stage}"));
        seed_vector(&failure_db.path);
        let canonical_before =
            std::fs::read(&failure_db.path).expect("canonical before probe failure");
        let native = NativeProcess::spawn(&failure_db.path, &[("AGDB_NATIVE_FAIL_POINT", stage)]);
        assert_ne!(
            native.finish().code(),
            Some(0),
            "{stage} failure stayed alive"
        );
        assert!(rename_probe_paths(&failure_db).is_empty());
        assert_eq!(std::fs::read(&failure_db.path).unwrap(), canonical_before);
    }
}

#[test]
fn repeated_probe_orphan_cleanup_kills_rename_without_growing_artifacts() {
    for stage in [
        "after_rename_probe_cleanup_claim",
        "after_rename_probe_cleanup_dir_fsync",
        "after_rename_probe_cleanup_unlink",
    ] {
        let db = TempDb::new(&format!("probe-cleanup-kill-{stage}"));
        seed_vector(&db.path);
        let orphan = db
            .dir
            .join(".state.json.rename_probe.0123456789abcdef0123456789abcdef.99.tmp");
        std::fs::write(&orphan, b"move-").expect("write partial probe orphan");
        let iterations = if stage == "after_rename_probe_cleanup_unlink" {
            1
        } else {
            2
        };
        for iteration in 0..iterations {
            let native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
            assert_native_killed(native.finish(), stage);
            assert!(
                rename_probe_paths(&db).len() <= 1,
                "{stage} iteration {iteration} grew cleanup artifacts"
            );
        }
        if stage == "after_rename_probe_cleanup_unlink" {
            assert!(
                rename_probe_paths(&db).is_empty(),
                "unlink stop point must already have removed the artifact"
            );
        }
        let mut reopened = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(
            reopened.send(json!({"id":1,"method":"ping","params":{}}))["ok"],
            json!(true)
        );
        assert_eq!(reopened.finish().code(), Some(0));
        assert!(rename_probe_paths(&db).is_empty(), "{stage} left artifacts");
        assert_eq!(generation(&db.path), 1);
    }
}

#[test]
fn startup_wal_retire_cleanup_kill_and_failure_seams_leave_no_orphans() {
    for (mode, env_name) in [
        ("kill", "AGDB_NATIVE_KILL_POINT"),
        ("failure", "AGDB_NATIVE_FAIL_POINT"),
    ] {
        for stage in [
            "after_startup_wal_retire_claim",
            "after_startup_wal_retire_dir_fsync",
            "after_startup_wal_retire_unlink",
        ] {
            let db = TempDb::new(&format!("startup-retire-{mode}-{stage}"));
            seed_vector(&db.path);
            let request = vector_upsert_for_document(1, "old-v", "old-document", [1.0, 0.0], "old");
            let retired = recognized_wal_retire_path(&db, 12);
            std::fs::write(&retired, encoded_wal_record(0, &request))
                .expect("write retired WAL orphan");

            let native = NativeProcess::spawn(&db.path, &[(env_name, stage)]);
            let status = native.finish();
            if mode == "kill" {
                assert_native_killed(status, stage);
            } else {
                assert_ne!(status.code(), Some(0), "{stage} failure stayed alive");
            }
            assert_eq!(generation(&db.path), 1);

            let mut reopened = NativeProcess::spawn(&db.path, &[]);
            assert_eq!(
                reopened.send(json!({"id":1,"method":"protocol_info","params":{}}))["ok"],
                json!(true),
                "{mode} {stage} did not recover"
            );
            assert_eq!(reopened.finish().code(), Some(0));
            assert!(
                recognized_wal_retire_paths(&db).is_empty(),
                "{mode} {stage} left a retired WAL artifact"
            );
            assert!(rename_probe_paths(&db).is_empty());
            assert_eq!(generation(&db.path), 1);
        }
    }
}

#[test]
fn startup_preserves_nonretirable_wal_retire_payloads_and_unknown_names() {
    let request =
        vector_upsert_for_document(1, "retained-v", "retained-document", [1.0, 0.0], "retained");
    for (label, wal_bytes) in [
        ("malformed", b"not-json\n".to_vec()),
        ("current", encoded_wal_record(1, &request)),
        ("future", encoded_wal_record(2, &request)),
    ] {
        let db = TempDb::new(&format!("retired-retain-{label}"));
        seed_vector(&db.path);
        let canonical_before = std::fs::read(&db.path).expect("canonical before retained WAL");
        let retired = recognized_wal_retire_path(&db, 8);
        std::fs::write(&retired, &wal_bytes).expect("write retained retired WAL");
        let native = NativeProcess::spawn(&db.path, &[]);
        assert_ne!(native.finish().code(), Some(0), "{label} startup succeeded");
        assert_eq!(std::fs::read(&retired).unwrap(), wal_bytes);
        assert_eq!(std::fs::read(&db.path).unwrap(), canonical_before);
        assert_eq!(generation(&db.path), 1);
    }

    let db = TempDb::new("retired-unknown-name");
    seed_vector(&db.path);
    let unknown = db.dir.join(".state.agdb.wal.unknown-stage.0123.tmp");
    let unknown_bytes = b"unknown-payload\n";
    std::fs::write(&unknown, unknown_bytes).expect("write unknown artifact");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(
        native.send(json!({"id":1,"method":"protocol_info","params":{}}))["ok"],
        json!(true)
    );
    assert_eq!(native.finish().code(), Some(0));
    assert_eq!(std::fs::read(&unknown).unwrap(), unknown_bytes);
}

#[cfg(unix)]
#[test]
fn startup_does_not_touch_wal_retire_symlink_or_hardlink_aliases() {
    for kind in ["symlink", "hardlink"] {
        let db = TempDb::new(&format!("retired-alias-{kind}"));
        seed_vector(&db.path);
        let sentinel = db.dir.join("retired-alias-sentinel");
        let sentinel_bytes = format!("retired-alias-{kind}\n").into_bytes();
        std::fs::write(&sentinel, &sentinel_bytes).expect("write alias sentinel");
        let retired = recognized_wal_retire_path(&db, 9);
        if kind == "symlink" {
            symlink(&sentinel, &retired).expect("create retired symlink");
        } else {
            hard_link(&sentinel, &retired).expect("create retired hardlink");
        }

        let mut native = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(
            native.send(json!({"id":1,"method":"protocol_info","params":{}}))["ok"],
            json!(true),
            "{kind} alias unexpectedly blocked startup"
        );
        assert_eq!(native.finish().code(), Some(0));
        assert_eq!(std::fs::read(&sentinel).unwrap(), sentinel_bytes);
        assert!(retired.exists());
        if kind == "symlink" {
            assert!(
                std::fs::symlink_metadata(&retired)
                    .unwrap()
                    .file_type()
                    .is_symlink()
            );
        } else {
            assert!(retired.exists());
        }
    }
}

#[cfg(unix)]
#[test]
fn startup_wal_retire_claim_rejects_path_swap_without_unlinking_replacement() {
    let db = TempDb::new("retired-claim-path-swap");
    seed_vector(&db.path);
    let old_request = vector_upsert_for_document(1, "old-v", "old-document", [1.0, 0.0], "old");
    let retired = recognized_wal_retire_path(&db, 10);
    let retired_raw = encoded_wal_record(0, &old_request);
    std::fs::write(&retired, &retired_raw).expect("write retired WAL candidate");
    let saved = db.dir.join("saved-retired-wal");
    let replacement = db.dir.join("replacement-retired-wal");
    let replacement_raw = b"replacement-must-survive\n".to_vec();
    std::fs::write(&replacement, &replacement_raw).expect("write replacement sentinel");
    let marker = db.dir.join("before-retired-claim.marker");
    let marker_value = marker.to_string_lossy().into_owned();

    let swap_path = retired.clone();
    let swap_saved = saved.clone();
    let swap_replacement = replacement.clone();
    let swap_marker = marker.clone();
    let swapper = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !swap_marker.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "native did not reach the startup WAL claim pausepoint"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        std::fs::rename(&swap_path, &swap_saved).expect("save validated retired WAL");
        std::fs::rename(&swap_replacement, &swap_path).expect("install replacement inode");
    });
    let native = NativeProcess::spawn(
        &db.path,
        &[
            (
                "AGDB_NATIVE_TEST_PAUSE_POINT",
                "before_startup_wal_retire_claim",
            ),
            ("AGDB_NATIVE_TEST_PAUSE_MS", "250"),
            ("AGDB_NATIVE_TEST_PAUSE_MARKER", marker_value.as_str()),
        ],
    );
    swapper.join().expect("WAL claim swapper exits");
    assert_ne!(native.finish().code(), Some(0));
    assert_eq!(std::fs::read(&saved).unwrap(), retired_raw);
    let claimed = recognized_wal_retire_paths(&db);
    assert_eq!(claimed.len(), 1, "replacement claim must be preserved");
    assert_eq!(std::fs::read(&claimed[0]).unwrap(), replacement_raw);
    assert_eq!(generation(&db.path), 1);
}

#[cfg(unix)]
#[test]
fn startup_wal_retire_claim_rejects_same_inode_append_without_unlinking_payload() {
    let db = TempDb::new("retired-claim-concurrent-append");
    seed_vector(&db.path);
    let old_request = vector_upsert_for_document(1, "old-v", "old-document", [1.0, 0.0], "old");
    let retired = recognized_wal_retire_path(&db, 11);
    let old_raw = encoded_wal_record(0, &old_request);
    std::fs::write(&retired, &old_raw).expect("write retired WAL candidate");
    let current_request = vector_upsert_for_document(2, "new-v", "new-document", [0.0, 1.0], "new");
    let appended_raw = encoded_wal_record(1, &current_request);
    let marker = db.dir.join("after-retired-claim.marker");
    let marker_value = marker.to_string_lossy().into_owned();

    let append_db = db.dir.clone();
    let append_marker = marker.clone();
    let append_bytes = appended_raw.clone();
    let appender = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !append_marker.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "native did not reach the post-claim pausepoint"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let claimed = std::fs::read_dir(&append_db)
            .expect("read claimed WAL directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains(".wal_retire.") && name.ends_with(".tmp"))
            })
            .expect("claimed retired WAL exists");
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&claimed)
            .expect("open claimed WAL for concurrent append");
        file.write_all(&append_bytes)
            .expect("append current-generation WAL record");
        file.sync_all().expect("sync concurrent append");
    });
    let native = NativeProcess::spawn(
        &db.path,
        &[
            (
                "AGDB_NATIVE_TEST_PAUSE_POINT",
                "after_startup_wal_retire_claim",
            ),
            ("AGDB_NATIVE_TEST_PAUSE_MS", "250"),
            ("AGDB_NATIVE_TEST_PAUSE_MARKER", marker_value.as_str()),
        ],
    );
    appender.join().expect("WAL claim appender exits");
    assert_ne!(native.finish().code(), Some(0));
    let claimed = recognized_wal_retire_paths(&db);
    assert_eq!(claimed.len(), 1, "changed claimed WAL must be preserved");
    let mut expected = old_raw;
    expected.extend_from_slice(&appended_raw);
    assert_eq!(std::fs::read(&claimed[0]).unwrap(), expected);
    assert_eq!(generation(&db.path), 1);
}

#[cfg(unix)]
#[test]
fn canonical_db_and_referenced_blob_aliases_fail_closed() {
    let db = TempDb::new("db-symlink");
    let real = db.dir.join("real.json");
    let alias = db.dir.join("alias.json");
    std::fs::write(&real, b"{}").expect("write real db");
    symlink(&real, &alias).expect("create db symlink");
    let native = NativeProcess::spawn(&alias, &[]);
    assert_ne!(native.finish().code(), Some(0));

    let hard_db = TempDb::new("db-hardlink");
    let hard_real = hard_db.dir.join("real.json");
    let hard_alias = hard_db.dir.join("alias.json");
    std::fs::write(&hard_real, b"{}").expect("write hardlink db");
    hard_link(&hard_real, &hard_alias).expect("create db hardlink");
    let native = NativeProcess::spawn(&hard_alias, &[]);
    assert_ne!(native.finish().code(), Some(0));

    let blob_symlink_db = TempDb::new("blob-symlink");
    seed_vector(&blob_symlink_db.path);
    let state: Value = serde_json::from_slice(
        &std::fs::read(&blob_symlink_db.path).expect("read blob symlink state"),
    )
    .expect("parse blob symlink state");
    let blob = blob_symlink_db
        .dir
        .join(state["vectorBlob"]["basename"].as_str().unwrap());
    let target = blob_symlink_db.dir.join("blob-target");
    let blob_bytes = std::fs::read(&blob).expect("read blob target");
    std::fs::write(&target, blob_bytes).expect("write blob target");
    std::fs::remove_file(&blob).expect("remove blob for symlink");
    symlink(&target, &blob).expect("create blob symlink");
    let native = NativeProcess::spawn(&blob_symlink_db.path, &[]);
    assert_ne!(native.finish().code(), Some(0));

    let blob_hardlink_db = TempDb::new("blob-hardlink");
    seed_vector(&blob_hardlink_db.path);
    let state: Value = serde_json::from_slice(
        &std::fs::read(&blob_hardlink_db.path).expect("read blob hardlink state"),
    )
    .expect("parse blob hardlink state");
    let blob = blob_hardlink_db
        .dir
        .join(state["vectorBlob"]["basename"].as_str().unwrap());
    hard_link(&blob, blob_hardlink_db.dir.join("blob-alias")).expect("create blob hardlink");
    let native = NativeProcess::spawn(&blob_hardlink_db.path, &[]);
    assert_ne!(native.finish().code(), Some(0));
}

#[cfg(unix)]
#[test]
fn idle_mutation_rejects_wal_aliases_and_replacement_without_touching_payload() {
    for kind in ["symlink", "hardlink", "replacement"] {
        let db = TempDb::new(&format!("idle-wal-{kind}"));
        seed_vector(&db.path);
        let canonical_before = std::fs::read(&db.path).expect("canonical state before WAL attack");
        let wal_path = db.path.with_extension("agdb.wal");
        let sentinel = db.dir.join("sentinel.wal");
        let sentinel_bytes = format!("sentinel-payload-{kind}\n").into_bytes();
        std::fs::write(&sentinel, &sentinel_bytes).expect("write sentinel payload");

        let mut native = NativeProcess::spawn(&db.path, &[]);
        let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
        assert_eq!(info["result"]["state"], json!("idle"));
        assert_eq!(info["result"]["generation"], json!(1));
        assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
        let original = db.dir.join(format!("held-original-{kind}.wal"));
        std::fs::rename(&wal_path, &original).expect("move held zero WAL out of pathname");

        match kind {
            "symlink" => symlink(&sentinel, &wal_path).expect("install WAL symlink attack"),
            "hardlink" => hard_link(&sentinel, &wal_path).expect("install WAL hardlink attack"),
            "replacement" => {
                std::fs::write(&wal_path, &sentinel_bytes).expect("install WAL replacement inode")
            }
            _ => unreachable!(),
        }

        let response = native.send(vector_upsert_for_document(
            3,
            &format!("attack-v-{kind}"),
            &format!("attack-document-{kind}"),
            [0.0, 1.0],
            "attack",
        ));
        assert_eq!(
            response["ok"],
            json!(false),
            "WAL {kind} attack was accepted"
        );
        assert!(response.get("result").is_none());
        assert_ne!(
            native.finish().code(),
            Some(0),
            "WAL {kind} attack was not fatal"
        );

        assert_eq!(
            std::fs::read(&sentinel).expect("read sentinel payload after attack"),
            sentinel_bytes
        );
        assert_eq!(
            std::fs::read(&wal_path).expect("read WAL payload after attack"),
            sentinel_bytes
        );
        assert_eq!(
            std::fs::read(&db.path).expect("canonical state after attack"),
            canonical_before
        );
        assert_eq!(generation(&db.path), 1, "WAL {kind} advanced generation");
    }
}

#[test]
fn startup_removes_only_recognized_nonce_temps() {
    let db = TempDb::new("temp-cleanup");
    let recognized = db
        .dir
        .join(".state.json.json_temp_sync.0123456789abcdef0123456789abcdef.0.tmp");
    let unknown = db.dir.join(".state.json.unknown.tmp");
    std::fs::write(&recognized, b"orphan").expect("write recognized temp");
    std::fs::write(&unknown, b"keep").expect("write unknown temp");
    let native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.finish().code(), Some(0));
    assert!(!recognized.exists());
    assert!(unknown.exists());
}

#[test]
fn relative_and_default_database_paths_resolve_existing_parents() {
    let db = TempDb::new("relative-default");
    let relative_dir = db.dir.join("relative");
    std::fs::create_dir(&relative_dir).expect("create relative parent");
    let relative = Path::new("relative/state.json");
    let mut native = NativeProcess::spawn_in_dir(relative, &db.dir, &[]);
    let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
    assert_eq!(info["result"]["generation"], json!(0));
    assert_eq!(native.finish().code(), Some(0));
    assert!(!relative_dir.join("state.json").exists());

    let mut default_native = NativeProcess::spawn_default_in_dir(&db.dir);
    assert_eq!(
        default_native.send(json!({"id":2,"method":"ping","params":{}}))["ok"],
        json!(true)
    );
    assert_eq!(default_native.finish().code(), Some(0));
    assert!(!db.dir.join("aira-graphdb-native.json").exists());
}

#[test]
fn wal_sync_failure_returns_failure_and_stops_request_service() {
    let db = TempDb::new("wal-failure");
    let mut native =
        NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", "before_wal_sync")]);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    let response = native.send(vector_upsert(1, [1.0, 0.0], "new"));
    assert_eq!(response["ok"], json!(false));
    assert_eq!(response["error"]["failureClass"], json!("IO_FAILURE"));
    assert_ne!(native.finish().code(), Some(0));
}

#[test]
fn empty_wal_after_before_write_failure_is_reused_by_fresh_commit() {
    for (label, variable, expect_kill) in [
        ("kill", "AGDB_NATIVE_KILL_POINT", true),
        ("failure", "AGDB_NATIVE_FAIL_POINT", false),
    ] {
        let db = TempDb::new(&format!("empty-wal-before-write-{label}"));
        let wal_path = db.path.with_extension("agdb.wal");
        let mut native = NativeProcess::spawn(&db.path, &[(variable, "before_wal_write")]);
        assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
        let mutation = vector_upsert_for_document(
            2,
            &format!("fresh-v-{label}"),
            &format!("fresh-document-{label}"),
            [0.0, 1.0],
            "fresh",
        );
        if expect_kill {
            native.send_without_read(mutation.clone());
            assert_native_killed(native.finish(), "before_wal_write");
        } else {
            let response = native.send(mutation.clone());
            assert_eq!(response["ok"], json!(false));
            assert!(response.get("result").is_none());
            assert_ne!(native.finish().code(), Some(0));
        }
        assert!(wal_path.exists(), "{label} must leave the create-new WAL");
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);
        assert!(!db.path.exists(), "{label} published canonical state");

        let mut recovery = NativeProcess::spawn(&db.path, &[]);
        let info = recovery.send(json!({"id":3,"method":"protocol_info","params":{}}));
        assert_eq!(info["ok"], json!(true), "{label} restart health");
        assert_eq!(
            info["result"]["state"],
            json!("idle"),
            "{label} restart state"
        );
        assert_eq!(info["result"]["generation"], json!(0));
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            0,
            "{label} restart must preserve a reusable zero WAL"
        );
        assert_eq!(recovery.finish().code(), Some(0));

        let mut fresh = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(fresh.send(batch_begin(4))["ok"], json!(true));
        assert_eq!(fresh.send(mutation)["ok"], json!(true));
        let commit = fresh.commit(5);
        assert_eq!(commit["ok"], json!(true), "{label} fresh commit: {commit}");
        assert_eq!(commit["result"]["generation"], json!(1));
        assert_eq!(fresh.finish().code(), Some(0));
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            0,
            "{label} successful commit must zero WAL"
        );

        let mut reader = NativeProcess::spawn(&db.path, &[]);
        let search = reader.send(vector_search(6, [0.0, 1.0]));
        assert_eq!(search["ok"], json!(true), "{label} fresh read");
        assert_eq!(search["result"].as_array().unwrap().len(), 1);
        assert_eq!(search["result"][0]["id"], json!(format!("fresh-v-{label}")));
        assert_eq!(reader.finish().code(), Some(0));
    }
}

#[cfg(unix)]
#[test]
fn committed_zero_wal_reuses_the_same_inode_without_parent_directory_fsync() {
    let db = TempDb::new("held-zero-wal-reuse");
    seed_vector(&db.path);
    let wal_path = db.path.with_extension("agdb.wal");
    let before = std::fs::metadata(&wal_path).expect("seeded zero WAL metadata");
    assert_eq!(before.len(), 0);

    let mut native = NativeProcess::spawn(
        &db.path,
        &[("AGDB_NATIVE_FAIL_POINT", "before_wal_dir_fsync")],
    );
    assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(3, [0.0, 1.0], "reused"))["ok"],
        json!(true)
    );
    let commit = native.commit(4);
    assert_eq!(
        commit["ok"],
        json!(true),
        "existing WAL required create-path fsync"
    );
    assert_eq!(commit["result"]["generation"], json!(2));
    assert_eq!(native.finish().code(), Some(0));

    let after = std::fs::metadata(&wal_path).expect("reused zero WAL metadata");
    assert_eq!(after.len(), 0);
    assert_eq!((after.dev(), after.ino()), (before.dev(), before.ino()));
    assert!(recognized_wal_retire_paths(&db).is_empty());
}

#[cfg(unix)]
#[test]
fn wal_path_swap_after_sync_fails_closed_before_identity_validation() {
    for (label, seed_existing_wal) in [("initial", false), ("existing", true)] {
        let db = TempDb::new(&format!("wal-path-swap-{label}"));
        if seed_existing_wal {
            seed_vector(&db.path);
        }
        let canonical_before = std::fs::read(&db.path).ok();
        let generation_before = generation(&db.path);
        let wal_path = db.path.with_extension("agdb.wal");
        let original_path = db.dir.join(format!("{label}-original.wal"));
        let replacement_path = db.dir.join(format!("{label}-replacement.wal"));
        let pause_marker = db.dir.join(format!("{label}-post-sync.marker"));
        let pause_marker_value = pause_marker.to_string_lossy().into_owned();
        let sentinel = format!("sentinel-{label}\n").into_bytes();
        std::fs::write(&replacement_path, &sentinel).expect("write WAL replacement sentinel");

        let mut native = NativeProcess::spawn(
            &db.path,
            &[
                (
                    "AGDB_NATIVE_TEST_PAUSE_POINT",
                    "after_wal_sync_before_identity_check",
                ),
                ("AGDB_NATIVE_TEST_PAUSE_MS", "1000"),
                ("AGDB_NATIVE_TEST_PAUSE_MARKER", pause_marker_value.as_str()),
            ],
        );
        assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
        let (baseline_len, mutation) = if seed_existing_wal {
            let first = vector_upsert_for_document(
                2,
                "existing-first-v",
                "existing-first-document",
                [1.0, 0.0],
                "existing-first",
            );
            assert_eq!(native.send(first)["ok"], json!(true));
            std::fs::remove_file(&pause_marker)
                .expect("clear the first mutation's post-sync marker");
            (
                std::fs::metadata(&wal_path)
                    .expect("existing WAL metadata")
                    .len(),
                vector_upsert_for_document(
                    3,
                    "existing-second-v",
                    "existing-second-document",
                    [0.0, 1.0],
                    "existing-second",
                ),
            )
        } else {
            (
                0,
                vector_upsert_for_document(
                    2,
                    "initial-v",
                    "initial-document",
                    [0.0, 1.0],
                    "initial",
                ),
            )
        };

        let swap_wal = wal_path.clone();
        let swap_marker = pause_marker.clone();
        let move_original = original_path.clone();
        let move_replacement = replacement_path.clone();
        let swapper = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                if swap_marker.exists() {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "native did not reach the post-WAL-sync pausepoint"
                );
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            std::fs::rename(&swap_wal, &move_original).expect("move original WAL inode");
            std::fs::rename(&move_replacement, &swap_wal).expect("install replacement WAL inode");
        });
        let response = native.send(mutation);
        swapper.join().expect("WAL swapper exits");
        assert_eq!(response["ok"], json!(false), "{label}: {response}");
        assert!(response.get("result").is_none(), "{label}: {response}");
        assert_ne!(native.finish().code(), Some(0), "{label} must fail closed");

        assert_eq!(std::fs::read(&wal_path).unwrap(), sentinel);
        assert!(
            std::fs::metadata(&wal_path)
                .expect("replacement WAL metadata")
                .is_file()
        );
        assert!(original_path.exists(), "{label} original inode was moved");
        assert!(
            std::fs::metadata(&original_path)
                .expect("original WAL metadata")
                .len()
                > baseline_len
        );
        assert_eq!(std::fs::read(&db.path).ok(), canonical_before);
        assert_eq!(generation(&db.path), generation_before);
    }
}

#[test]
fn valid_base_wal_enters_recovery_pending_without_exposing_or_replaying_data() {
    let db = TempDb::new("wal-recovery-pending");
    let request = vector_upsert_for_document(7, "old-v", "old-document", [1.0, 0.0], "old");
    let wal_path = db.path.with_extension("agdb.wal");
    let wal_bytes = write_wal_record(&db.path, 0, &request);
    let wal_digest = digest_hex(&wal_bytes);

    let mut native = NativeProcess::spawn(&db.path, &[]);
    let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
    assert_eq!(info["ok"], json!(true));
    assert_eq!(info["result"]["generation"], json!(0));
    assert_eq!(info["result"]["state"], json!("recoveryPending"));
    assert_eq!(info["result"]["recovery"]["baseGeneration"], json!(0));
    assert_eq!(info["result"]["recovery"]["walDigest"], json!(wal_digest));
    assert_eq!(info["result"]["recovery"]["recordCount"], json!(1));

    for (id, request) in [
        (2, vector_search(2, [1.0, 0.0])),
        (
            3,
            json!({
                "id": 3,
                "method": "get_nodes",
                "params": {"corpusId":"c1"}
            }),
        ),
        (
            4,
            json!({
                "id": 4,
                "method": "memory_load",
                "params": {"corpusId":"c1"}
            }),
        ),
        (
            5,
            json!({
                "id": 5,
                "method": "lexical_search",
                "params": {"corpusId":"c1", "query":"old"}
            }),
        ),
        (
            6,
            json!({
                "id": 6,
                "method": "projection_get_node_count",
                "params": {"corpusId":"c1"}
            }),
        ),
    ] {
        let response = native.send(request);
        assert_eq!(
            response["ok"],
            json!(false),
            "WAL-only data leaked: {response}"
        );
        assert!(
            response.get("result").is_none(),
            "blocked read returned data: {response}"
        );
        assert_eq!(response["id"], json!(id));
    }

    let methods = info["result"]["methods"]
        .as_array()
        .expect("method inventory");
    for method in methods {
        let name = method["name"].as_str().expect("method name");
        let classification = method["classification"]
            .as_str()
            .expect("method classification");
        if classification == "health" || name == "recovery_discard" {
            continue;
        }
        // A blocked mutator is allowed to fail-closed and terminate the
        // request service. Probe each policy tuple from a fresh pending
        // process so one such termination cannot hide later methods.
        let mut probe = NativeProcess::spawn(&db.path, &[]);
        let probe_info = probe.send(json!({
            "id": 200,
            "method": "protocol_info",
            "params": {}
        }));
        assert_eq!(probe_info["result"]["state"], json!("recoveryPending"));
        let response = probe.send(json!({
            "id": 100,
            "method": name,
            "params": {}
        }));
        assert_eq!(
            response["ok"],
            json!(false),
            "non-health method was allowed: {name}"
        );
        assert!(
            response.get("result").is_none(),
            "non-health result leaked: {response}"
        );
        let _ = probe.finish();
    }
    assert_eq!(
        native.send(json!({"id":101,"method":"ping","params":{}}))["ok"],
        json!(true)
    );

    let wrong_digest = "0".repeat(64);
    assert_ne!(wrong_digest, wal_digest);
    let wrong_digest_response = native.send(json!({
        "id": 102,
        "method": "recovery_discard",
        "params": {"baseGeneration":0, "walDigest":wrong_digest}
    }));
    assert_eq!(wrong_digest_response["ok"], json!(false));
    assert_eq!(
        std::fs::read(&wal_path).expect("WAL remains after wrong digest"),
        wal_bytes
    );

    let wrong_base_response = native.send(json!({
        "id": 103,
        "method": "recovery_discard",
        "params": {"baseGeneration":1, "walDigest":wal_digest}
    }));
    assert_eq!(wrong_base_response["ok"], json!(false));
    assert_eq!(
        std::fs::read(&wal_path).expect("WAL remains after wrong base"),
        wal_bytes
    );
    assert!(quarantined_wal_paths(&db.dir, &wal_bytes).is_empty());

    let discard = native.send(json!({
        "id": 104,
        "method": "recovery_discard",
        "params": {"baseGeneration":0, "walDigest":wal_digest}
    }));
    assert_eq!(
        discard["ok"],
        json!(true),
        "exact recovery discard failed: {discard}"
    );
    assert_eq!(discard["result"]["baseGeneration"], json!(0));
    assert_eq!(discard["result"]["walDigest"], json!(wal_digest));
    assert_eq!(discard["result"]["recordCount"], json!(1));
    assert_eq!(discard["result"]["quarantined"], json!(true));
    assert!(
        !wal_path.exists(),
        "discard must quarantine, not leave active WAL"
    );
    let quarantine = quarantined_wal_paths(&db.dir, &wal_bytes);
    assert_eq!(quarantine.len(), 1, "discarded WAL must remain recoverable");
    assert_eq!(
        native.send(json!({"id":105,"method":"protocol_info","params":{}}))["result"]["state"],
        json!("idle")
    );
    assert_eq!(native.finish().code(), Some(0));
    assert!(
        !db.path.exists(),
        "discard must not publish a canonical generation"
    );

    let mut unrelated = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(unrelated.send(batch_begin(106))["ok"], json!(true));
    assert_eq!(
        unrelated.send(vector_upsert_for_document(
            107,
            "new-v",
            "new-document",
            [0.0, 1.0],
            "new",
        ))["ok"],
        json!(true)
    );
    let commit = unrelated.commit(108);
    assert_eq!(
        commit["ok"],
        json!(true),
        "unrelated document commit failed: {commit}"
    );
    assert_eq!(commit["result"]["generation"], json!(1));
    assert_eq!(unrelated.finish().code(), Some(0));

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let old = reopened.send(vector_search(109, [1.0, 0.0]));
    let new = reopened.send(vector_search(110, [0.0, 1.0]));
    assert_eq!(old["ok"], json!(true));
    assert!(
        old["result"]
            .as_array()
            .expect("old search result")
            .is_empty()
    );
    let new_items = new["result"].as_array().expect("new search result");
    assert_eq!(new_items.len(), 1);
    assert_eq!(new_items[0]["id"], json!("new-v"));
    assert_eq!(reopened.finish().code(), Some(0));
    assert_eq!(quarantined_wal_paths(&db.dir, &wal_bytes), quarantine);
}

#[test]
fn recovery_pending_memory_save_file_rejects_before_reading_external_snapshot() {
    let db = TempDb::new("memory-save-file-recovery-gate");
    let unreadable_snapshot = db.dir.join("snapshot-sentinel-directory");
    std::fs::create_dir(&unreadable_snapshot).expect("create unreadable snapshot sentinel");
    let request =
        vector_upsert_for_document(1, "pending-v", "pending-document", [1.0, 0.0], "pending");
    let (mut native, wal_bytes, _) = open_recovery_pending(&db.path, &request);

    let response = native.send(memory_save_file(2, &unreadable_snapshot));
    assert_eq!(response["ok"], json!(false));
    assert_ne!(response["error"]["failureClass"], json!("IO_FAILURE"));
    let message = response["error"]["message"]
        .as_str()
        .expect("recovery rejection message");
    assert!(
        message.contains("recovery"),
        "wrong rejection boundary: {message}"
    );
    assert!(
        !message.contains("read snapshot"),
        "external snapshot was read: {message}"
    );
    assert_eq!(
        std::fs::read(db.path.with_extension("agdb.wal")).unwrap(),
        wal_bytes
    );
    assert_eq!(
        native.send(json!({"id":3,"method":"ping","params":{}}))["ok"],
        json!(true)
    );
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn idle_memory_save_file_rejects_before_reading_external_snapshot() {
    let db = TempDb::new("memory-save-file-idle-gate");
    let unreadable_snapshot = db.dir.join("snapshot-sentinel-directory");
    std::fs::create_dir(&unreadable_snapshot).expect("create unreadable snapshot sentinel");
    let mut native = NativeProcess::spawn(&db.path, &[]);

    let response = native.send(memory_save_file(1, &unreadable_snapshot));
    assert_eq!(response["ok"], json!(false));
    assert_ne!(response["error"]["failureClass"], json!("IO_FAILURE"));
    let message = response["error"]["message"]
        .as_str()
        .expect("idle rejection message");
    assert!(
        message.contains("active batch"),
        "wrong idle rejection boundary: {message}"
    );
    assert!(
        !message.contains("read snapshot"),
        "external snapshot was read: {message}"
    );
    assert!(!db.path.exists());
    assert!(!db.path.with_extension("agdb.wal").exists());
    assert_ne!(native.finish().code(), Some(0));
}

#[test]
fn recovery_discard_rejects_wal_replacement_and_append_without_quarantine() {
    let replacement_db = TempDb::new("recovery-wal-replacement");
    let request = vector_upsert_for_document(
        1,
        "replacement-v",
        "replacement-document",
        [1.0, 0.0],
        "replacement",
    );
    let (mut native, original, digest) = open_recovery_pending(&replacement_db.path, &request);
    let replacement = encoded_wal_record(
        0,
        &vector_upsert_for_document(
            2,
            "replacement-v2",
            "replacement-document-2",
            [0.0, 1.0],
            "replacement-2",
        ),
    );
    let replacement_tmp = replacement_db.dir.join("replacement.wal.tmp");
    std::fs::write(&replacement_tmp, &replacement).expect("write replacement WAL");
    std::fs::rename(
        &replacement_tmp,
        replacement_db.path.with_extension("agdb.wal"),
    )
    .expect("atomically replace active WAL");
    let response = native.send(recovery_discard(2, 0, &digest));
    assert_eq!(response["ok"], json!(false));
    assert!(response.get("result").is_none());
    assert_eq!(
        std::fs::read(replacement_db.path.with_extension("agdb.wal")).unwrap(),
        replacement
    );
    assert!(quarantined_wal_paths(&replacement_db.dir, &original).is_empty());
    assert!(quarantined_wal_paths(&replacement_db.dir, &replacement).is_empty());
    assert_ne!(native.finish().code(), Some(0));

    let append_db = TempDb::new("recovery-wal-append");
    let request =
        vector_upsert_for_document(4, "append-v", "append-document", [1.0, 0.0], "append");
    let (mut native, original, digest) = open_recovery_pending(&append_db.path, &request);
    let appended_record = encoded_wal_record(
        0,
        &vector_upsert_for_document(5, "append-v2", "append-document-2", [0.0, 1.0], "append-2"),
    );
    let wal_path = append_db.path.with_extension("agdb.wal");
    let mut appended = original.clone();
    appended.extend_from_slice(&appended_record);
    std::fs::write(&wal_path, &appended).expect("append WAL record");
    let response = native.send(recovery_discard(6, 0, &digest));
    assert_eq!(response["ok"], json!(false));
    assert!(response.get("result").is_none());
    assert_eq!(std::fs::read(&wal_path).unwrap(), appended);
    assert!(quarantined_wal_paths(&append_db.dir, &original).is_empty());
    assert!(quarantined_wal_paths(&append_db.dir, &appended).is_empty());
    assert_ne!(native.finish().code(), Some(0));
}

#[cfg(unix)]
#[test]
fn recovery_discard_rejects_same_inode_append_during_quarantine() {
    let db = TempDb::new("recovery-wal-concurrent-append");
    let request = vector_upsert_for_document(1, "race-v", "race-document", [1.0, 0.0], "race");
    let pause_marker = db.dir.join("recovery-quarantine.marker");
    let pause_marker_value = pause_marker.to_string_lossy().into_owned();
    let (mut native, original, digest) = open_recovery_pending_with_env(
        &db.path,
        &request,
        &[
            (
                "AGDB_NATIVE_TEST_PAUSE_POINT",
                "before_recovery_quarantine_rename",
            ),
            ("AGDB_NATIVE_TEST_PAUSE_MS", "250"),
            ("AGDB_NATIVE_TEST_PAUSE_MARKER", pause_marker_value.as_str()),
        ],
    );
    let appended_record = encoded_wal_record(
        0,
        &vector_upsert_for_document(2, "race-v2", "race-document-2", [0.0, 1.0], "race-append"),
    );
    let wal_path = db.path.with_extension("agdb.wal");
    let append_path = wal_path.clone();
    let append_marker = pause_marker.clone();
    let append_bytes = appended_record.clone();
    let appender = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while !append_marker.exists() {
            assert!(
                std::time::Instant::now() < deadline,
                "native did not reach recovery quarantine pausepoint"
            );
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&append_path)
            .expect("open recovery WAL during pause");
        file.write_all(&append_bytes)
            .expect("append recovery WAL during pause");
        file.sync_all().expect("sync appended recovery WAL");
    });
    let response = native.send(recovery_discard(3, 0, &digest));
    appender.join().expect("concurrent WAL appender exits");
    assert_eq!(response["ok"], json!(false));
    assert!(response.get("result").is_none());
    assert_ne!(native.finish().code(), Some(0));
    assert!(!wal_path.exists());
    let mut quarantined = original;
    quarantined.extend_from_slice(&appended_record);
    assert_eq!(quarantined_wal_paths(&db.dir, &quarantined).len(), 1);
    assert!(!db.path.exists());
}

#[cfg(unix)]
#[test]
fn recovery_discard_rejects_wal_symlink_and_hardlink_without_quarantine() {
    let symlink_db = TempDb::new("recovery-wal-symlink");
    let request =
        vector_upsert_for_document(1, "symlink-v", "symlink-document", [1.0, 0.0], "symlink");
    let (mut native, original, digest) = open_recovery_pending(&symlink_db.path, &request);
    let wal_path = symlink_db.path.with_extension("agdb.wal");
    let target = symlink_db.dir.join("symlink-target.wal");
    std::fs::write(&target, &original).expect("write symlink WAL target");
    std::fs::remove_file(&wal_path).expect("remove active WAL for symlink");
    symlink(&target, &wal_path).expect("create WAL symlink");
    let response = native.send(recovery_discard(2, 0, &digest));
    assert_eq!(response["ok"], json!(false));
    assert!(response.get("result").is_none());
    assert!(
        std::fs::symlink_metadata(&wal_path)
            .expect("symlink remains")
            .file_type()
            .is_symlink()
    );
    assert!(quarantined_wal_paths(&symlink_db.dir, &original).is_empty());
    assert_ne!(native.finish().code(), Some(0));

    let hardlink_db = TempDb::new("recovery-wal-hardlink");
    let request =
        vector_upsert_for_document(3, "hardlink-v", "hardlink-document", [1.0, 0.0], "hardlink");
    let (mut native, original, digest) = open_recovery_pending(&hardlink_db.path, &request);
    let wal_path = hardlink_db.path.with_extension("agdb.wal");
    let target = hardlink_db.dir.join("hardlink-target.wal");
    std::fs::write(&target, &original).expect("write hardlink WAL target");
    std::fs::remove_file(&wal_path).expect("remove active WAL for hardlink");
    hard_link(&target, &wal_path).expect("create WAL hardlink");
    let response = native.send(recovery_discard(4, 0, &digest));
    assert_eq!(response["ok"], json!(false));
    assert!(response.get("result").is_none());
    assert!(wal_path.exists());
    assert!(target.exists());
    assert!(quarantined_wal_paths(&hardlink_db.dir, &original).is_empty());
    assert_ne!(native.finish().code(), Some(0));
}

#[test]
fn recovery_quarantine_failpoints_never_return_a_success_token() {
    for stage in [
        "before_recovery_quarantine_rename",
        "after_recovery_quarantine_rename",
        "before_recovery_quarantine_dir_fsync",
        "after_recovery_quarantine_dir_fsync",
    ] {
        let db = TempDb::new(stage);
        let request =
            vector_upsert_for_document(1, "fault-v", "fault-document", [1.0, 0.0], "fault");
        let (mut native, wal_bytes, digest) = open_recovery_pending_with_env(
            &db.path,
            &request,
            &[("AGDB_NATIVE_FAIL_POINT", stage)],
        );
        let response = native.send(recovery_discard(2, 0, &digest));
        assert_eq!(response["ok"], json!(false), "fault point {stage}");
        assert!(
            response.get("result").is_none(),
            "fault point {stage} returned token"
        );
        assert_ne!(native.finish().code(), Some(0));
        assert!(
            !db.path.exists(),
            "fault point {stage} published canonical state"
        );
        let wal_path = db.path.with_extension("agdb.wal");
        if stage == "before_recovery_quarantine_rename" {
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert!(quarantined_wal_paths(&db.dir, &wal_bytes).is_empty());
        } else {
            assert!(!wal_path.exists());
            assert_eq!(quarantined_wal_paths(&db.dir, &wal_bytes).len(), 1);
        }
    }
}

#[cfg(unix)]
#[test]
fn recovery_quarantine_killpoints_leave_a_recoverable_boundary() {
    for stage in [
        "before_recovery_quarantine_rename",
        "after_recovery_quarantine_rename",
        "before_recovery_quarantine_dir_fsync",
        "after_recovery_quarantine_dir_fsync",
    ] {
        let db = TempDb::new(stage);
        let request = vector_upsert_for_document(1, "kill-v", "kill-document", [1.0, 0.0], "kill");
        let wal_bytes = write_wal_record(&db.path, 0, &request);
        let digest = digest_hex(&wal_bytes);
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
        let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
        assert_eq!(info["result"]["state"], json!("recoveryPending"));
        native.send_without_read(recovery_discard(2, 0, &digest));
        let status = native.finish();
        #[cfg(unix)]
        assert_eq!(status.signal(), Some(9), "kill point {stage}");

        let wal_path = db.path.with_extension("agdb.wal");
        if stage == "before_recovery_quarantine_rename" {
            assert_eq!(std::fs::read(&wal_path).unwrap(), wal_bytes);
            assert!(quarantined_wal_paths(&db.dir, &wal_bytes).is_empty());
            discard_pending_recovery(&db.path);
            assert!(!wal_path.exists());
            assert_eq!(quarantined_wal_paths(&db.dir, &wal_bytes).len(), 1);
        } else {
            assert!(!wal_path.exists());
            assert_eq!(quarantined_wal_paths(&db.dir, &wal_bytes).len(), 1);
            let mut reopened = NativeProcess::spawn(&db.path, &[]);
            let info = reopened.send(json!({"id":3,"method":"protocol_info","params":{}}));
            assert_eq!(info["result"]["state"], json!("idle"));
            assert_eq!(info["result"]["generation"], json!(0));
            assert_eq!(reopened.finish().code(), Some(0));
        }
        assert!(
            !db.path.exists(),
            "kill point {stage} published canonical state"
        );
    }
}

#[test]
fn legacy_committed_wal_is_skipped_but_future_and_malformed_wal_fail_closed() {
    let skipped = TempDb::new("wal-skipped");
    seed_vector(&skipped.path);
    let mut legacy: Value =
        serde_json::from_slice(&std::fs::read(&skipped.path).expect("read committed canonical"))
            .expect("parse committed canonical");
    legacy
        .as_object_mut()
        .expect("canonical object")
        .remove("commitEvidence");
    std::fs::write(
        &skipped.path,
        serde_json::to_vec(&legacy).expect("encode legacy canonical"),
    )
    .expect("write legacy canonical");
    let already_committed = json!({
        "id": 7,
        "method": "memory_upsert",
        "params": {
            "corpusId": "c1",
            "passages": [{"passageId":"p1", "text":"must-not-replay"}],
            "facts": [{"factId":"f1", "value":"must-not-replay"}],
            "schemas": []
        }
    });
    std::fs::write(
        skipped.path.with_extension("agdb.wal"),
        encoded_wal_record(0, &already_committed),
    )
    .expect("write already-committed WAL");
    let mut native = NativeProcess::spawn(&skipped.path, &[]);
    let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
    assert_eq!(info["ok"], json!(true));
    assert_eq!(info["result"]["generation"], json!(1));
    assert_eq!(info["result"]["state"], json!("idle"));
    assert_eq!(native.finish().code(), Some(0));
    assert_zero_wal(&skipped.path, "legacy committed residue");
    let mut skipped_read = NativeProcess::spawn(&skipped.path, &[]);
    let memory = skipped_read.send(json!({
        "id":2,
        "method":"memory_load",
        "params":{"corpusId":"c1"}
    }));
    assert_eq!(memory["ok"], json!(true));
    assert!(memory["result"]["passages"].as_array().unwrap().is_empty());
    assert!(memory["result"]["facts"].as_array().unwrap().is_empty());
    assert_eq!(skipped_read.finish().code(), Some(0));

    let future = TempDb::new("wal-future");
    seed_vector(&future.path);
    std::fs::write(
        future.path.with_extension("agdb.wal"),
        encoded_wal_record(99, &already_committed),
    )
    .expect("write future WAL");
    let native = NativeProcess::spawn(&future.path, &[]);
    assert_ne!(native.finish().code(), Some(0));
    assert!(future.path.with_extension("agdb.wal").exists());

    let malformed = TempDb::new("wal-malformed");
    seed_vector(&malformed.path);
    std::fs::write(malformed.path.with_extension("agdb.wal"), b"not-json\n")
        .expect("write malformed WAL");
    let native = NativeProcess::spawn(&malformed.path, &[]);
    assert_ne!(native.finish().code(), Some(0));
    assert!(malformed.path.with_extension("agdb.wal").exists());
}

#[test]
fn prepared_commit_is_idempotent_cas_bound_and_exposed_as_one_canonical_fact() {
    let db = TempDb::new("prepared-commit-evidence");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    let begin = native.send(batch_begin(1));
    let nonce = begin["result"]["transactionNonce"]
        .as_str()
        .expect("transaction nonce");
    assert_eq!(nonce.len(), 64);
    assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(
        native.send(vector_upsert(2, [1.0, 0.0], "evidence"))["ok"],
        json!(true)
    );

    let prepared = native.prepare_commit(3);
    assert_eq!(native.prepare_commit(4), prepared, "prepare is idempotent");
    assert_eq!(prepared["transactionNonce"], json!(nonce));
    assert_eq!(prepared["baseGeneration"], json!(0));
    assert_eq!(prepared["generation"], json!(1));
    assert!(prepared["walBytes"].as_u64().unwrap() > 0);
    assert_eq!(prepared["walRecordCount"], json!(1));
    let info = native.send(json!({"id":5,"method":"protocol_info","params":{}}));
    assert_eq!(info["result"]["state"], json!("prepared"));
    assert_eq!(info["result"]["lastCommitEvidence"], Value::Null);

    let mut changed = prepared.clone();
    changed["transactionNonce"] = json!("22".repeat(32));
    let rejected = native.send(batch_commit(6, changed));
    assert_eq!(rejected["ok"], json!(false));
    assert_eq!(generation(&db.path), 0);

    let committed = native.send(batch_commit(7, prepared.clone()));
    assert_eq!(committed["ok"], json!(true));
    let mut expected_commit = prepared;
    expected_commit["schema"] = json!("CommitEvidence@1");
    assert_eq!(committed["result"]["commitEvidence"], expected_commit);
    assert_eq!(native.finish().code(), Some(0));

    let canonical: Value = serde_json::from_slice(&std::fs::read(&db.path).unwrap()).unwrap();
    assert_eq!(canonical["commitEvidence"], expected_commit);
    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let reopened_info = reopened.send(json!({"id":8,"method":"protocol_info","params":{}}));
    assert_eq!(reopened_info["result"]["generation"], json!(1));
    assert_eq!(
        reopened_info["result"]["lastCommitEvidence"],
        expected_commit
    );
    assert_eq!(reopened.finish().code(), Some(0));
}

#[test]
fn negotiated_commit_emits_closed_progress_then_one_terminal_response() {
    let db = TempDb::new("negotiated-progress");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    let protocol = native.send(json!({"id":0,"method":"protocol_info","params":{}}));
    assert_eq!(
        protocol["result"]["progressPolicy"]["schema"],
        json!("NativeProgressPolicy@1")
    );
    assert_eq!(
        protocol["result"]["progressPolicySha256"],
        json!("61ce9d5474d536d42b624706abe3a989f59a1c0887d9d63ca5a4dd69120a2f07")
    );
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(2, [1.0, 0.0], "progress"))["ok"],
        json!(true)
    );
    let evidence = native.prepare_commit(3);
    let (frames, terminal) = native.send_with_progress(json!({
        "id": 4,
        "method": "batch_commit",
        "params": {"preparedCommitEvidence": evidence},
        "progressProtocolVersion": 1
    }));
    assert_eq!(terminal["id"], json!(4));
    assert_eq!(terminal["ok"], json!(true), "terminal response: {terminal}");
    assert!(terminal.get("kind").is_none());
    assert!(terminal.get("schema").is_none());

    let mut unique_phases = Vec::new();
    for (index, frame) in frames.iter().enumerate() {
        assert_eq!(frame["id"], json!(4));
        assert_eq!(frame["protocolVersion"], json!(1));
        assert_eq!(frame["method"], json!("batch_commit"));
        assert_eq!(frame["sequence"], json!((index + 1) as u64));
        assert!(frame.get("ok").is_none());
        assert!(frame.get("result").is_none());
        assert!(frame.get("error").is_none());
        let phase = frame["phase"].as_str().expect("closed progress phase");
        if unique_phases.last().copied() != Some(phase) {
            unique_phases.push(phase);
        }
    }
    assert_eq!(unique_phases, BATCH_COMMIT_PHASES);
    assert_eq!(frames.first().unwrap()["phase"], json!("admitted"));
    assert_eq!(frames.last().unwrap()["phase"], json!("complete"));

    let (status, tail) = native.finish_with_stdout_tail();
    assert_eq!(status.code(), Some(0));
    assert!(
        tail.is_empty(),
        "native emitted output after terminal: {tail}"
    );
}

#[test]
fn omitted_progress_negotiation_preserves_single_response_protocol() {
    let db = TempDb::new("progress-omitted");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(2, [1.0, 0.0], "no-progress"))["ok"],
        json!(true)
    );
    let terminal = native.commit(3);
    assert_eq!(terminal["ok"], json!(true));
    assert!(terminal.get("kind").is_none());
    let (status, tail) = native.finish_with_stdout_tail();
    assert_eq!(status.code(), Some(0));
    assert!(
        tail.is_empty(),
        "non-negotiated request emitted extra frames"
    );
}

#[test]
fn progress_negotiation_is_commit_only_and_exact_version() {
    for (label, request) in [
        (
            "wrong-method",
            json!({"id":1,"method":"ping","params":{},"progressProtocolVersion":1}),
        ),
        (
            "wrong-version",
            json!({"id":1,"method":"batch_commit","params":{},"progressProtocolVersion":2}),
        ),
    ] {
        let db = TempDb::new(label);
        let mut native = NativeProcess::spawn(&db.path, &[]);
        let response = native.send(request);
        assert_eq!(response["ok"], json!(false), "{label}: {response}");
        assert!(response.get("result").is_none());
        assert_ne!(native.finish().code(), Some(0));
        assert!(!db.path.exists());
        assert!(!db.path.with_extension("agdb.wal").exists());
    }
}

#[test]
fn different_transaction_evidence_cannot_publish_the_same_next_generation() {
    let first = TempDb::new("first-same-generation-transaction");
    let second = TempDb::new("second-same-generation-transaction");
    let mut first_native = NativeProcess::spawn(&first.path, &[]);
    let mut second_native = NativeProcess::spawn(&second.path, &[]);

    for native in [&mut first_native, &mut second_native] {
        assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(2, [1.0, 0.0], "same-wal"))["ok"],
            json!(true)
        );
    }
    let first_evidence = first_native.prepare_commit(3);
    let second_evidence = second_native.prepare_commit(3);
    assert_eq!(first_evidence["generation"], json!(1));
    assert_eq!(second_evidence["generation"], json!(1));
    assert_eq!(first_evidence["walSha256"], second_evidence["walSha256"]);
    assert_ne!(
        first_evidence["transactionNonce"],
        second_evidence["transactionNonce"]
    );

    let rejected = second_native.send(batch_commit(4, first_evidence.clone()));
    assert_eq!(rejected["ok"], json!(false));
    assert_eq!(generation(&second.path), 0);
    assert_eq!(
        second_native.send(batch_commit(5, second_evidence))["ok"],
        json!(true)
    );
    assert_eq!(
        first_native.send(batch_commit(4, first_evidence))["ok"],
        json!(true)
    );
    assert_eq!(first_native.finish().code(), Some(0));
    assert_eq!(second_native.finish().code(), Some(0));
}

#[test]
fn prepared_state_rejects_further_mutation_without_publishing() {
    let db = TempDb::new("prepared-rejects-mutation");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(2, [1.0, 0.0], "first"))["ok"],
        json!(true)
    );
    native.prepare_commit(3);
    let rejected = native.send(vector_upsert(4, [0.0, 1.0], "second"));
    assert_eq!(rejected["ok"], json!(false));
    assert_ne!(native.finish().code(), Some(0));
    assert_eq!(generation(&db.path), 0);
    assert!(db.path.with_extension("agdb.wal").exists());
}

#[test]
fn commit_rejects_wal_content_or_identity_change_after_prepare() {
    for replace_inode in [false, true] {
        let db = TempDb::new(if replace_inode {
            "prepared-wal-replaced"
        } else {
            "prepared-wal-appended"
        });
        let mut native = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(2, [1.0, 0.0], "first"))["ok"],
            json!(true)
        );
        let prepared = native.prepare_commit(3);
        let wal_path = db.path.with_extension("agdb.wal");
        if replace_inode {
            let original = db.dir.join("held-original.wal");
            let bytes = std::fs::read(&wal_path).expect("read prepared WAL");
            std::fs::rename(&wal_path, &original).expect("hold original WAL inode");
            std::fs::write(&wal_path, bytes).expect("install same-byte replacement inode");
        } else {
            let mut wal = std::fs::OpenOptions::new()
                .append(true)
                .open(&wal_path)
                .expect("open prepared WAL");
            wal.write_all(b" ").expect("append after prepare");
            wal.sync_all().expect("sync appended WAL");
        }
        let rejected = native.send(batch_commit(4, prepared));
        assert_eq!(rejected["ok"], json!(false));
        assert_ne!(native.finish().code(), Some(0));
        assert_eq!(generation(&db.path), 0);
        assert!(wal_path.exists());
    }
}

#[test]
fn exact_committed_residue_retires_but_mismatched_residue_fails_closed() {
    let exact = TempDb::new("exact-commit-residue");
    seed_vector(&exact.path);
    let exact_record = encoded_wal_record(0, &vector_upsert(1, [1.0, 0.0], "old"));
    std::fs::write(exact.path.with_extension("agdb.wal"), &exact_record)
        .expect("restore exact committed WAL residue");
    let mut reopened = NativeProcess::spawn(&exact.path, &[]);
    let info = reopened.send(json!({"id":1,"method":"protocol_info","params":{}}));
    assert_eq!(info["result"]["state"], json!("idle"));
    assert_eq!(info["result"]["generation"], json!(1));
    assert_eq!(reopened.finish().code(), Some(0));
    assert_zero_wal(&exact.path, "exact committed residue");

    let mismatch = TempDb::new("mismatched-commit-residue");
    seed_vector(&mismatch.path);
    let canonical_before = std::fs::read(&mismatch.path).expect("canonical before mismatch");
    let mismatched_record = encoded_wal_record(
        0,
        &vector_upsert_for_document(9, "other", "other-document", [0.0, 1.0], "other"),
    );
    let wal_path = mismatch.path.with_extension("agdb.wal");
    std::fs::write(&wal_path, &mismatched_record).expect("write mismatched residue");
    let failed = NativeProcess::spawn(&mismatch.path, &[]);
    assert_ne!(failed.finish().code(), Some(0));
    assert_eq!(std::fs::read(&mismatch.path).unwrap(), canonical_before);
    assert_eq!(std::fs::read(&wal_path).unwrap(), mismatched_record);
}

#[test]
fn invalid_canonical_commit_evidence_fails_closed_without_rewrite() {
    let db = TempDb::new("invalid-canonical-evidence");
    seed_vector(&db.path);
    let mut canonical: Value = serde_json::from_slice(&std::fs::read(&db.path).unwrap()).unwrap();
    canonical["commitEvidence"]["generation"] = json!(2);
    let invalid = serde_json::to_vec(&canonical).expect("encode invalid evidence");
    std::fs::write(&db.path, &invalid).expect("write invalid evidence");
    let failed = NativeProcess::spawn(&db.path, &[]);
    assert_ne!(failed.finish().code(), Some(0));
    assert_eq!(std::fs::read(&db.path).unwrap(), invalid);
}

#[test]
fn protocol_info_is_the_method_policy_and_unknown_is_not_a_read() {
    let db = TempDb::new("protocol");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    let info = native.send(json!({"id":1,"method":"protocol_info","params":{}}));
    assert_eq!(
        info["result"]["protocolVersion"],
        json!("native-method-policy@1")
    );
    assert_eq!(info["result"]["generation"], json!(0));
    assert_eq!(info["result"]["state"], json!("idle"));
    assert_eq!(info["result"]["recovery"], Value::Null);
    let methods = info["result"]["methods"]
        .as_array()
        .expect("method inventory");
    let actual: Vec<(String, String, bool)> = methods
        .iter()
        .map(|method| {
            (
                method["name"].as_str().expect("method name").to_string(),
                method["classification"]
                    .as_str()
                    .expect("method classification")
                    .to_string(),
                method["wal"].as_bool().expect("method WAL flag"),
            )
        })
        .collect();
    let expected = vec![
        ("ping", "health", false),
        ("protocol_info", "health", false),
        ("batch_begin", "transaction", false),
        ("batch_prepare_commit", "transaction", false),
        ("batch_commit", "commit", false),
        ("recovery_discard", "recovery", false),
        ("upsert_nodes", "mutation", true),
        ("upsert_edges", "mutation", true),
        ("get_node", "read", false),
        ("get_nodes", "read", false),
        ("get_edges", "read", false),
        ("get_adjacent", "read", false),
        ("delete_nodes", "mutation", true),
        ("delete_edges", "mutation", true),
        ("delete_by_document", "mutation", true),
        ("delete_by_corpus", "mutation", true),
        ("vector_upsert", "mutation", true),
        ("vector_search", "read", false),
        ("vector_delete_by_document", "mutation", true),
        ("memory_upsert", "mutation", true),
        ("memory_save", "mutation", true),
        ("memory_save_file", "mutation", true),
        ("memory_load", "read", false),
        ("memory_save_checkpoint", "mutation", true),
        ("memory_load_checkpoint", "read", false),
        ("memory_validate_integrity", "read", false),
        ("projection_get_transitions", "read", false),
        ("projection_get_dangling_nodes", "read", false),
        ("projection_get_node_count", "read", false),
        ("lexical_index_passages", "mutation", true),
        ("lexical_search", "read", false),
        ("lexical_delete_by_document", "mutation", true),
        ("cypher_query", "read", false),
        ("__debug_force_panic__", "debug", false),
    ]
    .into_iter()
    .map(|(name, classification, wal)| (name.to_string(), classification.to_string(), wal))
    .collect::<Vec<_>>();
    assert_eq!(
        actual, expected,
        "protocol_info is the complete method policy"
    );
    let unknown = native.send(json!({"id":2,"method":"never_a_read","params":{}}));
    assert_eq!(unknown["ok"], json!(false));
    assert_eq!(
        native.send(json!({"id":3,"method":"ping","params":{}}))["ok"],
        json!(true)
    );
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn mutation_requires_explicit_active_batch_and_recovery_commit_requires_new_mutation() {
    let db = TempDb::new("transaction-state");
    let mut outside = NativeProcess::spawn(&db.path, &[]);
    let response = outside.send(vector_upsert(1, [1.0, 0.0], "outside"));
    assert_eq!(response["ok"], json!(false));
    assert_eq!(response["error"]["failureClass"], json!("CLIENT_INPUT"));
    assert_ne!(outside.finish().code(), Some(0));
    assert!(!db.path.exists());
    assert!(!db.path.with_extension("agdb.wal").exists());

    let source = db.dir.join("mid-batch.json");
    std::fs::write(
        &source,
        json!({
            "corpusId": "c1",
            "facts": [{"factId":"f1","value":"before"}],
            "passages": [],
            "schemas": []
        })
        .to_string(),
    )
    .expect("write snapshot source");
    let mut mid_batch = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(mid_batch.send(batch_begin(2))["ok"], json!(true));
    assert_eq!(
        mid_batch.send(memory_save_file(3, &source))["ok"],
        json!(true)
    );
    assert_eq!(mid_batch.commit(4)["ok"], json!(true));
    assert_eq!(mid_batch.finish().code(), Some(0));
    assert_eq!(generation(&db.path), 1);

    let mut bare = NativeProcess::spawn(&db.path, &[]);
    let bare_commit = bare.send(batch_commit_without_evidence(5));
    assert_eq!(bare_commit["ok"], json!(false));
    assert_eq!(bare.send(batch_begin(6))["ok"], json!(true));
    let recovery_commit = bare.send(batch_commit_without_evidence(7));
    assert_eq!(recovery_commit["ok"], json!(false));
    assert_eq!(bare.finish().code(), Some(0));
    assert_eq!(generation(&db.path), 1);
}

#[test]
fn eof_mid_batch_preserves_wal_without_publishing_generation() {
    let db = TempDb::new("eof-mid-batch");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(2, [1.0, 0.0], "uncommitted"))["ok"],
        json!(true)
    );
    assert_eq!(native.finish().code(), Some(0));
    assert!(!db.path.exists());
    let wal_path = db.path.with_extension("agdb.wal");
    let wal_bytes = std::fs::read(&wal_path).expect("EOF preserves WAL");
    let wal_digest = digest_hex(&wal_bytes);

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let info = reopened.send(json!({"id":3,"method":"protocol_info","params":{}}));
    assert_eq!(info["result"]["generation"], json!(0));
    assert_eq!(info["result"]["state"], json!("recoveryPending"));
    assert_eq!(info["result"]["recovery"]["walDigest"], json!(wal_digest));
    let search = reopened.send(json!({
        "id":4,
        "method":"vector_search",
        "params":{"corpusId":"c1","namespace":"default","queryVector":[1.0,0.0],"topK":1}
    }));
    assert_eq!(search["ok"], json!(false));
    assert!(search.get("result").is_none());
    let discard = reopened.send(json!({
        "id":5,
        "method":"recovery_discard",
        "params":{"baseGeneration":0,"walDigest":wal_digest}
    }));
    assert_eq!(discard["ok"], json!(true));
    assert_eq!(reopened.finish().code(), Some(0));
    assert!(!wal_path.exists());
    assert_eq!(quarantined_wal_paths(&db.dir, &wal_bytes).len(), 1);
    assert!(!db.path.exists(), "EOF must not publish a generation");
}

#[test]
fn memory_save_file_wal_is_self_contained_after_source_replacement() {
    let db = TempDb::new("memory-save-file");
    let source = db.dir.join("snapshot.json");
    let original = json!({
        "corpusId": "c1",
        "facts": [{"factId":"f1","value":"original"}],
        "passages": [{"passageId":"p1","text":"original"}],
        "schemas": []
    });
    std::fs::write(&source, original.to_string()).expect("write original source");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(native.send(memory_save_file(2, &source))["ok"], json!(true));
    let wal_before_source_replacement =
        std::fs::read_to_string(db.path.with_extension("agdb.wal")).expect("canonical memory WAL");
    assert!(wal_before_source_replacement.contains("\"method\":\"memory_save\""));
    assert!(!wal_before_source_replacement.contains("memory_save_file"));
    assert!(!wal_before_source_replacement.contains("snapshot.json"));
    std::fs::write(
        &source,
        json!({"corpusId":"c1","facts":[{"factId":"f1","value":"replaced"}]}).to_string(),
    )
    .expect("replace source after acknowledgement");
    std::fs::remove_file(&source).expect("delete source after replacement");
    assert_eq!(native.commit(3)["ok"], json!(true));
    assert_eq!(native.finish().code(), Some(0));

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let snapshot = reopened.send(json!({
        "id":3,
        "method":"memory_load",
        "params":{"corpusId":"c1"}
    }));
    assert_eq!(snapshot["result"], original);
    assert_eq!(reopened.finish().code(), Some(0));
    assert_zero_wal(&db.path, "committed self-contained memory WAL");
}

#[test]
fn mixed_valid_invalid_mutations_fail_closed_before_wal_append() {
    let cases = [
        (
            "nodes",
            json!({
                "method":"upsert_nodes",
                "params":{"nodes":[
                    {"nodeId":"n1","corpusId":"c1","layer":"l","ref":"r","label":"L"},
                    {"nodeId":"broken"}
                ]}
            }),
        ),
        (
            "edges",
            json!({
                "method":"upsert_edges",
                "params":{"edges":[
                    {"edgeId":"e1","corpusId":"c1","sourceNodeId":"n1","targetNodeId":"n2","relation":"r","weight":1.0},
                    {"edgeId":"broken"}
                ]}
            }),
        ),
        (
            "delete_nodes",
            json!({"method":"delete_nodes","params":{"corpusId":"c1","nodeIds":["n1",3]}}),
        ),
        (
            "delete_edges",
            json!({"method":"delete_edges","params":{"corpusId":"c1","edgeIds":["e1",3]}}),
        ),
        (
            "delete_document",
            json!({"method":"delete_by_document","params":{"corpusId":"c1","documentId":3}}),
        ),
        (
            "delete_corpus",
            json!({"method":"delete_by_corpus","params":{"corpusId":3}}),
        ),
        (
            "vectors",
            json!({
                "method":"vector_upsert",
                "params":{"records":[
                    {"id":"v1","corpusId":"c1","namespace":"default","values":[1.0,0.0],"metadata":{}},
                    {"id":"broken"}
                ]}
            }),
        ),
        (
            "vector_delete",
            json!({"method":"vector_delete_by_document","params":{"corpusId":"c1","documentId":3}}),
        ),
        (
            "memory",
            json!({
                "method":"memory_upsert",
                "params":{"corpusId":"c1","facts":[{"factId":"f1"},{}]}
            }),
        ),
        (
            "checkpoint",
            json!({"method":"memory_save_checkpoint","params":{"checkpoint":{}}}),
        ),
        (
            "lexical",
            json!({
                "method":"lexical_index_passages",
                "params":{"corpusId":"c1","passages":[
                    {"passageId":"p1","metadata":{"documentId":"d1"},"text":"ok"},
                    {"passageId":"broken"}
                ]}
            }),
        ),
        (
            "lexical_delete",
            json!({"method":"lexical_delete_by_document","params":{"corpusId":"c1","documentId":3}}),
        ),
    ];
    for (label, request) in cases {
        let db = TempDb::new(label);
        let mut native = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(native.send(batch_begin(1))["ok"], json!(true), "{label}");
        let mut request = request;
        request["id"] = json!(2);
        let response = native.send(request);
        assert_eq!(response["ok"], json!(false), "{label}: {response}");
        assert_ne!(native.finish().code(), Some(0), "{label} must fail closed");
        assert!(!db.path.exists(), "{label} published canonical JSON");
        assert!(
            !db.path.with_extension("agdb.wal").exists(),
            "{label} appended WAL before validation"
        );
    }
}

#[test]
fn mutation_preflight_does_not_clone_the_server_or_dispatch_a_clone() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/aira-graphdb-native.rs");
    let source = std::fs::read_to_string(source_path).expect("native source");
    assert!(
        !source.contains("struct Server: Clone"),
        "Server must not be Clone for mutation preflight"
    );
    assert!(
        !source.contains("server.clone().handle_prepared"),
        "mutation preflight must validate through the live server without cloning it"
    );
}

#[test]
fn commit_path_has_no_output_sized_state_blob_or_json_buffers() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/aira-graphdb-native.rs");
    let source = std::fs::read_to_string(source_path).expect("native source");
    let persist_start = source.find("fn persist(").expect("persist function");
    let persist_end = source[persist_start..]
        .find("fn persist_if_needed")
        .map(|offset| persist_start + offset)
        .expect("persist function boundary");
    let persist = &source[persist_start..persist_end];
    for forbidden in [
        "self.state.clone()",
        "build_vector_blob_payload",
        "serde_json::to_vec(&persisted_state)",
    ] {
        assert!(
            !persist.contains(forbidden),
            "commit path reintroduced output-sized materialization: {forbidden}"
        );
    }
    for required in ["stream_vector_blob_temp", "stream_canonical_temp"] {
        assert!(
            persist.contains(required),
            "commit path bypassed the streaming publication authority: {required}"
        );
    }

    for function in ["fn stream_vector_blob_temp(", "fn stream_canonical_temp("] {
        let start = source
            .find(function)
            .expect("streaming publication function");
        let body = &source[start..];
        let end = body[function.len()..]
            .find("\n    fn ")
            .map(|offset| function.len() + offset)
            .unwrap_or(body.len());
        let stream = &body[..end];
        assert!(
            stream.contains("bounded_publication_writer(&mut file"),
            "{function} bypassed the bounded publication cache writer"
        );
        assert!(
            stream.contains("buffered_artifact_writer(bounded)"),
            "{function} bypassed the bounded canonical buffering/hash writer"
        );
    }
}

#[test]
fn prepare_and_commit_use_rolling_and_streamed_wal_evidence() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/aira-graphdb-native.rs");
    let source = std::fs::read_to_string(source_path).expect("native source");
    let prepare_start = source.find("fn prepare_commit(").expect("prepare function");
    let prepare_end = source[prepare_start..]
        .find("fn persist(")
        .map(|offset| prepare_start + offset)
        .expect("prepare boundary");
    let prepare = &source[prepare_start..prepare_end];
    assert!(prepare.contains("self.wal_hasher.clone().finalize()"));
    for forbidden in [
        "scan_wal_",
        "read_to_end",
        "Vec<WalRecord>",
        "serde_json::from_",
    ] {
        assert!(
            !prepare.contains(forbidden),
            "prepare regained WAL materialization via {forbidden}"
        );
    }

    let append_start = source.find("fn wal_append(").expect("append function");
    let append_end = source[append_start..]
        .find("fn replay_wal(")
        .map(|offset| append_start + offset)
        .expect("append boundary");
    let append = &source[append_start..append_end];
    assert!(append.contains("stream_wal_record"));
    assert!(append.contains("BufWriter::with_capacity"));
    assert!(append.contains("self.wal_file.take()"));
    assert!(append.contains("self.wal_file = Some(file)"));
    assert!(!append.contains("serde_json::to_vec"));

    let persist_start = source.find("fn persist(").expect("persist function");
    let persist_end = source[persist_start..]
        .find("fn persist_if_needed")
        .map(|offset| persist_start + offset)
        .expect("persist boundary");
    let persist = &source[persist_start..persist_end];
    assert!(persist.contains("scan_wal_with_identity_progress(false, progress)"));
    assert!(!persist.contains("Vec<WalRecord>"));
    assert!(!persist.contains("wal_raw"));
}

#[test]
fn negotiated_progress_observes_every_output_sized_commit_stream() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/aira-graphdb-native.rs");
    let source = std::fs::read_to_string(source_path).expect("native source");
    for required in [
        "scan_wal_with_identity_progress(false, progress)",
        "stream_vector_blob_temp(",
        "validate_blob_streaming_progress(",
        "stream_canonical_temp(",
        "hash_wal_file_progress(&mut held_wal, progress)",
    ] {
        assert!(
            source.contains(required),
            "negotiated progress lost output-sized stream observation: {required}"
        );
    }
}

#[test]
fn wal_retirement_zeros_and_syncs_the_held_descriptor_without_path_replacement() {
    let source_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/bin/aira-graphdb-native.rs");
    let source = std::fs::read_to_string(source_path).expect("native source");
    let retire_start = source
        .find("fn retire_wal_exact(")
        .expect("WAL retirement function");
    let retire_end = source[retire_start..]
        .find("fn digest_bytes(")
        .map(|offset| retire_start + offset)
        .expect("WAL retirement boundary");
    let retire = &source[retire_start..retire_end];

    for required in [
        "held_wal.set_len(0)",
        "held_wal.sync_all()",
        "self.wal_file = Some(held_wal)",
    ] {
        assert!(
            retire.contains(required),
            "held-descriptor retirement lost {required}"
        );
    }
    for forbidden in ["rename_noreplace", "remove_file", "sync_parent_dir"] {
        assert!(
            !retire.contains(forbidden),
            "WAL retirement regained pathname mutation via {forbidden}"
        );
    }
}

#[cfg(target_os = "linux")]
#[test]
fn repeated_small_mutations_have_delta_bounded_peak_rss_after_representative_state() {
    const REPRESENTATIVE_VECTOR_COUNT: usize = 80_000;
    const BASELINE_READS: usize = 32;
    const SMALL_MUTATIONS: usize = 32;

    let db = TempDb::new("rss-delta-bound");
    let mut seed = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(seed.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        seed.send(bulk_vector_upsert(1, REPRESENTATIVE_VECTOR_COUNT))["ok"],
        json!(true)
    );
    assert_eq!(seed.commit(2)["ok"], json!(true));
    assert_eq!(seed.finish().code(), Some(0));

    let state_bytes = std::fs::metadata(&db.path)
        .expect("representative canonical state")
        .len()
        + std::fs::read(&db.path)
            .ok()
            .and_then(|raw| serde_json::from_slice::<Value>(&raw).ok())
            .and_then(|state| {
                state["vectorBlob"]["basename"]
                    .as_str()
                    .map(|name| db.dir.join(name))
            })
            .and_then(|blob| std::fs::metadata(blob).ok().map(|metadata| metadata.len()))
            .unwrap_or(0);
    assert!(
        state_bytes > 8 * 1024 * 1024,
        "representative state was too small for an ownership-boundary RSS gate: {state_bytes}"
    );

    let mut native = NativeProcess::spawn(&db.path, &[]);
    let mut baseline_peak = native.rss_bytes();
    for id in 0..BASELINE_READS {
        let response = native.send(vector_search(10 + id as u64, [1.0, 0.0]));
        assert_eq!(response["ok"], json!(true));
        baseline_peak = baseline_peak.max(native.rss_bytes());
    }

    assert_eq!(native.send(batch_begin(100))["ok"], json!(true));
    let mut mutation_peak = baseline_peak.max(native.rss_bytes());
    for index in 0..SMALL_MUTATIONS {
        let (response, _, request_peak) = native.send_with_peak_rss(vector_upsert_for_document(
            200 + index as u64,
            &format!("bulk-v-{index}"),
            &format!("bulk-document-{index}"),
            [0.0, 1.0],
            "delta",
        ));
        assert_eq!(response["ok"], json!(true), "small mutation {index}");
        mutation_peak = mutation_peak.max(request_peak);
    }
    let evidence = native.prepare_commit(500);
    let (commit, _, commit_peak) = native.send_with_peak_rss(batch_commit(500, evidence));
    assert_eq!(
        commit["ok"],
        json!(true),
        "streaming commit failed: {commit}"
    );
    assert_eq!(commit["result"]["generation"], json!(2));
    mutation_peak = mutation_peak.max(commit_peak);
    assert_eq!(native.finish().code(), Some(0));

    let growth = mutation_peak.saturating_sub(baseline_peak);
    let sample_delta = serde_json::to_vec(&vector_upsert_for_document(
        0,
        "delta-v-sample",
        "delta-document-sample",
        [0.0, 1.0],
        "delta",
    ))
    .expect("serialize representative mutation")
    .len() as u64;
    let delta_budget = 16 * 1024 * 1024 + sample_delta * SMALL_MUTATIONS as u64 * 16;
    assert!(
        growth <= delta_budget,
        "peak RSS grew with representative state rather than request delta: stateBytes={state_bytes} baselinePeak={baseline_peak} mutationPeak={mutation_peak} growth={growth} deltaBudget={delta_budget}"
    );
}
