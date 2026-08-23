use std::io::{BufRead, BufReader, Write};
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
use std::os::unix::fs::symlink;
#[cfg(unix)]
use std::os::unix::process::ExitStatusExt;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};

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

    fn finish(mut self) -> ExitStatus {
        self.stdin.take();
        self.child.wait().expect("native exits")
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

fn batch_commit(id: u64) -> Value {
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

fn seed_vector(path: &Path) {
    let mut native = NativeProcess::spawn(path, &[]);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, [1.0, 0.0], "old"))["ok"],
        json!(true)
    );
    let commit = native.send(batch_commit(2));
    assert_eq!(commit["ok"], json!(true));
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
    let commit = native.send(batch_commit(2));
    assert_eq!(commit["result"]["generation"], json!(1));
    let descriptor = &commit["result"]["vectorBlob"];
    let basename = descriptor["basename"].as_str().expect("blob basename");
    assert!(!Path::new(basename).is_absolute());
    assert!(!basename.contains('/'));
    assert_eq!(descriptor["format"], json!(1));
    assert_eq!(native.finish().code(), Some(0));

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
    probe_vector_pair(&db.path);
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
        "after_wal_retire",
        "after_final_dir_fsync",
    ] {
        let db = TempDb::new(stage);
        seed_vector(&db.path);
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_KILL_POINT", stage)]);
        assert_eq!(native.send(batch_begin(2))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(3, [0.0, 1.0], "new"))["ok"],
            json!(true)
        );
        native.send_without_read(batch_commit(4));
        assert_native_killed(native.finish(), stage);
        if stage == "after_json_dir_fsync" {
            assert!(
                db.path.with_extension("agdb.wal").exists(),
                "base-generation WAL must remain after JSON publication"
            );
        }
        probe_vector_pair(&db.path);
        if stage == "after_json_dir_fsync" {
            assert!(
                !db.path.with_extension("agdb.wal").exists(),
                "reopen must retire WAL already included by JSON"
            );
        }
    }
}

#[test]
fn injected_write_sync_rename_and_directory_failures_never_return_a_token() {
    for stage in [
        "blob_temp_sync_create",
        "blob_temp_sync_write",
        "blob_temp_sync_fsync",
        "blob_rename",
        "blob_dir_fsync",
        "json_temp_sync_create",
        "json_temp_sync_write",
        "json_temp_sync_fsync",
        "json_rename",
        "json_dir_fsync",
        "wal_retire",
        "final_dir_fsync",
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
            let commit = native.send(batch_commit(7));
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
fn empty_wal_after_before_write_failure_is_retired_before_fresh_commit() {
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
        assert!(!wal_path.exists(), "{label} restart must retire empty WAL");
        assert_eq!(recovery.finish().code(), Some(0));

        let mut fresh = NativeProcess::spawn(&db.path, &[]);
        assert_eq!(fresh.send(batch_begin(4))["ok"], json!(true));
        assert_eq!(fresh.send(mutation)["ok"], json!(true));
        let commit = fresh.send(batch_commit(5));
        assert_eq!(commit["ok"], json!(true), "{label} fresh commit: {commit}");
        assert_eq!(commit["result"]["generation"], json!(1));
        assert_eq!(fresh.finish().code(), Some(0));
        assert!(
            !wal_path.exists(),
            "{label} successful commit must retire WAL"
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
            let length = std::fs::metadata(&wal_path)
                .expect("existing WAL after first mutation")
                .len();
            (
                length,
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
        let move_original = original_path.clone();
        let move_replacement = replacement_path.clone();
        let swapper = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
            loop {
                let ready = std::fs::metadata(&swap_wal)
                    .map(|metadata| metadata.len() > baseline_len)
                    .unwrap_or(false);
                if ready {
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
    let commit = unrelated.send(batch_commit(108));
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
    let (mut native, original, digest) = open_recovery_pending_with_env(
        &db.path,
        &request,
        &[
            (
                "AGDB_NATIVE_TEST_PAUSE_POINT",
                "before_recovery_quarantine_rename",
            ),
            ("AGDB_NATIVE_TEST_PAUSE_MS", "250"),
        ],
    );
    let appended_record = encoded_wal_record(
        0,
        &vector_upsert_for_document(2, "race-v2", "race-document-2", [0.0, 1.0], "race-append"),
    );
    let wal_path = db.path.with_extension("agdb.wal");
    let append_path = wal_path.clone();
    let append_bytes = appended_record.clone();
    let appender = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
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
fn committed_wal_is_skipped_but_future_and_malformed_wal_fail_closed() {
    let skipped = TempDb::new("wal-skipped");
    seed_vector(&skipped.path);
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
    assert!(!skipped.path.with_extension("agdb.wal").exists());
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
    assert_eq!(mid_batch.send(batch_commit(4))["ok"], json!(true));
    assert_eq!(mid_batch.finish().code(), Some(0));
    assert_eq!(generation(&db.path), 1);

    let mut bare = NativeProcess::spawn(&db.path, &[]);
    let bare_commit = bare.send(batch_commit(5));
    assert_eq!(bare_commit["ok"], json!(false));
    assert_eq!(bare.send(batch_begin(6))["ok"], json!(true));
    let recovery_commit = bare.send(batch_commit(7));
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
    assert_eq!(native.send(batch_commit(3))["ok"], json!(true));
    assert_eq!(native.finish().code(), Some(0));

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let snapshot = reopened.send(json!({
        "id":3,
        "method":"memory_load",
        "params":{"corpusId":"c1"}
    }));
    assert_eq!(snapshot["result"], original);
    assert_eq!(reopened.finish().code(), Some(0));
    assert!(!db.path.with_extension("agdb.wal").exists());
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
    assert_eq!(seed.send(batch_commit(2))["ok"], json!(true));
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
