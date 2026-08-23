use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
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
