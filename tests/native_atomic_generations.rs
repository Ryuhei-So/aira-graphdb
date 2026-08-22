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
    json!({
        "id": id,
        "method": "vector_upsert",
        "params": {
            "records": [{
                "id": "v1",
                "corpusId": "c1",
                "namespace": "default",
                "values": values,
                "metadata": {"documentId": "d1", "generation": generation}
            }]
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
        "blob_temp_sync",
        "blob_rename",
        "blob_dir_fsync",
        "json_temp_sync",
        "json_rename",
        "json_dir_fsync",
        "wal_retire",
        "final_dir_fsync",
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
        if stage == "json_dir_fsync" {
            assert!(
                db.path.with_extension("agdb.wal").exists(),
                "base-generation WAL must remain after JSON publication"
            );
        }
        probe_vector_pair(&db.path);
        if stage == "json_dir_fsync" {
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
        "blob_temp_sync_write",
        "blob_temp_sync_fsync",
        "blob_rename",
        "blob_dir_fsync",
        "json_temp_sync_write",
        "json_temp_sync_fsync",
        "json_rename",
        "json_dir_fsync",
        "wal_retire",
        "final_dir_fsync",
    ] {
        let db = TempDb::new(stage);
        seed_vector(&db.path);
        let before = std::fs::read(&db.path).expect("base canonical JSON");
        let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", stage)]);
        assert_eq!(native.send(batch_begin(5))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(6, [0.0, 1.0], "new"))["ok"],
            json!(true)
        );
        let commit = native.send(batch_commit(7));
        assert_eq!(commit["ok"], json!(false), "failpoint {stage}");
        assert!(
            commit.get("result").is_none(),
            "failpoint {stage} returned token"
        );
        assert_ne!(native.finish().code(), Some(0));

        let canonical_after = std::fs::read(&db.path).expect("canonical JSON remains readable");
        let after_generation = generation(&db.path);
        assert!(after_generation == 1 || after_generation == 2);
        if after_generation == 1 {
            assert_eq!(canonical_after, before, "failpoint {stage} advanced JSON");
        }
        probe_vector_pair(&db.path);
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
    let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", "wal_sync")]);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    let response = native.send(vector_upsert(1, [1.0, 0.0], "new"));
    assert_eq!(response["ok"], json!(false));
    assert_eq!(response["error"]["failureClass"], json!("IO_FAILURE"));
    assert_ne!(native.finish().code(), Some(0));
}

#[test]
fn wal_base_generation_replays_once_skips_committed_and_rejects_future() {
    let db = TempDb::new("wal-replay");
    seed_vector(&db.path);
    let request = json!({
        "id": 7,
        "method": "memory_upsert",
        "params": {
            "corpusId": "c1",
            "passages": [{"passageId":"p1", "text":"one"}],
            "facts": [{"factId":"f1", "value":"one"}],
            "schemas": [{"schemaId":"s1", "name":"one"}]
        }
    });
    let record = json!({"version":2,"baseGeneration":1,"request":request});
    std::fs::write(
        db.path.with_extension("agdb.wal"),
        format!("{}\n{}\n", record, record),
    )
    .expect("write generation-bound WAL");
    let mut native = NativeProcess::spawn(&db.path, &[]);
    let snapshot = native.send(json!({
        "id": 8,
        "method": "memory_load",
        "params": {"corpusId":"c1"}
    }));
    assert_eq!(snapshot["result"]["passages"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["result"]["facts"].as_array().unwrap().len(), 1);
    assert_eq!(snapshot["result"]["schemas"].as_array().unwrap().len(), 1);
    assert_eq!(native.finish().code(), Some(0));
    assert!(db.path.with_extension("agdb.wal").exists());

    let mut recovery = NativeProcess::spawn(&db.path, &[]);
    assert_eq!(recovery.send(batch_begin(9))["ok"], json!(true));
    assert_eq!(recovery.send(request.clone())["ok"], json!(true));
    assert_eq!(recovery.send(batch_commit(10))["ok"], json!(true));
    assert_eq!(recovery.finish().code(), Some(0));
    assert!(!db.path.with_extension("agdb.wal").exists());

    let future = TempDb::new("wal-future");
    seed_vector(&future.path);
    let future_record = json!({"version":2,"baseGeneration":99,"request":request});
    std::fs::write(
        future.path.with_extension("agdb.wal"),
        format!("{}\n", future_record),
    )
    .expect("write future WAL");
    let native = NativeProcess::spawn(&future.path, &[]);
    assert_ne!(native.finish().code(), Some(0));

    let malformed = TempDb::new("wal-malformed");
    seed_vector(&malformed.path);
    std::fs::write(malformed.path.with_extension("agdb.wal"), b"not-json\n")
        .expect("write malformed WAL");
    let native = NativeProcess::spawn(&malformed.path, &[]);
    assert_ne!(native.finish().code(), Some(0));
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
    let methods = info["result"]["methods"]
        .as_array()
        .expect("method inventory");
    let vector_upsert = methods
        .iter()
        .find(|method| method["name"] == json!("vector_upsert"))
        .expect("vector_upsert policy");
    assert_eq!(vector_upsert["classification"], json!("mutation"));
    assert_eq!(vector_upsert["wal"], json!(true));
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
    assert!(db.path.with_extension("agdb.wal").exists());

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let info = reopened.send(json!({"id":3,"method":"protocol_info","params":{}}));
    assert_eq!(info["result"]["generation"], json!(0));
    let search = reopened.send(json!({
        "id":4,
        "method":"vector_search",
        "params":{"corpusId":"c1","namespace":"default","queryVector":[1.0,0.0],"topK":1}
    }));
    assert_eq!(
        search["result"][0]["metadata"]["generation"],
        json!("uncommitted")
    );
    assert_eq!(reopened.finish().code(), Some(0));
    assert!(db.path.with_extension("agdb.wal").exists());
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
    std::fs::write(
        &source,
        json!({"corpusId":"c1","facts":[{"factId":"f1","value":"replaced"}]}).to_string(),
    )
    .expect("replace source after acknowledgement");
    std::fs::remove_file(&source).expect("delete source after replacement");
    assert_eq!(native.finish().code(), Some(0));

    let mut reopened = NativeProcess::spawn(&db.path, &[]);
    let snapshot = reopened.send(json!({
        "id":3,
        "method":"memory_load",
        "params":{"corpusId":"c1"}
    }));
    assert_eq!(snapshot["result"], original);
    assert_eq!(reopened.finish().code(), Some(0));

    let wal =
        std::fs::read_to_string(db.path.with_extension("agdb.wal")).expect("canonical memory WAL");
    assert!(wal.contains("\"method\":\"memory_save\""));
    assert!(!wal.contains("memory_save_file"));
    assert!(!wal.contains("snapshot.json"));
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
