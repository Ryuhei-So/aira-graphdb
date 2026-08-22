use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

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
        command
            .arg("--db")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
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

fn seed_vector(path: &Path) {
    let mut native = NativeProcess::spawn(path, &[]);
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
fn wal_sync_failure_returns_failure_and_stops_request_service() {
    let db = TempDb::new("wal-failure");
    let mut native = NativeProcess::spawn(&db.path, &[("AGDB_NATIVE_FAIL_POINT", "wal_sync")]);
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
