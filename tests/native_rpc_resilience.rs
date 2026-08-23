use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

struct NativeProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    db_path: PathBuf,
}

impl NativeProcess {
    fn spawn() -> Self {
        Self::spawn_with_env(&[])
    }

    fn spawn_with_env(envs: &[(&str, &str)]) -> Self {
        let bin = Self::native_sidecar_bin_path();
        let db_path = temp_db_path();
        let mut command = Command::new(bin);
        command
            .arg("--db")
            .arg(&db_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in envs {
            command.env(k, v);
        }
        let mut child = command.spawn().expect("native sidecar should start");
        let stdin = child.stdin.take().expect("stdin pipe");
        let stdout = BufReader::new(child.stdout.take().expect("stdout pipe"));
        Self {
            child,
            stdin,
            stdout,
            db_path,
        }
    }

    fn native_sidecar_bin_path() -> String {
        if let Ok(bin) = std::env::var("CARGO_BIN_EXE_aira-graphdb-native") {
            return bin;
        }
        let exe = std::env::current_exe().expect("current_exe");
        let debug_dir = exe
            .parent()
            .and_then(|p| p.parent())
            .expect("test binary must be in target/<profile>/deps");
        let candidate = debug_dir.join("aira-graphdb-native");
        candidate
            .to_str()
            .expect("binary path must be valid utf-8")
            .to_string()
    }

    fn send_raw(&mut self, line: &str) -> Value {
        self.stdin
            .write_all(line.as_bytes())
            .expect("write request line");
        self.stdin.write_all(b"\n").expect("write newline");
        self.stdin.flush().expect("flush request");
        let mut response = String::new();
        self.stdout.read_line(&mut response).expect("read response");
        serde_json::from_str(response.trim()).expect("response must be valid json")
    }

    fn send_json(&mut self, request: Value) -> Value {
        self.send_raw(&request.to_string())
    }

    fn ensure_alive(&mut self) {
        assert!(self.child.try_wait().expect("query child status").is_none());
    }

    fn wait_for_exit_code(&mut self, timeout: Duration) -> Option<i32> {
        let start = Instant::now();
        loop {
            match self.child.try_wait().expect("query child status") {
                Some(status) => return status.code(),
                None => {
                    if start.elapsed() >= timeout {
                        return None;
                    }
                    thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    fn audit_events(&self) -> Vec<Value> {
        let path = self.db_path.with_extension("native-audit.log");
        let Ok(raw) = std::fs::read_to_string(path) else {
            return Vec::new();
        };
        raw.lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .collect()
    }
}

impl Drop for NativeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.db_path);
        let _ = std::fs::remove_file(self.db_path.with_extension("native-audit.log"));
    }
}

fn temp_db_path() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("agdb-native-rpc-{nanos}.json"))
}

#[test]
fn invalid_json_returns_contract_error_and_process_survives() {
    let mut proc = NativeProcess::spawn();
    let invalid = proc.send_raw("{\"id\":1,\"method\":\"ping\"");
    assert_eq!(invalid["ok"], json!(false));
    assert_eq!(invalid["error"]["code"], json!("INVALID_REQUEST_JSON"));
    assert_eq!(invalid["error"]["failureClass"], json!("CLIENT_INPUT"));

    let ping = proc.send_json(json!({
        "id": 2,
        "method": "ping",
        "params": {}
    }));
    assert_eq!(ping["ok"], json!(true));
    assert_eq!(ping["result"]["pong"], json!(true));
    proc.ensure_alive();
    let events = proc.audit_events();
    assert!(events.iter().any(|event| {
        event.get("errorCode") == Some(&json!("INVALID_REQUEST_JSON"))
            && event.get("failureClass") == Some(&json!("CLIENT_INPUT"))
            && event.get("requestId").is_some()
            && event.get("timestamp").is_some()
    }));
}

#[test]
fn unsupported_method_returns_client_input_failure_class() {
    let mut proc = NativeProcess::spawn();
    let response = proc.send_json(json!({
        "id": 3,
        "method": "unknown_method",
        "params": {}
    }));
    assert_eq!(response["ok"], json!(false));
    assert_eq!(response["error"]["code"], json!("UNSUPPORTED_FEATURE"));
    assert_eq!(response["error"]["failureClass"], json!("CLIENT_INPUT"));
    proc.ensure_alive();
    let events = proc.audit_events();
    assert!(events.iter().any(|event| {
        event.get("errorCode") == Some(&json!("UNSUPPORTED_FEATURE"))
            && event.get("failureClass") == Some(&json!("CLIENT_INPUT"))
            && event.get("requestId").is_some()
            && event.get("timestamp").is_some()
    }));
}

#[test]
fn invalid_mutation_payload_fails_closed_without_publishing_a_prefix() {
    let mut proc = NativeProcess::spawn();
    let begin = proc.send_json(json!({
        "id": 4,
        "method": "batch_begin",
        "params": {}
    }));
    assert_eq!(begin["ok"], json!(true));
    let response = proc.send_json(json!({
        "id": 5,
        "method": "upsert_nodes",
        "params": {
            "nodes": [{ "nodeId": 10 }]
        }
    }));
    assert_eq!(response["ok"], json!(false));
    assert_eq!(response["error"]["code"], json!("REQUEST_EXECUTION_FAILED"));
    assert_eq!(response["error"]["failureClass"], json!("CLIENT_INPUT"));
    assert_ne!(
        proc.wait_for_exit_code(Duration::from_secs(3)),
        Some(0),
        "a mutator validation error must stop the native writer"
    );
    let events = proc.audit_events();
    assert!(events.iter().any(|event| {
        event.get("errorCode") == Some(&json!("REQUEST_EXECUTION_FAILED"))
            && event.get("failureClass") == Some(&json!("CLIENT_INPUT"))
            && event.get("requestId").is_some()
            && event.get("timestamp").is_some()
    }));
}

#[test]
fn panic_is_auto_logged_as_process_crash_with_cause() {
    let mut proc = NativeProcess::spawn_with_env(&[("AGDB_ENABLE_TEST_CRASH", "1")]);
    proc.stdin
        .write_all(br#"{"id":999,"method":"__debug_force_panic__","params":{}}"#)
        .expect("write request line");
    proc.stdin.write_all(b"\n").expect("write newline");
    proc.stdin.flush().expect("flush request");

    let exit_code = proc
        .wait_for_exit_code(Duration::from_secs(3))
        .expect("process should exit after forced panic");
    assert_ne!(exit_code, 0);

    let events = proc.audit_events();
    let crash = events
        .iter()
        .find(|event| event.get("errorCode") == Some(&json!("PROCESS_CRASH")))
        .expect("PROCESS_CRASH event should be recorded");
    assert_eq!(crash.get("lastRequestId"), Some(&json!("999")));
    assert!(crash.get("processExitCode").is_some());
    assert!(crash.get("uptimeSec").is_some());
    assert!(crash.get("timestamp").is_some());
    assert!(crash.get("cause").is_some());
}
