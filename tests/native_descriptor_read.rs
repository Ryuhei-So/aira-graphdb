#![cfg(unix)]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("aira-graphdb-descriptor-{label}-{nonce}"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn fixture() -> (PathBuf, PathBuf, String) {
    let canonical_path = temp_path("canonical.json");
    let blob_path = temp_path("vectors.vblob");
    let mut blob = b"AGVB".to_vec();
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.extend_from_slice(&1.0f64.to_le_bytes());
    blob.extend_from_slice(&0.0f64.to_le_bytes());
    let blob_sha256 = sha256_hex(&blob);
    let canonical = json!({
        "nodes": {},
        "edges": {},
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
        "checkpoints": {},
        "generation": 1,
        "vectorBlob": {
            "basename": "vectors.vblob",
            "size": blob.len(),
            "sha256": blob_sha256,
            "format": 1
        }
    });
    fs::write(&canonical_path, serde_json::to_vec(&canonical).unwrap()).unwrap();
    fs::write(&blob_path, blob).unwrap();
    (canonical_path, blob_path, blob_sha256)
}

fn legacy_fixture() -> (PathBuf, PathBuf) {
    let canonical_path = temp_path("legacy-canonical.json");
    let blob_path = temp_path("legacy-vectors.vblob");
    let mut blob = b"AGVB".to_vec();
    blob.extend_from_slice(&1u16.to_le_bytes());
    blob.extend_from_slice(&1.0f64.to_le_bytes());
    blob.extend_from_slice(&0.0f64.to_le_bytes());
    let canonical = json!({
        "nodes": {}, "edges": {}, "vectors": {}, "passages": {},
        "snapshots": {}, "checkpoints": {}, "generation": 0
    });
    fs::write(&canonical_path, serde_json::to_vec(&canonical).unwrap()).unwrap();
    fs::write(&blob_path, blob).unwrap();
    (canonical_path, blob_path)
}

fn spawn_descriptor(canonical: &Path, blob: &Path) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    spawn_descriptor_mode(canonical, blob, false)
}

fn spawn_descriptor_mode(
    canonical: &Path,
    blob: &Path,
    legacy_generation0: bool,
) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let canonical_file = OpenOptions::new().read(true).open(canonical).unwrap();
    let blob_file = OpenOptions::new().read(true).open(blob).unwrap();
    let canonical_fd = canonical_file.as_raw_fd();
    let blob_fd = blob_file.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"));
    let mut args = vec![
        "--descriptor-read".to_string(),
        "--canonical-fd".to_string(),
        canonical_fd.to_string(),
        "--vector-blob-fd".to_string(),
        blob_fd.to_string(),
        "--expected-generation".to_string(),
        if legacy_generation0 { "0" } else { "1" }.to_string(),
    ];
    if legacy_generation0 {
        args.push("--legacy-generation0".to_string());
        args.push("--legacy-binding-hash".to_string());
        args.push("ab".repeat(32));
    }
    command
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    unsafe {
        command.pre_exec(move || {
            for fd in [canonical_fd, blob_fd] {
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags < 0 || libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    let mut child = command.spawn().expect("descriptor native starts");
    drop(canonical_file);
    drop(blob_file);
    let stdin = child.stdin.take().expect("native stdin");
    let stdout = BufReader::new(child.stdout.take().expect("native stdout"));
    (child, stdin, stdout)
}

fn send(stdin: &mut ChildStdin, stdout: &mut BufReader<ChildStdout>, request: Value) -> Value {
    writeln!(stdin, "{}", request).unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

fn send_padded_ping(
    stdin: &mut ChildStdin,
    stdout: &mut BufReader<ChildStdout>,
    id: u64,
    frame_bytes_including_newline: usize,
) -> Value {
    let request = json!({"id": id, "method": "ping", "params": {}}).to_string();
    let padding = frame_bytes_including_newline
        .checked_sub(request.len() + 1)
        .expect("requested frame can contain ping JSON");
    stdin.write_all(&vec![b' '; padding]).unwrap();
    stdin.write_all(request.as_bytes()).unwrap();
    stdin.write_all(b"\n").unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    serde_json::from_str(line.trim()).unwrap()
}

fn remove_fixture(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_file(path.with_extension("native-audit.log"));
    let _ = fs::remove_file(path.with_extension("agdb.wal"));
}

#[test]
fn real_native_descriptor_mode_handshakes_rejects_mutation_and_eof_writes_nothing() {
    let (canonical, blob, blob_sha256) = fixture();
    let sidecars = [
        canonical.with_extension("native-audit.log"),
        canonical.with_extension("agdb.wal"),
    ];
    let (mut child, mut stdin, mut stdout) = spawn_descriptor(&canonical, &blob);
    let info = send(
        &mut stdin,
        &mut stdout,
        json!({"id": 1, "method": "protocol_info", "params": {}}),
    );
    assert_eq!(info["ok"], true);
    assert_eq!(info["result"]["accessMode"], "descriptor-read-only");
    assert_eq!(info["result"]["generation"], 1);
    assert_eq!(info["result"]["vectorBlobSha256"], blob_sha256);
    assert_eq!(
        info["result"]["methodInventorySha256"],
        "126080ca61644282fd7ed09b10d5fd0571eba49c6dc19132d4cc6168d07eaf1f"
    );
    let inventory = info["result"]["methods"].as_array().unwrap();
    assert_eq!(
        inventory
            .iter()
            .map(|method| method["name"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["ping", "protocol_info"]
    );
    let mutation = send(
        &mut stdin,
        &mut stdout,
        json!({"id": 2, "method": "batch_begin", "params": {}}),
    );
    assert_eq!(mutation["ok"], false);
    assert_eq!(mutation["error"]["code"], "DESCRIPTOR_READ_ONLY_METHOD");
    let legacy_read = send(
        &mut stdin,
        &mut stdout,
        json!({"id": 3, "method": "memory_load", "params": {}}),
    );
    assert_eq!(legacy_read["ok"], false);
    assert_eq!(legacy_read["error"]["code"], "DESCRIPTOR_READ_ONLY_METHOD");
    drop(stdin);
    assert!(child.wait().unwrap().success());
    assert!(sidecars.iter().all(|path| !path.exists()));
    remove_fixture(&canonical);
    let _ = fs::remove_file(blob);
}

#[test]
fn real_native_descriptor_mode_rejects_missing_fds_before_protocol() {
    let output = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"))
        .args([
            "--descriptor-read",
            "--canonical-fd",
            "99",
            "--vector-blob-fd",
            "100",
            "--expected-generation",
            "1",
        ])
        .output()
        .expect("native executable runs");
    assert!(!output.status.success());
    let confused = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"))
        .args(["--descriptor-read=true", "--db", "ignored.json"])
        .output()
        .expect("native executable runs");
    assert!(!confused.status.success());
}

#[test]
fn real_native_descriptor_protocol_rejects_frame_cumulative_and_count_overflow() {
    const FRAME_LIMIT: usize = 2 * 1024 * 1024;

    let (canonical, blob, _) = fixture();
    let (mut child, mut stdin, mut stdout) = spawn_descriptor(&canonical, &blob);
    for id in 0..32 {
        let response = send_padded_ping(&mut stdin, &mut stdout, id, FRAME_LIMIT);
        assert_eq!(response["ok"], true, "exact input boundary frame {id}");
    }
    stdin.write_all(b"\n").unwrap();
    stdin.flush().unwrap();
    drop(stdin);
    assert!(!child.wait().unwrap().success(), "64MiB + 1 must fail");
    remove_fixture(&canonical);
    let _ = fs::remove_file(&blob);

    let (canonical, blob, _) = fixture();
    let (mut child, mut stdin, _stdout) = spawn_descriptor(&canonical, &blob);
    stdin.write_all(&vec![b'x'; FRAME_LIMIT + 1]).unwrap();
    stdin.write_all(b"\n").unwrap();
    drop(stdin);
    assert!(!child.wait().unwrap().success());
    remove_fixture(&canonical);
    let _ = fs::remove_file(&blob);

    let (canonical, blob, _) = fixture();
    let (mut child, mut stdin, mut stdout) = spawn_descriptor(&canonical, &blob);
    for id in 0..4096 {
        let response = send(
            &mut stdin,
            &mut stdout,
            json!({"id": id, "method": "ping", "params": {}}),
        );
        assert_eq!(response["ok"], true);
    }
    writeln!(
        stdin,
        "{}",
        json!({"id": 4096, "method": "ping", "params": {}})
    )
    .unwrap();
    stdin.flush().unwrap();
    drop(stdin);
    assert!(!child.wait().unwrap().success());
    remove_fixture(&canonical);
    let _ = fs::remove_file(&blob);
}

#[test]
fn real_native_descriptor_generation_zero_requires_explicit_legacy_metadata() {
    let (canonical, blob) = legacy_fixture();
    let (mut child, mut stdin, mut stdout) = spawn_descriptor_mode(&canonical, &blob, true);
    let info = send(
        &mut stdin,
        &mut stdout,
        json!({"id": 1, "method": "protocol_info", "params": {}}),
    );
    assert_eq!(info["ok"], true);
    assert_eq!(info["result"]["generation"], 0);
    assert_eq!(info["result"]["legacyGeneration0"], true);
    drop(stdin);
    assert!(child.wait().unwrap().success());
    remove_fixture(&canonical);
    let _ = fs::remove_file(&blob);

    let (canonical, blob) = legacy_fixture();
    let output = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"))
        .args([
            "--descriptor-read",
            "--canonical-fd",
            "99",
            "--vector-blob-fd",
            "100",
            "--expected-generation",
            "0",
        ])
        .output()
        .expect("native executable runs");
    assert!(!output.status.success());
    remove_fixture(&canonical);
    let _ = fs::remove_file(&blob);
}
