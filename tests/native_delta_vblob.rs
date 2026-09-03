//! Adversarial acceptance tests for format 2 vector blob segments
//! (literature-hub #482, boundary 1).
//!
//! A committed generation no longer rewrites every vector. It publishes a
//! segment holding only the vectors first written in that generation plus a
//! parent link (basename/size/sha256/format) to the previous generation's
//! blob. The native resolves the lineage on open and fails closed on any
//! missing, tampered, truncated, cyclic, non-monotonic, or over-long chain.
//!
//! Format 2 layout, all little-endian:
//! `AGVB` | u16 version=2 | u64 segment generation | u16 parent basename len |
//! parent basename bytes | (when parent present) u64 parent size |
//! 32-byte parent sha256 | u16 parent format | f64 payload.

#![cfg(unix)]

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Read as _, Write};
use std::os::fd::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, ExitStatus, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC: &[u8; 4] = b"AGVB";
const DB_STEM: &str = "state";

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
        let dir = std::env::temp_dir().join(format!("aira-graphdb-delta-{label}-{nonce}"));
        fs::create_dir(&dir).expect("create temporary database directory");
        let path = dir.join(format!("{DB_STEM}.json"));
        Self { dir, path }
    }

    fn state(&self) -> Value {
        serde_json::from_slice(&fs::read(&self.path).expect("canonical JSON")).expect("state JSON")
    }

    fn descriptor_path(&self) -> PathBuf {
        self.dir.join(
            self.state()["vectorBlob"]["basename"]
                .as_str()
                .expect("descriptor basename"),
        )
    }

    fn blob_paths(&self) -> Vec<PathBuf> {
        let mut paths = fs::read_dir(&self.dir)
            .expect("read database directory")
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "vblob"))
            .collect::<Vec<_>>();
        paths.sort();
        paths
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

struct NativeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
}

impl NativeProcess {
    fn spawn(path: &Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"))
            .arg("--db")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("native binary starts");
        Self {
            stdin: Some(child.stdin.take().expect("native stdin")),
            stdout: BufReader::new(child.stdout.take().expect("native stdout")),
            child,
        }
    }

    fn send(&mut self, request: Value) -> Value {
        let stdin = self.stdin.as_mut().expect("native stdin is open");
        writeln!(stdin, "{request}").expect("write request");
        stdin.flush().expect("flush request");
        loop {
            let mut line = String::new();
            let read = self
                .stdout
                .read_line(&mut line)
                .expect("read native response");
            assert_ne!(read, 0, "native stdout closed before a response");
            let value: Value = serde_json::from_str(line.trim()).expect("native response is JSON");
            if value.get("ok").is_some() {
                return value;
            }
            assert_eq!(
                value["kind"],
                json!("progress"),
                "unexpected frame: {value}"
            );
        }
    }

    fn commit(&mut self, id: u64) -> Value {
        let prepared = self.send(json!({"id": id, "method": "batch_prepare_commit", "params": {}}));
        assert_eq!(prepared["ok"], json!(true), "prepare failed: {prepared}");
        self.send(json!({
            "id": id + 1,
            "method": "batch_commit",
            "params": {"preparedCommitEvidence": prepared["result"]}
        }))
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

fn batch_begin(id: u64) -> Value {
    json!({"id": id, "method": "batch_begin", "params": {}})
}

fn vector_upsert(id: u64, vector_id: &str, document_id: &str, values: [f64; 2]) -> Value {
    json!({
        "id": id,
        "method": "vector_upsert",
        "params": {"records": [{
            "id": vector_id,
            "corpusId": "c1",
            "namespace": "default",
            "values": values,
            "metadata": {"documentId": document_id}
        }]}
    })
}

fn upsert_node(id: u64, node_id: &str) -> Value {
    json!({
        "id": id,
        "method": "upsert_nodes",
        "params": {"nodes": [{
            "nodeId": node_id,
            "corpusId": "c1",
            "layer": "entity",
            "ref": {"documentId": "d-node"},
            "label": node_id
        }]}
    })
}

fn vector_delete_by_document(id: u64, document_id: &str) -> Value {
    json!({
        "id": id,
        "method": "vector_delete_by_document",
        "params": {"corpusId": "c1", "documentId": document_id}
    })
}

fn search_ids(native: &mut NativeProcess, id: u64, query: [f64; 2]) -> Vec<String> {
    let response = native.send(json!({
        "id": id,
        "method": "vector_search",
        "params": {"corpusId": "c1", "namespace": "default", "queryVector": query, "threshold": 0.99, "topK": 10}
    }));
    assert_eq!(response["ok"], json!(true), "search failed: {response}");
    let mut ids = response["result"]
        .as_array()
        .expect("search result array")
        .iter()
        .map(|hit| hit["id"].as_str().expect("hit id").to_string())
        .collect::<Vec<_>>();
    ids.sort();
    ids
}

fn protocol_info(native: &mut NativeProcess, id: u64) -> Value {
    let response = native.send(json!({"id": id, "method": "protocol_info", "params": {}}));
    assert_eq!(
        response["ok"],
        json!(true),
        "protocol_info failed: {response}"
    );
    response["result"].clone()
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn sha256_raw(hex: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    for (index, chunk) in hex.as_bytes().chunks(2).enumerate() {
        out[index] = u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap();
    }
    out
}

struct Parent<'a> {
    basename: &'a str,
    size: u64,
    sha256: &'a str,
    format: u16,
}

fn encode_v2(generation: u64, parent: Option<Parent<'_>>, values: &[f64]) -> Vec<u8> {
    let mut blob = MAGIC.to_vec();
    blob.extend_from_slice(&2u16.to_le_bytes());
    blob.extend_from_slice(&generation.to_le_bytes());
    match parent {
        None => blob.extend_from_slice(&0u16.to_le_bytes()),
        Some(parent) => {
            blob.extend_from_slice(&(parent.basename.len() as u16).to_le_bytes());
            blob.extend_from_slice(parent.basename.as_bytes());
            blob.extend_from_slice(&parent.size.to_le_bytes());
            blob.extend_from_slice(&sha256_raw(parent.sha256));
            blob.extend_from_slice(&parent.format.to_le_bytes());
        }
    }
    for value in values {
        blob.extend_from_slice(&value.to_le_bytes());
    }
    blob
}

fn encode_v1(values: &[f64]) -> Vec<u8> {
    let mut blob = MAGIC.to_vec();
    blob.extend_from_slice(&1u16.to_le_bytes());
    for value in values {
        blob.extend_from_slice(&value.to_le_bytes());
    }
    blob
}

/// Decoded view of a published segment: (version, segment generation,
/// parent basename, payload bytes).
fn decode_header(raw: &[u8]) -> (u16, Option<u64>, Option<String>, usize) {
    assert_eq!(&raw[..4], MAGIC);
    let version = u16::from_le_bytes(raw[4..6].try_into().unwrap());
    if version == 1 {
        return (1, None, None, raw.len() - 6);
    }
    assert_eq!(version, 2);
    let generation = u64::from_le_bytes(raw[6..14].try_into().unwrap());
    let parent_len = u16::from_le_bytes(raw[14..16].try_into().unwrap()) as usize;
    if parent_len == 0 {
        return (2, Some(generation), None, raw.len() - 16);
    }
    let basename = String::from_utf8(raw[16..16 + parent_len].to_vec()).unwrap();
    let payload_offset = 16 + parent_len + 8 + 32 + 2;
    (
        2,
        Some(generation),
        Some(basename),
        raw.len() - payload_offset,
    )
}

fn blob_basename(generation: u64, raw: &[u8]) -> String {
    format!("{DB_STEM}.g{generation:020}.{}.vblob", sha256_hex(raw))
}

fn descriptor(basename: &str, raw: &[u8], format: u16) -> Value {
    json!({"basename": basename, "size": raw.len(), "sha256": sha256_hex(raw), "format": format})
}

fn vector_entry(blob_ref: Value) -> Value {
    json!({
        "id": "v1",
        "corpusId": "c1",
        "namespace": "default",
        "blobRef": blob_ref,
        "metadata": {"documentId": "d1"}
    })
}

fn write_canonical(db: &TempDb, generation: u64, descriptor: Value, vectors: Value) {
    let canonical = json!({
        "nodes": {}, "edges": {}, "vectors": vectors, "passages": {},
        "snapshots": {}, "checkpoints": {},
        "generation": generation,
        "vectorBlob": descriptor
    });
    fs::write(&db.path, serde_json::to_vec(&canonical).unwrap()).expect("write canonical JSON");
}

/// Hand-build a lineage of `length` segments: a parentless format 2 base at
/// generation 1 holding `c1:v1`, then empty delta segments up to
/// generation `length`. Returns the raw bytes of every segment, base first.
fn write_chain(db: &TempDb, length: u64) -> Vec<(String, Vec<u8>)> {
    assert!(length >= 1);
    let base = encode_v2(1, None, &[1.0, 0.0]);
    let base_name = blob_basename(1, &base);
    fs::write(db.dir.join(&base_name), &base).unwrap();
    let mut segments = vec![(base_name, base)];
    for generation in 2..=length {
        let (parent_name, parent_raw) = segments.last().unwrap();
        let parent_sha = sha256_hex(parent_raw);
        let segment = encode_v2(
            generation,
            Some(Parent {
                basename: parent_name,
                size: parent_raw.len() as u64,
                sha256: &parent_sha,
                format: 2,
            }),
            &[],
        );
        let name = blob_basename(generation, &segment);
        fs::write(db.dir.join(&name), &segment).unwrap();
        segments.push((name, segment));
    }
    let (head_name, head_raw) = segments.last().unwrap();
    write_canonical(
        db,
        length,
        descriptor(head_name, head_raw, 2),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    segments
}

fn seed(db: &TempDb) {
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, "v1", "d1", [1.0, 0.0]))["ok"],
        json!(true)
    );
    let commit = native.commit(2);
    assert_eq!(commit["ok"], json!(true), "seed commit failed: {commit}");
    assert_eq!(commit["result"]["generation"], json!(1));
    assert_eq!(native.finish().code(), Some(0));
}

fn commit_second_vector(db: &TempDb) {
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, "v2", "d2", [0.0, 1.0]))["ok"],
        json!(true)
    );
    let commit = native.commit(2);
    assert_eq!(commit["ok"], json!(true), "delta commit failed: {commit}");
    assert_eq!(commit["result"]["generation"], json!(2));
    assert_eq!(native.finish().code(), Some(0));
}

fn assert_open_fails_closed(db: &TempDb, context: &str) {
    let before = fs::read(&db.path).expect("canonical JSON before open");
    let blobs_before = db
        .blob_paths()
        .iter()
        .map(|path| (path.clone(), fs::read(path).unwrap()))
        .collect::<Vec<_>>();
    let native = NativeProcess::spawn(&db.path);
    let status = native.finish();
    assert_ne!(status.code(), Some(0), "{context}: native must fail closed");
    assert_eq!(
        fs::read(&db.path).unwrap(),
        before,
        "{context}: canonical JSON must not change"
    );
    let blobs_after = db
        .blob_paths()
        .iter()
        .map(|path| (path.clone(), fs::read(path).unwrap()))
        .collect::<Vec<_>>();
    assert_eq!(
        blobs_after, blobs_before,
        "{context}: blobs must not be rewritten or removed"
    );
}

// ---------------------------------------------------------------------------
// Positive shape of the new layout (needed to make the negative tests honest).
// ---------------------------------------------------------------------------

#[test]
fn first_commit_from_empty_publishes_parentless_format2_base() {
    let db = TempDb::new("base");
    seed(&db);
    let state = db.state();
    assert_eq!(state["vectorBlob"]["format"], json!(2));
    let raw = fs::read(db.descriptor_path()).unwrap();
    let (version, generation, parent, payload) = decode_header(&raw);
    assert_eq!((version, generation, parent), (2, Some(1), None));
    assert_eq!(payload, 2 * 8);
    assert_eq!(
        state["vectors"]["c1:v1"]["blobRef"],
        json!({"offset": 0, "len": 2, "gen": 1})
    );
    assert_eq!(db.blob_paths().len(), 1);
}

#[test]
fn delta_commit_appends_only_new_vectors_and_keeps_the_base_immutable() {
    let db = TempDb::new("delta");
    seed(&db);
    let base_path = db.descriptor_path();
    let base_raw = fs::read(&base_path).unwrap();
    let base_name = base_path.file_name().unwrap().to_str().unwrap().to_string();

    commit_second_vector(&db);

    let state = db.state();
    assert_eq!(state["generation"], json!(2));
    let head_raw = fs::read(db.descriptor_path()).unwrap();
    let (version, generation, parent, payload) = decode_header(&head_raw);
    assert_eq!((version, generation), (2, Some(2)));
    assert_eq!(parent.as_deref(), Some(base_name.as_str()));
    assert_eq!(payload, 2 * 8, "segment carries exactly the one new vector");
    assert_eq!(
        fs::read(&base_path).unwrap(),
        base_raw,
        "base bytes are immutable"
    );
    assert_eq!(
        state["vectors"]["c1:v1"]["blobRef"],
        json!({"offset": 0, "len": 2, "gen": 1})
    );
    assert_eq!(
        state["vectors"]["c1:v2"]["blobRef"],
        json!({"offset": 0, "len": 2, "gen": 2})
    );
    assert_eq!(db.blob_paths().len(), 2);

    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(search_ids(&mut native, 10, [1.0, 0.0]), vec!["v1"]);
    assert_eq!(search_ids(&mut native, 11, [0.0, 1.0]), vec!["v2"]);
    let info = protocol_info(&mut native, 12);
    let lineage = info["vectorBlobLineage"].as_array().expect("lineage array");
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0], state["vectorBlob"], "descriptor first");
    assert_eq!(lineage[1]["basename"], json!(base_name), "base last");
    assert_eq!(
        info["vectorBlob"], state["vectorBlob"],
        "existing descriptor field unchanged"
    );
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn updated_vector_lands_in_the_newest_segment_and_search_returns_the_new_value() {
    let db = TempDb::new("update");
    seed(&db);
    let base_path = db.descriptor_path();
    let base_raw = fs::read(&base_path).unwrap();
    {
        let mut native = NativeProcess::spawn(&db.path);
        assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
        assert_eq!(
            native.send(vector_upsert(1, "v1", "d1", [0.0, 1.0]))["ok"],
            json!(true)
        );
        assert_eq!(native.commit(2)["ok"], json!(true));
        assert_eq!(native.finish().code(), Some(0));
    }
    let state = db.state();
    assert_eq!(state["vectors"]["c1:v1"]["blobRef"]["gen"], json!(2));
    assert_eq!(
        fs::read(&base_path).unwrap(),
        base_raw,
        "old bytes stay in the immutable base"
    );
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(search_ids(&mut native, 10, [0.0, 1.0]), vec!["v1"]);
    assert!(
        search_ids(&mut native, 11, [1.0, 0.0]).is_empty(),
        "stale base bytes are unreachable"
    );
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn deleted_vector_does_not_resurrect_from_an_older_segment() {
    let db = TempDb::new("delete");
    seed(&db);
    commit_second_vector(&db);
    {
        let mut native = NativeProcess::spawn(&db.path);
        assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
        assert_eq!(
            native.send(vector_delete_by_document(1, "d1"))["ok"],
            json!(true)
        );
        assert_eq!(native.commit(2)["ok"], json!(true));
        assert_eq!(native.finish().code(), Some(0));
    }
    assert_eq!(db.state()["generation"], json!(3));
    let mut native = NativeProcess::spawn(&db.path);
    assert!(search_ids(&mut native, 10, [1.0, 0.0]).is_empty());
    assert_eq!(search_ids(&mut native, 11, [0.0, 1.0]), vec!["v2"]);
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn node_only_commit_publishes_an_empty_delta_segment_that_reopens() {
    let db = TempDb::new("node-only");
    seed(&db);
    {
        let mut native = NativeProcess::spawn(&db.path);
        assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
        assert_eq!(native.send(upsert_node(1, "n1"))["ok"], json!(true));
        assert_eq!(native.commit(2)["ok"], json!(true));
        assert_eq!(native.finish().code(), Some(0));
    }
    let (_, generation, parent, payload) = decode_header(&fs::read(db.descriptor_path()).unwrap());
    assert_eq!(generation, Some(2));
    assert!(parent.is_some());
    assert_eq!(payload, 0);
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(search_ids(&mut native, 10, [1.0, 0.0]), vec!["v1"]);
    assert_eq!(native.finish().code(), Some(0));
}

#[test]
fn format1_base_is_accepted_as_parent_of_the_first_delta_commit() {
    let db = TempDb::new("format1-parent");
    let base = encode_v1(&[1.0, 0.0]);
    let base_name = blob_basename(1, &base);
    fs::write(db.dir.join(&base_name), &base).unwrap();
    write_canonical(
        &db,
        1,
        descriptor(&base_name, &base, 1),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2}))}),
    );
    commit_second_vector(&db);
    let state = db.state();
    let (version, generation, parent, _) = decode_header(&fs::read(db.descriptor_path()).unwrap());
    assert_eq!((version, generation), (2, Some(2)));
    assert_eq!(parent.as_deref(), Some(base_name.as_str()));
    assert_eq!(
        state["vectors"]["c1:v1"]["blobRef"],
        json!({"offset": 0, "len": 2}),
        "format 1 refs stay gen-less"
    );
    assert_eq!(state["vectors"]["c1:v2"]["blobRef"]["gen"], json!(2));
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(search_ids(&mut native, 10, [1.0, 0.0]), vec!["v1"]);
    assert_eq!(search_ids(&mut native, 11, [0.0, 1.0]), vec!["v2"]);
    let lineage = protocol_info(&mut native, 12)["vectorBlobLineage"].clone();
    assert_eq!(lineage[1]["format"], json!(1));
    assert_eq!(native.finish().code(), Some(0));
}

// ---------------------------------------------------------------------------
// Negative tests (packet §8, 1–5, 10, 11).
// ---------------------------------------------------------------------------

#[test]
fn missing_parent_segment_fails_closed_without_rewriting_anything() {
    let db = TempDb::new("missing-parent");
    seed(&db);
    let base_path = db.descriptor_path();
    commit_second_vector(&db);
    fs::remove_file(&base_path).unwrap();
    assert_open_fails_closed(&db, "missing parent");
}

#[test]
fn tampered_parent_payload_byte_fails_closed() {
    let db = TempDb::new("tampered-parent");
    seed(&db);
    let base_path = db.descriptor_path();
    commit_second_vector(&db);
    let mut raw = fs::read(&base_path).unwrap();
    let last = raw.len() - 1;
    raw[last] ^= 0x01;
    fs::write(&base_path, raw).unwrap();
    assert_open_fails_closed(&db, "tampered parent");
}

#[test]
fn truncated_parent_fails_closed() {
    let db = TempDb::new("truncated-parent");
    seed(&db);
    let base_path = db.descriptor_path();
    commit_second_vector(&db);
    let mut raw = fs::read(&base_path).unwrap();
    raw.truncate(raw.len() - 1);
    fs::write(&base_path, raw).unwrap();
    assert_open_fails_closed(&db, "truncated parent");
}

#[test]
fn parent_size_or_hash_mismatch_inside_the_child_header_fails_closed() {
    let db = TempDb::new("header-mismatch");
    let base = encode_v2(1, None, &[1.0, 0.0]);
    let base_name = blob_basename(1, &base);
    fs::write(db.dir.join(&base_name), &base).unwrap();
    let base_sha = sha256_hex(&base);
    for (label, size, sha) in [
        ("size", base.len() as u64 + 1, base_sha.clone()),
        ("sha", base.len() as u64, "00".repeat(32)),
    ] {
        let child = encode_v2(
            2,
            Some(Parent {
                basename: &base_name,
                size,
                sha256: &sha,
                format: 2,
            }),
            &[],
        );
        let child_name = blob_basename(2, &child);
        fs::write(db.dir.join(&child_name), &child).unwrap();
        write_canonical(
            &db,
            2,
            descriptor(&child_name, &child, 2),
            json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
        );
        assert_open_fails_closed(&db, label);
        fs::remove_file(db.dir.join(&child_name)).unwrap();
    }
}

#[test]
fn lineage_at_the_limit_opens_and_one_over_the_limit_fails_closed() {
    let probe = TempDb::new("limit-probe");
    seed(&probe);
    let mut native = NativeProcess::spawn(&probe.path);
    let limit = protocol_info(&mut native, 1)["limits"]["vectorBlob"]["maxLineage"]
        .as_u64()
        .expect("native advertises its lineage limit");
    assert_eq!(native.finish().code(), Some(0));
    assert!(limit >= 2);

    let at_limit = TempDb::new("limit-at");
    write_chain(&at_limit, limit);
    let mut native = NativeProcess::spawn(&at_limit.path);
    assert_eq!(search_ids(&mut native, 1, [1.0, 0.0]), vec!["v1"]);
    assert_eq!(
        protocol_info(&mut native, 2)["vectorBlobLineage"]
            .as_array()
            .unwrap()
            .len() as u64,
        limit
    );
    assert_eq!(native.finish().code(), Some(0));

    let over = TempDb::new("limit-over");
    write_chain(&over, limit + 1);
    assert_open_fails_closed(&over, "lineage over limit");
}

#[test]
fn commit_at_the_lineage_limit_fails_closed_without_publishing() {
    let probe = TempDb::new("commit-limit-probe");
    seed(&probe);
    let mut native = NativeProcess::spawn(&probe.path);
    let limit = protocol_info(&mut native, 1)["limits"]["vectorBlob"]["maxLineage"]
        .as_u64()
        .unwrap();
    assert_eq!(native.finish().code(), Some(0));

    let db = TempDb::new("commit-limit");
    write_chain(&db, limit);
    let json_before = fs::read(&db.path).unwrap();
    let blobs_before = db.blob_paths();
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(native.send(batch_begin(0))["ok"], json!(true));
    assert_eq!(
        native.send(vector_upsert(1, "v2", "d2", [0.0, 1.0]))["ok"],
        json!(true)
    );
    let commit = native.commit(2);
    assert_eq!(
        commit["ok"],
        json!(false),
        "commit must refuse to extend an exhausted lineage"
    );
    assert_eq!(fs::read(&db.path).unwrap(), json_before);
    assert_eq!(db.blob_paths(), blobs_before, "no segment may be published");
    drop(native);
}

#[test]
fn parent_generation_that_is_not_lower_fails_closed() {
    for parent_generation in [2u64, 3u64] {
        let db = TempDb::new("non-monotonic");
        let parent = encode_v2(parent_generation, None, &[1.0, 0.0]);
        let parent_name = blob_basename(parent_generation, &parent);
        fs::write(db.dir.join(&parent_name), &parent).unwrap();
        let parent_sha = sha256_hex(&parent);
        let child = encode_v2(
            2,
            Some(Parent {
                basename: &parent_name,
                size: parent.len() as u64,
                sha256: &parent_sha,
                format: 2,
            }),
            &[],
        );
        let child_name = blob_basename(2, &child);
        fs::write(db.dir.join(&child_name), &child).unwrap();
        write_canonical(
            &db,
            2,
            descriptor(&child_name, &child, 2),
            json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": parent_generation}))}),
        );
        assert_open_fails_closed(
            &db,
            &format!("parent generation {parent_generation} >= child 2"),
        );
    }
}

#[test]
fn self_referencing_parent_fails_closed() {
    let db = TempDb::new("self-parent");
    // The basename embeds the content hash, so a true self-reference cannot
    // exist; the closest forgery is a segment whose parent link names the
    // basename the descriptor will use, with the segment's own size.
    let probe = encode_v2(
        2,
        Some(Parent {
            basename: "x",
            size: 0,
            sha256: &"00".repeat(32),
            format: 2,
        }),
        &[],
    );
    let forged_name = blob_basename(2, &probe);
    let child = encode_v2(
        2,
        Some(Parent {
            basename: &forged_name,
            size: probe.len() as u64,
            sha256: &sha256_hex(&probe),
            format: 2,
        }),
        &[],
    );
    fs::write(db.dir.join(&forged_name), &child).unwrap();
    write_canonical(
        &db,
        2,
        json!({"basename": forged_name, "size": child.len(), "sha256": sha256_hex(&child), "format": 2}),
        json!({}),
    );
    assert_open_fails_closed(&db, "self-referencing parent");
}

#[test]
fn descriptor_segment_generation_must_equal_the_canonical_generation() {
    let db = TempDb::new("descriptor-generation");
    let base = encode_v2(1, None, &[1.0, 0.0]);
    let base_name = blob_basename(1, &base);
    fs::write(db.dir.join(&base_name), &base).unwrap();
    let base_sha = sha256_hex(&base);
    let child = encode_v2(
        2,
        Some(Parent {
            basename: &base_name,
            size: base.len() as u64,
            sha256: &base_sha,
            format: 2,
        }),
        &[],
    );
    let child_name = blob_basename(2, &child);
    fs::write(db.dir.join(&child_name), &child).unwrap();
    write_canonical(
        &db,
        3,
        descriptor(&child_name, &child, 2),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    assert_open_fails_closed(&db, "descriptor generation 2 under canonical generation 3");
}

#[test]
fn blob_ref_naming_an_absent_segment_generation_fails_closed() {
    let db = TempDb::new("dangling-ref");
    write_chain(&db, 2);
    let mut state = db.state();
    state["vectors"]["c1:v1"]["blobRef"]["gen"] = json!(7);
    fs::write(&db.path, serde_json::to_vec(&state).unwrap()).unwrap();
    assert_open_fails_closed(&db, "blobRef.gen without a matching segment");

    let mut state = db.state();
    state["vectors"]["c1:v1"]["blobRef"] = json!({"offset": 0, "len": 2});
    fs::write(&db.path, serde_json::to_vec(&state).unwrap()).unwrap();
    assert_open_fails_closed(&db, "gen-less blobRef without a format 1 base");
}

#[test]
fn descriptor_format_field_must_match_the_file_version() {
    let db = TempDb::new("format-field");
    let v2 = encode_v2(1, None, &[1.0, 0.0]);
    let v2_name = blob_basename(1, &v2);
    fs::write(db.dir.join(&v2_name), &v2).unwrap();
    write_canonical(
        &db,
        1,
        descriptor(&v2_name, &v2, 1),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    assert_open_fails_closed(&db, "format 1 descriptor over a version 2 file");

    let v1 = encode_v1(&[1.0, 0.0]);
    let v1_name = blob_basename(1, &v1);
    fs::write(db.dir.join(&v1_name), &v1).unwrap();
    write_canonical(
        &db,
        1,
        descriptor(&v1_name, &v1, 2),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2}))}),
    );
    assert_open_fails_closed(&db, "format 2 descriptor over a version 1 file");
}

#[test]
fn unknown_blob_version_fails_closed() {
    let db = TempDb::new("version-3");
    let mut raw = encode_v2(1, None, &[1.0, 0.0]);
    raw[4..6].copy_from_slice(&3u16.to_le_bytes());
    let name = blob_basename(1, &raw);
    fs::write(db.dir.join(&name), &raw).unwrap();
    write_canonical(
        &db,
        1,
        descriptor(&name, &raw, 3),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    assert_open_fails_closed(&db, "version 3");
}

/// Spawn the native against `db`, expect a non-zero exit, and return stderr.
fn open_failure_stderr(db: &TempDb) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"))
        .arg("--db")
        .arg(&db.path)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("native binary runs");
    assert_ne!(output.status.code(), Some(0), "native must fail closed");
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn lineage_byte_ceiling_is_enforced_from_descriptors_before_any_segment_is_read() {
    let probe = TempDb::new("bytes-probe");
    seed(&probe);
    let mut native = NativeProcess::spawn(&probe.path);
    let max_bytes = protocol_info(&mut native, 1)["limits"]["vectorBlob"]["maxLineageBytes"]
        .as_u64()
        .expect("native advertises its lineage byte ceiling");
    assert_eq!(native.finish().code(), Some(0));

    // Head descriptor alone claims more than the ceiling. The file on disk is
    // tiny, so a size-first implementation that read it would report a size
    // mismatch; the ceiling must be reported instead, i.e. before the read.
    let head_only = TempDb::new("bytes-head");
    let base = encode_v2(1, None, &[1.0, 0.0]);
    let base_name = blob_basename(1, &base);
    fs::write(head_only.dir.join(&base_name), &base).unwrap();
    write_canonical(
        &head_only,
        1,
        json!({"basename": base_name, "size": max_bytes + 1, "sha256": sha256_hex(&base), "format": 2}),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    let stderr = open_failure_stderr(&head_only);
    assert!(
        stderr.contains("lineage exceeds") && stderr.contains("bytes"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("size does not match"),
        "must fail on the ceiling, not after reading: {stderr}"
    );

    // Two honest segments whose *sum* crosses the ceiling: the child's header
    // claims a parent of exactly the ceiling. The parent file is small and is
    // never opened because the running total is rejected first.
    let summed = TempDb::new("bytes-sum");
    let parent = encode_v2(1, None, &[1.0, 0.0]);
    let parent_name = blob_basename(1, &parent);
    fs::write(summed.dir.join(&parent_name), &parent).unwrap();
    let child = encode_v2(
        2,
        Some(Parent {
            basename: &parent_name,
            size: max_bytes,
            sha256: &sha256_hex(&parent),
            format: 2,
        }),
        &[],
    );
    let child_name = blob_basename(2, &child);
    fs::write(summed.dir.join(&child_name), &child).unwrap();
    write_canonical(
        &summed,
        2,
        descriptor(&child_name, &child, 2),
        json!({"c1:v1": vector_entry(json!({"offset": 0, "len": 2, "gen": 1}))}),
    );
    let stderr = open_failure_stderr(&summed);
    assert!(
        stderr.contains("lineage exceeds") && stderr.contains("bytes"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("size does not match"),
        "parent must not be read: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Descriptor read-only mode: one inherited fd cannot carry a lineage.
// ---------------------------------------------------------------------------

fn spawn_descriptor_mode(
    canonical: &Path,
    blob: &Path,
    expected_generation: u64,
) -> (Child, ChildStdin, BufReader<ChildStdout>) {
    let canonical_file = OpenOptions::new().read(true).open(canonical).unwrap();
    let blob_file = OpenOptions::new().read(true).open(blob).unwrap();
    let canonical_fd = canonical_file.as_raw_fd();
    let blob_fd = blob_file.as_raw_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"));
    command
        .args([
            "--descriptor-read",
            "--canonical-fd",
            &canonical_fd.to_string(),
            "--vector-blob-fd",
            &blob_fd.to_string(),
            "--expected-generation",
            &expected_generation.to_string(),
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
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

#[test]
fn descriptor_mode_rejects_a_chained_segment_and_accepts_a_parentless_one() {
    let chained = TempDb::new("descriptor-chained");
    write_chain(&chained, 2);
    let (mut child, stdin, mut stdout) =
        spawn_descriptor_mode(&chained.path, &chained.descriptor_path(), 2);
    drop(stdin);
    let mut tail = String::new();
    stdout.read_to_string(&mut tail).unwrap();
    let status = child.wait().unwrap();
    assert_ne!(
        status.code(),
        Some(0),
        "chained segment must fail closed in descriptor mode"
    );
    assert!(
        !tail.contains("\"ok\":true"),
        "descriptor mode must not answer over a partial lineage: {tail}"
    );

    let base = TempDb::new("descriptor-base");
    write_chain(&base, 1);
    let (mut child, mut stdin, mut stdout) =
        spawn_descriptor_mode(&base.path, &base.descriptor_path(), 1);
    writeln!(
        stdin,
        "{}",
        json!({"id": 1, "method": "protocol_info", "params": {}})
    )
    .unwrap();
    stdin.flush().unwrap();
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    let info: Value = serde_json::from_str(line.trim()).unwrap();
    assert_eq!(
        info["ok"],
        json!(true),
        "parentless format 2 must be readable: {info}"
    );
    assert_eq!(info["result"]["accessMode"], json!("descriptor-read-only"));
    assert_eq!(info["result"]["vectorBlob"]["format"], json!(2));
    assert_eq!(
        info["result"]["vectorBlobLineage"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    drop(stdin);
    assert_eq!(child.wait().unwrap().code(), Some(0));
}
