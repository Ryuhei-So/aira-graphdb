//! Paged ranking-graph read (literature-hub #594).
//!
//! The whole-corpus `projection_get_transitions` reply outgrew the owner's
//! line bound. `projection_get_transitions_page` serves the same entries in a
//! total order, cut against the bounded indexing response cap and pinned to
//! one committed generation. Harness style follows `native_memory_reads.rs`:
//! a real sidecar process, one JSON line per request, and a liveness check
//! after every rejection.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;

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
        let dir =
            std::env::temp_dir().join(format!("aira-graphdb-projection-pages-{label}-{nonce}"));
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
    fn spawn(path: &Path) -> Self {
        let bin = env!("CARGO_BIN_EXE_aira-graphdb-native");
        let mut child = Command::new(bin)
            .arg("--db")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("native sidecar should start");
        let stdin = child.stdin.take().expect("stdin pipe");
        let stdout = BufReader::new(child.stdout.take().expect("stdout pipe"));
        Self {
            child,
            stdin: Some(stdin),
            stdout,
        }
    }

    /// The raw reply line (without the newline) and its parsed value.
    fn send_raw(&mut self, request: Value) -> (String, Value) {
        let stdin = self.stdin.as_mut().expect("stdin is open");
        stdin
            .write_all(request.to_string().as_bytes())
            .expect("write request line");
        stdin.write_all(b"\n").expect("write newline");
        stdin.flush().expect("flush request");
        let mut response = String::new();
        self.stdout.read_line(&mut response).expect("read response");
        assert!(
            !response.is_empty(),
            "native closed stdout while answering {request}"
        );
        let line = response.trim_end_matches('\n').to_string();
        let value = serde_json::from_str(&line).expect("response must be valid json");
        (line, value)
    }

    fn send(&mut self, request: Value) -> Value {
        self.send_raw(request).1
    }

    fn ensure_alive(&mut self) {
        assert!(
            self.child.try_wait().expect("query child status").is_none(),
            "native sidecar exited"
        );
    }

    fn commit(&mut self, id: u64) -> Value {
        let prepared = self.send(json!({
            "id": id,
            "method": "batch_prepare_commit",
            "params": {}
        }));
        assert_eq!(prepared["ok"], json!(true), "prepare failed: {prepared}");
        self.send(json!({
            "id": id,
            "method": "batch_commit",
            "params": {"preparedCommitEvidence": prepared["result"].clone()}
        }))
    }

    fn upsert_edges(&mut self, id: u64, edges: Vec<Value>) {
        assert_eq!(
            self.send(json!({"id": id, "method": "batch_begin", "params": {}}))["ok"],
            json!(true)
        );
        let upsert = self.send(json!({
            "id": id + 1,
            "method": "upsert_edges",
            "params": {"edges": edges}
        }));
        assert_eq!(upsert["ok"], json!(true), "upsert_edges failed: {upsert}");
        let commit = self.commit(id + 2);
        assert_eq!(commit["ok"], json!(true), "commit failed: {commit}");
    }

    fn generation(&mut self) -> u64 {
        let info = self.send(json!({"id": 9, "method": "protocol_info", "params": {}}));
        info["result"]["generation"]
            .as_u64()
            .expect("protocol_info generation")
    }

    fn finish(mut self) -> Option<i32> {
        drop(self.stdin.take());
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("query child status") {
                return status.code();
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "native did not exit after stdin EOF"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for NativeProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn edge(edge_id: &str, source: &str, target: &str, weight: f64) -> Value {
    json!({
        "edgeId": edge_id,
        "corpusId": "c1",
        "sourceNodeId": source,
        "targetNodeId": target,
        "relation": "r",
        "weight": weight
    })
}

fn page(corpus_id: &str, generation: Value, offset: Value) -> Value {
    json!({
        "id": 7,
        "method": "projection_get_transitions_page",
        "params": {"corpusId": corpus_id, "generation": generation, "offset": offset}
    })
}

fn assert_client_rejection(response: &Value, context: &str) {
    assert_eq!(response["ok"], json!(false), "{context}: {response}");
    assert_eq!(
        response["error"]["code"],
        json!("REQUEST_EXECUTION_FAILED"),
        "{context}: {response}"
    );
    assert_eq!(
        response["error"]["failureClass"],
        json!("CLIENT_INPUT"),
        "{context}: {response}"
    );
}

type Entry = (String, String, u64);

fn entry_key(value: &Value) -> Entry {
    (
        value["sourceNodeId"].as_str().expect("source").to_string(),
        value["targetNodeId"].as_str().expect("target").to_string(),
        value["weight"].as_f64().expect("weight").to_bits(),
    )
}

fn multiset(entries: &[Value]) -> BTreeMap<Entry, usize> {
    let mut counts = BTreeMap::new();
    for entry in entries {
        *counts.entry(entry_key(entry)).or_default() += 1;
    }
    counts
}

/// Reads every page from offset 0, asserting the page protocol on the way.
/// Returns the entries, the page count, and the widest reply line.
fn read_all_pages(native: &mut NativeProcess, corpus_id: &str) -> (Vec<Value>, usize, usize) {
    let (line, first) = native.send_raw(page(corpus_id, Value::Null, json!(0)));
    assert_eq!(first["ok"], json!(true), "first page failed: {first}");
    let generation = first["result"]["generation"].clone();
    let total = first["result"]["totalEntries"].as_u64().expect("total") as usize;
    let mut widest = line.len();
    let mut pages = 1;
    let mut entries: Vec<Value> = first["result"]["entries"]
        .as_array()
        .expect("entries")
        .clone();
    let mut next = first["result"]["nextOffset"].clone();
    while !next.is_null() {
        assert_eq!(next, json!(entries.len()), "nextOffset equals entries read");
        let (line, reply) = native.send_raw(page(corpus_id, generation.clone(), next.clone()));
        assert_eq!(reply["ok"], json!(true), "page at {next} failed: {reply}");
        assert_eq!(reply["result"]["generation"], generation);
        assert_eq!(reply["result"]["offset"], next);
        assert_eq!(reply["result"]["totalEntries"], json!(total));
        let chunk = reply["result"]["entries"].as_array().expect("entries");
        assert!(!chunk.is_empty(), "a non-final page is never empty");
        entries.extend(chunk.iter().cloned());
        widest = widest.max(line.len());
        pages += 1;
        next = reply["result"]["nextOffset"].clone();
    }
    assert_eq!(entries.len(), total, "pages deliver exactly totalEntries");
    (entries, pages, widest)
}

#[test]
fn protocol_info_advertises_the_projection_page_method_and_limits() {
    let db = TempDb::new("protocol-info");
    let mut native = NativeProcess::spawn(&db.path);
    let info = native.send(json!({"id": 1, "method": "protocol_info", "params": {}}));
    assert_eq!(info["ok"], json!(true));
    assert_eq!(
        info["result"]["limits"]["projectionRead"],
        json!({
            "schema": "native-projection-read@1",
            "maxResponseBytes": MAX_RESPONSE_BYTES,
            "order": "source-target-key@1",
        })
    );
    let methods = info["result"]["methods"].as_array().expect("methods");
    let entry = methods
        .iter()
        .find(|method| method["name"] == json!("projection_get_transitions_page"))
        .expect("projection_get_transitions_page is advertised");
    assert_eq!(entry["classification"], json!("read"));
    assert_eq!(entry["wal"], json!(false));
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn pages_deliver_the_legacy_multiset_in_total_order_within_the_byte_cap() {
    let db = TempDb::new("multiset");
    let mut native = NativeProcess::spawn(&db.path);
    // Long node ids make the corpus span several 8 MiB pages with few edges.
    let pad = "n".repeat(2000);
    let mut id = 100;
    for batch in 0..6 {
        let edges = (0..1000)
            .map(|i| {
                let n = batch * 1000 + i;
                edge(
                    &format!("e{n:05}"),
                    &format!("{pad}:{:03}", n % 97),
                    &format!("{pad}:{:03}", (n * 7) % 89),
                    f64::from(n % 5) * 0.25 + 0.1,
                )
            })
            .collect();
        native.upsert_edges(id, edges);
        id += 10;
    }
    // Exact (source, target, weight) duplicates under distinct edge ids are
    // distinct entries and must each arrive exactly once.
    native.upsert_edges(
        id,
        vec![
            edge("dup-b", "dup-src", "dup-tgt", 0.5),
            edge("dup-a", "dup-src", "dup-tgt", 0.5),
            edge("dup-c", "dup-src", "dup-tgt", 0.5),
        ],
    );

    let (entries, pages, widest) = read_all_pages(&mut native, "c1");
    assert!(pages >= 3, "fixture must span several pages, got {pages}");
    assert!(
        widest <= MAX_RESPONSE_BYTES,
        "a page line is {widest} bytes, above the cap"
    );
    assert_eq!(entries.len(), 6003);

    let legacy = native.send(json!({
        "id": 3,
        "method": "projection_get_transitions",
        "params": {"corpusId": "c1"}
    }));
    let legacy = legacy["result"].as_array().expect("legacy array");
    assert_eq!(multiset(&entries), multiset(legacy));
    let duplicates = entries
        .iter()
        .filter(|entry| entry["sourceNodeId"] == json!("dup-src"))
        .count();
    assert_eq!(duplicates, 3);

    // Non-decreasing by (source, target); ties are broken by the unique edge
    // key, which the pages do not expose, so equal pairs may only be adjacent.
    let pairs = entries
        .iter()
        .map(|entry| {
            (
                entry["sourceNodeId"].as_str().unwrap().as_bytes().to_vec(),
                entry["targetNodeId"].as_str().unwrap().as_bytes().to_vec(),
            )
        })
        .collect::<Vec<_>>();
    assert!(pairs.windows(2).all(|pair| pair[0] <= pair[1]));

    // Deterministic: a second full read yields the identical sequence.
    let (again, _, _) = read_all_pages(&mut native, "c1");
    assert_eq!(again, entries);
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn page_order_is_stable_across_processes() {
    let db = TempDb::new("stable-order");
    {
        let mut native = NativeProcess::spawn(&db.path);
        let edges = (0..200)
            // Many edges share (source, target) but differ in weight, so
            // only the edge-key tie-break fixes their relative order.
            .map(|i| {
                edge(
                    &format!("e{i}"),
                    &format!("s{}", i % 3),
                    "t",
                    f64::from(i) * 0.5,
                )
            })
            .collect();
        native.upsert_edges(1, edges);
        assert_eq!(native.finish(), Some(0));
    }
    let first = {
        let mut native = NativeProcess::spawn(&db.path);
        let (entries, _, _) = read_all_pages(&mut native, "c1");
        assert_eq!(native.finish(), Some(0));
        entries
    };
    let second = {
        let mut native = NativeProcess::spawn(&db.path);
        let (entries, _, _) = read_all_pages(&mut native, "c1");
        assert_eq!(native.finish(), Some(0));
        entries
    };
    assert_eq!(first, second);
}

#[test]
fn a_read_spanning_a_commit_fails_closed_and_restarts_at_the_new_generation() {
    let db = TempDb::new("generation-pin");
    let mut native = NativeProcess::spawn(&db.path);
    let pad = "n".repeat(3000);
    let edges = (0..4000)
        .map(|i| edge(&format!("e{i}"), &format!("{pad}{i}"), "t", 1.0))
        .collect();
    native.upsert_edges(1, edges);
    let pinned = native.generation();

    let first = native.send(page("c1", Value::Null, json!(0)));
    assert_eq!(first["result"]["generation"], json!(pinned));
    let next = first["result"]["nextOffset"].clone();
    assert!(
        next.is_u64(),
        "fixture must span pages: {}",
        first["result"]["totalEntries"]
    );

    native.upsert_edges(20, vec![edge("late", "late-src", "t", 1.0)]);
    let stale = native.send(page("c1", json!(pinned), next));
    assert_client_rejection(&stale, "page pinned to a superseded generation");
    assert!(
        stale["error"]["message"]
            .as_str()
            .unwrap()
            .contains("does not match committed generation"),
        "{stale}"
    );
    native.ensure_alive();

    let (entries, _, _) = read_all_pages(&mut native, "c1");
    assert_eq!(entries.len(), 4001);
    assert!(
        entries
            .iter()
            .any(|entry| entry["sourceNodeId"] == json!("late-src"))
    );
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn pages_are_rejected_while_a_batch_is_open() {
    let db = TempDb::new("open-batch");
    let mut native = NativeProcess::spawn(&db.path);
    native.upsert_edges(1, vec![edge("e1", "a", "b", 1.0)]);
    let generation = native.generation();
    assert_eq!(
        native.send(json!({"id": 10, "method": "batch_begin", "params": {}}))["ok"],
        json!(true)
    );
    let upsert = native.send(json!({
        "id": 11,
        "method": "upsert_edges",
        "params": {"edges": [edge("e2", "uncommitted", "b", 1.0)]}
    }));
    assert_eq!(upsert["ok"], json!(true));
    for request in [
        page("c1", Value::Null, json!(0)),
        page("c1", json!(generation), json!(0)),
    ] {
        let response = native.send(request);
        assert_client_rejection(&response, "page during an open batch");
        native.ensure_alive();
    }
    assert_eq!(native.commit(12)["ok"], json!(true));
    let (entries, _, _) = read_all_pages(&mut native, "c1");
    assert_eq!(entries.len(), 2);
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn malformed_page_requests_are_rejected_and_the_native_stays_up() {
    let db = TempDb::new("malformed");
    let mut native = NativeProcess::spawn(&db.path);
    native.upsert_edges(
        1,
        vec![edge("e1", "a", "b", 1.0), edge("e2", "b", "a", 1.0)],
    );
    let generation = json!(native.generation());
    let cases = [
        (
            "null generation past offset 0",
            json!({"corpusId": "c1", "generation": null, "offset": 1}),
        ),
        (
            "offset at totalEntries",
            json!({"corpusId": "c1", "generation": generation, "offset": 2}),
        ),
        (
            "offset beyond totalEntries",
            json!({"corpusId": "c1", "generation": generation, "offset": 3}),
        ),
        (
            "negative offset",
            json!({"corpusId": "c1", "generation": generation, "offset": -1}),
        ),
        (
            "fractional offset",
            json!({"corpusId": "c1", "generation": generation, "offset": 0.5}),
        ),
        (
            "string offset",
            json!({"corpusId": "c1", "generation": generation, "offset": "0"}),
        ),
        (
            "string generation",
            json!({"corpusId": "c1", "generation": "1", "offset": 0}),
        ),
        (
            "negative generation",
            json!({"corpusId": "c1", "generation": -1, "offset": 0}),
        ),
        ("missing generation", json!({"corpusId": "c1", "offset": 0})),
        (
            "missing offset",
            json!({"corpusId": "c1", "generation": generation}),
        ),
        (
            "unknown param",
            json!({"corpusId": "c1", "generation": generation, "offset": 0, "limit": 1}),
        ),
        (
            "empty corpus id",
            json!({"corpusId": "", "generation": generation, "offset": 0}),
        ),
        (
            "oversized corpus id",
            json!({"corpusId": "c".repeat(1025), "generation": generation, "offset": 0}),
        ),
        ("params not an object", json!([])),
    ];
    for (context, params) in cases {
        let response = native.send(json!({
            "id": 5,
            "method": "projection_get_transitions_page",
            "params": params
        }));
        assert_client_rejection(&response, context);
        native.ensure_alive();
    }
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn an_unknown_or_empty_corpus_is_one_empty_final_page() {
    let db = TempDb::new("empty");
    let mut native = NativeProcess::spawn(&db.path);
    let generation = native.generation();
    let reply = native.send(page("nobody", Value::Null, json!(0)));
    assert_eq!(
        reply["result"],
        json!({
            "generation": generation,
            "offset": 0,
            "nextOffset": null,
            "totalEntries": 0,
            "entries": [],
        })
    );
    let pinned = native.send(page("nobody", json!(generation), json!(0)));
    assert_eq!(pinned["result"]["totalEntries"], json!(0));
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn an_entry_larger_than_a_page_is_an_error_not_an_empty_page() {
    let db = TempDb::new("oversized-entry");
    let mut native = NativeProcess::spawn(&db.path);
    let huge = "h".repeat(MAX_RESPONSE_BYTES);
    native.upsert_edges(
        1,
        vec![edge("big", &huge, "t", 1.0), edge("small", "a", "b", 1.0)],
    );
    // "a" sorts before "h...": the small entry fills the first page alone.
    let first = native.send(page("c1", Value::Null, json!(0)));
    assert_eq!(first["ok"], json!(true), "{first}");
    assert_eq!(first["result"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(first["result"]["nextOffset"], json!(1));
    let generation = first["result"]["generation"].clone();
    let second = native.send(page("c1", generation, json!(1)));
    assert_client_rejection(&second, "oversized entry");
    assert!(
        second["error"]["message"]
            .as_str()
            .unwrap()
            .contains("exceeds the page byte limit"),
        "{second}"
    );
    native.ensure_alive();
    assert_eq!(native.finish(), Some(0));
}
