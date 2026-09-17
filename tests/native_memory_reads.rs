//! Targeted memory read methods (literature-hub #545, PR-A).
//!
//! Harness style follows `native_rpc_resilience.rs`: a real sidecar process,
//! one JSON line per request, and an explicit liveness check after every
//! rejection so a bounded read can never take the native down.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

const MAX_IDS_PER_REQUEST: usize = 4096;
const MAX_ENTITIES_PER_REQUEST: usize = 64;
const MAX_LIMIT: usize = 100;
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
        let dir = std::env::temp_dir().join(format!("aira-graphdb-memory-reads-{label}-{nonce}"));
        std::fs::create_dir(&dir).expect("create temporary database directory");
        let path = dir.join("state.json");
        Self { dir, path }
    }

    fn wal_path(&self) -> PathBuf {
        self.path.with_extension("agdb.wal")
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

    fn send(&mut self, request: Value) -> Value {
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
        serde_json::from_str(response.trim()).expect("response must be valid json")
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

fn batch_begin(id: u64) -> Value {
    json!({"id": id, "method": "batch_begin", "params": {}})
}

fn read(method: &str, params: Value) -> Value {
    json!({"id": 1, "method": method, "params": params})
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

fn fixture_upsert(id: u64) -> Value {
    json!({
        "id": id,
        "method": "memory_upsert",
        "params": {
            "corpusId": "c1",
            "passages": [
                {"passageId":"p1","corpusId":"c1","text":"first"},
                {"passageId":"p2","corpusId":"c1","text":"second"},
                {"passageId":"p3","corpusId":"c1","text":"third"}
            ],
            "facts": [
                {"factId":"f3","corpusId":"c1","schemaId":"s1","state":"active",
                 "headEntity":"École","tailEntity":"Paris"},
                {"factId":"f1","corpusId":"c1","schemaId":"s1","state":"inactive",
                 "headEntity":"Kelvin","tailEntity":"Lord"},
                {"factId":"f2","corpusId":"c1","schemaId":"s1","state":"active",
                 "headEntity":"Berlin","tailEntity":"kelvin"},
                {"factId":"f4","corpusId":"c1","schemaId":"s1","state":"active",
                 "headEntity":"Nobody","tailEntity":"Else"}
            ],
            "schemas": [
                {"schemaId":"s1","corpusId":"c1","state":"stable"}
            ]
        }
    })
}

fn seed_fixture(db: &TempDb) {
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    let upsert = native.send(fixture_upsert(2));
    assert_eq!(upsert["ok"], json!(true), "seed upsert failed: {upsert}");
    assert_eq!(native.commit(3)["ok"], json!(true));
    assert_eq!(native.finish(), Some(0));
}

fn ids(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{prefix}{index}")).collect()
}

fn seed_passages(db: &TempDb, count: usize, text_bytes: usize) {
    let text = "x".repeat(text_bytes);
    let passages = ids("p", count)
        .into_iter()
        .map(|passage_id| json!({"passageId": passage_id, "corpusId": "c1", "text": text}))
        .collect::<Vec<_>>();
    let mut native = NativeProcess::spawn(&db.path);
    assert_eq!(native.send(batch_begin(1))["ok"], json!(true));
    let upsert = native.send(json!({
        "id": 2,
        "method": "memory_upsert",
        "params": {"corpusId": "c1", "passages": passages}
    }));
    assert_eq!(upsert["ok"], json!(true), "bulk upsert failed: {upsert}");
    assert_eq!(native.commit(3)["ok"], json!(true));
    assert_eq!(native.finish(), Some(0));
}

fn result_ids(response: &Value, key: &str) -> Vec<String> {
    response["result"]
        .as_array()
        .unwrap_or_else(|| panic!("result must be an array: {response}"))
        .iter()
        .map(|item| item[key].as_str().expect("string id").to_string())
        .collect()
}

#[test]
fn protocol_info_advertises_memory_read_methods_and_limits() {
    let db = TempDb::new("protocol-info");
    let mut native = NativeProcess::spawn(&db.path);
    let info = native.send(read("protocol_info", json!({})));
    assert_eq!(info["ok"], json!(true));
    assert_eq!(
        info["result"]["protocolVersion"],
        json!("native-method-policy@1")
    );
    assert_eq!(
        info["result"]["limits"]["memoryRead"],
        json!({
            "schema": "native-memory-read@1",
            "maxIdsPerRequest": MAX_IDS_PER_REQUEST,
            "maxEntitiesPerRequest": MAX_ENTITIES_PER_REQUEST,
            "maxLimit": MAX_LIMIT,
        })
    );
    assert_eq!(
        info["result"]["limits"]["indexingMemory"]["maxResponseBytes"],
        json!(MAX_RESPONSE_BYTES)
    );
    let methods = info["result"]["methods"]
        .as_array()
        .expect("methods inventory");
    for name in [
        "memory_get_passages_by_ids",
        "memory_get_facts_by_ids",
        "memory_find_facts_by_entities",
        "memory_section_counts",
    ] {
        let entry = methods
            .iter()
            .find(|method| method["name"] == json!(name))
            .unwrap_or_else(|| panic!("{name} is missing from protocol_info.methods"));
        assert_eq!(entry["classification"], json!("read"), "{name}");
        assert_eq!(entry["wal"], json!(false), "{name}");
    }
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn passages_by_ids_returns_request_order_and_omits_unknown_ids() {
    let db = TempDb::new("passages-by-ids");
    seed_fixture(&db);
    let mut native = NativeProcess::spawn(&db.path);

    let response = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds":["p3","missing","p1","p3","p2"]}),
    ));
    assert_eq!(response["ok"], json!(true), "{response}");
    assert_eq!(result_ids(&response, "passageId"), ["p3", "p1", "p2"]);
    assert_eq!(response["result"][0]["text"], json!("third"));

    let empty = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds":[]}),
    ));
    assert_eq!(empty["result"], json!([]));

    let unknown_corpus = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"nope","passageIds":["p1"]}),
    ));
    assert_eq!(unknown_corpus["ok"], json!(true), "{unknown_corpus}");
    assert_eq!(unknown_corpus["result"], json!([]));

    let all_unknown = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds":["nope-1","nope-2"]}),
    ));
    assert_eq!(all_unknown["result"], json!([]));
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn passages_by_ids_rejects_over_bound_and_inexact_params_and_survives() {
    let db = TempDb::new("passages-by-ids-bounds");
    seed_fixture(&db);
    let mut native = NativeProcess::spawn(&db.path);

    let at_bound = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds": ids("p", MAX_IDS_PER_REQUEST)}),
    ));
    assert_eq!(at_bound["ok"], json!(true), "{at_bound}");

    let over_bound = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds": ids("p", MAX_IDS_PER_REQUEST + 1)}),
    ));
    assert_client_rejection(&over_bound, "over maxIdsPerRequest");
    native.ensure_alive();

    for (context, params) in [
        (
            "extra param",
            json!({"corpusId":"c1","passageIds":["p1"],"extra":1}),
        ),
        ("missing ids", json!({"corpusId":"c1"})),
        ("wrong id key", json!({"corpusId":"c1","factIds":["p1"]})),
        ("non-string id", json!({"corpusId":"c1","passageIds":[1]})),
        ("empty id", json!({"corpusId":"c1","passageIds":[""]})),
        ("empty corpus", json!({"corpusId":"","passageIds":["p1"]})),
        ("ids not array", json!({"corpusId":"c1","passageIds":"p1"})),
    ] {
        let response = native.send(read("memory_get_passages_by_ids", params));
        assert_client_rejection(&response, context);
        native.ensure_alive();
    }
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn facts_by_ids_returns_request_order_and_omits_unknown_ids() {
    let db = TempDb::new("facts-by-ids");
    seed_fixture(&db);
    let mut native = NativeProcess::spawn(&db.path);

    let response = native.send(read(
        "memory_get_facts_by_ids",
        json!({"corpusId":"c1","factIds":["f2","f1","zzz","f1","f4"]}),
    ));
    assert_eq!(response["ok"], json!(true), "{response}");
    assert_eq!(result_ids(&response, "factId"), ["f2", "f1", "f4"]);
    assert_eq!(response["result"][1]["state"], json!("inactive"));

    let unknown_corpus = native.send(read(
        "memory_get_facts_by_ids",
        json!({"corpusId":"nope","factIds":["f1"]}),
    ));
    assert_eq!(unknown_corpus["result"], json!([]));

    let over_bound = native.send(read(
        "memory_get_facts_by_ids",
        json!({"corpusId":"c1","factIds": ids("f", MAX_IDS_PER_REQUEST + 1)}),
    ));
    assert_client_rejection(&over_bound, "over maxIdsPerRequest");
    native.ensure_alive();

    let inexact = native.send(read(
        "memory_get_facts_by_ids",
        json!({"corpusId":"c1","factIds":["f1"],"state":"any"}),
    ));
    assert_client_rejection(&inexact, "extra param");
    native.ensure_alive();
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn find_facts_by_entities_case_folds_with_unicode16_and_orders_by_fact_id() {
    let db = TempDb::new("facts-by-entities");
    seed_fixture(&db);
    let mut native = NativeProcess::spawn(&db.path);

    // Kelvin sign U+212A folds to ASCII "k" under Unicode 16 full lowercase,
    // which ASCII-only lowering would never match. "ÉCOLE" folds to "école".
    let response = native.send(read(
        "memory_find_facts_by_entities",
        json!({"corpusId":"c1","entities":["\u{212A}ELVIN","ÉCOLE"],"state":"any","limit":10}),
    ));
    assert_eq!(response["ok"], json!(true), "{response}");
    assert_eq!(result_ids(&response, "factId"), ["f1", "f2", "f3"]);

    let active = native.send(read(
        "memory_find_facts_by_entities",
        json!({"corpusId":"c1","entities":["kelvin","école"],"state":"active","limit":10}),
    ));
    assert_eq!(result_ids(&active, "factId"), ["f2", "f3"]);

    let truncated = native.send(read(
        "memory_find_facts_by_entities",
        json!({"corpusId":"c1","entities":["KELVIN","école"],"state":"any","limit":2}),
    ));
    assert_eq!(result_ids(&truncated, "factId"), ["f1", "f2"]);

    let again = native.send(read(
        "memory_find_facts_by_entities",
        json!({"corpusId":"c1","entities":["KELVIN","école"],"state":"any","limit":2}),
    ));
    assert_eq!(again["result"], truncated["result"], "order is stable");

    for (context, params) in [
        (
            "no match",
            json!({"corpusId":"c1","entities":["nowhere"],"state":"any","limit":10}),
        ),
        (
            "unknown corpus",
            json!({"corpusId":"nope","entities":["kelvin"],"state":"any","limit":10}),
        ),
        (
            "zero limit",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"any","limit":0}),
        ),
        (
            "empty entities",
            json!({"corpusId":"c1","entities":[],"state":"any","limit":10}),
        ),
    ] {
        let response = native.send(read("memory_find_facts_by_entities", params));
        assert_eq!(response["ok"], json!(true), "{context}: {response}");
        assert_eq!(response["result"], json!([]), "{context}");
    }

    let at_bound = native.send(read(
        "memory_find_facts_by_entities",
        json!({"corpusId":"c1","entities": ids("e", MAX_ENTITIES_PER_REQUEST),"state":"any","limit":MAX_LIMIT}),
    ));
    assert_eq!(at_bound["ok"], json!(true), "{at_bound}");

    for (context, params) in [
        (
            "over maxEntitiesPerRequest",
            json!({"corpusId":"c1","entities": ids("e", MAX_ENTITIES_PER_REQUEST + 1),"state":"any","limit":1}),
        ),
        (
            "over maxLimit",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"any","limit":MAX_LIMIT + 1}),
        ),
        (
            "negative limit",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"any","limit":-1}),
        ),
        (
            "bad state",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"inactive","limit":1}),
        ),
        (
            "missing state",
            json!({"corpusId":"c1","entities":["kelvin"],"limit":1}),
        ),
        (
            "extra param",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"any","limit":1,"x":1}),
        ),
        (
            "non-string entity",
            json!({"corpusId":"c1","entities":[7],"state":"any","limit":1}),
        ),
    ] {
        let response = native.send(read("memory_find_facts_by_entities", params));
        assert_client_rejection(&response, context);
        native.ensure_alive();
    }
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn section_counts_reports_each_section_and_zero_for_unknown_corpus() {
    let db = TempDb::new("section-counts");
    seed_fixture(&db);
    let mut native = NativeProcess::spawn(&db.path);

    let counts = native.send(read("memory_section_counts", json!({"corpusId":"c1"})));
    assert_eq!(counts["ok"], json!(true), "{counts}");
    assert_eq!(
        counts["result"],
        json!({"passages": 3, "facts": 4, "schemas": 1})
    );

    let unknown = native.send(read("memory_section_counts", json!({"corpusId":"nope"})));
    assert_eq!(
        unknown["result"],
        json!({"passages": 0, "facts": 0, "schemas": 0})
    );

    let inexact = native.send(read(
        "memory_section_counts",
        json!({"corpusId":"c1","section":"facts"}),
    ));
    assert_client_rejection(&inexact, "extra param");
    native.ensure_alive();
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn by_ids_reply_at_max_id_count_stays_within_budget_and_over_budget_is_rejected() {
    let db = TempDb::new("reply-budget");
    // 4096 passages of 2 KiB each: the full reply is ~8.4 MB, above the
    // 8 MiB cap; a request for 3072 of them is within it.
    seed_passages(&db, MAX_IDS_PER_REQUEST, 2048);
    let mut native = NativeProcess::spawn(&db.path);

    let within = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds": ids("p", 3072)}),
    ));
    assert_eq!(within["ok"], json!(true), "{}", within["error"]);
    assert_eq!(within["result"].as_array().unwrap().len(), 3072);
    assert!(within.to_string().len() <= MAX_RESPONSE_BYTES);

    let over = native.send(read(
        "memory_get_passages_by_ids",
        json!({"corpusId":"c1","passageIds": ids("p", MAX_IDS_PER_REQUEST)}),
    ));
    assert_client_rejection(&over, "reply over maxResponseBytes");
    assert!(
        over["error"]["message"]
            .as_str()
            .unwrap()
            .contains("byte limit"),
        "{over}"
    );
    native.ensure_alive();

    let counts = native.send(read("memory_section_counts", json!({"corpusId":"c1"})));
    assert_eq!(counts["result"]["passages"], json!(MAX_IDS_PER_REQUEST));
    assert_eq!(native.finish(), Some(0));
}

#[test]
fn reads_never_touch_the_wal_and_see_the_working_state_inside_a_batch() {
    let db = TempDb::new("wal-and-batch");
    seed_fixture(&db);

    let mut reader = NativeProcess::spawn(&db.path);
    let wal_before = std::fs::metadata(db.wal_path()).map(|meta| meta.len()).ok();
    for (method, params) in [
        (
            "memory_get_passages_by_ids",
            json!({"corpusId":"c1","passageIds":["p1"]}),
        ),
        (
            "memory_get_facts_by_ids",
            json!({"corpusId":"c1","factIds":["f1"]}),
        ),
        (
            "memory_find_facts_by_entities",
            json!({"corpusId":"c1","entities":["kelvin"],"state":"any","limit":5}),
        ),
        ("memory_section_counts", json!({"corpusId":"c1"})),
    ] {
        assert_eq!(
            reader.send(read(method, params))["ok"],
            json!(true),
            "{method}"
        );
    }
    let wal_after = std::fs::metadata(db.wal_path()).map(|meta| meta.len()).ok();
    assert_eq!(wal_before, wal_after, "read-only session changed the WAL");
    assert_eq!(reader.finish(), Some(0));
    assert_eq!(
        std::fs::metadata(db.wal_path())
            .map(|meta| meta.len())
            .unwrap_or(0),
        0,
        "read-only session left WAL bytes behind"
    );

    // Documented behaviour, identical to memory_get_schemas_by_ids: on the
    // writer connection an uncommitted upsert is visible to the new reads.
    // Committed-generation isolation for other clients is the owner's lease
    // gate, not the native. The batch is abandoned here, never committed.
    let mut writer = NativeProcess::spawn(&db.path);
    assert_eq!(writer.send(batch_begin(1))["ok"], json!(true));
    assert_eq!(
        writer.send(json!({
            "id": 2,
            "method": "memory_upsert",
            "params": {
                "corpusId": "c1",
                "passages": [{"passageId":"p9","corpusId":"c1","text":"uncommitted"}]
            }
        }))["ok"],
        json!(true)
    );
    let inside = writer.send(json!({
        "id": 3,
        "method": "memory_get_passages_by_ids",
        "params": {"corpusId":"c1","passageIds":["p9"]}
    }));
    assert_eq!(result_ids(&inside, "passageId"), ["p9"]);
    assert_eq!(
        writer.send(json!({"id":4,"method":"memory_section_counts","params":{"corpusId":"c1"}}))["result"]
            ["passages"],
        json!(4)
    );
    drop(writer);
}
