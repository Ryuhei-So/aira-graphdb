//! `memory_delete_document` (#57, literature-hub #614).
//!
//! Real sidecar process, one JSON line per request, temporary directories
//! only. Fixture: documents W and X share schema `s1`, the entity `alcohol`
//! and one fact; schema `s2` and the entities `injury`, `smoking`, `cancer`
//! belong to W alone. W is the first source of `s1`, so its vector must be
//! re-keyed, not dropped.

use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

const C: &str = "c1";
const W: &str = "doc-w";
const X: &str = "doc-x";
const F_W1: &str = "fact:doc-w:s1:Alcohol:causes:Injury";
const F_W2: &str = "fact:doc-w:s2:Smoking:causes:Cancer";
const F_X1: &str = "fact:doc-x:s1:Alcohol:causes:Liver";
const F_SHARED: &str = "fact:shared:s1:Alcohol:causes:Falls";
const P_W: &str = "passage:doc-w:0";
const P_X: &str = "passage:doc-x:0";
const UPDATED_AT: &str = "2026-10-11T00:00:00.000Z";

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
        let dir = std::env::temp_dir().join(format!("aira-graphdb-delete-doc-{label}-{nonce}"));
        std::fs::create_dir(&dir).expect("create temporary database directory");
        let path = dir.join("state.json");
        Self { dir, path }
    }

    fn wal_len(&self) -> u64 {
        std::fs::metadata(self.path.with_extension("agdb.wal")).map_or(0, |m| m.len())
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Native {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Native {
    fn spawn(path: &Path) -> Self {
        Self::spawn_with_env(path, &[])
    }

    fn spawn_with_env(path: &Path, envs: &[(&str, &str)]) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_aira-graphdb-native"));
        command
            .arg("--db")
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (name, value) in envs {
            command.env(name, value);
        }
        let mut child = command.spawn().expect("native sidecar should start");
        let stdin = child.stdin.take().expect("stdin pipe");
        let stdout = BufReader::new(child.stdout.take().expect("stdout pipe"));
        Self {
            child,
            stdin: Some(stdin),
            stdout,
            next_id: 1,
        }
    }

    fn call(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let request = json!({"id": id, "method": method, "params": params});
        let stdin = self.stdin.as_mut().expect("stdin is open");
        stdin
            .write_all(request.to_string().as_bytes())
            .expect("write request line");
        stdin.write_all(b"\n").expect("write newline");
        stdin.flush().expect("flush request");
        let mut response = String::new();
        self.stdout.read_line(&mut response).expect("read response");
        assert!(!response.is_empty(), "native closed stdout on {request}");
        serde_json::from_str(response.trim()).expect("response must be valid json")
    }

    fn ok(&mut self, method: &str, params: Value) -> Value {
        let response = self.call(method, params);
        assert_eq!(response["ok"], json!(true), "{method} failed: {response}");
        response["result"].clone()
    }

    fn begin(&mut self) {
        self.ok("batch_begin", json!({}));
    }

    fn commit(&mut self) -> u64 {
        let prepared = self.ok("batch_prepare_commit", json!({}));
        let token = self.ok("batch_commit", json!({"preparedCommitEvidence": prepared}));
        token["generation"].as_u64().expect("committed generation")
    }

    fn info(&mut self) -> Value {
        self.ok("protocol_info", json!({}))
    }

    fn delete(&mut self, document_id: &str) -> Value {
        self.ok(
            "memory_delete_document",
            json!({"corpusId": C, "documentId": document_id, "updatedAt": UPDATED_AT}),
        )
    }

    fn ensure_alive(&mut self) {
        assert!(
            self.child.try_wait().expect("query child status").is_none(),
            "native sidecar exited"
        );
    }

    fn wait_exit(mut self) -> Option<i32> {
        drop(self.stdin.take());
        let start = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("query child status") {
                return status.code();
            }
            assert!(
                start.elapsed() < Duration::from_secs(30),
                "native did not exit"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Native {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn schema(
    id: &str,
    head: &str,
    tail: &str,
    sources: &[&str],
    facts: &[&str],
    freq: u64,
    state: &str,
) -> Value {
    json!({
        "schemaId": id,
        "corpusId": C,
        "headType": head,
        "relation": "causes",
        "tailType": tail,
        "canonicalKey": format!("{}::causes::{}", head.to_lowercase(), tail.to_lowercase()),
        "aliases": [],
        "frequency": freq,
        "state": state,
        "stabilizationThreshold": 3,
        "factIds": facts,
        "sourceDocumentIds": sources,
        "version": 1,
        "createdAt": "2026-01-01T00:00:00.000Z",
        "updatedAt": "2026-01-01T00:00:00.000Z",
    })
}

fn fact(
    id: &str,
    schema_id: &str,
    head: &str,
    tail: &str,
    sources: &[&str],
    passages: &[&str],
) -> Value {
    json!({
        "factId": id,
        "corpusId": C,
        "schemaId": schema_id,
        "headEntity": head,
        "headType": "Exposure",
        "relation": "causes",
        "tailEntity": tail,
        "tailType": "Outcome",
        "state": "active",
        "passageIds": passages,
        "sourceDocumentIds": sources,
        "confidence": 0.9,
        "createdAt": "2026-01-01T00:00:00.000Z",
        "updatedAt": "2026-01-01T00:00:00.000Z",
    })
}

fn passage(id: &str, document_id: &str, text: &str) -> Value {
    json!({
        "passageId": id,
        "corpusId": C,
        "text": text,
        "normalizedText": text,
        "metadata": {"documentId": document_id, "chunkId": id},
        "factIds": [],
        "entityMentions": [],
    })
}

fn node(node_id: &str, layer: &str, r#ref: Value) -> Value {
    json!({"nodeId": node_id, "corpusId": C, "layer": layer, "ref": r#ref, "label": node_id})
}

fn edge(edge_id: &str, source: &str, target: &str, relation: &str) -> Value {
    json!({
        "edgeId": edge_id,
        "corpusId": C,
        "sourceNodeId": source,
        "targetNodeId": target,
        "relation": relation,
        "weight": 1.0,
        "bridgeKind": null,
    })
}

fn vector(
    id: &str,
    namespace: &str,
    layer: &str,
    document_id: Option<&str>,
    values: [f64; 2],
) -> Value {
    let mut metadata = json!({"nodeId": id, "layer": layer});
    if let Some(document_id) = document_id {
        metadata["documentId"] = json!(document_id);
    }
    json!({"id": id, "corpusId": C, "namespace": namespace, "values": values, "metadata": metadata})
}

fn fact_node_id(fact_id: &str) -> String {
    format!("fact:{fact_id}")
}

fn passage_node_id(passage_id: &str) -> String {
    format!("passage:{passage_id}")
}

fn cooccur(head: &str, tail: &str, fact_id: &str) -> Value {
    edge(
        &format!("entity-cooccur:{head}:{tail}:{fact_id}"),
        &format!("entity:{head}"),
        &format!("entity:{tail}"),
        "entity_cooccurrence",
    )
}

fn mention(entity: &str, passage_id: &str) -> Value {
    edge(
        &format!("entity-mention:{entity}:{passage_id}"),
        &format!("entity:{entity}"),
        &passage_node_id(passage_id),
        "entity_mention",
    )
}

fn instance(schema_id: &str, fact_id: &str) -> Value {
    edge(
        &format!("schema-instance:{schema_id}:{fact_id}"),
        &format!("schema:{schema_id}"),
        &fact_node_id(fact_id),
        "schema_instance",
    )
}

fn evidence(fact_id: &str, passage_id: &str) -> Value {
    edge(
        &format!("fact-evidence:{fact_id}:{passage_id}"),
        &fact_node_id(fact_id),
        &passage_node_id(passage_id),
        "fact_evidence",
    )
}

fn snapshot_schemas() -> (Value, Value) {
    (
        schema(
            "s1",
            "Exposure",
            "Outcome",
            &[W, X],
            &[F_W1, F_X1, F_SHARED],
            3,
            "stable",
        ),
        schema("s2", "Habit", "Disease", &[W], &[F_W2], 1, "pending"),
    )
}

fn snapshot_facts() -> Vec<Value> {
    vec![
        fact(F_W1, "s1", "Alcohol", "Injury", &[W], &[P_W]),
        fact(F_W2, "s2", "Smoking", "Cancer", &[W], &[P_W]),
        fact(F_X1, "s1", "Alcohol", "Liver", &[X], &[P_X]),
        fact(F_SHARED, "s1", "Alcohol", "Falls", &[W, X], &[P_W, P_X]),
    ]
}

/// Graph, vector and lexical part of the fixture. Schema nodes are added by
/// the lane-specific seeding below.
fn seed_graph_vectors_lexical(native: &mut Native) {
    let facts = snapshot_facts();
    let mut nodes: Vec<Value> = facts
        .iter()
        .map(|fact| {
            node(
                &fact_node_id(fact["factId"].as_str().unwrap()),
                "fact",
                fact.clone(),
            )
        })
        .collect();
    nodes.push(node(
        &passage_node_id(P_W),
        "passage",
        passage(P_W, W, "alcohol injury smoking cancer"),
    ));
    nodes.push(node(
        &passage_node_id(P_X),
        "passage",
        passage(P_X, X, "alcohol liver falls"),
    ));
    for entity in ["alcohol", "injury", "smoking", "cancer", "liver", "falls"] {
        nodes.push(node(
            &format!("entity:{entity}"),
            "entity",
            json!({"entityName": entity, "factCount": 7}),
        ));
    }
    native.ok("upsert_nodes", json!({"nodes": nodes}));
    native.ok(
        "upsert_edges",
        json!({"edges": [
            instance("s1", F_W1),
            instance("s2", F_W2),
            instance("s1", F_X1),
            instance("s1", F_SHARED),
            evidence(F_W1, P_W),
            evidence(F_W2, P_W),
            evidence(F_X1, P_X),
            evidence(F_SHARED, P_W),
            evidence(F_SHARED, P_X),
            cooccur("alcohol", "injury", F_W1),
            cooccur("smoking", "cancer", F_W2),
            cooccur("alcohol", "liver", F_X1),
            cooccur("alcohol", "falls", F_SHARED),
            mention("alcohol", P_W),
            mention("injury", P_W),
            mention("smoking", P_W),
            mention("cancer", P_W),
            mention("alcohol", P_X),
            mention("liver", P_X),
            mention("falls", P_W),
            mention("falls", P_X),
            edge("type-bridge:s1:s2", "schema:s1", "schema:s2", "type_based_bridge"),
        ]}),
    );
    native.ok(
        "vector_upsert",
        json!({"records": [
            vector("schema:s1", "schema", "ontology", Some(W), [1.0, 0.0]),
            vector("schema:s2", "schema", "ontology", Some(W), [0.0, 1.0]),
            vector(&fact_node_id(F_W1), "fact", "fact", Some(W), [1.0, 0.1]),
            vector(&fact_node_id(F_W2), "fact", "fact", Some(W), [1.0, 0.2]),
            vector(&fact_node_id(F_X1), "fact", "fact", Some(X), [1.0, 0.3]),
            vector(&fact_node_id(F_SHARED), "fact", "fact", Some(W), [1.0, 0.4]),
            vector(&passage_node_id(P_W), "passage", "passage", Some(W), [0.5, 0.5]),
            vector(&passage_node_id(P_X), "passage", "passage", Some(X), [0.4, 0.6]),
            vector("entity:alcohol", "entity", "entity", None, [0.9, 0.1]),
            vector("entity:injury", "entity", "entity", None, [0.8, 0.2]),
            vector("entity:smoking", "entity", "entity", None, [0.7, 0.3]),
            vector("entity:cancer", "entity", "entity", None, [0.6, 0.4]),
            vector("entity:liver", "entity", "entity", None, [0.5, 0.5]),
            vector("entity:falls", "entity", "entity", None, [0.4, 0.6]),
        ]}),
    );
    native.ok(
        "lexical_index_passages",
        json!({"corpusId": C, "passages": [
            passage(P_W, W, "alcohol injury"),
            passage(P_X, X, "alcohol liver"),
        ]}),
    );
}

/// Legacy lane: full schema objects in the snapshot and as node refs.
fn seed_legacy(native: &mut Native) {
    let (s1, s2) = snapshot_schemas();
    native.begin();
    native.ok(
        "memory_upsert",
        json!({
            "corpusId": C,
            "passages": [passage(P_W, W, "w"), passage(P_X, X, "x")],
            "facts": snapshot_facts(),
            "schemas": [s1.clone(), s2.clone()],
        }),
    );
    native.ok(
        "upsert_nodes",
        json!({"nodes": [node("schema:s1", "ontology", s1), node("schema:s2", "ontology", s2)]}),
    );
    seed_graph_vectors_lexical(native);
    assert_eq!(native.commit(), 1);
}

/// Canonical C1-S lane: schemas created through merge intents, schema nodes
/// hydrated by the native from the snapshot.
fn seed_canonical(native: &mut Native) {
    let (s1, s2) = snapshot_schemas();
    native.begin();
    native.ok(
        "memory_upsert",
        json!({
            "corpusId": C,
            "passages": [passage(P_W, W, "w"), passage(P_X, X, "x")],
            "facts": snapshot_facts(),
            "schemaMerges": [
                {"mode": "create", "expectedAbsent": true, "schema": s1},
                {"mode": "create", "expectedAbsent": true, "schema": s2},
            ],
        }),
    );
    native.ok(
        "upsert_nodes",
        json!({
            "nodes": [],
            "schemaRefHydration": "memory-schema@1",
            "schemaNodeRefs": [
                {"nodeId": "schema:s1", "corpusId": C, "schemaId": "s1", "label": "Exposure causes Outcome"},
                {"nodeId": "schema:s2", "corpusId": C, "schemaId": "s2", "label": "Habit causes Disease"},
            ],
        }),
    );
    seed_graph_vectors_lexical(native);
    assert_eq!(native.commit(), 1);
}

fn expected_delete_counts() -> Value {
    json!({
        "corpusId": C,
        "documentId": W,
        "noop": false,
        "removedPassages": 1,
        "removedFacts": 2,
        "prunedSharedFacts": 1,
        "prunedSchemas": 2,
        "schemaFrequencyAdjusted": 2,
        "removedPassageNodes": 1,
        "removedFactNodes": 2,
        "removedEntityNodes": 3,
        "removedOtherNodes": 0,
        "prunedSharedNodes": 1,
        "prunedOntologyNodes": 1,
        "removedOntologyNodes": 1,
        // instance x2, evidence x3, cooccur x2, mention x5, type bridge x1
        "removedEdges": 13,
        // schema:s2, two W facts, W passage, three orphan entities
        "removedVectors": 7,
        // schema:s1 (W was first source) and the shared fact
        "rekeyedVectors": 2,
        "removedLexicalPassages": 1,
    })
}

fn ids(items: &Value, key: &str) -> Vec<String> {
    let mut ids: Vec<String> = items
        .as_array()
        .expect("array result")
        .iter()
        .map(|item| item[key].as_str().expect("id").to_string())
        .collect();
    ids.sort();
    ids
}

fn vectors(native: &mut Native, namespace: &str) -> Value {
    native.ok(
        "vector_search",
        json!({"corpusId": C, "namespace": namespace, "queryVector": [1.0, 1.0], "topK": 100}),
    )
}

fn find<'a>(items: &'a Value, key: &str, id: &str) -> Option<&'a Value> {
    items
        .as_array()
        .expect("array result")
        .iter()
        .find(|item| item[key] == json!(id))
}

/// Full observable state used to compare before/after restart or recovery.
fn observe(native: &mut Native) -> Value {
    let memory = native.ok("memory_load", json!({"corpusId": C}));
    let nodes = native.ok("get_nodes", json!({"corpusId": C}));
    let edges = native.ok("get_edges", json!({"corpusId": C}));
    let lexical = native.ok(
        "lexical_search",
        json!({"corpusId": C, "query": "alcohol", "topK": 10}),
    );
    let mut vector_sets = serde_json::Map::new();
    for namespace in ["schema", "fact", "passage", "entity"] {
        vector_sets.insert(namespace.to_string(), vectors(native, namespace));
    }
    json!({"memory": memory, "nodes": nodes, "edges": edges, "lexical": lexical, "vectors": vector_sets})
}

fn assert_post_delete_state(native: &mut Native) {
    let memory = native.ok("memory_load", json!({"corpusId": C}));
    assert_eq!(ids(&memory["passages"], "passageId"), vec![P_X.to_string()]);
    assert_eq!(
        ids(&memory["facts"], "factId"),
        vec![F_X1.to_string(), F_SHARED.to_string()]
    );
    let shared = find(&memory["facts"], "factId", F_SHARED).expect("shared fact kept");
    assert_eq!(shared["sourceDocumentIds"], json!([X]));
    assert_eq!(
        shared["passageIds"],
        json!([P_X]),
        "W's passage ids are pruned"
    );
    assert_eq!(shared["updatedAt"], json!(UPDATED_AT));
    let s1 = find(&memory["schemas"], "schemaId", "s1").expect("s1 kept");
    assert_eq!(s1["sourceDocumentIds"], json!([X]));
    assert_eq!(s1["factIds"], json!([F_X1, F_SHARED]));
    assert_eq!(s1["frequency"], json!(2));
    assert_eq!(s1["state"], json!("pending"));
    assert_eq!(s1["updatedAt"], json!(UPDATED_AT));
    // Support 0: the snapshot schema is kept (DeleteDocumentService rule).
    let s2 = find(&memory["schemas"], "schemaId", "s2").expect("s2 kept at support 0");
    assert_eq!(s2["sourceDocumentIds"], json!([]));
    assert_eq!(s2["factIds"], json!([]));
    assert_eq!(s2["frequency"], json!(0));
    assert_eq!(s2["state"], json!("pending"));

    let nodes = native.ok("get_nodes", json!({"corpusId": C}));
    let mut expected_nodes = vec![
        "entity:alcohol".to_string(),
        "entity:falls".to_string(),
        "entity:liver".to_string(),
        fact_node_id(F_SHARED),
        fact_node_id(F_X1),
        passage_node_id(P_X),
        "schema:s1".to_string(),
    ];
    expected_nodes.sort();
    assert_eq!(ids(&nodes, "nodeId"), expected_nodes);
    let schema_node = find(&nodes, "nodeId", "schema:s1").expect("schema node");
    assert_eq!(
        schema_node["ref"], *s1,
        "schema node mirrors the snapshot schema"
    );
    let shared_node = find(&nodes, "nodeId", &fact_node_id(F_SHARED)).expect("shared fact node");
    assert_eq!(
        shared_node["ref"], *shared,
        "shared fact node mirrors the snapshot fact"
    );
    let alcohol = find(&nodes, "nodeId", "entity:alcohol").expect("alcohol");
    assert_eq!(
        alcohol["ref"]["factCount"],
        json!(7),
        "factCount is left unchanged"
    );

    let edges = native.ok("get_edges", json!({"corpusId": C}));
    let mut expected_edges = vec![
        format!("schema-instance:s1:{F_X1}"),
        format!("schema-instance:s1:{F_SHARED}"),
        format!("fact-evidence:{F_X1}:{P_X}"),
        format!("fact-evidence:{F_SHARED}:{P_X}"),
        format!("entity-cooccur:alcohol:liver:{F_X1}"),
        format!("entity-cooccur:alcohol:falls:{F_SHARED}"),
        format!("entity-mention:alcohol:{P_X}"),
        format!("entity-mention:liver:{P_X}"),
        format!("entity-mention:falls:{P_X}"),
    ];
    expected_edges.sort();
    assert_eq!(ids(&edges, "edgeId"), expected_edges);

    let schema_vectors = vectors(native, "schema");
    assert_eq!(ids(&schema_vectors, "id"), vec!["schema:s1".to_string()]);
    let s1_vector = find(&schema_vectors, "id", "schema:s1").unwrap();
    assert_eq!(
        s1_vector["metadata"]["documentId"],
        json!(X),
        "re-keyed to the remaining source"
    );
    // The re-keyed vector keeps its values: cosine([1,0],[1,1]) = 1/sqrt(2).
    let score = s1_vector["score"].as_f64().unwrap();
    assert!(
        (score - std::f64::consts::FRAC_1_SQRT_2).abs() < 1e-9,
        "score {score}"
    );
    let fact_vectors = vectors(native, "fact");
    assert_eq!(
        ids(&fact_vectors, "id"),
        vec![fact_node_id(F_X1), fact_node_id(F_SHARED)]
    );
    assert_eq!(
        find(&fact_vectors, "id", &fact_node_id(F_SHARED)).unwrap()["metadata"]["documentId"],
        json!(X)
    );
    assert_eq!(
        ids(&vectors(native, "passage"), "id"),
        vec![passage_node_id(P_X)]
    );
    assert_eq!(
        ids(&vectors(native, "entity"), "id"),
        vec![
            "entity:alcohol".to_string(),
            "entity:falls".to_string(),
            "entity:liver".to_string()
        ]
    );

    let lexical = native.ok(
        "lexical_search",
        json!({"corpusId": C, "query": "alcohol", "topK": 10}),
    );
    assert_eq!(ids(&lexical, "passageId"), vec![P_X.to_string()]);
}

#[test]
fn legacy_lane_delete_preserves_shared_state_and_survives_restart() {
    let db = TempDb::new("legacy");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    native.begin();
    assert_eq!(native.delete(W), expected_delete_counts());
    assert_post_delete_state(&mut native);
    assert_eq!(native.commit(), 2);
    assert_post_delete_state(&mut native);
    let before = observe(&mut native);
    assert_eq!(native.wait_exit(), Some(0));

    let mut reopened = Native::spawn(&db.path);
    assert_eq!(reopened.info()["generation"], json!(2));
    assert_post_delete_state(&mut reopened);
    assert_eq!(observe(&mut reopened), before);
}

#[test]
fn canonical_lane_delete_then_reprojected_merge_commits_one_generation() {
    let db = TempDb::new("canonical");
    let mut native = Native::spawn(&db.path);
    seed_canonical(&mut native);
    let project = |native: &mut Native| {
        native.ok(
            "memory_get_schemas_by_ids",
            json!({
                "corpusId": C,
                "schemaIds": ["s1"],
                "projection": "canonicalization@1",
                "contributionDocumentId": W,
            }),
        )[0]
        .clone()
    };
    let stale = project(&mut native);
    assert_eq!(stale["contributionPresent"], json!(true));
    assert_eq!(stale["firstSourceDocumentId"], json!(W));

    native.begin();
    assert_eq!(native.delete(W), expected_delete_counts());
    assert_post_delete_state(&mut native);

    // A read inside the active batch sees the post-delete schema.
    let fresh = project(&mut native);
    assert_eq!(fresh["contributionPresent"], json!(false));
    assert_eq!(fresh["firstSourceDocumentId"], json!(X));
    assert_eq!(fresh["frequency"], json!(2));
    assert_eq!(fresh["state"], json!("pending"));
    assert_ne!(fresh["mergeToken"], stale["mergeToken"]);

    // Replace: merge intent rebuilt from the fresh projection.
    let new_fact = "fact:doc-w:s1:Alcohol:causes:Burns";
    native.ok(
        "memory_upsert",
        json!({
            "corpusId": C,
            "passages": [passage("passage:doc-w:0", W, "alcohol burns")],
            "facts": [fact(new_fact, "s1", "Alcohol", "Burns", &[W], &["passage:doc-w:0"])],
            "schemaMerges": [{
                "mode": "merge",
                "schemaId": "s1",
                "expectedMergeToken": fresh["mergeToken"],
                "contributionDocumentId": W,
                "frequencyDelta": 1,
                "desiredState": "stable",
                "stabilizationThreshold": 3,
                "updatedAt": UPDATED_AT,
                "aliasAdditions": [],
                "factIdAdditions": [],
            }],
        }),
    );
    native.ok(
        "upsert_nodes",
        json!({
            "nodes": [node(&fact_node_id(new_fact), "fact", fact(new_fact, "s1", "Alcohol", "Burns", &[W], &["passage:doc-w:0"]))],
            "schemaRefHydration": "memory-schema@1",
            "schemaNodeRefs": [
                {"nodeId": "schema:s1", "corpusId": C, "schemaId": "s1", "label": "Exposure causes Outcome"},
            ],
        }),
    );
    assert_eq!(
        native.commit(),
        2,
        "delete and upsert publish exactly one generation"
    );

    let after = project(&mut native);
    assert_eq!(after["contributionPresent"], json!(true));
    assert_eq!(after["frequency"], json!(3));
    assert_eq!(after["firstSourceDocumentId"], json!(X));
    let nodes = native.ok("get_nodes", json!({"corpusId": C, "layer": "ontology"}));
    assert_eq!(ids(&nodes, "nodeId"), vec!["schema:s1".to_string()]);
    assert_eq!(nodes[0]["ref"]["sourceDocumentIds"], json!([X, W]));
    assert_eq!(native.wait_exit(), Some(0));
}

#[test]
fn absent_document_is_a_reported_noop_and_delete_is_idempotent() {
    let db = TempDb::new("noop");
    let mut native = Native::spawn(&db.path);
    // No snapshot for the corpus at all.
    native.begin();
    let absent = native.delete("doc-absent");
    assert_eq!(absent["noop"], json!(true));
    for (key, value) in absent.as_object().unwrap() {
        if !matches!(key.as_str(), "noop" | "corpusId" | "documentId") {
            assert_eq!(*value, json!(0), "{key}");
        }
    }
    assert_eq!(native.commit(), 1);

    seed_legacy_from(&mut native, 2);
    native.begin();
    assert_eq!(native.delete("doc-absent")["noop"], json!(true));
    assert_eq!(native.delete(W)["noop"], json!(false));
    let again = native.delete(W);
    assert_eq!(
        again["noop"],
        json!(true),
        "second delete is a no-op: {again}"
    );
    assert_eq!(native.commit(), 3);
    assert_post_delete_state(&mut native);
    assert_eq!(native.wait_exit(), Some(0));
}

fn seed_legacy_from(native: &mut Native, expected_generation: u64) {
    let (s1, s2) = snapshot_schemas();
    native.begin();
    native.ok(
        "memory_upsert",
        json!({
            "corpusId": C,
            "passages": [passage(P_W, W, "w"), passage(P_X, X, "x")],
            "facts": snapshot_facts(),
            "schemas": [s1.clone(), s2.clone()],
        }),
    );
    native.ok(
        "upsert_nodes",
        json!({"nodes": [node("schema:s1", "ontology", s1), node("schema:s2", "ontology", s2)]}),
    );
    seed_graph_vectors_lexical(native);
    assert_eq!(native.commit(), expected_generation);
}

#[test]
fn malformed_params_and_stored_shapes_are_rejected_before_the_wal() {
    let malformed = [
        json!({"corpusId": C, "documentId": W}),
        json!({"corpusId": C, "documentId": W, "updatedAt": UPDATED_AT, "extra": 1}),
        json!({"corpusId": C, "documentId": "", "updatedAt": UPDATED_AT}),
        json!({"corpusId": C, "documentId": 7, "updatedAt": UPDATED_AT}),
        json!({"corpusId": C, "documentId": W, "updatedAt": "x".repeat(129)}),
    ];
    for params in malformed {
        let db = TempDb::new("params");
        let mut native = Native::spawn(&db.path);
        native.begin();
        let response = native.call("memory_delete_document", params.clone());
        assert_eq!(response["ok"], json!(false), "{params}");
        assert_eq!(response["error"]["failureClass"], json!("CLIENT_INPUT"));
        // A rejected mutation is fatal for this native process.
        assert_eq!(native.wait_exit(), Some(1));
        assert_eq!(
            db.wal_len(),
            0,
            "rejection happened before the WAL: {params}"
        );
    }

    // A stored snapshot item that memory_upsert would also refuse.
    let db = TempDb::new("stored");
    let mut native = Native::spawn(&db.path);
    native.begin();
    native.ok(
        "memory_save",
        json!({"snapshot": {"corpusId": C, "passages": [], "facts": ["not-an-object"], "schemas": []}}),
    );
    assert_eq!(native.commit(), 1);
    native.begin();
    let response = native.call(
        "memory_delete_document",
        json!({"corpusId": C, "documentId": W, "updatedAt": UPDATED_AT}),
    );
    assert_eq!(response["ok"], json!(false));
    assert_eq!(native.wait_exit(), Some(1));
    assert_eq!(db.wal_len(), 0);
    let mut reopened = Native::spawn(&db.path);
    let info = reopened.info();
    assert_eq!(info["state"], json!("idle"));
    assert_eq!(info["generation"], json!(1));
}

fn recover(path: &Path) -> Native {
    let mut native = Native::spawn(path);
    let info = native.info();
    assert_eq!(info["state"], json!("recoveryPending"), "{info}");
    let recovery = info["recovery"].clone();
    native.ok(
        "recovery_discard",
        json!({"baseGeneration": recovery["baseGeneration"], "walDigest": recovery["walDigest"]}),
    );
    native
}

#[test]
fn abort_or_failure_before_commit_leaves_the_prior_generation_intact() {
    // EOF after the delete, before commit.
    let db = TempDb::new("abort-eof");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    let prior = observe(&mut native);
    native.begin();
    native.delete(W);
    assert_eq!(native.wait_exit(), Some(0));
    let mut recovered = recover(&db.path);
    assert_eq!(recovered.info()["generation"], json!(1));
    assert_eq!(observe(&mut recovered), prior);
    drop(recovered);

    // The following upsert fails after the delete: the batch is fatal.
    let db = TempDb::new("abort-upsert");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    let prior = observe(&mut native);
    native.begin();
    native.delete(W);
    let failed = native.call(
        "memory_upsert",
        json!({"corpusId": C, "schemas": [], "schemaMerges": []}),
    );
    assert_eq!(failed["ok"], json!(false));
    assert_eq!(native.wait_exit(), Some(1));
    let mut recovered = recover(&db.path);
    assert_eq!(recovered.info()["generation"], json!(1));
    assert_eq!(observe(&mut recovered), prior);
}

#[test]
fn delete_persists_across_delta_segments_and_compaction() {
    let db = TempDb::new("compaction");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    // A delete-only commit publishes a delta segment that carries every
    // surviving reference, including the re-keyed one.
    native.begin();
    native.delete(W);
    assert_eq!(native.commit(), 2);
    let delta_state = observe(&mut native);
    assert_eq!(
        native.info()["vectorBlobLineage"].as_array().unwrap().len(),
        2
    );
    assert_eq!(native.wait_exit(), Some(0));

    let mut reopened = Native::spawn(&db.path);
    assert_post_delete_state(&mut reopened);
    assert_eq!(observe(&mut reopened), delta_state);
    assert_eq!(reopened.wait_exit(), Some(0));

    // Force compaction on the next commit: only the live set is rewritten.
    let mut compacting = Native::spawn_with_env(
        &db.path,
        &[("AGDB_NATIVE_TEST_COMPACT_AT_LINEAGE_BYTES", "1")],
    );
    compacting.begin();
    assert_eq!(compacting.delete("doc-absent")["noop"], json!(true));
    assert_eq!(compacting.commit(), 3);
    let info = compacting.info();
    assert_eq!(info["vectorBlobCompactions"]["count"], json!(1), "{info}");
    assert_eq!(info["vectorBlobLineage"].as_array().unwrap().len(), 1);
    assert_eq!(compacting.wait_exit(), Some(0));

    let mut reopened = Native::spawn(&db.path);
    assert_eq!(reopened.info()["generation"], json!(3));
    assert_post_delete_state(&mut reopened);
    assert_eq!(observe(&mut reopened), delta_state);
}

#[test]
fn protocol_info_advertises_memory_delete_document_as_a_wal_mutation() {
    let db = TempDb::new("protocol");
    let mut native = Native::spawn(&db.path);
    let info = native.info();
    let method = info["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|method| method["name"] == json!("memory_delete_document"))
        .expect("memory_delete_document is advertised")
        .clone();
    assert_eq!(method["classification"], json!("mutation"));
    assert_eq!(method["wal"], json!(true));
}

/// M1: a non-ontology node that happens to use a `schema:` id must follow its
/// own layer rule and never reach the ontology step (it used to panic after
/// the WAL append).
#[test]
fn non_ontology_node_with_a_schema_id_is_removed_without_panicking() {
    let db = TempDb::new("schema-id-fact-layer");
    let mut native = Native::spawn(&db.path);
    native.begin();
    native.ok(
        "memory_upsert",
        json!({"corpusId": C, "schemas": [schema("s1", "A", "B", &[W], &[], 1, "pending")]}),
    );
    native.ok(
        "upsert_nodes",
        json!({"nodes": [node("schema:s1", "fact", json!({"sourceDocumentIds": [W]}))]}),
    );
    assert_eq!(native.commit(), 1);

    native.begin();
    let result = native.delete(W);
    assert_eq!(result["removedFactNodes"], json!(1), "{result}");
    assert_eq!(result["removedOntologyNodes"], json!(0));
    assert_eq!(result["prunedSchemas"], json!(1));
    native.ensure_alive();
    assert_eq!(native.commit(), 2);
    let nodes = native.ok("get_nodes", json!({"corpusId": C}));
    assert_eq!(nodes, json!([]));
    assert_eq!(native.wait_exit(), Some(0));
}

/// An ontology node with no snapshot schema is pruned in place, or removed
/// with its vector at support 0.
#[test]
fn graph_only_ontology_nodes_are_pruned_or_removed() {
    let db = TempDb::new("graph-only-ontology");
    let mut native = Native::spawn(&db.path);
    native.begin();
    native.ok(
        "upsert_nodes",
        json!({"nodes": [
            node("schema:g1", "ontology", json!({"schemaId": "g1", "sourceDocumentIds": [W, X], "factIds": [F_W1, F_X1]})),
            node("schema:g2", "ontology", json!({"schemaId": "g2", "sourceDocumentIds": [W], "factIds": [F_W2]})),
            node(&fact_node_id(F_W1), "fact", fact(F_W1, "g1", "Alcohol", "Injury", &[W], &[P_W])),
        ]}),
    );
    native.ok(
        "upsert_edges",
        json!({"edges": [
            edge("schema-instance:g1:x", "schema:g1", &fact_node_id(F_W1), "schema_instance"),
            edge("type-bridge:g1:g2", "schema:g1", "schema:g2", "type_based_bridge"),
        ]}),
    );
    native.ok(
        "vector_upsert",
        json!({"records": [
            vector("schema:g1", "schema", "ontology", Some(W), [1.0, 0.0]),
            vector("schema:g2", "schema", "ontology", Some(W), [0.0, 1.0]),
        ]}),
    );
    assert_eq!(native.commit(), 1);

    native.begin();
    let result = native.delete(W);
    assert_eq!(result["prunedOntologyNodes"], json!(1), "{result}");
    assert_eq!(result["removedOntologyNodes"], json!(1));
    assert_eq!(result["removedFactNodes"], json!(1));
    assert_eq!(result["rekeyedVectors"], json!(1));
    assert_eq!(result["removedVectors"], json!(1));
    assert_eq!(result["removedEdges"], json!(2));
    assert_eq!(native.commit(), 2);

    let nodes = native.ok("get_nodes", json!({"corpusId": C}));
    assert_eq!(ids(&nodes, "nodeId"), vec!["schema:g1".to_string()]);
    assert_eq!(nodes[0]["ref"]["sourceDocumentIds"], json!([X]));
    assert_eq!(nodes[0]["ref"]["factIds"], json!([F_X1]));
    assert_eq!(nodes[0]["ref"]["updatedAt"], json!(UPDATED_AT));
    let schema_vectors = vectors(&mut native, "schema");
    assert_eq!(ids(&schema_vectors, "id"), vec!["schema:g1".to_string()]);
    assert_eq!(schema_vectors[0]["metadata"]["documentId"], json!(X));
    assert_eq!(native.ok("get_edges", json!({"corpusId": C})), json!([]));
    assert_eq!(native.wait_exit(), Some(0));
}

fn observe_corpus(native: &mut Native, corpus: &str) -> Value {
    let mut out = serde_json::Map::new();
    out.insert(
        "memory".into(),
        native.ok("memory_load", json!({"corpusId": corpus})),
    );
    out.insert(
        "nodes".into(),
        native.ok("get_nodes", json!({"corpusId": corpus})),
    );
    out.insert(
        "edges".into(),
        native.ok("get_edges", json!({"corpusId": corpus})),
    );
    out.insert(
        "lexical".into(),
        native.ok(
            "lexical_search",
            json!({"corpusId": corpus, "query": "alcohol", "topK": 10}),
        ),
    );
    for namespace in ["schema", "fact", "passage", "entity"] {
        out.insert(
            namespace.into(),
            native.ok(
                "vector_search",
                json!({"corpusId": corpus, "namespace": namespace, "queryVector": [1.0, 1.0], "topK": 100}),
            ),
        );
    }
    Value::Object(out)
}

/// Deleting W in one corpus never touches the same document id elsewhere.
#[test]
fn delete_is_isolated_to_its_corpus() {
    const C2: &str = "c2";
    let db = TempDb::new("isolation");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    native.begin();
    let mut s1 = schema("s1", "Exposure", "Outcome", &[W], &["f2"], 3, "stable");
    s1["corpusId"] = json!(C2);
    let mut p = passage(P_W, W, "alcohol in c2");
    p["corpusId"] = json!(C2);
    native.ok(
        "memory_upsert",
        json!({
            "corpusId": C2,
            "passages": [p.clone()],
            "facts": [{"factId": "f2", "corpusId": C2, "schemaId": "s1", "state": "active",
                       "headEntity": "Alcohol", "tailEntity": "Injury",
                       "sourceDocumentIds": [W], "passageIds": [P_W]}],
            "schemas": [s1.clone()],
        }),
    );
    let c2_node = |node_id: &str, layer: &str, r#ref: Value| json!({"nodeId": node_id, "corpusId": C2, "layer": layer, "ref": r#ref, "label": node_id});
    native.ok(
        "upsert_nodes",
        json!({"nodes": [
            c2_node("schema:s1", "ontology", s1),
            c2_node(&passage_node_id(P_W), "passage", p.clone()),
            c2_node("entity:alcohol", "entity", json!({"entityName": "alcohol", "factCount": 1})),
        ]}),
    );
    native.ok(
        "upsert_edges",
        json!({"edges": [{
            "edgeId": format!("entity-mention:alcohol:{P_W}"), "corpusId": C2,
            "sourceNodeId": "entity:alcohol", "targetNodeId": passage_node_id(P_W),
            "relation": "entity_mention", "weight": 1.0, "bridgeKind": null,
        }]}),
    );
    let c2_vector = |id: &str, namespace: &str, layer: &str, document_id: Option<&str>| {
        let mut record = vector(id, namespace, layer, document_id, [0.3, 0.7]);
        record["corpusId"] = json!(C2);
        record
    };
    native.ok(
        "vector_upsert",
        json!({"records": [
            c2_vector("schema:s1", "schema", "ontology", Some(W)),
            c2_vector(&passage_node_id(P_W), "passage", "passage", Some(W)),
            c2_vector("entity:alcohol", "entity", "entity", None),
        ]}),
    );
    native.ok(
        "lexical_index_passages",
        json!({"corpusId": C2, "passages": [p]}),
    );
    assert_eq!(native.commit(), 2);
    let before = observe_corpus(&mut native, C2);
    assert_eq!(before["schema"][0]["metadata"]["documentId"], json!(W));

    native.begin();
    assert_eq!(native.delete(W), expected_delete_counts());
    assert_eq!(native.commit(), 3);
    assert_post_delete_state(&mut native);
    assert_eq!(observe_corpus(&mut native, C2), before, "c2 is untouched");
    assert_eq!(native.wait_exit(), Some(0));
}

/// UNSUPPORTED BEHAVIOUR, documented on purpose (consumer contract). A
/// legacy-lane replace that upserts full schemas read before the delete
/// silently restores W's pruned schema state, because legacy schema writes
/// replace whole objects without CAS. Replace is supported on the canonical
/// lane only; a legacy-lane replace must rebuild finalSchemas after the
/// delete, or must not be used.
#[test]
fn unsupported_legacy_stale_schema_upsert_after_delete_reintroduces_w() {
    let db = TempDb::new("legacy-hazard");
    let mut native = Native::spawn(&db.path);
    seed_legacy(&mut native);
    let (stale_s1, _) = snapshot_schemas();
    native.begin();
    native.delete(W);
    native.ok(
        "memory_upsert",
        json!({"corpusId": C, "schemas": [stale_s1]}),
    );
    assert_eq!(native.commit(), 2);
    let memory = native.ok("memory_load", json!({"corpusId": C}));
    let s1 = find(&memory["schemas"], "schemaId", "s1").unwrap();
    assert_eq!(
        s1["sourceDocumentIds"],
        json!([W, X]),
        "stale write restores W"
    );
    assert_eq!(
        s1["factIds"],
        json!([F_W1, F_X1, F_SHARED]),
        "and W's fact ids"
    );
    assert_eq!(s1["frequency"], json!(3));
}
