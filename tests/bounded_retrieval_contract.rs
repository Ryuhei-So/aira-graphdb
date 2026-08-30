use aira_graphdb::bounded_retrieval_contract as contract;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CONTRACT_DIR: &str = "spec/contracts/bounded-retrieval";
const CONTRACT_FILE: &str = "bounded-retrieval-contract.json";
const FIXTURE_FILE: &str = "bounded-retrieval-fixture.json";
const MANIFEST_FILE: &str = "bounded-retrieval-fixture.manifest.json";
const PIN_FILE: &str = "bounded-retrieval-pin.json";

fn fixture() -> Value {
    serde_json::from_slice(include_bytes!(
        "../spec/contracts/bounded-retrieval/bounded-retrieval-fixture.json"
    ))
    .expect("fixture JSON")
}

fn fixture_exchange(method: &str) -> (Value, Value) {
    let exchange = fixture()["exchanges"][method].clone();
    (exchange["request"].clone(), exchange["result"].clone())
}

fn normalize(dependency: &str, value: &str) -> Option<String> {
    (dependency == contract::NORMALIZATION_DIGEST).then(|| value.to_lowercase())
}

struct TestDir(PathBuf);

impl TestDir {
    fn copy_from(source: &Path) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "aira-graphdb-bounded-retrieval-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create test directory");
        for entry in fs::read_dir(source).expect("read source directory") {
            let entry = entry.expect("read source entry");
            fs::copy(entry.path(), path.join(entry.file_name())).expect("copy source artifact");
        }
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn write_json(path: &Path, value: &Value) {
    fs::write(
        path,
        serde_json::to_vec_pretty(value).expect("serialize test JSON"),
    )
    .expect("write test JSON");
}

#[test]
fn retrieval_artifact_is_exactly_pinned_and_embedded() {
    contract::verify_artifact_dir(Path::new(CONTRACT_DIR)).expect("closed retrieval pin");
    contract::verify_embedded_artifacts().expect("embedded retrieval pin");

    let info = contract::protocol_info().expect("protocol metadata");
    assert_eq!(info.contract_sha256, contract::CONTRACT_SHA256);
    assert_eq!(info.manifest_sha256, contract::RETRIEVAL_MANIFEST_SHA256);
    assert_eq!(info.source_commit, contract::SOURCE_COMMIT);
    assert_eq!(
        info.methods
            .iter()
            .map(|method| method.name.as_str())
            .collect::<Vec<_>>(),
        contract::METHODS
    );
}

#[test]
fn closed_file_set_rejects_missing_extra_symlink_and_noncanonical_paths() {
    let source = Path::new(CONTRACT_DIR);

    let missing = TestDir::copy_from(source);
    fs::remove_file(missing.0.join(FIXTURE_FILE)).expect("remove fixture");
    assert!(contract::verify_artifact_dir(&missing.0).is_err());

    let extra = TestDir::copy_from(source);
    fs::write(extra.0.join("unexpected.json"), b"{}").expect("write extra file");
    assert!(contract::verify_artifact_dir(&extra.0).is_err());

    let traversal = TestDir::copy_from(source);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(traversal.0.join(PIN_FILE)).expect("read pin"))
            .expect("parse pin");
    pin["artifacts"][0]["localFile"] = json!("../escape.json");
    write_json(&traversal.0.join(PIN_FILE), &pin);
    assert!(contract::verify_artifact_dir(&traversal.0).is_err());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;
        let linked = TestDir::copy_from(source);
        let path = linked.0.join(CONTRACT_FILE);
        fs::remove_file(&path).expect("remove contract");
        symlink(FIXTURE_FILE, &path).expect("create symlink");
        assert!(contract::verify_artifact_dir(&linked.0).is_err());
    }
}

#[test]
fn pin_hash_bytes_version_and_manifest_cross_links_fail_closed() {
    let source = Path::new(CONTRACT_DIR);

    let modified = TestDir::copy_from(source);
    let mut bytes = fs::read(modified.0.join(CONTRACT_FILE)).expect("read contract");
    bytes[0] ^= 1;
    fs::write(modified.0.join(CONTRACT_FILE), bytes).expect("modify contract");
    assert!(contract::verify_artifact_dir(&modified.0).is_err());

    let bad_pin_version = TestDir::copy_from(source);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(bad_pin_version.0.join(PIN_FILE)).expect("read pin"))
            .expect("parse pin");
    pin["pinVersion"] = json!("future@2");
    write_json(&bad_pin_version.0.join(PIN_FILE), &pin);
    assert!(contract::verify_artifact_dir(&bad_pin_version.0).is_err());

    let bad_manifest = TestDir::copy_from(source);
    let manifest_path = bad_manifest.0.join(MANIFEST_FILE);
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    manifest["operationSemanticDigests"][contract::CANDIDATE_SEARCH]["sha256"] = json!("00");
    write_json(&manifest_path, &manifest);
    assert!(contract::verify_artifact_dir(&bad_manifest.0).is_err());
    assert!(contract::verify_manifest_bytes(&serde_json::to_vec(&manifest).unwrap()).is_err());

    let unknown_dependency = TestDir::copy_from(source);
    let mut manifest: Value = serde_json::from_slice(
        &fs::read(unknown_dependency.0.join(MANIFEST_FILE)).expect("read manifest"),
    )
    .expect("parse manifest");
    manifest["dependencies"][0]["id"] = json!("unknown-dependency@99");
    write_json(&unknown_dependency.0.join(MANIFEST_FILE), &manifest);
    assert!(contract::verify_artifact_dir(&unknown_dependency.0).is_err());
}

#[test]
fn unknown_schema_ir_and_dependency_are_rejected_before_semantics() {
    let mut unknown_schema: Value = serde_json::from_slice(include_bytes!(
        "../spec/contracts/bounded-retrieval/bounded-retrieval-contract.json"
    ))
    .expect("contract JSON");
    unknown_schema["operations"][contract::CANDIDATE_SEARCH]["request"]["fields"]["corpusId"]["kind"] =
        json!("futureSchema");
    let bytes = serde_json::to_vec(&unknown_schema).expect("serialize schema mutation");
    assert!(contract::parse_producer_contract(&bytes).is_err());

    let mut unknown_ir: Value = serde_json::from_slice(include_bytes!(
        "../spec/contracts/bounded-retrieval/bounded-retrieval-contract.json"
    ))
    .expect("contract JSON");
    unknown_ir["operations"][contract::CANDIDATE_SEARCH]["refinement"]["requestAssertions"][0]["value"]
        ["value"]["op"] = json!("futureOpcode");
    let bytes = serde_json::to_vec(&unknown_ir).expect("serialize IR mutation");
    assert!(contract::parse_producer_contract(&bytes).is_err());

    let mut unknown_dependency: Value = serde_json::from_slice(include_bytes!(
        "../spec/contracts/bounded-retrieval/bounded-retrieval-contract.json"
    ))
    .expect("contract JSON");
    unknown_dependency["operations"][contract::FACT_EXPAND]["result"]["fields"]["facts"]["items"]
        ["fields"]["fact"]["dependency"] = json!("unknown@1");
    let bytes = serde_json::to_vec(&unknown_dependency).expect("serialize dependency mutation");
    assert!(contract::parse_producer_contract(&bytes).is_err());
}

#[test]
fn pinned_producer_fixture_validates_through_schema_and_ir() {
    for method in contract::METHODS {
        let (request, result) = fixture_exchange(method);
        contract::validate_semantic_request(method, &request).expect("fixture request");
        contract::validate_semantic_exchange(method, &request, &result, &normalize)
            .expect("fixture exchange");
    }
}

#[test]
fn producer_slot_domain_digest_score_and_result_cross_links_fail_closed() {
    let (candidate, candidate_result) = fixture_exchange(contract::CANDIDATE_SEARCH);
    let mut wrong_order = candidate.clone();
    wrong_order["slots"].as_array_mut().unwrap().swap(0, 1);
    assert!(contract::validate_semantic_request(contract::CANDIDATE_SEARCH, &wrong_order).is_err());

    let mut wrong_cardinality = candidate.clone();
    wrong_cardinality["slots"].as_array_mut().unwrap().pop();
    assert!(
        contract::validate_semantic_request(contract::CANDIDATE_SEARCH, &wrong_cardinality)
            .is_err()
    );

    for (field, value) in [("slotId", json!("wrong")), ("namespace", json!("wrong"))] {
        let mut wrong = candidate.clone();
        wrong["slots"][0][field] = value;
        assert!(
            contract::validate_semantic_request(contract::CANDIDATE_SEARCH, &wrong).is_err(),
            "candidate {field} must be rejected"
        );
    }
    let mut empty_corpus = candidate.clone();
    empty_corpus["corpusId"] = json!("");
    assert!(
        contract::validate_semantic_request(contract::CANDIDATE_SEARCH, &empty_corpus).is_err()
    );
    let mut unknown_param = candidate.clone();
    unknown_param["unexpected"] = json!(true);
    assert!(
        contract::validate_semantic_request(contract::CANDIDATE_SEARCH, &unknown_param).is_err()
    );

    for score in [-1.000_001, 1.000_001] {
        let mut bad_result = candidate_result.clone();
        bad_result["slots"][0]["hits"][0]["score"] = json!(score);
        assert!(
            contract::validate_semantic_exchange(
                contract::CANDIDATE_SEARCH,
                &candidate,
                &bad_result,
                &normalize,
            )
            .is_err()
        );
    }
    let mut bad_id = candidate_result.clone();
    bad_id["slots"][0]["hits"][0]["id"] = json!("fact:fixture-passage-1");
    assert!(
        contract::validate_semantic_exchange(
            contract::CANDIDATE_SEARCH,
            &candidate,
            &bad_id,
            &normalize,
        )
        .is_err()
    );
    let mut duplicate = candidate_result.clone();
    let hit = duplicate["slots"][0]["hits"][0].clone();
    duplicate["slots"][0]["hits"] = json!([hit.clone(), hit]);
    assert!(
        contract::validate_semantic_exchange(
            contract::CANDIDATE_SEARCH,
            &candidate,
            &duplicate,
            &normalize,
        )
        .is_err()
    );

    let (mut fact, fact_result) = fixture_exchange(contract::FACT_EXPAND);
    fact["plan"]["normalizationContractDigest"] = json!("unsupported@1");
    assert!(contract::validate_semantic_request(contract::FACT_EXPAND, &fact).is_err());
    let (mut fact, _) = fixture_exchange(contract::FACT_EXPAND);
    fact["plan"]["seedEntities"][0]["key"] = json!("");
    assert!(contract::validate_semantic_request(contract::FACT_EXPAND, &fact).is_err());
    let mut malformed_fact = fact_result.clone();
    malformed_fact["facts"][0]["fact"]
        .as_object_mut()
        .unwrap()
        .remove("factId");
    assert!(
        contract::validate_semantic_exchange(
            contract::FACT_EXPAND,
            &fixture_exchange(contract::FACT_EXPAND).0,
            &malformed_fact,
            &normalize,
        )
        .is_err()
    );

    let (mut ppr, ppr_result) = fixture_exchange(contract::PPR_MATERIALIZE);
    ppr["plan"]["seeds"][0]["score"] = json!(-1);
    contract::validate_semantic_request(contract::PPR_MATERIALIZE, &ppr)
        .expect("signed PPR seed scores are algebraically valid");
    let duplicate_seed = ppr["plan"]["seeds"][0].clone();
    ppr["plan"]["seeds"]
        .as_array_mut()
        .unwrap()
        .push(duplicate_seed);
    assert!(contract::validate_semantic_request(contract::PPR_MATERIALIZE, &ppr).is_err());
    let mut bad_rank = ppr_result.clone();
    bad_rank["rankedFacts"][0]["rank"] = json!(2);
    assert!(
        contract::validate_semantic_exchange(
            contract::PPR_MATERIALIZE,
            &fixture_exchange(contract::PPR_MATERIALIZE).0,
            &bad_rank,
            &normalize,
        )
        .is_err()
    );

    assert!(serde_json::from_str::<Value>(r#"{"score":NaN}"#).is_err());
}

#[test]
fn request_envelope_is_complete_and_reader_admission_is_fail_closed() {
    let (params, _) = fixture_exchange(contract::CANDIDATE_SEARCH);
    let frame = serde_json::to_vec(&json!({
        "id": 7,
        "method": contract::CANDIDATE_SEARCH,
        "expectedGeneration": 12,
        "remainingBudgetMs": 60_000,
        "params": params,
    }))
    .expect("request frame")
    .into_iter()
    .chain(std::iter::once(b'\n'))
    .collect::<Vec<_>>();
    let request = contract::parse_request_frame(&frame).expect("complete request");
    let request_json = serde_json::to_value(&request).expect("request envelope JSON");
    assert_eq!(
        request_json
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        [
            "expectedGeneration",
            "id",
            "method",
            "params",
            "remainingBudgetMs"
        ]
        .into_iter()
        .map(String::from)
        .collect::<BTreeSet<_>>()
    );
    let admission = contract::admit_request(&request, 12, contract::ReaderState::Idle)
        .expect("idle current generation admission");
    assert_eq!(
        admission.native_remainder_ms,
        contract::MAX_OPERATION_DEADLINE_MS
    );
    assert_eq!(admission.resource_usage.search_slots, 3);

    let mut unknown_field: Value = serde_json::from_slice(&frame[..frame.len() - 1]).unwrap();
    unknown_field["role"] = json!("caller");
    let unknown_frame = serde_json::to_vec(&unknown_field)
        .unwrap()
        .into_iter()
        .chain(std::iter::once(b'\n'))
        .collect::<Vec<_>>();
    assert!(contract::parse_request_frame(&unknown_frame).is_err());

    assert!(contract::parse_request_frame(&frame[..frame.len() - 1]).is_err());
    assert!(contract::admit_request(&request, 13, contract::ReaderState::Idle).is_err());
    assert!(contract::admit_request(&request, 12, contract::ReaderState::RecoveryPending).is_err());
    assert!(contract::admit_request(&request, 12, contract::ReaderState::Busy).is_err());

    let mut future = request.clone();
    future.expected_generation = contract::MAX_SAFE_GENERATION + 1;
    assert!(contract::admit_request(&future, 12, contract::ReaderState::Idle).is_err());

    let too_large = vec![b'x'; contract::MAX_INPUT_FRAME_BYTES as usize + 1];
    assert!(contract::parse_request_frame(&too_large).is_err());
}

#[test]
fn every_resource_cap_rejects_only_after_the_boundary() {
    let caps = [
        ("vector_dimensions", contract::MAX_VECTOR_DIMENSIONS),
        ("vector_comparisons", contract::MAX_VECTOR_COMPARISONS),
        ("search_slots", contract::MAX_SEARCH_SLOTS),
        ("facts_inspected", contract::MAX_FACTS_INSPECTED),
        ("graph_nodes", contract::MAX_GRAPH_NODES),
        ("graph_edges", contract::MAX_GRAPH_EDGES),
        ("iterations", contract::MAX_ITERATIONS),
        ("search_result_limit", contract::MAX_SEARCH_RESULT_LIMIT),
        ("expansion_results", contract::MAX_EXPANSION_RESULTS),
        ("seeds", contract::MAX_SEEDS),
        ("returned_passages", contract::MAX_RETURNED_PASSAGES),
        ("returned_facts", contract::MAX_RETURNED_FACTS),
        ("combined_objects", contract::MAX_COMBINED_OBJECTS),
    ];
    for (field, maximum) in caps {
        let mut usage = contract::ResourceUsage::default();
        match field {
            "vector_dimensions" => usage.vector_dimensions = maximum,
            "vector_comparisons" => usage.vector_comparisons = maximum,
            "search_slots" => usage.search_slots = maximum,
            "facts_inspected" => usage.facts_inspected = maximum,
            "graph_nodes" => usage.graph_nodes = maximum,
            "graph_edges" => usage.graph_edges = maximum,
            "iterations" => usage.iterations = maximum,
            "search_result_limit" => usage.search_result_limit = maximum,
            "expansion_results" => usage.expansion_results = maximum,
            "seeds" => usage.seeds = maximum,
            "returned_passages" => usage.returned_passages = maximum,
            "returned_facts" => usage.returned_facts = maximum,
            "combined_objects" => usage.combined_objects = maximum,
            _ => unreachable!(),
        }
        usage.check().expect(field);
        match field {
            "vector_dimensions" => usage.vector_dimensions += 1,
            "vector_comparisons" => usage.vector_comparisons += 1,
            "search_slots" => usage.search_slots += 1,
            "facts_inspected" => usage.facts_inspected += 1,
            "graph_nodes" => usage.graph_nodes += 1,
            "graph_edges" => usage.graph_edges += 1,
            "iterations" => usage.iterations += 1,
            "search_result_limit" => usage.search_result_limit += 1,
            "expansion_results" => usage.expansion_results += 1,
            "seeds" => usage.seeds += 1,
            "returned_passages" => usage.returned_passages += 1,
            "returned_facts" => usage.returned_facts += 1,
            "combined_objects" => usage.combined_objects += 1,
            _ => unreachable!(),
        }
        assert!(usage.check().is_err(), "{field} + 1 must fail");
    }
}

#[test]
fn operation_specific_request_caps_are_bounded() {
    let (mut candidate, _) = fixture_exchange(contract::CANDIDATE_SEARCH);
    for slot in candidate["slots"].as_array_mut().unwrap() {
        slot["queryVector"] = json!(vec![0.0; contract::MAX_VECTOR_DIMENSIONS as usize]);
    }
    contract::ResourceUsage::for_request(contract::CANDIDATE_SEARCH, &candidate)
        .expect("dimension boundary");
    candidate["slots"][0]["queryVector"] =
        json!(vec![0.0; contract::MAX_VECTOR_DIMENSIONS as usize + 1]);
    assert!(contract::ResourceUsage::for_request(contract::CANDIDATE_SEARCH, &candidate).is_err());

    let (mut fact, _) = fixture_exchange(contract::FACT_EXPAND);
    fact["plan"]["seedEntities"] = json!(
        (0..contract::MAX_SEEDS)
            .map(|index| json!({"key": format!("k{index}"), "score": 1}))
            .collect::<Vec<_>>()
    );
    fact["plan"]["limit"] = json!(contract::MAX_EXPANSION_RESULTS);
    contract::validate_semantic_request(contract::FACT_EXPAND, &fact).expect("fact boundary");
    contract::ResourceUsage::for_request(contract::FACT_EXPAND, &fact).expect("fact caps");
    fact["plan"]["seedEntities"]
        .as_array_mut()
        .unwrap()
        .push(json!({"key": "over", "score": 1}));
    assert!(contract::ResourceUsage::for_request(contract::FACT_EXPAND, &fact).is_err());

    let (mut ppr, _) = fixture_exchange(contract::PPR_MATERIALIZE);
    ppr["plan"]["seeds"] = json!(
        (0..contract::MAX_SEEDS)
            .map(|index| json!({"nodeId": format!("fact:k{index}"), "score": 1}))
            .collect::<Vec<_>>()
    );
    ppr["plan"]["maxIterations"] = json!(contract::MAX_ITERATIONS);
    ppr["plan"]["passageLimit"] = json!(contract::MAX_RETURNED_PASSAGES);
    ppr["plan"]["entityLimit"] = json!(contract::MAX_RETURNED_FACTS);
    contract::validate_semantic_request(contract::PPR_MATERIALIZE, &ppr).expect("PPR boundary");
    contract::ResourceUsage::for_request(contract::PPR_MATERIALIZE, &ppr).expect("PPR caps");
    ppr["plan"]["maxIterations"] = json!(contract::MAX_ITERATIONS + 1);
    assert!(contract::ResourceUsage::for_request(contract::PPR_MATERIALIZE, &ppr).is_err());
}

#[test]
fn output_caps_and_checked_work_formula_cover_boundaries_and_overflow() {
    let output = contract::OutputCounts {
        returned_passages: contract::MAX_RETURNED_PASSAGES,
        returned_facts: contract::MAX_COMBINED_OBJECTS - contract::MAX_RETURNED_PASSAGES,
        combined_objects: contract::MAX_COMBINED_OBJECTS,
    };
    output.check().expect("output boundary");
    assert!(
        contract::OutputCounts {
            combined_objects: contract::MAX_COMBINED_OBJECTS + 1,
            ..output
        }
        .check()
        .is_err()
    );
    assert_eq!(
        contract::OutputCounts::checked_combined_objects(100, 100).unwrap(),
        200
    );
    assert!(contract::OutputCounts::checked_combined_objects(u64::MAX, 1).is_err());
    assert!(
        contract::OutputCounts {
            returned_passages: 100,
            returned_facts: 100,
            combined_objects: 199,
        }
        .check()
        .is_err()
    );

    let work = contract::WorkCounts {
        vector_comparisons: 2,
        facts_inspected: 3,
        iterations: 4,
        nodes_initialized: 5,
        edges_visited: 6,
        objects_considered_for_encoding: 7,
    };
    assert_eq!(work.checked_work_units().unwrap(), 56);
    assert_eq!(work.check().unwrap(), 56);
    assert!(
        contract::WorkCounts {
            vector_comparisons: u64::MAX,
            facts_inspected: 1,
            ..Default::default()
        }
        .checked_work_units()
        .is_err()
    );
    assert!(
        contract::WorkCounts {
            nodes_initialized: u64::MAX,
            edges_visited: 1,
            ..Default::default()
        }
        .checked_work_units()
        .is_err()
    );
    assert!(
        contract::WorkCounts {
            iterations: u64::MAX,
            nodes_initialized: 2,
            ..Default::default()
        }
        .checked_work_units()
        .is_err()
    );
    for (field, maximum) in [
        ("vector", contract::MAX_VECTOR_COMPARISONS),
        ("facts", contract::MAX_FACTS_INSPECTED),
        ("iterations", contract::MAX_ITERATIONS),
        ("nodes", contract::MAX_GRAPH_NODES),
        ("edges", contract::MAX_GRAPH_EDGES),
        ("objects", contract::MAX_COMBINED_OBJECTS),
    ] {
        let mut value = contract::WorkCounts::default();
        match field {
            "vector" => value.vector_comparisons = maximum,
            "facts" => value.facts_inspected = maximum,
            "iterations" => value.iterations = maximum,
            "nodes" => value.nodes_initialized = maximum,
            "edges" => value.edges_visited = maximum,
            "objects" => value.objects_considered_for_encoding = maximum,
            _ => unreachable!(),
        }
        value.check().expect(field);
        match field {
            "vector" => value.vector_comparisons += 1,
            "facts" => value.facts_inspected += 1,
            "iterations" => value.iterations += 1,
            "nodes" => value.nodes_initialized += 1,
            "edges" => value.edges_visited += 1,
            "objects" => value.objects_considered_for_encoding += 1,
            _ => unreachable!(),
        }
        assert!(value.check().is_err(), "{field} + 1 must fail");
    }
}

#[test]
fn checked_allocation_formula_covers_each_boundary_and_overflow() {
    let input = contract::AllocationInput {
        vector_values: 2,
        seed_entries: 3,
        result_entries: 4,
        ppr_score_entries: 5,
        heap_entries: 6,
        response_buffer_bytes: 7,
    };
    assert_eq!(input.checked_capacity_bytes().unwrap(), 935);
    assert_eq!(input.check().unwrap(), 935);

    let response_boundary = contract::AllocationInput {
        response_buffer_bytes: contract::MAX_RESPONSE_FRAME_BYTES,
        ..Default::default()
    };
    response_boundary
        .check()
        .expect("response allocation boundary");
    assert!(
        contract::AllocationInput {
            response_buffer_bytes: contract::MAX_RESPONSE_FRAME_BYTES + 1,
            ..Default::default()
        }
        .check()
        .is_err()
    );
    let transient_boundary = contract::AllocationInput {
        vector_values: contract::MAX_TRANSIENT_BYTES / 8,
        ..Default::default()
    };
    assert_eq!(
        transient_boundary.check().unwrap(),
        contract::MAX_TRANSIENT_BYTES
    );
    assert!(
        contract::AllocationInput {
            vector_values: contract::MAX_TRANSIENT_BYTES / 8 + 1,
            ..Default::default()
        }
        .check()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            vector_values: u64::MAX,
            ..Default::default()
        }
        .checked_capacity_bytes()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            seed_entries: u64::MAX,
            ..Default::default()
        }
        .checked_capacity_bytes()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            result_entries: u64::MAX,
            ..Default::default()
        }
        .checked_capacity_bytes()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            ppr_score_entries: u64::MAX,
            ..Default::default()
        }
        .checked_capacity_bytes()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            heap_entries: u64::MAX,
            ..Default::default()
        }
        .checked_capacity_bytes()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            response_buffer_bytes: u64::MAX,
            ..Default::default()
        }
        .check()
        .is_err()
    );
    assert!(
        contract::AllocationInput {
            vector_values: contract::MAX_TRANSIENT_BYTES,
            ..Default::default()
        }
        .check()
        .is_err()
    );
}

#[test]
fn decreasing_remainder_and_fake_clock_deadline_never_reset() {
    let mut remainder = contract::SessionRemainder::new(u64::MAX);
    assert_eq!(
        remainder.remaining_ms(),
        contract::MAX_OPERATION_DEADLINE_MS
    );
    assert_eq!(remainder.accept_decreasing(50_000).unwrap(), 50_000);
    assert!(remainder.accept_decreasing(50_001).is_err());
    assert_eq!(remainder.accept_decreasing(0).unwrap(), 0);

    let mut deadline = contract::MonotonicDeadline::new(100, 60_000).unwrap();
    assert_eq!(deadline.deadline_ms(), 60_100);
    assert_eq!(deadline.remaining_ms(101).unwrap(), 59_999);
    deadline
        .checkpoint(101, 1_024)
        .expect("exact checkpoint interval");
    assert_eq!(deadline.completed_units(), 1_024);
    assert_eq!(deadline.remaining_ms(102).unwrap(), 59_998);
    assert!(matches!(
        deadline.checkpoint(102, 2_049),
        Err(contract::DeadlineError::CheckpointGap)
    ));
    assert!(matches!(
        deadline.checkpoint(60_100, 1_024),
        Err(contract::DeadlineError::DeadlineExceeded)
    ));
    assert_eq!(deadline.completed_units(), 1_024);
    assert!(matches!(
        deadline.remaining_ms(60_099),
        Err(contract::DeadlineError::ClockRegression)
    ));
    assert!(matches!(
        contract::MonotonicDeadline::new(u64::MAX, 1),
        Err(contract::DeadlineError::ArithmeticOverflow)
    ));

    let mut zero_budget = contract::MonotonicDeadline::new(0, 0).unwrap();
    assert!(matches!(
        zero_budget.before_allocation(0, 0),
        Err(contract::DeadlineError::DeadlineExceeded)
    ));
    assert!(
        contract::encode_success(
            1,
            contract::MAX_SAFE_GENERATION + 1,
            &json!({}),
            contract::WorkCounts::default(),
        )
        .is_err()
    );
}

#[test]
fn counters_survive_deadline_error_and_error_wire_has_no_result() {
    let work = contract::WorkCounts {
        vector_comparisons: 11,
        facts_inspected: 13,
        ..Default::default()
    };
    let counters = contract::WorkCounters::from_work(work, 0).unwrap();
    assert_eq!(counters.work_units, 24);
    let error_frame = contract::encode_error(5, 9, work, "DEADLINE", "operation deadline exceeded")
        .expect("bounded error frame");
    assert!(error_frame.len() as u64 <= contract::MAX_RESPONSE_FRAME_BYTES);
    let error_json: Value = serde_json::from_slice(&error_frame).expect("error JSON");
    assert_eq!(error_json["ok"], json!(false));
    assert!(error_json.get("result").is_none());
    assert_eq!(error_json["counters"]["workUnits"], json!(24));
    assert_eq!(
        error_json["counters"]["responseBytes"],
        json!(error_frame.len())
    );
}

#[test]
fn bounded_object_and_response_frames_reject_overflow_without_partial_output() {
    let object_boundary = Value::String("x".repeat(contract::MAX_OBJECT_BYTES as usize - 2));
    assert_eq!(
        contract::encode_object(&object_boundary).unwrap().len() as u64,
        contract::MAX_OBJECT_BYTES
    );
    let object_over = Value::String("x".repeat(contract::MAX_OBJECT_BYTES as usize - 1));
    assert!(contract::encode_object(&object_over).is_err());

    let work = contract::WorkCounts::default();
    let mut accepted = None;
    let mut rejected = None;
    let mut low = 0_usize;
    let mut high = contract::MAX_RESPONSE_FRAME_BYTES as usize;
    while low < high {
        let middle = low + (high - low) / 2;
        let result = Value::String("x".repeat(middle));
        if contract::encode_success(1, 0, &result, work).is_ok() {
            accepted = Some(middle);
            low = middle + 1;
        } else {
            rejected = Some(middle);
            high = middle;
        }
    }
    let accepted = accepted.expect("some result fits");
    let rejected = rejected.unwrap_or(low);
    let frame = contract::encode_success(1, 0, &Value::String("x".repeat(accepted)), work)
        .expect("largest accepted result");
    assert!(frame.len() as u64 <= contract::MAX_RESPONSE_FRAME_BYTES);
    let response: Value = serde_json::from_slice(&frame).expect("success JSON");
    assert_eq!(response["ok"], json!(true));
    assert_eq!(response["counters"]["responseBytes"], json!(frame.len()));
    assert!(contract::encode_success(1, 0, &Value::String("x".repeat(rejected)), work).is_err());
}

#[test]
fn protocol_info_is_exactly_three_read_wal_free_operations() {
    let info = contract::protocol_info().unwrap();
    let names = info
        .methods
        .iter()
        .map(|method| method.name.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        names,
        contract::METHODS.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(info.methods.len(), 3);
    assert!(info.methods.iter().all(|method| {
        method.classification == "read"
            && !method.wal
            && method.semantic_digest.len() == 64
            && method.request_schema_sha256.len() == 64
            && method.result_schema_sha256.len() == 64
    }));
    assert!(!names.contains("memory_load"));
    assert!(!names.contains("full_snapshot"));
    assert!(!names.contains("global_projection"));
    assert!(!names.contains("legacy_fallback"));
    assert_eq!(contract::operation_names().unwrap(), contract::METHODS);
    let expected_digests = [
        (
            contract::CANDIDATE_SEARCH,
            "4255d5a4076b27d264841579c6dbd3064f90b3e1a89b0386acaad44cb562eb1a",
        ),
        (
            contract::FACT_EXPAND,
            "296423c9a1b5d5627175e537f0df5be884b17ea15ab9efe4b6875843194bc60d",
        ),
        (
            contract::PPR_MATERIALIZE,
            "cd1f4bfeb39a01b4f12db1da9a7a05d8179f27eba878d1fd7aa935eb151834ab",
        ),
    ];
    for (method, digest) in expected_digests {
        assert_eq!(
            info.methods
                .iter()
                .find(|entry| entry.name == method)
                .unwrap()
                .semantic_digest,
            digest
        );
    }
    assert_eq!(
        info.limits.max_operation_deadline_ms,
        contract::MAX_OPERATION_DEADLINE_MS
    );
}
