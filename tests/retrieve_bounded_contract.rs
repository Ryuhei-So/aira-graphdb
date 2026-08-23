use serde_json::Value;

const CONTRACT: &str = include_str!("../spec/contracts/retrieve-bounded.v1.0.0.json");

fn contract() -> Value {
    serde_json::from_str(CONTRACT).expect("retrieve_bounded contract must be valid JSON")
}

#[test]
fn declares_a_versioned_read_without_wal() {
    let contract = contract();
    assert_eq!(contract["spec_id"], "AGDB-RETRIEVE-BOUNDED@1.0.0");
    assert_eq!(contract["protocol_version"], "retrieve-bounded@1.0.0");
    assert_eq!(contract["method"], "retrieve_bounded");
    assert_eq!(contract["classification"], "read");
    assert_eq!(contract["wal"], false);
    assert_eq!(
        contract["authority"],
        "native-owner-committed-generation"
    );
}

#[test]
fn requires_every_independent_work_and_response_cap() {
    let contract = contract();
    let required = contract["request"]["required"]
        .as_array()
        .expect("request required fields");
    for field in [
        "queryVector",
        "topK",
        "topM",
        "maxIterations",
        "maxVisitedNodes",
        "maxVisitedEdges",
        "maxWorkUnits",
        "deadlineMs",
        "maxResponseBytes",
    ] {
        assert!(
            required.iter().any(|value| value == field),
            "missing required cap {field}"
        );
    }

    let limits = contract["request"]["limits"]
        .as_object()
        .expect("request limits");
    for field in [
        "dimensions",
        "topK",
        "topM",
        "maxIterations",
        "maxVisitedNodes",
        "maxVisitedEdges",
        "maxWorkUnits",
        "deadlineMs",
        "maxResponseBytes",
    ] {
        let limit = limits.get(field).expect("declared limit");
        let minimum = limit["minimum"].as_u64().expect("minimum");
        let maximum = limit["maximum"].as_u64().expect("maximum");
        assert!(minimum <= maximum);
    }
    assert_eq!(
        contract["request"]["unknownValues"],
        "fail_closed_before_allocation_or_work"
    );
}

#[test]
fn response_is_bounded_and_generation_pinned() {
    let contract = contract();
    let required = contract["response"]["required"]
        .as_array()
        .expect("response required fields");
    for field in [
        "protocolVersion",
        "generation",
        "candidates",
        "passages",
        "facts",
        "work",
    ] {
        assert!(required.iter().any(|value| value == field));
    }
    assert_eq!(
        contract["response"]["materialization"],
        "only bounded candidates, passages, and facts; never a full memory snapshot or projection"
    );
    for field in [
        "iterations",
        "visitedNodes",
        "visitedEdges",
        "workUnits",
        "elapsedMs",
        "responseBytes",
    ] {
        assert!(contract["response"]["workRequired"]
            .as_array()
            .expect("work fields")
            .iter()
            .any(|value| value == field));
    }
}

#[test]
fn golden_ranking_declares_deterministic_ties() {
    let contract = contract();
    assert_eq!(contract["response"]["ranking"]["scoreOrder"], "descending");
    assert_eq!(
        contract["response"]["ranking"]["tieBreakers"][0]["field"],
        "id"
    );
    assert_eq!(
        contract["response"]["ranking"]["tieBreakers"][0]["order"],
        "ascending"
    );
    assert_eq!(
        contract["goldenRanking"]["expectedIds"],
        serde_json::json!(["candidate-a", "candidate-b", "candidate-c"])
    );
}

#[test]
fn failure_semantics_forbid_partial_or_mutating_retrieval() {
    let contract = contract();
    assert_eq!(contract["failureSemantics"]["partialResponse"], "forbidden");
    assert_eq!(contract["failureSemantics"]["canonicalMutation"], "forbidden");
    assert_eq!(contract["failureSemantics"]["generationAdvance"], "forbidden");
    assert_eq!(
        contract["failureSemantics"]["recoveryPendingOrPartialGeneration"],
        "fail_closed"
    );
}
