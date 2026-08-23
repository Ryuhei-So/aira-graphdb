use aira_graphdb::errors::{ErrorCode, ErrorRegistry};
use serde_json::{Value, json};

const CONTRACT: &str = include_str!("../spec/contracts/retrieve-bounded.v1.0.0.json");
const GOLDEN: &str = include_str!("../spec/contracts/retrieve-bounded-golden.v1.0.0.json");
const NEGATIVE: &str =
    include_str!("../spec/contracts/retrieve-bounded-negative-fixtures.v1.0.0.json");

fn contract() -> Value {
    serde_json::from_str(CONTRACT).expect("retrieve_bounded contract must be valid JSON")
}

fn golden() -> Value {
    serde_json::from_str(GOLDEN).expect("retrieve_bounded golden fixture must be valid JSON")
}

fn negative() -> Value {
    serde_json::from_str(NEGATIVE).expect("negative fixtures must be valid JSON")
}

#[test]
fn declares_versioned_read_and_owner_generation_authority() {
    let contract = contract();
    assert_eq!(contract["spec_id"], "AGDB-RETRIEVE-BOUNDED@1.0.0");
    assert_eq!(contract["contractVersion"], "retrieve-bounded@1.0.0");
    assert_eq!(contract["method"], "retrieve_bounded");
    assert_eq!(contract["classification"], "read");
    assert_eq!(contract["wal"], false);
    assert_eq!(contract["additionalProperties"], false);
    assert_eq!(
        contract["authority"]["canonical"],
        "native-owner-committed-generation"
    );
    assert_eq!(
        contract["authority"]["transport"],
        "private-stdio-owner-capability"
    );
    assert_eq!(contract["authority"]["roleField"], "forbidden");
    assert_eq!(contract["authority"]["leaseField"], "forbidden");
    assert_eq!(contract["generation"]["requestField"], "expectedGeneration");
    assert_eq!(contract["generation"]["type"], "u64");
    assert!(
        contract["generation"]["pin"]
            .as_str()
            .unwrap()
            .contains("immutable")
    );
    assert!(
        contract["generation"]["publication"]
            .as_str()
            .unwrap()
            .contains("blocks")
    );
}

#[test]
fn request_schema_is_strict_and_caps_every_work_axis() {
    let request = contract()["requestSchema"].clone();
    assert_eq!(request["type"], "object");
    assert_eq!(request["additionalProperties"], false);
    let required = request["required"].as_array().expect("required fields");
    for field in [
        "protocolVersion",
        "expectedGeneration",
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
            "missing {field}"
        );
    }
    assert_eq!(
        request["properties"]["protocolVersion"]["const"],
        "retrieve-bounded@1.0.0"
    );
    assert_eq!(
        request["properties"]["expectedGeneration"]["maximum"],
        json!(u64::MAX)
    );
    assert_eq!(request["properties"]["queryVector"]["maxItems"], 4096);
    assert_eq!(
        request["properties"]["queryVector"]["items"]["finite"],
        true
    );
    for (name, max) in [
        ("topK", 10000),
        ("topM", 10000),
        ("maxIterations", 512),
        ("maxVisitedNodes", 200000),
        ("maxVisitedEdges", 500000),
        ("maxWorkUnits", 2000000),
        ("deadlineMs", 10000),
        ("maxResponseBytes", 1048576),
    ] {
        let limits = if name == "topK" {
            &request["$defs"]["topK"]
        } else if name == "topM" {
            &request["$defs"]["topM"]
        } else {
            &request["properties"][name]
        };
        assert_eq!(limits["minimum"], 1);
        assert_eq!(limits["maximum"], max);
    }
    assert_eq!(
        request["properties"]["deadlineMs"]["authority"],
        "owner-native-monotonic-timeout"
    );
    assert!(request["properties"]["deadlineMs"]["checkFrequency"].is_string());
    assert_eq!(request["$defs"]["vectorThreshold"]["minimum"], -1.0);
    assert_eq!(request["$defs"]["vectorThreshold"]["maximum"], 1.0);
    assert_eq!(
        request["$defs"]["vectorThreshold"]["authority"],
        "existing-exact-vector-primitive"
    );
}

#[test]
fn response_schema_bounds_items_text_metadata_and_work() {
    let contract = contract();
    let response = &contract["responseSchema"];
    assert_eq!(response["type"], "object");
    assert_eq!(response["additionalProperties"], false);
    for field in [
        "protocolVersion",
        "generation",
        "candidates",
        "passages",
        "facts",
        "work",
    ] {
        assert!(
            response["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == field)
        );
    }
    assert_eq!(
        response["properties"]["candidates"]["maxItemsFrom"],
        "request.topK"
    );
    for list in ["passages", "facts"] {
        assert_eq!(response["properties"][list]["maxItemsFrom"], "request.topM");
    }
    for item in ["candidate", "passage", "fact"] {
        assert_eq!(response["$defs"][item]["additionalProperties"], false);
    }
    assert_eq!(response["$defs"]["boundedText"]["maxUtf8Bytes"], 1048576);
    assert_eq!(response["$defs"]["boundedMetadata"]["maxUtf8Bytes"], 65536);
    for field in [
        "iterations",
        "visitedNodes",
        "visitedEdges",
        "workUnits",
        "elapsedMs",
        "responseBytes",
    ] {
        assert!(
            response["$defs"]["work"]["required"]
                .as_array()
                .unwrap()
                .iter()
                .any(|value| value == field)
        );
    }
    assert_eq!(
        contract["materialization"],
        "only bounded candidates, passages, and facts; never a full memory snapshot or global projection"
    );
}

#[test]
fn ranking_contract_has_formula_dedupe_and_total_order() {
    let ranking = contract()["ranking"].clone();
    for field in [
        "candidateSeeds",
        "propagation",
        "materializedScore",
        "dedupe",
        "passageAndFactLimit",
        "totalOrder",
    ] {
        assert!(ranking[field].is_string(), "missing ranking rule {field}");
    }
    assert!(
        ranking["candidateSeeds"]
            .as_str()
            .unwrap()
            .contains("threshold")
    );
    assert!(ranking["propagation"].as_str().unwrap().contains("d=0.85"));
    assert!(
        ranking["propagation"]
            .as_str()
            .unwrap()
            .contains("sumOutgoingWeight")
    );
    assert!(
        ranking["passageAndFactLimit"]
            .as_str()
            .unwrap()
            .contains("combined")
    );
    assert!(
        ranking["totalOrder"]
            .as_str()
            .unwrap()
            .contains("descending")
    );
    assert!(
        ranking["totalOrder"]
            .as_str()
            .unwrap()
            .contains("ascending")
    );
}

#[test]
fn golden_fixture_asserts_full_response_independently_of_expected_id_lists() {
    let golden = golden();
    assert_eq!(golden["protocolVersion"], "retrieve-bounded@1.0.0");
    assert_eq!(golden["expectedGeneration"], 7);
    assert_eq!(golden["request"]["expectedGeneration"], 7);
    assert_eq!(golden["vectors"].as_array().unwrap().len(), 3);
    assert_eq!(golden["nodes"].as_array().unwrap().len(), 3);
    assert_eq!(golden["edges"].as_array().unwrap().len(), 2);
    assert_eq!(golden["memory"]["passages"].as_array().unwrap().len(), 3);
    assert_eq!(golden["memory"]["facts"].as_array().unwrap().len(), 2);
    let expected = json!({
        "protocolVersion": "retrieve-bounded@1.0.0",
        "generation": 7,
        "candidates": [
            {"id": "seed-a", "score": 1.0, "metadata": {"documentId": "doc-a"}},
            {"id": "seed-b", "score": 1.0, "metadata": {"documentId": "doc-b"}}
        ],
        "passages": [
            {"passageId": "p-a", "documentId": "doc-a", "score": 0.5, "text": "alpha", "metadata": {"documentId": "doc-a"}},
            {"passageId": "p-b", "documentId": "doc-b", "score": 0.5, "text": "beta", "metadata": {"documentId": "doc-b"}}
        ],
        "facts": [],
        "work": {"iterations": 2, "visitedNodes": 2, "visitedEdges": 2, "workUnits": 8, "elapsedMs": 0, "responseBytes": 531}
    });
    assert_eq!(golden["expectedResponse"], expected);
}

#[test]
fn all_failure_matrix_codes_are_central_registry_entries_with_behavior() {
    let registry = ErrorRegistry::load().expect("central error registry should load");
    for code in [
        ErrorCode::RecoveryPending,
        ErrorCode::RetrieveBoundedStaleGeneration,
        ErrorCode::RetrieveBoundedTimeout,
        ErrorCode::RetrieveBoundedTransportFailure,
        ErrorCode::RetrieveBoundedOom,
        ErrorCode::RetrieveBoundedUnavailable,
    ] {
        let definition = registry.definition(code).expect("error code is registered");
        assert!(!definition.failure_class.is_empty());
        assert!(matches!(
            definition.failure_class.as_str(),
            "CLIENT_INPUT" | "IO_FAILURE" | "OOM" | "TIMEOUT" | "INTERNAL_BUG"
        ));
    }
}

#[test]
fn shared_negative_fixtures_cover_validation_and_admission_boundaries() {
    let fixtures = negative();
    assert_eq!(fixtures["contractVersion"], "retrieve-bounded@1.0.0");
    let cases = fixtures["cases"].as_array().expect("negative cases");
    for name in [
        "unknown_field",
        "wrong_version",
        "wrong_type_topK",
        "nonfinite_vector",
        "dimensions_at_max",
        "dimensions_over_max",
        "topK_at_max",
        "topK_over_max",
        "topM_at_max",
        "topM_over_max",
        "stale_generation",
        "recovery_pending",
    ] {
        assert!(
            cases.iter().any(|case| case["name"] == name),
            "missing fixture {name}"
        );
    }
    for case in cases {
        assert!(case["expectedCode"].is_null() || case["expectedCode"].is_string());
        assert!(case["mutation"].is_object() || case["precondition"].is_string());
    }
    assert_eq!(fixtures["baseRequest"]["queryVectorLength"], 2);
    assert_eq!(fixtures["baseRequest"]["expectedGeneration"], 7);
}
