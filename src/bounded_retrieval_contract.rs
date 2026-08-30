//! The contract-first boundary for the three bounded retrieval operations.
//!
//! This module deliberately stops at validation, accounting, framing, and
//! protocol metadata.  It contains no candidate, fact-expansion, or PPR
//! algorithm.  The producer's pinned contract and refinement IR are parsed and
//! interpreted here so that native does not grow a second Synapse policy
//! implementation.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Component, Path};
use std::sync::OnceLock;

pub const CANDIDATE_SEARCH: &str = "candidate_search_bounded@1";
pub const FACT_EXPAND: &str = "fact_expand_bounded@1";
pub const PPR_MATERIALIZE: &str = "ppr_materialize_bounded@1";
pub const METHODS: [&str; 3] = [CANDIDATE_SEARCH, FACT_EXPAND, PPR_MATERIALIZE];

pub const CONTRACT_VERSION: &str = "aira-synapse-bounded-retrieval-contract@1";
pub const REFINEMENT_IR_VERSION: &str = "aira-synapse-refinement-ir@1";
pub const NORMALIZATION_DIGEST: &str =
    "v15-entity-normalization-ecmascript-tolowercase-unicode16.0.0@1";
pub const CONTRACT_SHA256: &str =
    "3ef47549760243ef968f160bc6d944225d23fd8eee7f33a8368f95da85a06aaf";
pub const RETRIEVAL_MANIFEST_SHA256: &str =
    "6b9faa96f424ef14c08176c2ace115ad7dd0f18b2a9ec72ed3b545ae2fe67f4a";
pub const SOURCE_REPOSITORY: &str = "https://github.com/Ryuhei-So/aira-synapse";
pub const SOURCE_BRANCH: &str = "production-runtime";
pub const SOURCE_COMMIT: &str = "a2f53107658db177a6fb0c388827f9a1121921be";

pub const MAX_SAFE_GENERATION: u64 = 9_007_199_254_740_991;

pub const MAX_VECTOR_DIMENSIONS: u64 = 4_096;
pub const MAX_VECTOR_COMPARISONS: u64 = 1_500_000;
pub const MAX_SEARCH_SLOTS: u64 = 8;
pub const MAX_FACTS_INSPECTED: u64 = 1_500_000;
pub const MAX_GRAPH_NODES: u64 = 1_500_000;
pub const MAX_GRAPH_EDGES: u64 = 4_000_000;
pub const MAX_ITERATIONS: u64 = 128;
pub const MAX_SEARCH_RESULT_LIMIT: u64 = 100;
pub const MAX_EXPANSION_RESULTS: u64 = 64;
pub const MAX_SEEDS: u64 = 512;
pub const MAX_RETURNED_PASSAGES: u64 = 100;
pub const MAX_RETURNED_FACTS: u64 = 100;
pub const MAX_COMBINED_OBJECTS: u64 = 128;

pub const MAX_INPUT_FRAME_BYTES: u64 = 256 * 1024;
pub const MAX_OBJECT_BYTES: u64 = 256 * 1024;
pub const MAX_RESPONSE_FRAME_BYTES: u64 = 2 * 1024 * 1024;
pub const MAX_TRANSIENT_BYTES: u64 = 512 * 1024 * 1024;
pub const MAX_OPERATION_DEADLINE_MS: u64 = 60_000;
pub const DEADLINE_CHECK_INTERVAL_UNITS: u64 = 1_024;

const PIN_FILE: &str = "bounded-retrieval-pin.json";
const CONTRACT_FILE: &str = "bounded-retrieval-contract.json";
const FIXTURE_FILE: &str = "bounded-retrieval-fixture.json";
const MANIFEST_FILE: &str = "bounded-retrieval-fixture.manifest.json";
const EXPECTED_PIN_VERSION: &str = "aira-graphdb-bounded-retrieval-pin@1";
const EXPECTED_MANIFEST_VERSION: &str = "aira-synapse-bounded-retrieval-manifest@1";
const EXPECTED_FIXTURE_VERSION: &str = "aira-synapse-bounded-retrieval-fixture@1";
const MAX_ARTIFACT_BYTES: u64 = 128 * 1024;
const MAX_TOTAL_ARTIFACT_BYTES: u64 = 256 * 1024;

const PIN_BYTES: &[u8] =
    include_bytes!("../spec/contracts/bounded-retrieval/bounded-retrieval-pin.json");
const CONTRACT_BYTES: &[u8] =
    include_bytes!("../spec/contracts/bounded-retrieval/bounded-retrieval-contract.json");
const FIXTURE_BYTES: &[u8] =
    include_bytes!("../spec/contracts/bounded-retrieval/bounded-retrieval-fixture.json");
const MANIFEST_BYTES: &[u8] =
    include_bytes!("../spec/contracts/bounded-retrieval/bounded-retrieval-fixture.manifest.json");

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContractError(pub String);

impl fmt::Display for ContractError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ContractError {}

fn error(message: impl Into<String>) -> ContractError {
    ContractError(message.into())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PinnedArtifact {
    pub bytes: u64,
    pub local_file: String,
    pub sha256: String,
    pub source_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestDependency {
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_version: Option<String>,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub normalization_digest: Option<String>,
    pub path: String,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unicode_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct OperationDigest {
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetrievalPin {
    pub artifacts: Vec<PinnedArtifact>,
    pub dependencies: Vec<ManifestDependency>,
    pub fixture_manifest_file: String,
    pub fixture_manifest_sha256: String,
    pub operation_semantic_digests: BTreeMap<String, OperationDigest>,
    pub pin_version: String,
    pub source_branch: String,
    pub source_commit: String,
    pub source_repository: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RetrievalManifest {
    contract_bytes: u64,
    contract_file: String,
    contract_sha256: String,
    contract_version: String,
    dependencies: Vec<ManifestDependency>,
    fixture_bytes: u64,
    fixture_file: String,
    fixture_sha256: String,
    fixture_version: String,
    manifest_version: String,
    operation_semantic_digests: BTreeMap<String, OperationDigest>,
}

fn expected_artifacts() -> Vec<PinnedArtifact> {
    vec![
        PinnedArtifact {
            bytes: 59_523,
            local_file: CONTRACT_FILE.to_string(),
            sha256: CONTRACT_SHA256.to_string(),
            source_path:
                "packages/memgraphrag/tests/fixtures/bounded-retrieval/bounded-retrieval-contract.json"
                    .to_string(),
        },
        PinnedArtifact {
            bytes: 12_061,
            local_file: FIXTURE_FILE.to_string(),
            sha256:
                "9bdfbee39b4d1ba01f82df101de287756a9c089796bbfaa571aa88ec3437b210".to_string(),
            source_path:
                "packages/memgraphrag/tests/fixtures/bounded-retrieval/bounded-retrieval-fixture.json"
                    .to_string(),
        },
        PinnedArtifact {
            bytes: 3_047,
            local_file: MANIFEST_FILE.to_string(),
            sha256: RETRIEVAL_MANIFEST_SHA256.to_string(),
            source_path:
                "packages/memgraphrag/tests/fixtures/bounded-retrieval/bounded-retrieval-fixture.manifest.json"
                    .to_string(),
        },
    ]
}

fn expected_dependencies() -> Vec<ManifestDependency> {
    vec![
        ManifestDependency {
            bytes: 6_211,
            contract_version: Some("aira-synapse-domain-contract@1".to_string()),
            id: "aira-synapse-domain-contract@1".to_string(),
            manifest_version: None,
            normalization_digest: None,
            path: "packages/memgraphrag/tests/fixtures/bounded-domain-contract.json".to_string(),
            sha256: "2d05969a93037c6c22f46b77e068d5d5d2fa5f85988eb923e7f7d7d78ab46ef1".to_string(),
            unicode_version: None,
            format_version: None,
        },
        ManifestDependency {
            bytes: 455,
            contract_version: Some("aira-synapse-domain-contract@1".to_string()),
            id: "aira-synapse-bounded-domain-manifest@1".to_string(),
            manifest_version: Some("aira-synapse-bounded-domain-manifest@1".to_string()),
            normalization_digest: None,
            path: "packages/memgraphrag/tests/fixtures/bounded-domain-fixture.manifest.json"
                .to_string(),
            sha256: "b50a00b5a87f91305b41d97b6f91cd19741e4bbc8ea1b4e4e976c83f40b4e821".to_string(),
            unicode_version: None,
            format_version: None,
        },
        ManifestDependency {
            bytes: 2_018,
            contract_version: None,
            id: "V15UnicodeLowercaseManifest@1".to_string(),
            manifest_version: Some("V15UnicodeLowercaseManifest@1".to_string()),
            normalization_digest: Some(NORMALIZATION_DIGEST.to_string()),
            path: "packages/memgraphrag/tests/fixtures/unicode16-lowercase.manifest.json"
                .to_string(),
            sha256: "4f323ec6366aba4f497d4de5eec26759a898242b3e26e46d23a232881ca28960".to_string(),
            unicode_version: Some("16.0.0".to_string()),
            format_version: None,
        },
        ManifestDependency {
            bytes: 68_942,
            contract_version: None,
            id:
                "v15-entity-normalization-ecmascript-tolowercase-unicode16.0.0@1:native-rust-lookup"
                    .to_string(),
            manifest_version: None,
            normalization_digest: Some(NORMALIZATION_DIGEST.to_string()),
            path: "packages/memgraphrag/tests/fixtures/unicode16-lowercase.lookup.rs".to_string(),
            sha256: "4637b1b21285887f291ab36c19edb5bb94d1660364eb948159117c0d493d8f59".to_string(),
            unicode_version: Some("16.0.0".to_string()),
            format_version: None,
        },
        ManifestDependency {
            bytes: 5_560_651,
            contract_version: None,
            id: "v15-entity-normalization-ecmascript-tolowercase-unicode16.0.0@1:conformance"
                .to_string(),
            manifest_version: None,
            normalization_digest: Some(NORMALIZATION_DIGEST.to_string()),
            path: "packages/memgraphrag/tests/fixtures/unicode16-lowercase.conformance.bin"
                .to_string(),
            sha256: "c89cc745f040ba8518473fb8b1511596a34e48616e2f3f3609237fdd74e4ae8a".to_string(),
            unicode_version: Some("16.0.0".to_string()),
            format_version: Some("U16LOW1".to_string()),
        },
    ]
}

fn expected_operation_digests() -> BTreeMap<String, OperationDigest> {
    BTreeMap::from([
        (
            CANDIDATE_SEARCH.to_string(),
            OperationDigest {
                bytes: 20_276,
                sha256: "4255d5a4076b27d264841579c6dbd3064f90b3e1a89b0386acaad44cb562eb1a"
                    .to_string(),
            },
        ),
        (
            FACT_EXPAND.to_string(),
            OperationDigest {
                bytes: 12_258,
                sha256: "296423c9a1b5d5627175e537f0df5be884b17ea15ab9efe4b6875843194bc60d"
                    .to_string(),
            },
        ),
        (
            PPR_MATERIALIZE.to_string(),
            OperationDigest {
                bytes: 14_685,
                sha256: "cd1f4bfeb39a01b4f12db1da9a7a05d8179f27eba878d1fd7aa935eb151834ab"
                    .to_string(),
            },
        ),
    ])
}

fn expected_pin() -> RetrievalPin {
    RetrievalPin {
        artifacts: expected_artifacts(),
        dependencies: expected_dependencies(),
        fixture_manifest_file: MANIFEST_FILE.to_string(),
        fixture_manifest_sha256: RETRIEVAL_MANIFEST_SHA256.to_string(),
        operation_semantic_digests: expected_operation_digests(),
        pin_version: EXPECTED_PIN_VERSION.to_string(),
        source_branch: SOURCE_BRANCH.to_string(),
        source_commit: SOURCE_COMMIT.to_string(),
        source_repository: SOURCE_REPOSITORY.to_string(),
    }
}

fn expected_manifest() -> RetrievalManifest {
    RetrievalManifest {
        contract_bytes: 59_523,
        contract_file: CONTRACT_FILE.to_string(),
        contract_sha256: CONTRACT_SHA256.to_string(),
        contract_version: CONTRACT_VERSION.to_string(),
        dependencies: expected_dependencies(),
        fixture_bytes: 12_061,
        fixture_file: FIXTURE_FILE.to_string(),
        fixture_sha256: "9bdfbee39b4d1ba01f82df101de287756a9c089796bbfaa571aa88ec3437b210"
            .to_string(),
        fixture_version: EXPECTED_FIXTURE_VERSION.to_string(),
        manifest_version: EXPECTED_MANIFEST_VERSION.to_string(),
        operation_semantic_digests: expected_operation_digests(),
    }
}

fn parse_json<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    label: &str,
) -> Result<T, ContractError> {
    serde_json::from_slice(bytes).map_err(|e| error(format!("invalid {label}: {e}")))
}

fn regular_file_size(path: &Path) -> Result<u64, ContractError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|e| error(format!("{}: {e}", path.display())))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(error(format!(
            "{} is not a regular non-symlink file",
            path.display()
        )));
    }
    let bytes = metadata.len();
    if bytes > MAX_ARTIFACT_BYTES {
        return Err(error(format!(
            "{} exceeds per-file byte cap",
            path.display()
        )));
    }
    Ok(bytes)
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, ContractError> {
    regular_file_size(path)?;
    fs::read(path).map_err(|e| error(format!("{}: {e}", path.display())))
}

fn verify_artifact_bytes(root: &Path, artifact: &PinnedArtifact) -> Result<(), ContractError> {
    let local = Path::new(&artifact.local_file);
    if local.components().count() != 1
        || !matches!(local.components().next(), Some(Component::Normal(_)))
    {
        return Err(error(format!("invalid localFile {}", artifact.local_file)));
    }
    let bytes = read_regular_file(&root.join(local))?;
    if bytes.len() as u64 != artifact.bytes {
        return Err(error(format!(
            "byte length mismatch for {}",
            artifact.local_file
        )));
    }
    if sha256_hex(&bytes) != artifact.sha256 {
        return Err(error(format!(
            "SHA-256 mismatch for {}",
            artifact.local_file
        )));
    }
    Ok(())
}

/// Verify the committed retrieval artifact directory as a closed file set.
/// The source metadata and all five dependency pins are checked independently
/// of the files' current JSON formatting.
pub fn verify_artifact_dir(root: &Path) -> Result<(), ContractError> {
    let expected = expected_pin();
    let mut expected_files = BTreeSet::from([PIN_FILE.to_string()]);
    expected_files.extend(expected.artifacts.iter().map(|a| a.local_file.clone()));
    let actual_files = fs::read_dir(root)
        .map_err(|e| error(format!("read retrieval contract directory: {e}")))?
        .map(|entry| {
            entry
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .map_err(|e| error(format!("read retrieval contract entry: {e}")))
        })
        .collect::<Result<BTreeSet<_>, _>>()?;
    if actual_files != expected_files {
        return Err(error("bounded retrieval directory file set mismatch"));
    }

    let pin_bytes = read_regular_file(&root.join(PIN_FILE))?;
    let pin: RetrievalPin = parse_json(&pin_bytes, "bounded retrieval pin")?;
    if pin != expected {
        return Err(error("bounded retrieval pin metadata mismatch"));
    }
    let mut total = 0_u64;
    for artifact in &pin.artifacts {
        total = total
            .checked_add(regular_file_size(&root.join(&artifact.local_file))?)
            .ok_or_else(|| error("bounded retrieval aggregate byte count overflow"))?;
        if total > MAX_TOTAL_ARTIFACT_BYTES {
            return Err(error("bounded retrieval aggregate byte cap exceeded"));
        }
        verify_artifact_bytes(root, artifact)?;
    }
    if total != expected.artifacts.iter().map(|a| a.bytes).sum::<u64>() {
        return Err(error("bounded retrieval aggregate byte count mismatch"));
    }

    let manifest_bytes = read_regular_file(&root.join(MANIFEST_FILE))?;
    if sha256_hex(&manifest_bytes) != RETRIEVAL_MANIFEST_SHA256 {
        return Err(error("retrieval manifest SHA-256 mismatch"));
    }
    verify_manifest_bytes(&manifest_bytes)?;
    parse_producer_contract(&read_regular_file(&root.join(CONTRACT_FILE))?)?;
    validate_fixture_bytes(&read_regular_file(&root.join(FIXTURE_FILE))?)?;
    Ok(())
}

/// Verify the embedded production-runtime artifact used by this binary.
pub fn verify_embedded_artifacts() -> Result<(), ContractError> {
    let expected = expected_pin();
    let pin: RetrievalPin = parse_json(PIN_BYTES, "embedded retrieval pin")?;
    if pin != expected {
        return Err(error("embedded retrieval pin metadata mismatch"));
    }
    for artifact in &expected.artifacts {
        let bytes = match artifact.local_file.as_str() {
            CONTRACT_FILE => CONTRACT_BYTES,
            FIXTURE_FILE => FIXTURE_BYTES,
            MANIFEST_FILE => MANIFEST_BYTES,
            _ => return Err(error("embedded retrieval artifact set mismatch")),
        };
        if bytes.len() as u64 != artifact.bytes || sha256_hex(bytes) != artifact.sha256 {
            return Err(error(format!(
                "embedded SHA-256 mismatch for {}",
                artifact.local_file
            )));
        }
    }
    verify_manifest_bytes(MANIFEST_BYTES)?;
    parse_producer_contract(CONTRACT_BYTES)?;
    validate_fixture_bytes(FIXTURE_BYTES)?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub enum ContractNode {
    String,
    Number,
    Boolean,
    Literal(Vec<Value>),
    Array(Box<ContractNode>),
    Tuple(Vec<ContractNode>),
    Optional(Box<ContractNode>),
    Object(BTreeMap<String, ContractNode>),
    ExternalRef {
        dependency: String,
        reference_kind: String,
    },
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, ContractError> {
    value
        .as_object()
        .ok_or_else(|| error(format!("{label} must be an object")))
}

fn exact_keys(
    map: &Map<String, Value>,
    expected: &[&str],
    label: &str,
) -> Result<(), ContractError> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(error(format!("{label} has unknown or missing field")));
    }
    Ok(())
}

fn parse_contract_node(value: &Value, label: &str) -> Result<ContractNode, ContractError> {
    let map = object(value, label)?;
    let kind = map
        .get("kind")
        .and_then(Value::as_str)
        .ok_or_else(|| error(format!("{label}.kind must be a string")))?;
    match kind {
        "string" => {
            exact_keys(map, &["kind"], label)?;
            Ok(ContractNode::String)
        }
        "number" => {
            exact_keys(map, &["kind"], label)?;
            Ok(ContractNode::Number)
        }
        "boolean" => {
            exact_keys(map, &["kind"], label)?;
            Ok(ContractNode::Boolean)
        }
        "literal" => {
            exact_keys(map, &["kind", "values"], label)?;
            let values = map
                .get("values")
                .and_then(Value::as_array)
                .ok_or_else(|| error(format!("{label}.values must be an array")))?;
            if values.is_empty() || values.iter().any(|v| !is_scalar(v)) {
                return Err(error(format!("{label}.values must be nonempty scalars")));
            }
            Ok(ContractNode::Literal(values.clone()))
        }
        "array" => {
            exact_keys(map, &["kind", "items"], label)?;
            Ok(ContractNode::Array(Box::new(parse_contract_node(
                map.get("items").expect("checked items"),
                &format!("{label}.items"),
            )?)))
        }
        "tuple" => {
            exact_keys(map, &["kind", "items"], label)?;
            let items = map
                .get("items")
                .and_then(Value::as_array)
                .ok_or_else(|| error(format!("{label}.items must be an array")))?;
            if items.is_empty() {
                return Err(error(format!("{label}.items must be nonempty")));
            }
            items
                .iter()
                .enumerate()
                .map(|(index, item)| parse_contract_node(item, &format!("{label}.items[{index}]")))
                .collect::<Result<Vec<_>, _>>()
                .map(ContractNode::Tuple)
        }
        "optional" => {
            exact_keys(map, &["kind", "value"], label)?;
            Ok(ContractNode::Optional(Box::new(parse_contract_node(
                map.get("value").expect("checked value"),
                &format!("{label}.value"),
            )?)))
        }
        "object" => {
            exact_keys(map, &["kind", "fields"], label)?;
            let fields = object(
                map.get("fields").expect("checked fields"),
                &format!("{label}.fields"),
            )?;
            fields
                .iter()
                .map(|(field, node)| {
                    Ok((
                        field.clone(),
                        parse_contract_node(node, &format!("{label}.{field}"))?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, ContractError>>()
                .map(ContractNode::Object)
        }
        "externalRef" => {
            exact_keys(map, &["dependency", "kind", "referenceKind"], label)?;
            let dependency = map
                .get("dependency")
                .and_then(Value::as_str)
                .ok_or_else(|| error(format!("{label}.dependency must be a string")))?;
            let reference_kind = map
                .get("referenceKind")
                .and_then(Value::as_str)
                .ok_or_else(|| error(format!("{label}.referenceKind must be a string")))?;
            Ok(ContractNode::ExternalRef {
                dependency: dependency.to_string(),
                reference_kind: reference_kind.to_string(),
            })
        }
        unknown => Err(error(format!("unsupported contract schema kind {unknown}"))),
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(
        value,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)
    )
}

fn parse_domain_contract() -> Result<BTreeMap<String, ContractNode>, ContractError> {
    const DOMAIN_BYTES: &[u8] =
        include_bytes!("../spec/contracts/bounded-domain/bounded-domain-contract.json");
    const DOMAIN_MANIFEST_BYTES: &[u8] =
        include_bytes!("../spec/contracts/bounded-domain/bounded-domain-fixture.manifest.json");
    if DOMAIN_BYTES.len() as u64 != 6_211
        || sha256_hex(DOMAIN_BYTES)
            != "2d05969a93037c6c22f46b77e068d5d5d2fa5f85988eb923e7f7d7d78ab46ef1"
        || DOMAIN_MANIFEST_BYTES.len() as u64 != 455
        || sha256_hex(DOMAIN_MANIFEST_BYTES)
            != "b50a00b5a87f91305b41d97b6f91cd19741e4bbc8ea1b4e4e976c83f40b4e821"
    {
        return Err(error("delegated dependency bytes or SHA-256 mismatch"));
    }
    let value: Value = parse_json(DOMAIN_BYTES, "delegated domain contract")?;
    let map = object(&value, "delegated domain contract")?;
    exact_keys(
        map,
        &["contractVersion", "contracts"],
        "delegated domain contract",
    )?;
    if map.get("contractVersion").and_then(Value::as_str) != Some("aira-synapse-domain-contract@1")
    {
        return Err(error("delegated domain contract version mismatch"));
    }
    let contracts = object(
        map.get("contracts").expect("checked contracts"),
        "delegated domain contract.contracts",
    )?;
    contracts
        .iter()
        .map(|(kind, node)| {
            Ok((
                kind.clone(),
                parse_contract_node(node, &format!("domain contract {kind}"))?,
            ))
        })
        .collect()
}

fn validate_schema(
    node: &ContractNode,
    value: &Value,
    domain: &BTreeMap<String, ContractNode>,
    path: &str,
) -> Result<(), ContractError> {
    match node {
        ContractNode::String => {
            if !value.is_string() {
                return Err(error(format!("{path} must be a string")));
            }
        }
        ContractNode::Number => {
            if value.as_f64().is_none_or(|number| !number.is_finite()) {
                return Err(error(format!("{path} must be a finite number")));
            }
        }
        ContractNode::Boolean => {
            if !value.is_boolean() {
                return Err(error(format!("{path} must be a boolean")));
            }
        }
        ContractNode::Literal(values) => {
            if !values.iter().any(|expected| expected == value) {
                return Err(error(format!("{path} is not an allowed literal")));
            }
        }
        ContractNode::Array(item) => {
            let values = value
                .as_array()
                .ok_or_else(|| error(format!("{path} must be an array")))?;
            for (index, value) in values.iter().enumerate() {
                validate_schema(item, value, domain, &format!("{path}[{index}]"))?;
            }
        }
        ContractNode::Tuple(items) => {
            let values = value
                .as_array()
                .ok_or_else(|| error(format!("{path} must be a tuple")))?;
            if values.len() != items.len() {
                return Err(error(format!("{path} tuple cardinality mismatch")));
            }
            for (index, (item, value)) in items.iter().zip(values).enumerate() {
                validate_schema(item, value, domain, &format!("{path}[{index}]"))?;
            }
        }
        ContractNode::Optional(item) => {
            if value.is_null() {
                return Err(error(format!("{path} optional value cannot be null")));
            }
            validate_schema(item, value, domain, path)?;
        }
        ContractNode::Object(fields) => {
            let map = object(value, path)?;
            let expected = fields.keys().map(String::as_str).collect::<BTreeSet<_>>();
            let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
            if actual.iter().any(|field| !expected.contains(field)) {
                return Err(error(format!("{path} contains an unknown field")));
            }
            for (field, child) in fields {
                match map.get(field) {
                    Some(value) => {
                        validate_schema(child, value, domain, &format!("{path}.{field}"))?
                    }
                    None if matches!(child, ContractNode::Optional(_)) => {}
                    None => return Err(error(format!("{path}.{field} is missing"))),
                }
            }
        }
        ContractNode::ExternalRef {
            dependency,
            reference_kind,
        } => {
            if dependency != "aira-synapse-domain-contract@1" {
                return Err(error(format!("unknown contract dependency {dependency}")));
            }
            let child = domain.get(reference_kind).ok_or_else(|| {
                error(format!(
                    "unknown delegated domain reference {reference_kind}"
                ))
            })?;
            validate_schema(child, value, domain, path)?;
        }
    }
    Ok(())
}

fn validate_fixture_bytes(bytes: &[u8]) -> Result<(), ContractError> {
    let fixture: Value = parse_json(bytes, "bounded retrieval fixture")?;
    let map = object(&fixture, "bounded retrieval fixture")?;
    exact_keys(
        map,
        &["exchanges", "fixtureVersion"],
        "bounded retrieval fixture",
    )?;
    if map.get("fixtureVersion").and_then(Value::as_str) != Some(EXPECTED_FIXTURE_VERSION) {
        return Err(error("bounded retrieval fixture version mismatch"));
    }
    let exchanges = object(
        map.get("exchanges").expect("checked exchanges"),
        "bounded retrieval fixture.exchanges",
    )?;
    let expected = METHODS.iter().copied().collect::<BTreeSet<_>>();
    let actual = exchanges
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(error("bounded retrieval fixture operation set mismatch"));
    }
    for method in METHODS {
        let exchange = object(
            exchanges.get(method).expect("checked method"),
            &format!("fixture exchange {method}"),
        )?;
        exact_keys(
            exchange,
            &["request", "result"],
            &format!("fixture exchange {method}"),
        )?;
    }
    Ok(())
}

/// Validate the retrieval manifest's exact version, five dependency pins, and
/// operation digest cross-links without reading any source dependency.
pub fn verify_manifest_bytes(bytes: &[u8]) -> Result<(), ContractError> {
    let manifest: RetrievalManifest = parse_json(bytes, "bounded retrieval manifest")?;
    if manifest != expected_manifest() {
        return Err(error("retrieval manifest cross-link mismatch"));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct OperationContract {
    pub request: ContractNode,
    pub result: ContractNode,
    request_assertions: Vec<Value>,
    exchange_assertions: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct ProducerContract {
    pub contract_version: String,
    pub refinement_ir_version: String,
    pub operations: BTreeMap<String, OperationContract>,
    domain_contract: BTreeMap<String, ContractNode>,
}

#[derive(Debug, Clone, Copy)]
struct IrNodeSpec {
    role: &'static str,
    fields: &'static [(&'static str, &'static str)],
}

const IR_NODE_SPECS: &[(&str, IrNodeSpec)] = &[
    (
        "all",
        IrNodeSpec {
            role: "expression",
            fields: &[("values", "expression_array_nonempty")],
        },
    ),
    (
        "any",
        IrNodeSpec {
            role: "expression",
            fields: &[("values", "expression_array_nonempty")],
        },
    ),
    (
        "array_at",
        IrNodeSpec {
            role: "expression",
            fields: &[("array", "expression"), ("index", "expression")],
        },
    ),
    (
        "array_length",
        IrNodeSpec {
            role: "expression",
            fields: &[("value", "expression")],
        },
    ),
    (
        "coalesce",
        IrNodeSpec {
            role: "expression",
            fields: &[("values", "expression_array_nonempty")],
        },
    ),
    (
        "concat",
        IrNodeSpec {
            role: "expression",
            fields: &[("values", "expression_array_nonempty")],
        },
    ),
    (
        "corpus_eq_ref",
        IrNodeSpec {
            role: "assertion",
            fields: &[("corpus", "expression"), ("expected", "expression")],
        },
    ),
    (
        "eq",
        IrNodeSpec {
            role: "expression",
            fields: &[("left", "expression"), ("right", "expression")],
        },
    ),
    (
        "field_eq_ref",
        IrNodeSpec {
            role: "assertion",
            fields: &[("expected", "expression"), ("value", "expression")],
        },
    ),
    (
        "finite_range",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("maximum", "number"),
                ("minimum", "number"),
                ("value", "expression"),
            ],
        },
    ),
    (
        "for_each",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("collection", "expression"),
                ("predicate", "expression"),
                ("scope", "string"),
            ],
        },
    ),
    (
        "iteration_pointer",
        IrNodeSpec {
            role: "expression",
            fields: &[("path", "json_pointer"), ("scope", "string")],
        },
    ),
    (
        "length_eq",
        IrNodeSpec {
            role: "assertion",
            fields: &[("actual", "expression"), ("expected", "expression")],
        },
    ),
    (
        "length_lte_ref",
        IrNodeSpec {
            role: "assertion",
            fields: &[("actual", "expression"), ("limit", "expression")],
        },
    ),
    (
        "literal",
        IrNodeSpec {
            role: "expression",
            fields: &[("value", "scalar")],
        },
    ),
    (
        "lt",
        IrNodeSpec {
            role: "expression",
            fields: &[("left", "expression"), ("right", "expression")],
        },
    ),
    (
        "lte",
        IrNodeSpec {
            role: "expression",
            fields: &[("left", "expression"), ("right", "expression")],
        },
    ),
    (
        "map_lookup",
        IrNodeSpec {
            role: "expression",
            fields: &[
                ("key", "expression"),
                ("keyField", "string"),
                ("map", "expression"),
                ("valueField", "string"),
            ],
        },
    ),
    (
        "max",
        IrNodeSpec {
            role: "expression",
            fields: &[("values", "expression_array_nonempty")],
        },
    ),
    (
        "multiply",
        IrNodeSpec {
            role: "expression",
            fields: &[("left", "expression"), ("right", "expression")],
        },
    ),
    (
        "normalize_ref",
        IrNodeSpec {
            role: "expression",
            fields: &[("dependency", "string"), ("value", "expression")],
        },
    ),
    (
        "not",
        IrNodeSpec {
            role: "expression",
            fields: &[("value", "expression")],
        },
    ),
    (
        "ordered_score_desc_id_asc",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("collection", "expression"),
                ("id", "expression"),
                ("idOrder", "id_order"),
                ("scope", "string"),
                ("score", "expression"),
            ],
        },
    ),
    (
        "pointer",
        IrNodeSpec {
            role: "expression",
            fields: &[("path", "json_pointer"), ("root", "root")],
        },
    ),
    (
        "prefixed_identity",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("identity", "expression"),
                ("prefix", "string"),
                ("value", "expression"),
            ],
        },
    ),
    (
        "rank_is_index_plus_one",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("collection", "expression"),
                ("rank", "expression"),
                ("scope", "string"),
            ],
        },
    ),
    (
        "safe_integer_range",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("maximum", "expression"),
                ("minimum", "number"),
                ("value", "expression"),
            ],
        },
    ),
    (
        "set_contains",
        IrNodeSpec {
            role: "expression",
            fields: &[("set", "expression"), ("value", "expression")],
        },
    ),
    (
        "tuple_tags",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("actual", "expression"),
                ("expected", "string_array"),
                ("field", "string"),
            ],
        },
    ),
    (
        "unique_by",
        IrNodeSpec {
            role: "assertion",
            fields: &[
                ("collection", "expression"),
                ("key", "expression"),
                ("scope", "string"),
            ],
        },
    ),
];

fn ir_spec(op: &str) -> Option<IrNodeSpec> {
    IR_NODE_SPECS
        .iter()
        .find(|(name, _)| *name == op)
        .map(|(_, spec)| *spec)
}

fn validate_json_pointer(path: &str, label: &str) -> Result<(), ContractError> {
    if path.is_empty() {
        return Ok(());
    }
    if !path.starts_with('/') {
        return Err(error(format!("{label} must be an RFC 6901 pointer")));
    }
    for segment in path[1..].split('/') {
        let mut chars = segment.chars();
        while let Some(character) = chars.next() {
            if character == '~' && !matches!(chars.next(), Some('0' | '1')) {
                return Err(error(format!("{label} contains an invalid escape")));
            }
        }
    }
    Ok(())
}

fn validate_ir_field(
    value: &Value,
    marker: &str,
    nodes: &BTreeMap<String, IrNodeSpec>,
    label: &str,
) -> Result<(), ContractError> {
    match marker {
        "expression" => validate_ir_expression(value, "expression", nodes, label),
        "expression_array_nonempty" => {
            let values = value
                .as_array()
                .ok_or_else(|| error(format!("{label} must be an expression array")))?;
            if values.is_empty() {
                return Err(error(format!("{label} must be nonempty")));
            }
            for (index, value) in values.iter().enumerate() {
                validate_ir_expression(value, "expression", nodes, &format!("{label}[{index}]"))?;
            }
            Ok(())
        }
        "number" => {
            if value.as_f64().is_none_or(|number| !number.is_finite()) {
                Err(error(format!("{label} must be a finite JSON number")))
            } else {
                Ok(())
            }
        }
        "scalar" => {
            if is_scalar(value) {
                Ok(())
            } else {
                Err(error(format!("{label} must be a scalar")))
            }
        }
        "string" => value
            .as_str()
            .map(|_| ())
            .ok_or_else(|| error(format!("{label} must be a string"))),
        "json_pointer" => value
            .as_str()
            .ok_or_else(|| error(format!("{label} must be a string")))
            .and_then(|path| validate_json_pointer(path, label)),
        "root" => match value.as_str() {
            Some("request") | Some("result") => Ok(()),
            _ => Err(error(format!("{label} must select request or result"))),
        },
        "id_order" => match value.as_str() {
            Some("unicode_utf16_code_unit_asc") => Ok(()),
            _ => Err(error(format!("{label} has an unsupported id order"))),
        },
        "string_array" => {
            let values = value
                .as_array()
                .ok_or_else(|| error(format!("{label} must be a string array")))?;
            if values.is_empty() || values.iter().any(|value| !value.is_string()) {
                Err(error(format!("{label} must be a nonempty string array")))
            } else {
                Ok(())
            }
        }
        unknown => Err(error(format!("unknown refinement field marker {unknown}"))),
    }
}

fn validate_ir_expression(
    value: &Value,
    expected_role: &str,
    nodes: &BTreeMap<String, IrNodeSpec>,
    label: &str,
) -> Result<(), ContractError> {
    let map = object(value, label)?;
    let op = map
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| error(format!("{label}.op must be a string")))?;
    let spec = nodes
        .get(op)
        .ok_or_else(|| error(format!("unknown refinement opcode {op}")))?;
    if spec.role != expected_role {
        return Err(error(format!(
            "refinement opcode {op} has role {}, expected {expected_role}",
            spec.role
        )));
    }
    let mut fields = vec!["op"];
    fields.extend(spec.fields.iter().map(|(field, _)| *field));
    exact_keys(map, &fields, label)?;
    for (field, marker) in spec.fields {
        let field_value = map.get(*field).expect("exact_keys checked IR field");
        validate_ir_field(field_value, marker, nodes, &format!("{label}.{field}"))?;
    }
    if op == "normalize_ref"
        && map.get("dependency").and_then(Value::as_str) != Some(NORMALIZATION_DIGEST)
    {
        return Err(error("normalize_ref names an unpinned dependency"));
    }
    Ok(())
}

fn parse_refinement_nodes(value: &Value) -> Result<BTreeMap<String, IrNodeSpec>, ContractError> {
    let map = object(value, "refinementNodes")?;
    let expected = IR_NODE_SPECS
        .iter()
        .map(|(name, _)| *name)
        .collect::<BTreeSet<_>>();
    let actual = map.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(error("refinement IR node set mismatch"));
    }
    let mut nodes = BTreeMap::new();
    for (name, value) in map {
        let declaration = object(value, &format!("refinementNodes.{name}"))?;
        exact_keys(
            declaration,
            &["fields", "role"],
            &format!("refinementNodes.{name}"),
        )?;
        let role = declaration
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(|| error(format!("refinementNodes.{name}.role must be a string")))?;
        let fields = object(
            declaration.get("fields").expect("checked fields"),
            &format!("refinementNodes.{name}.fields"),
        )?;
        let expected_spec = ir_spec(name).expect("static IR spec set");
        if role != expected_spec.role {
            return Err(error(format!("refinement node {name} role mismatch")));
        }
        let expected_fields = expected_spec
            .fields
            .iter()
            .map(|(field, marker)| (*field, *marker))
            .collect::<BTreeMap<_, _>>();
        let actual_fields = fields
            .iter()
            .map(|(field, marker)| {
                marker
                    .as_str()
                    .map(|marker| (field.as_str(), marker))
                    .ok_or_else(|| {
                        error(format!(
                            "refinement node {name} field marker must be a string"
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        if actual_fields != expected_fields {
            return Err(error(format!(
                "refinement node {name} field grammar mismatch"
            )));
        }
        // The declaration is checked against the native interpreter grammar;
        // retaining the static grammar avoids leaking one allocation per
        // contract parse while still making the producer bytes authoritative
        // for the presence and exact shape of every node.
        nodes.insert(name.clone(), expected_spec);
    }
    Ok(nodes)
}

/// Parse and bind the producer contract.  The parser rejects unknown schema
/// kinds, dependency names, refinement opcodes, roles, and field grammars.
pub fn parse_producer_contract(bytes: &[u8]) -> Result<ProducerContract, ContractError> {
    let value: Value = parse_json(bytes, "bounded retrieval contract")?;
    let map = object(&value, "bounded retrieval contract")?;
    exact_keys(
        map,
        &[
            "contractVersion",
            "operations",
            "refinementIrVersion",
            "refinementNodes",
        ],
        "bounded retrieval contract",
    )?;
    let contract_version = map
        .get("contractVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| error("contractVersion must be a string"))?;
    if contract_version != CONTRACT_VERSION {
        return Err(error("bounded retrieval contract version mismatch"));
    }
    let refinement_ir_version = map
        .get("refinementIrVersion")
        .and_then(Value::as_str)
        .ok_or_else(|| error("refinementIrVersion must be a string"))?;
    if refinement_ir_version != REFINEMENT_IR_VERSION {
        return Err(error("refinement IR version mismatch"));
    }
    let nodes = parse_refinement_nodes(map.get("refinementNodes").expect("checked nodes"))?;
    let domain_contract = parse_domain_contract()?;
    let operations_value = object(
        map.get("operations").expect("checked operations"),
        "bounded retrieval contract.operations",
    )?;
    let expected_methods = METHODS.iter().copied().collect::<BTreeSet<_>>();
    let actual_methods = operations_value
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if actual_methods != expected_methods {
        return Err(error("bounded retrieval operation set mismatch"));
    }
    let mut operations = BTreeMap::new();
    for method in METHODS {
        let operation = object(
            operations_value.get(method).expect("checked operation"),
            &format!("operation {method}"),
        )?;
        exact_keys(
            operation,
            &["refinement", "request", "result"],
            &format!("operation {method}"),
        )?;
        let request = parse_contract_node(
            operation.get("request").expect("checked request"),
            &format!("operation {method}.request"),
        )?;
        let result = parse_contract_node(
            operation.get("result").expect("checked result"),
            &format!("operation {method}.result"),
        )?;
        validate_external_dependencies(&request, &domain_contract)?;
        validate_external_dependencies(&result, &domain_contract)?;
        let refinement = object(
            operation.get("refinement").expect("checked refinement"),
            &format!("operation {method}.refinement"),
        )?;
        exact_keys(
            refinement,
            &["exchangeAssertions", "requestAssertions", "version"],
            &format!("operation {method}.refinement"),
        )?;
        if refinement.get("version").and_then(Value::as_str) != Some(REFINEMENT_IR_VERSION) {
            return Err(error(format!(
                "operation {method} refinement version mismatch"
            )));
        }
        let request_assertions = parse_assertions(
            refinement
                .get("requestAssertions")
                .expect("checked request assertions"),
            &nodes,
            &format!("operation {method}.requestAssertions"),
        )?;
        let exchange_assertions = parse_assertions(
            refinement
                .get("exchangeAssertions")
                .expect("checked exchange assertions"),
            &nodes,
            &format!("operation {method}.exchangeAssertions"),
        )?;
        operations.insert(
            method.to_string(),
            OperationContract {
                request,
                result,
                request_assertions,
                exchange_assertions,
            },
        );
    }
    Ok(ProducerContract {
        contract_version: contract_version.to_string(),
        refinement_ir_version: refinement_ir_version.to_string(),
        operations,
        domain_contract,
    })
}

fn validate_external_dependencies(
    node: &ContractNode,
    domain_contract: &BTreeMap<String, ContractNode>,
) -> Result<(), ContractError> {
    match node {
        ContractNode::ExternalRef {
            dependency,
            reference_kind,
        } => {
            if dependency != "aira-synapse-domain-contract@1" {
                return Err(error(format!("unknown contract dependency {dependency}")));
            }
            if !domain_contract.contains_key(reference_kind) {
                return Err(error(format!(
                    "unknown delegated domain reference {reference_kind}"
                )));
            }
        }
        ContractNode::Array(item) | ContractNode::Optional(item) => {
            validate_external_dependencies(item, domain_contract)?;
        }
        ContractNode::Tuple(items) => {
            for item in items {
                validate_external_dependencies(item, domain_contract)?;
            }
        }
        ContractNode::Object(fields) => {
            for item in fields.values() {
                validate_external_dependencies(item, domain_contract)?;
            }
        }
        ContractNode::String
        | ContractNode::Number
        | ContractNode::Boolean
        | ContractNode::Literal(_) => {}
    }
    Ok(())
}

fn parse_assertions(
    value: &Value,
    nodes: &BTreeMap<String, IrNodeSpec>,
    label: &str,
) -> Result<Vec<Value>, ContractError> {
    let assertions = value
        .as_array()
        .ok_or_else(|| error(format!("{label} must be an array")))?;
    if assertions.is_empty() {
        return Err(error(format!("{label} must be nonempty")));
    }
    for (index, assertion) in assertions.iter().enumerate() {
        validate_ir_expression(assertion, "assertion", nodes, &format!("{label}[{index}]"))?;
    }
    Ok(assertions.to_vec())
}

fn pinned_contract() -> Result<&'static ProducerContract, ContractError> {
    static CONTRACT: OnceLock<Result<ProducerContract, ContractError>> = OnceLock::new();
    CONTRACT
        .get_or_init(|| {
            verify_embedded_artifacts()?;
            parse_producer_contract(CONTRACT_BYTES)
        })
        .as_ref()
        .map_err(|e| e.clone())
}

pub fn operation_names() -> Result<Vec<&'static str>, ContractError> {
    let contract = pinned_contract()?;
    let names = METHODS
        .iter()
        .copied()
        .filter(|name| contract.operations.contains_key(*name))
        .collect::<Vec<_>>();
    if names.len() != METHODS.len() {
        return Err(error("pinned contract operation inventory is incomplete"));
    }
    Ok(names)
}

pub type Normalizer<'a> = dyn Fn(&str, &str) -> Option<String> + 'a;

struct EvalContext<'a> {
    request: &'a Value,
    result: Option<&'a Value>,
    normalizer: &'a Normalizer<'a>,
}

enum EvalValue<'a> {
    Ref(&'a Value),
    Refs(Vec<&'a Value>),
    Owned(Value),
}

impl<'a> EvalValue<'a> {
    fn refs(&self) -> Vec<&Value> {
        match self {
            Self::Ref(value) => vec![value],
            Self::Refs(values) => values.clone(),
            Self::Owned(value) => vec![value],
        }
    }

    fn one(&self, label: &str) -> Result<&Value, ContractError> {
        match self {
            Self::Ref(value) => Ok(value),
            Self::Owned(value) => Ok(value),
            Self::Refs(values) if values.len() == 1 => Ok(values[0]),
            Self::Refs(_) => Err(error(format!("{label} expected one value"))),
        }
    }
}

fn json_pointer_segment(segment: &str) -> Result<String, ContractError> {
    let mut decoded = String::with_capacity(segment.len());
    let mut chars = segment.chars();
    while let Some(character) = chars.next() {
        if character == '~' {
            match chars.next() {
                Some('0') => decoded.push('~'),
                Some('1') => decoded.push('/'),
                _ => return Err(error("invalid JSON pointer escape")),
            }
        } else {
            decoded.push(character);
        }
    }
    Ok(decoded)
}

fn pointer_values<'a>(root: &'a Value, path: &str) -> Result<Vec<&'a Value>, ContractError> {
    if path.is_empty() {
        return Ok(vec![root]);
    }
    let mut current = vec![root];
    for raw_segment in path[1..].split('/') {
        let segment = json_pointer_segment(raw_segment)?;
        let mut next = Vec::new();
        for value in current {
            if segment == "*" {
                match value {
                    Value::Array(values) => next.extend(values.iter()),
                    Value::Object(values) => next.extend(values.values()),
                    _ => return Err(error("JSON pointer wildcard requires a collection")),
                }
            } else {
                match value {
                    Value::Object(values) => values
                        .get(&segment)
                        .map(|value| next.push(value))
                        .ok_or_else(|| error(format!("JSON pointer field {segment} is missing")))?,
                    Value::Array(values) => {
                        let index = segment
                            .parse::<usize>()
                            .map_err(|_| error("JSON pointer array index is invalid"))?;
                        next.push(
                            values
                                .get(index)
                                .ok_or_else(|| error("JSON pointer array index is missing"))?,
                        );
                    }
                    _ => return Err(error("JSON pointer traversed a scalar")),
                }
            }
        }
        if next.is_empty() {
            return Err(error("JSON pointer selected no values"));
        }
        current = next;
    }
    Ok(current)
}

fn pointer_value<'a>(root: &'a Value, path: &str) -> Result<EvalValue<'a>, ContractError> {
    let values = pointer_values(root, path)?;
    if path.split('/').any(|segment| segment == "*") {
        Ok(EvalValue::Refs(values))
    } else {
        Ok(EvalValue::Ref(values[0]))
    }
}

fn current_pointer<'a>(
    current: Option<(&str, &'a Value)>,
    scope: &str,
    path: &str,
) -> Result<EvalValue<'a>, ContractError> {
    let (current_scope, value) = current.ok_or_else(|| error("iteration pointer has no scope"))?;
    if current_scope != scope {
        return Err(error(format!("iteration scope mismatch: {scope}")));
    }
    pointer_value(value, path)
}

fn values_equal(left: &EvalValue<'_>, right: &EvalValue<'_>) -> Result<bool, ContractError> {
    match (left, right) {
        (EvalValue::Refs(_), _) | (_, EvalValue::Refs(_)) => {
            Err(error("array of values cannot be compared as a scalar"))
        }
        (EvalValue::Ref(left), EvalValue::Ref(right)) => Ok(*left == *right),
        (EvalValue::Ref(left), EvalValue::Owned(right))
        | (EvalValue::Owned(right), EvalValue::Ref(left)) => Ok(*left == right),
        (EvalValue::Owned(left), EvalValue::Owned(right)) => Ok(left == right),
    }
}

fn bool_value(value: &EvalValue<'_>, label: &str) -> Result<bool, ContractError> {
    value
        .one(label)?
        .as_bool()
        .ok_or_else(|| error(format!("{label} must evaluate to boolean")))
}

fn number_value(value: &EvalValue<'_>, label: &str) -> Result<f64, ContractError> {
    let number = value
        .one(label)?
        .as_f64()
        .ok_or_else(|| error(format!("{label} must evaluate to number")))?;
    if !number.is_finite() {
        return Err(error(format!("{label} must be finite")));
    }
    Ok(number)
}

fn string_value(value: &EvalValue<'_>, label: &str) -> Result<String, ContractError> {
    value
        .one(label)?
        .as_str()
        .map(str::to_string)
        .ok_or_else(|| error(format!("{label} must evaluate to string")))
}

fn array_values<'a>(value: &EvalValue<'a>, label: &str) -> Result<&'a Vec<Value>, ContractError> {
    match value {
        EvalValue::Ref(value) => value
            .as_array()
            .ok_or_else(|| error(format!("{label} must evaluate to array"))),
        EvalValue::Refs(_) | EvalValue::Owned(_) => {
            Err(error(format!("{label} must evaluate to a borrowed array")))
        }
    }
}

fn scalar_number(number: f64, label: &str) -> Result<EvalValue<'static>, ContractError> {
    Number::from_f64(number)
        .map(|number| EvalValue::Owned(Value::Number(number)))
        .ok_or_else(|| error(format!("{label} produced a non-finite number")))
}

fn scalar_u64(number: u64) -> EvalValue<'static> {
    EvalValue::Owned(Value::Number(Number::from(number)))
}

fn eval<'a>(
    expression: &Value,
    context: &EvalContext<'a>,
    current: Option<(&str, &'a Value)>,
) -> Result<EvalValue<'a>, ContractError> {
    let map = object(expression, "refinement expression")?;
    let op = map
        .get("op")
        .and_then(Value::as_str)
        .ok_or_else(|| error("refinement expression op is missing"))?;
    let field = |name: &str| {
        map.get(name)
            .ok_or_else(|| error(format!("refinement opcode {op} field {name} is missing")))
    };
    match op {
        "literal" => Ok(EvalValue::Owned(field("value")?.clone())),
        "pointer" => {
            let path = field("path")?
                .as_str()
                .ok_or_else(|| error("pointer path is not a string"))?;
            let root = match field("root")?.as_str() {
                Some("request") => context.request,
                Some("result") => context
                    .result
                    .ok_or_else(|| error("result pointer used without a result"))?,
                _ => return Err(error("pointer root is invalid")),
            };
            pointer_value(root, path)
        }
        "iteration_pointer" => {
            let path = field("path")?
                .as_str()
                .ok_or_else(|| error("iteration pointer path is not a string"))?;
            let scope = field("scope")?
                .as_str()
                .ok_or_else(|| error("iteration pointer scope is not a string"))?;
            current_pointer(current, scope, path)
        }
        "array_length" => {
            let value = eval(field("value")?, context, current)?;
            let length = match &value {
                EvalValue::Refs(values) => values.len(),
                _ => value.one("array_length")?.as_array().map_or(0, Vec::len),
            };
            if !matches!(&value, EvalValue::Refs(_))
                && value.one("array_length")?.as_array().is_none()
            {
                return Err(error("array_length requires an array"));
            }
            Ok(scalar_u64(length as u64))
        }
        "array_at" => {
            let array = eval(field("array")?, context, current)?;
            let index = number_value(&eval(field("index")?, context, current)?, "array index")?;
            if index < 0.0 || index.fract() != 0.0 || index > usize::MAX as f64 {
                return Err(error("array index is not a safe integer"));
            }
            let value = array_values(&array, "array_at")?
                .get(index as usize)
                .ok_or_else(|| error("array_at index is missing"))?;
            Ok(EvalValue::Ref(value))
        }
        "all" | "any" => {
            let values = field("values")?
                .as_array()
                .ok_or_else(|| error(format!("{op}.values is not an array")))?;
            let mut result = op == "all";
            for value in values {
                let next = bool_value(&eval(value, context, current)?, op)?;
                if op == "all" {
                    result &= next;
                    if !result {
                        break;
                    }
                } else {
                    result |= next;
                    if result {
                        break;
                    }
                }
            }
            Ok(EvalValue::Owned(Value::Bool(result)))
        }
        "coalesce" => {
            let values = field("values")?
                .as_array()
                .ok_or_else(|| error("coalesce.values is not an array"))?;
            for expression in values {
                let value = eval(expression, context, current)?;
                if !value.one("coalesce")?.is_null() {
                    return Ok(value);
                }
            }
            Ok(EvalValue::Owned(Value::Null))
        }
        "concat" => {
            let values = field("values")?
                .as_array()
                .ok_or_else(|| error("concat.values is not an array"))?;
            let mut result = String::new();
            for expression in values {
                result.push_str(&string_value(
                    &eval(expression, context, current)?,
                    "concat",
                )?);
            }
            Ok(EvalValue::Owned(Value::String(result)))
        }
        "eq" => Ok(EvalValue::Owned(Value::Bool(values_equal(
            &eval(field("left")?, context, current)?,
            &eval(field("right")?, context, current)?,
        )?))),
        "lt" | "lte" => {
            let left = number_value(&eval(field("left")?, context, current)?, op)?;
            let right = number_value(&eval(field("right")?, context, current)?, op)?;
            Ok(EvalValue::Owned(Value::Bool(if op == "lt" {
                left < right
            } else {
                left <= right
            })))
        }
        "multiply" => {
            let left = number_value(&eval(field("left")?, context, current)?, "multiply")?;
            let right = number_value(&eval(field("right")?, context, current)?, "multiply")?;
            scalar_number(left * right, "multiply")
        }
        "max" => {
            let values = field("values")?
                .as_array()
                .ok_or_else(|| error("max.values is not an array"))?;
            let mut maximum = f64::NEG_INFINITY;
            for expression in values {
                maximum = maximum.max(number_value(&eval(expression, context, current)?, "max")?);
            }
            scalar_number(maximum, "max")
        }
        "not" => Ok(EvalValue::Owned(Value::Bool(!bool_value(
            &eval(field("value")?, context, current)?,
            "not",
        )?))),
        "normalize_ref" => {
            let dependency = field("dependency")?
                .as_str()
                .ok_or_else(|| error("normalize_ref dependency is not a string"))?;
            let value = string_value(&eval(field("value")?, context, current)?, "normalize_ref")?;
            let normalized = (context.normalizer)(dependency, &value).ok_or_else(|| {
                error(format!("normalization dependency {dependency} unavailable"))
            })?;
            Ok(EvalValue::Owned(Value::String(normalized)))
        }
        "map_lookup" => {
            let key = eval(field("key")?, context, current)?;
            let key_field = field("keyField")?
                .as_str()
                .ok_or_else(|| error("map_lookup keyField is not a string"))?;
            let value_field = field("valueField")?
                .as_str()
                .ok_or_else(|| error("map_lookup valueField is not a string"))?;
            let map = eval(field("map")?, context, current)?;
            let entries = array_values(&map, "map_lookup")?;
            for entry in entries {
                let entry_map = entry
                    .as_object()
                    .ok_or_else(|| error("map_lookup entry is not an object"))?;
                if let Some(candidate) = entry_map.get(key_field) {
                    if values_equal(&EvalValue::Ref(candidate), &key)? {
                        return Ok(EvalValue::Ref(
                            entry_map
                                .get(value_field)
                                .ok_or_else(|| error("map_lookup value field is missing"))?,
                        ));
                    }
                }
            }
            Ok(EvalValue::Owned(Value::Null))
        }
        "set_contains" => {
            let set = eval(field("set")?, context, current)?;
            let values = array_values(&set, "set_contains")?;
            let value = eval(field("value")?, context, current)?;
            let mut contains = false;
            for candidate in values {
                if values_equal(&EvalValue::Ref(candidate), &value)? {
                    contains = true;
                    break;
                }
            }
            Ok(EvalValue::Owned(Value::Bool(contains)))
        }
        "field_eq_ref" => Ok(EvalValue::Owned(Value::Bool(values_equal(
            &eval(field("value")?, context, current)?,
            &eval(field("expected")?, context, current)?,
        )?))),
        "finite_range" => {
            let minimum = field("minimum")?
                .as_f64()
                .ok_or_else(|| error("finite_range minimum is not a number"))?;
            let maximum = field("maximum")?
                .as_f64()
                .ok_or_else(|| error("finite_range maximum is not a number"))?;
            let value = eval(field("value")?, context, current)?;
            let valid = value
                .refs()
                .iter()
                .map(|value| {
                    value.as_f64().is_some_and(|number| {
                        number.is_finite() && number >= minimum && number <= maximum
                    })
                })
                .all(|valid| valid);
            Ok(EvalValue::Owned(Value::Bool(valid)))
        }
        "safe_integer_range" => {
            let minimum = field("minimum")?
                .as_f64()
                .ok_or_else(|| error("safe_integer_range minimum is not a number"))?;
            let maximum = number_value(
                &eval(field("maximum")?, context, current)?,
                "safe_integer_range maximum",
            )?;
            let value = eval(field("value")?, context, current)?;
            let valid = value.refs().iter().all(|value| {
                value.as_u64().is_some_and(|number| {
                    let number = number as f64;
                    number >= minimum && number <= maximum && number <= MAX_SAFE_GENERATION as f64
                })
            });
            Ok(EvalValue::Owned(Value::Bool(valid)))
        }
        "tuple_tags" => {
            let expected = field("expected")?
                .as_array()
                .ok_or_else(|| error("tuple_tags expected is not an array"))?;
            let field_name = field("field")?
                .as_str()
                .ok_or_else(|| error("tuple_tags field is not a string"))?;
            let actual = eval(field("actual")?, context, current)?;
            let actual = actual
                .one("tuple_tags")?
                .as_array()
                .ok_or_else(|| error("tuple_tags actual is not an array"))?;
            let valid = actual.len() == expected.len()
                && actual.iter().zip(expected).all(|(actual, expected)| {
                    actual
                        .as_object()
                        .and_then(|map| map.get(field_name))
                        .zip(expected.as_str())
                        .is_some_and(|(actual, expected)| {
                            actual == &Value::String(expected.to_string())
                        })
                });
            Ok(EvalValue::Owned(Value::Bool(valid)))
        }
        "length_eq" | "length_lte_ref" => {
            let actual = eval(field("actual")?, context, current)?;
            let expected_name = if op == "length_eq" {
                "expected"
            } else {
                "limit"
            };
            let expected = eval(field(expected_name)?, context, current)?;
            let actual_len = actual
                .one("length assertion")?
                .as_array()
                .ok_or_else(|| error("length assertion actual is not an array"))?
                .len() as u64;
            let expected_len = number_value(&expected, "length assertion expected")?;
            Ok(EvalValue::Owned(Value::Bool(if op == "length_eq" {
                actual_len as f64 == expected_len
            } else {
                actual_len as f64 <= expected_len
            })))
        }
        "unique_by" => {
            let collection = eval(field("collection")?, context, current)?;
            let collection = collection
                .one("unique_by")?
                .as_array()
                .ok_or_else(|| error("unique_by collection is not an array"))?;
            let scope = field("scope")?
                .as_str()
                .ok_or_else(|| error("unique_by scope is not a string"))?;
            let key_expression = field("key")?;
            let mut keys = Vec::new();
            for item in collection {
                let key = eval(key_expression, context, Some((scope, item)))?;
                if keys
                    .iter()
                    .any(|seen: &EvalValue<'_>| values_equal(seen, &key).unwrap_or(false))
                {
                    return Ok(EvalValue::Owned(Value::Bool(false)));
                }
                keys.push(key);
            }
            Ok(EvalValue::Owned(Value::Bool(true)))
        }
        "ordered_score_desc_id_asc" => {
            let collection = eval(field("collection")?, context, current)?;
            let collection = collection
                .one("ordered_score_desc_id_asc")?
                .as_array()
                .ok_or_else(|| error("ordered collection is not an array"))?;
            let scope = field("scope")?
                .as_str()
                .ok_or_else(|| error("ordered scope is not a string"))?;
            let score_expression = field("score")?;
            let id_expression = field("id")?;
            let mut previous: Option<(f64, String)> = None;
            for item in collection {
                let score = number_value(
                    &eval(score_expression, context, Some((scope, item)))?,
                    "ordered score",
                )?;
                let id = string_value(
                    &eval(id_expression, context, Some((scope, item)))?,
                    "ordered id",
                )?;
                if let Some((previous_score, previous_id)) = &previous {
                    if score > *previous_score
                        || (score.total_cmp(previous_score).is_eq()
                            && id.encode_utf16().cmp(previous_id.encode_utf16()).is_lt())
                    {
                        return Ok(EvalValue::Owned(Value::Bool(false)));
                    }
                }
                previous = Some((score, id));
            }
            Ok(EvalValue::Owned(Value::Bool(true)))
        }
        "for_each" => {
            let collection = eval(field("collection")?, context, current)?;
            let collection = collection
                .one("for_each")?
                .as_array()
                .ok_or_else(|| error("for_each collection is not an array"))?;
            let scope = field("scope")?
                .as_str()
                .ok_or_else(|| error("for_each scope is not a string"))?;
            let predicate = field("predicate")?;
            for item in collection {
                if !bool_value(
                    &eval(predicate, context, Some((scope, item)))?,
                    "for_each predicate",
                )? {
                    return Ok(EvalValue::Owned(Value::Bool(false)));
                }
            }
            Ok(EvalValue::Owned(Value::Bool(true)))
        }
        "rank_is_index_plus_one" => {
            let collection = eval(field("collection")?, context, current)?;
            let collection = collection
                .one("rank_is_index_plus_one")?
                .as_array()
                .ok_or_else(|| error("rank collection is not an array"))?;
            let scope = field("scope")?
                .as_str()
                .ok_or_else(|| error("rank scope is not a string"))?;
            let rank_expression = field("rank")?;
            for (index, item) in collection.iter().enumerate() {
                let rank = eval(rank_expression, context, Some((scope, item)))?;
                let expected = (index as u64)
                    .checked_add(1)
                    .ok_or_else(|| error("rank counter overflow"))?;
                if rank.one("rank")?.as_u64() != Some(expected) {
                    return Ok(EvalValue::Owned(Value::Bool(false)));
                }
            }
            Ok(EvalValue::Owned(Value::Bool(true)))
        }
        "prefixed_identity" => {
            let identity = string_value(&eval(field("identity")?, context, current)?, "identity")?;
            let prefix = field("prefix")?
                .as_str()
                .ok_or_else(|| error("prefix is not a string"))?;
            let value = string_value(&eval(field("value")?, context, current)?, "value")?;
            Ok(EvalValue::Owned(Value::Bool(
                value == format!("{prefix}{identity}"),
            )))
        }
        "corpus_eq_ref" => Ok(EvalValue::Owned(Value::Bool(values_equal(
            &eval(field("corpus")?, context, current)?,
            &eval(field("expected")?, context, current)?,
        )?))),
        unknown => Err(error(format!("unsupported refinement opcode {unknown}"))),
    }
}

fn evaluate_assertions(
    assertions: &[Value],
    request: &Value,
    result: Option<&Value>,
    normalizer: &Normalizer<'_>,
) -> Result<(), ContractError> {
    let context = EvalContext {
        request,
        result,
        normalizer,
    };
    for (index, assertion) in assertions.iter().enumerate() {
        if !bool_value(&eval(assertion, &context, None)?, "assertion")? {
            return Err(error(format!(
                "producer refinement assertion {index} failed"
            )));
        }
    }
    Ok(())
}

/// Validate an operation request using the schema and request assertions
/// embedded in the pinned producer contract.
pub fn validate_semantic_request(method: &str, request: &Value) -> Result<(), ContractError> {
    let no_normalizer = |_: &str, _: &str| None;
    validate_semantic_request_with_normalizer(method, request, &no_normalizer)
}

pub fn validate_semantic_request_with_normalizer(
    method: &str,
    request: &Value,
    normalizer: &Normalizer<'_>,
) -> Result<(), ContractError> {
    let contract = pinned_contract()?;
    let operation = contract
        .operations
        .get(method)
        .ok_or_else(|| error(format!("unsupported bounded retrieval operation {method}")))?;
    validate_schema(
        &operation.request,
        request,
        &contract.domain_contract,
        "request",
    )?;
    evaluate_assertions(&operation.request_assertions, request, None, normalizer)
}

/// Validate a producer/native exchange without copying either the request or
/// result.  The normalizer is an injected authority selected by the caller;
/// this module only enforces the dependency name declared in the producer IR.
pub fn validate_semantic_exchange(
    method: &str,
    request: &Value,
    result: &Value,
    normalizer: &Normalizer<'_>,
) -> Result<(), ContractError> {
    let contract = pinned_contract()?;
    let operation = contract
        .operations
        .get(method)
        .ok_or_else(|| error(format!("unsupported bounded retrieval operation {method}")))?;
    validate_schema(
        &operation.request,
        request,
        &contract.domain_contract,
        "request",
    )?;
    validate_schema(
        &operation.result,
        result,
        &contract.domain_contract,
        "result",
    )?;
    evaluate_assertions(&operation.request_assertions, request, None, normalizer)?;
    evaluate_assertions(
        &operation.exchange_assertions,
        request,
        Some(result),
        normalizer,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResourceUsage {
    pub vector_dimensions: u64,
    pub vector_comparisons: u64,
    pub search_slots: u64,
    pub facts_inspected: u64,
    pub graph_nodes: u64,
    pub graph_edges: u64,
    pub iterations: u64,
    pub search_result_limit: u64,
    pub expansion_results: u64,
    pub seeds: u64,
    pub returned_passages: u64,
    pub returned_facts: u64,
    pub combined_objects: u64,
}

impl ResourceUsage {
    pub fn check(&self) -> Result<(), ContractError> {
        let limits = [
            (
                "vector dimensions",
                self.vector_dimensions,
                MAX_VECTOR_DIMENSIONS,
            ),
            (
                "vector comparisons",
                self.vector_comparisons,
                MAX_VECTOR_COMPARISONS,
            ),
            ("search slots", self.search_slots, MAX_SEARCH_SLOTS),
            ("facts inspected", self.facts_inspected, MAX_FACTS_INSPECTED),
            ("graph nodes", self.graph_nodes, MAX_GRAPH_NODES),
            ("graph edges", self.graph_edges, MAX_GRAPH_EDGES),
            ("iterations", self.iterations, MAX_ITERATIONS),
            (
                "search result limit",
                self.search_result_limit,
                MAX_SEARCH_RESULT_LIMIT,
            ),
            (
                "expansion results",
                self.expansion_results,
                MAX_EXPANSION_RESULTS,
            ),
            ("seeds", self.seeds, MAX_SEEDS),
            (
                "returned passages",
                self.returned_passages,
                MAX_RETURNED_PASSAGES,
            ),
            ("returned facts", self.returned_facts, MAX_RETURNED_FACTS),
            (
                "combined objects",
                self.combined_objects,
                MAX_COMBINED_OBJECTS,
            ),
        ];
        for (name, actual, maximum) in limits {
            if actual > maximum {
                return Err(error(format!("{name} exceeds native hard cap")));
            }
        }
        Ok(())
    }

    pub fn for_request(method: &str, params: &Value) -> Result<Self, ContractError> {
        let mut usage = Self::default();
        match method {
            CANDIDATE_SEARCH => {
                let slots = params
                    .get("slots")
                    .and_then(Value::as_array)
                    .ok_or_else(|| error("candidate slots are missing"))?;
                usage.search_slots = slots.len() as u64;
                usage.vector_dimensions = slots
                    .iter()
                    .map(|slot| {
                        slot.get("queryVector")
                            .and_then(Value::as_array)
                            .map(|vector| vector.len() as u64)
                            .ok_or_else(|| error("candidate queryVector is missing"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .max()
                    .unwrap_or(0);
                usage.search_result_limit = slots
                    .iter()
                    .map(|slot| {
                        slot.get("limit")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| error("candidate limit is not a safe integer"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .max()
                    .unwrap_or(0);
            }
            FACT_EXPAND => {
                let plan = params
                    .get("plan")
                    .and_then(Value::as_object)
                    .ok_or_else(|| error("fact plan is missing"))?;
                usage.seeds = plan
                    .get("seedEntities")
                    .and_then(Value::as_array)
                    .ok_or_else(|| error("fact seedEntities is missing"))?
                    .len() as u64;
                usage.expansion_results = plan
                    .get("limit")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| error("fact limit is not a safe integer"))?;
            }
            PPR_MATERIALIZE => {
                let plan = params
                    .get("plan")
                    .and_then(Value::as_object)
                    .ok_or_else(|| error("PPR plan is missing"))?;
                usage.seeds = plan
                    .get("seeds")
                    .and_then(Value::as_array)
                    .ok_or_else(|| error("PPR seeds are missing"))?
                    .len() as u64;
                usage.iterations = plan
                    .get("maxIterations")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| error("PPR maxIterations is not a safe integer"))?;
                usage.returned_passages = plan
                    .get("passageLimit")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| error("PPR passageLimit is not a safe integer"))?;
                usage.returned_facts = plan
                    .get("entityLimit")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| error("PPR entityLimit is not a safe integer"))?;
            }
            _ => return Err(error(format!("unsupported bounded operation {method}"))),
        }
        usage.check()?;
        Ok(usage)
    }

    pub fn with_output_counts(mut self, output: OutputCounts) -> Result<Self, ContractError> {
        self.returned_passages = output.returned_passages;
        self.returned_facts = output.returned_facts;
        self.combined_objects = output.combined_objects;
        self.check()?;
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OutputCounts {
    pub returned_passages: u64,
    pub returned_facts: u64,
    pub combined_objects: u64,
}

impl OutputCounts {
    pub fn checked_combined_objects(
        returned_passages: u64,
        returned_facts: u64,
    ) -> Result<u64, ContractError> {
        returned_passages
            .checked_add(returned_facts)
            .ok_or_else(|| error("combined object count overflow"))
    }

    pub fn check(&self) -> Result<(), ContractError> {
        if self.returned_passages > MAX_RETURNED_PASSAGES
            || self.returned_facts > MAX_RETURNED_FACTS
            || self.combined_objects > MAX_COMBINED_OBJECTS
        {
            return Err(error("returned object count exceeds native hard cap"));
        }
        if self.combined_objects
            < self
                .returned_passages
                .checked_add(self.returned_facts)
                .ok_or_else(|| error("combined object count overflow"))?
        {
            return Err(error("combined object count is below returned objects"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkCounts {
    pub vector_comparisons: u64,
    pub facts_inspected: u64,
    pub iterations: u64,
    pub nodes_initialized: u64,
    pub edges_visited: u64,
    pub objects_considered_for_encoding: u64,
}

impl WorkCounts {
    /// Exactly the work expression frozen by the data-plane design.
    pub fn checked_work_units(&self) -> Result<u64, ContractError> {
        let graph_iteration_units = self
            .nodes_initialized
            .checked_add(self.edges_visited)
            .ok_or_else(|| error("iteration work addition overflow"))?;
        let repeated_graph_work = self
            .iterations
            .checked_mul(graph_iteration_units)
            .ok_or_else(|| error("iteration work multiplication overflow"))?;
        self.vector_comparisons
            .checked_add(self.facts_inspected)
            .and_then(|value| value.checked_add(repeated_graph_work))
            .and_then(|value| value.checked_add(self.objects_considered_for_encoding))
            .ok_or_else(|| error("aggregate work addition overflow"))
    }

    pub fn check(&self) -> Result<u64, ContractError> {
        if self.vector_comparisons > MAX_VECTOR_COMPARISONS
            || self.facts_inspected > MAX_FACTS_INSPECTED
            || self.iterations > MAX_ITERATIONS
            || self.nodes_initialized > MAX_GRAPH_NODES
            || self.edges_visited > MAX_GRAPH_EDGES
            || self.objects_considered_for_encoding > MAX_COMBINED_OBJECTS
        {
            return Err(error("work count exceeds native hard cap"));
        }
        self.checked_work_units()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AllocationInput {
    pub vector_values: u64,
    pub seed_entries: u64,
    pub result_entries: u64,
    pub ppr_score_entries: u64,
    pub heap_entries: u64,
    pub response_buffer_bytes: u64,
}

const VECTOR_VALUE_BYTES: u64 = 8;
const SEED_ENTRY_BYTES: u64 = 64;
const RESULT_ENTRY_BYTES: u64 = 64;
const SCORE_VALUE_BYTES: u64 = 8;
const HEAP_ENTRY_BYTES: u64 = 64;

impl AllocationInput {
    pub fn checked_capacity_bytes(&self) -> Result<u64, ContractError> {
        let vector = self
            .vector_values
            .checked_mul(VECTOR_VALUE_BYTES)
            .ok_or_else(|| error("vector allocation multiplication overflow"))?;
        let seeds = self
            .seed_entries
            .checked_mul(SEED_ENTRY_BYTES)
            .ok_or_else(|| error("seed allocation multiplication overflow"))?;
        let results = self
            .result_entries
            .checked_mul(RESULT_ENTRY_BYTES)
            .ok_or_else(|| error("result allocation multiplication overflow"))?;
        let scores = self
            .ppr_score_entries
            .checked_mul(2)
            .and_then(|value| value.checked_mul(SCORE_VALUE_BYTES))
            .ok_or_else(|| error("PPR score allocation multiplication overflow"))?;
        let heaps = self
            .heap_entries
            .checked_mul(HEAP_ENTRY_BYTES)
            .ok_or_else(|| error("heap allocation multiplication overflow"))?;
        vector
            .checked_add(seeds)
            .and_then(|value| value.checked_add(results))
            .and_then(|value| value.checked_add(scores))
            .and_then(|value| value.checked_add(heaps))
            .and_then(|value| value.checked_add(self.response_buffer_bytes))
            .ok_or_else(|| error("transient allocation addition overflow"))
    }

    pub fn check(&self) -> Result<u64, ContractError> {
        if self.response_buffer_bytes > MAX_RESPONSE_FRAME_BYTES {
            return Err(error("response buffer exceeds response frame cap"));
        }
        let bytes = self.checked_capacity_bytes()?;
        if bytes > MAX_TRANSIENT_BYTES {
            return Err(error("transient allocation exceeds native hard cap"));
        }
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionRemainder {
    remaining_ms: u64,
}

impl SessionRemainder {
    pub fn new(owner_remaining_ms: u64) -> Self {
        Self {
            remaining_ms: owner_remaining_ms.min(MAX_OPERATION_DEADLINE_MS),
        }
    }

    pub fn remaining_ms(&self) -> u64 {
        self.remaining_ms
    }

    pub fn accept_decreasing(&mut self, owner_remaining_ms: u64) -> Result<u64, ContractError> {
        let candidate = owner_remaining_ms.min(MAX_OPERATION_DEADLINE_MS);
        if candidate > self.remaining_ms {
            return Err(error("session remainder cannot increase"));
        }
        self.remaining_ms = candidate;
        Ok(candidate)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MonotonicDeadline {
    deadline_ms: u64,
    last_now_ms: u64,
    completed_units: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadlineError {
    ClockRegression,
    DeadlineExceeded,
    CheckpointGap,
    WorkRegression,
    ArithmeticOverflow,
}

impl fmt::Display for DeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ClockRegression => "monotonic clock moved backwards",
            Self::DeadlineExceeded => "operation deadline exceeded",
            Self::CheckpointGap => "deadline checkpoint gap exceeds 1024 work units",
            Self::WorkRegression => "completed work counter moved backwards",
            Self::ArithmeticOverflow => "deadline arithmetic overflow",
        })
    }
}

impl std::error::Error for DeadlineError {}

impl MonotonicDeadline {
    pub fn new(now_ms: u64, owner_remaining_ms: u64) -> Result<Self, DeadlineError> {
        let remaining = owner_remaining_ms.min(MAX_OPERATION_DEADLINE_MS);
        let deadline_ms = now_ms
            .checked_add(remaining)
            .ok_or(DeadlineError::ArithmeticOverflow)?;
        Ok(Self {
            deadline_ms,
            last_now_ms: now_ms,
            completed_units: 0,
        })
    }

    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    pub fn completed_units(&self) -> u64 {
        self.completed_units
    }

    pub fn remaining_ms(&mut self, now_ms: u64) -> Result<u64, DeadlineError> {
        self.observe_clock(now_ms)?;
        Ok(self.deadline_ms.saturating_sub(now_ms))
    }

    pub fn checkpoint(&mut self, now_ms: u64, completed_units: u64) -> Result<(), DeadlineError> {
        self.observe_clock(now_ms)?;
        if completed_units < self.completed_units {
            return Err(DeadlineError::WorkRegression);
        }
        if completed_units - self.completed_units > DEADLINE_CHECK_INTERVAL_UNITS {
            return Err(DeadlineError::CheckpointGap);
        }
        self.completed_units = completed_units;
        if now_ms >= self.deadline_ms {
            return Err(DeadlineError::DeadlineExceeded);
        }
        Ok(())
    }

    pub fn before_allocation(
        &mut self,
        now_ms: u64,
        completed_units: u64,
    ) -> Result<(), DeadlineError> {
        self.checkpoint(now_ms, completed_units)
    }

    pub fn before_materialization(
        &mut self,
        now_ms: u64,
        completed_units: u64,
    ) -> Result<(), DeadlineError> {
        self.checkpoint(now_ms, completed_units)
    }

    fn observe_clock(&mut self, now_ms: u64) -> Result<(), DeadlineError> {
        if now_ms < self.last_now_ms {
            return Err(DeadlineError::ClockRegression);
        }
        self.last_now_ms = now_ms;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkCounters {
    pub vector_comparisons: u64,
    pub facts_inspected: u64,
    pub iterations: u64,
    pub nodes_initialized: u64,
    pub edges_visited: u64,
    pub objects_considered_for_encoding: u64,
    pub work_units: u64,
    pub response_bytes: u64,
}

impl WorkCounters {
    pub fn from_work(work: WorkCounts, response_bytes: u64) -> Result<Self, ContractError> {
        let work_units = work.check()?;
        if response_bytes > MAX_RESPONSE_FRAME_BYTES {
            return Err(error("response bytes exceed native hard cap"));
        }
        Ok(Self {
            vector_comparisons: work.vector_comparisons,
            facts_inspected: work.facts_inspected,
            iterations: work.iterations,
            nodes_initialized: work.nodes_initialized,
            edges_visited: work.edges_visited,
            objects_considered_for_encoding: work.objects_considered_for_encoding,
            work_units,
            response_bytes,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BoundedRequest {
    pub id: u64,
    pub method: String,
    pub expected_generation: u64,
    pub remaining_budget_ms: u64,
    pub params: Value,
}

pub fn parse_request_frame(frame: &[u8]) -> Result<BoundedRequest, ContractError> {
    if frame.len() as u64 > MAX_INPUT_FRAME_BYTES {
        return Err(error("bounded request frame exceeds 256KiB"));
    }
    if frame.last().copied() != Some(b'\n') || frame[..frame.len() - 1].contains(&b'\n') {
        return Err(error("bounded request is not one complete JSON-RPC line"));
    }
    if frame.len() == 1 {
        return Err(error("bounded request JSON is empty"));
    }
    parse_json(&frame[..frame.len() - 1], "bounded request envelope")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderState {
    Idle,
    RecoveryPending,
    Busy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestAdmission {
    pub native_remainder_ms: u64,
    pub resource_usage: ResourceUsage,
}

pub fn admit_request(
    request: &BoundedRequest,
    committed_generation: u64,
    state: ReaderState,
) -> Result<RequestAdmission, ContractError> {
    if request.expected_generation > MAX_SAFE_GENERATION
        || committed_generation > MAX_SAFE_GENERATION
    {
        return Err(error("generation exceeds JSON-safe integer range"));
    }
    if request.expected_generation != committed_generation {
        return Err(error("requested generation is stale"));
    }
    if state != ReaderState::Idle {
        return Err(error("descriptor reader is not idle"));
    }
    let contract = pinned_contract()?;
    if !contract.operations.contains_key(&request.method) {
        return Err(error("unsupported bounded retrieval method"));
    }
    validate_semantic_request(&request.method, &request.params)?;
    let resource_usage = ResourceUsage::for_request(&request.method, &request.params)?;
    Ok(RequestAdmission {
        native_remainder_ms: request.remaining_budget_ms.min(MAX_OPERATION_DEADLINE_MS),
        resource_usage,
    })
}

pub const REQUEST_EXECUTION_FAILED: &str = "REQUEST_EXECUTION_FAILED";

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SuccessEnvelope<'a> {
    id: u64,
    ok: bool,
    generation: u64,
    result: &'a Value,
    counters: WorkCounters,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ErrorEnvelope<'a> {
    id: u64,
    ok: bool,
    generation: u64,
    error: StructuredError<'a>,
    counters: WorkCounters,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StructuredError<'a> {
    code: &'a str,
    failure_class: &'a str,
    reason: &'a str,
}

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: u64,
}

impl LimitedWriter {
    fn new(limit: u64) -> Result<Self, ContractError> {
        let capacity =
            usize::try_from(limit).map_err(|_| error("byte limit does not fit usize"))?;
        Ok(Self {
            bytes: Vec::with_capacity(capacity),
            limit,
        })
    }
}

impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .bytes
            .len()
            .try_into()
            .ok()
            .and_then(|length: u64| length.checked_add(bytes.len() as u64))
            .ok_or_else(|| std::io::Error::other("bounded writer byte count overflow"))?;
        if next > self.limit {
            return Err(std::io::Error::other("bounded writer limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn serialize_limited<T: Serialize>(value: &T, limit: u64) -> Result<Vec<u8>, ContractError> {
    let mut writer = LimitedWriter::new(limit)?;
    serde_json::to_writer(&mut writer, value)
        .map_err(|e| error(format!("bounded serialization failed: {e}")))?;
    Ok(writer.bytes)
}

fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, ContractError> {
    let mut bytes = serialize_limited(value, MAX_RESPONSE_FRAME_BYTES - 1)?;
    bytes.push(b'\n');
    Ok(bytes)
}

/// Encode one complete success frame.  The result is borrowed for every
/// serialization attempt; only the bounded output buffer is materialized.
pub fn encode_success(
    id: u64,
    generation: u64,
    result: &Value,
    work: WorkCounts,
) -> Result<Vec<u8>, ContractError> {
    if generation > MAX_SAFE_GENERATION {
        return Err(error("response generation exceeds JSON-safe integer range"));
    }
    let mut advertised_response_bytes = 0_u64;
    for _ in 0..8 {
        let counters = WorkCounters::from_work(work, advertised_response_bytes)?;
        let envelope = SuccessEnvelope {
            id,
            ok: true,
            generation,
            result,
            counters,
        };
        let bytes = encode_frame(&envelope)?;
        let actual = bytes.len() as u64;
        if actual == advertised_response_bytes {
            return Ok(bytes);
        }
        advertised_response_bytes = actual;
    }
    Err(error("response counter did not reach a fixed point"))
}

/// Encode a structured failure.  It deliberately has no `result` member, so
/// deadline, cap, and validation failures cannot accidentally publish partial
/// work.
pub fn encode_error(
    id: u64,
    generation: u64,
    work: WorkCounts,
    failure_class: &'static str,
    reason: &'static str,
) -> Result<Vec<u8>, ContractError> {
    if generation > MAX_SAFE_GENERATION {
        return Err(error("response generation exceeds JSON-safe integer range"));
    }
    let mut advertised_response_bytes = 0_u64;
    for _ in 0..8 {
        let counters = WorkCounters::from_work(work, advertised_response_bytes)?;
        let envelope = ErrorEnvelope {
            id,
            ok: false,
            generation,
            error: StructuredError {
                code: REQUEST_EXECUTION_FAILED,
                failure_class,
                reason,
            },
            counters,
        };
        let bytes = encode_frame(&envelope)?;
        let actual = bytes.len() as u64;
        if actual == advertised_response_bytes {
            return Ok(bytes);
        }
        advertised_response_bytes = actual;
    }
    Err(error("error response counter did not reach a fixed point"))
}

pub fn encode_object(value: &Value) -> Result<Vec<u8>, ContractError> {
    serialize_limited(value, MAX_OBJECT_BYTES)
}

fn contract_node_json(node: &ContractNode) -> Value {
    match node {
        ContractNode::String => serde_json::json!({"kind": "string"}),
        ContractNode::Number => serde_json::json!({"kind": "number"}),
        ContractNode::Boolean => serde_json::json!({"kind": "boolean"}),
        ContractNode::Literal(values) => serde_json::json!({"kind": "literal", "values": values}),
        ContractNode::Array(item) => serde_json::json!({
            "items": contract_node_json(item),
            "kind": "array",
        }),
        ContractNode::Tuple(items) => serde_json::json!({
            "items": items.iter().map(contract_node_json).collect::<Vec<_>>(),
            "kind": "tuple",
        }),
        ContractNode::Optional(item) => serde_json::json!({
            "kind": "optional",
            "value": contract_node_json(item),
        }),
        ContractNode::Object(fields) => {
            let fields = fields
                .iter()
                .map(|(name, node)| (name.clone(), contract_node_json(node)))
                .collect::<Map<_, _>>();
            serde_json::json!({"fields": fields, "kind": "object"})
        }
        ContractNode::ExternalRef {
            dependency,
            reference_kind,
        } => serde_json::json!({
            "dependency": dependency,
            "kind": "externalRef",
            "referenceKind": reference_kind,
        }),
    }
}

fn schema_digest(node: &ContractNode) -> String {
    let bytes =
        serde_json::to_vec(&contract_node_json(node)).expect("contract node is serializable");
    sha256_hex(&bytes)
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolMethod {
    pub name: String,
    pub classification: &'static str,
    pub wal: bool,
    pub semantic_bytes: u64,
    pub semantic_digest: String,
    pub request_schema_sha256: String,
    pub result_schema_sha256: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolLimits {
    pub max_vector_dimensions: u64,
    pub max_vector_comparisons: u64,
    pub max_search_slots: u64,
    pub max_facts_inspected: u64,
    pub max_graph_nodes: u64,
    pub max_graph_edges: u64,
    pub max_iterations: u64,
    pub max_search_result_limit: u64,
    pub max_expansion_results: u64,
    pub max_seeds: u64,
    pub max_returned_passages: u64,
    pub max_returned_facts: u64,
    pub max_combined_objects: u64,
    pub max_input_frame_bytes: u64,
    pub max_object_bytes: u64,
    pub max_response_frame_bytes: u64,
    pub max_transient_bytes: u64,
    pub max_operation_deadline_ms: u64,
    pub deadline_check_interval_units: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BoundedProtocolInfo {
    pub protocol_version: &'static str,
    pub contract_version: &'static str,
    pub refinement_ir_version: &'static str,
    pub contract_sha256: &'static str,
    pub manifest_sha256: &'static str,
    pub source_repository: &'static str,
    pub source_branch: &'static str,
    pub source_commit: &'static str,
    pub classification: &'static str,
    pub wal: bool,
    pub methods: Vec<ProtocolMethod>,
    pub limits: ProtocolLimits,
    pub method_inventory_sha256: String,
}

pub fn protocol_methods() -> Result<Vec<ProtocolMethod>, ContractError> {
    let contract = pinned_contract()?;
    let digests = expected_operation_digests();
    let mut methods = Vec::with_capacity(METHODS.len());
    for method in METHODS {
        let operation = contract
            .operations
            .get(method)
            .ok_or_else(|| error("pinned method inventory is incomplete"))?;
        let digest = digests
            .get(method)
            .ok_or_else(|| error("pinned operation digest is missing"))?;
        methods.push(ProtocolMethod {
            name: method.to_string(),
            classification: "read",
            wal: false,
            semantic_bytes: digest.bytes,
            semantic_digest: digest.sha256.clone(),
            request_schema_sha256: schema_digest(&operation.request),
            result_schema_sha256: schema_digest(&operation.result),
        });
    }
    if methods.len() != METHODS.len()
        || methods
            .iter()
            .map(|method| method.name.as_str())
            .collect::<Vec<_>>()
            != METHODS.to_vec()
    {
        return Err(error(
            "bounded method inventory is not exactly the pinned three",
        ));
    }
    Ok(methods)
}

pub fn method_inventory_lines() -> Result<String, ContractError> {
    protocol_methods().map(|methods| {
        methods
            .iter()
            .map(|method| {
                format!(
                    "{}\t{}\t{}\t{}\t{}\n",
                    method.name,
                    method.classification,
                    method.wal,
                    method.semantic_digest,
                    method.request_schema_sha256,
                )
            })
            .collect()
    })
}

pub fn method_inventory_sha256() -> Result<String, ContractError> {
    Ok(sha256_hex(method_inventory_lines()?.as_bytes()))
}

pub fn protocol_info() -> Result<BoundedProtocolInfo, ContractError> {
    let methods = protocol_methods()?;
    Ok(BoundedProtocolInfo {
        protocol_version: "aira-graphdb-bounded-retrieval@1",
        contract_version: CONTRACT_VERSION,
        refinement_ir_version: REFINEMENT_IR_VERSION,
        contract_sha256: CONTRACT_SHA256,
        manifest_sha256: RETRIEVAL_MANIFEST_SHA256,
        source_repository: SOURCE_REPOSITORY,
        source_branch: SOURCE_BRANCH,
        source_commit: SOURCE_COMMIT,
        classification: "read",
        wal: false,
        methods,
        limits: ProtocolLimits {
            max_vector_dimensions: MAX_VECTOR_DIMENSIONS,
            max_vector_comparisons: MAX_VECTOR_COMPARISONS,
            max_search_slots: MAX_SEARCH_SLOTS,
            max_facts_inspected: MAX_FACTS_INSPECTED,
            max_graph_nodes: MAX_GRAPH_NODES,
            max_graph_edges: MAX_GRAPH_EDGES,
            max_iterations: MAX_ITERATIONS,
            max_search_result_limit: MAX_SEARCH_RESULT_LIMIT,
            max_expansion_results: MAX_EXPANSION_RESULTS,
            max_seeds: MAX_SEEDS,
            max_returned_passages: MAX_RETURNED_PASSAGES,
            max_returned_facts: MAX_RETURNED_FACTS,
            max_combined_objects: MAX_COMBINED_OBJECTS,
            max_input_frame_bytes: MAX_INPUT_FRAME_BYTES,
            max_object_bytes: MAX_OBJECT_BYTES,
            max_response_frame_bytes: MAX_RESPONSE_FRAME_BYTES,
            max_transient_bytes: MAX_TRANSIENT_BYTES,
            max_operation_deadline_ms: MAX_OPERATION_DEADLINE_MS,
            deadline_check_interval_units: DEADLINE_CHECK_INTERVAL_UNITS,
        },
        method_inventory_sha256: method_inventory_sha256()?,
    })
}

pub fn protocol_info_value() -> Result<Value, ContractError> {
    serde_json::to_value(protocol_info()?)
        .map_err(|e| error(format!("protocol metadata encode failed: {e}")))
}
