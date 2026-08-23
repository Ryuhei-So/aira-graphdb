use serde::Deserialize;
use std::collections::{HashMap, HashSet};

pub const TYPEMAP_SPEC_ID: &str = "AGDB-TYPEMAP-P0@1.0.0";
pub const CYPHER_SPEC_ID: &str = "AGDB-CYPHER-P0-GRAMMAR@1.0.0";
pub const ERROR_SPEC_ID: &str = "AGDB-ERROR-CODES@1.0.0";
pub const OPENCYPHER9_SPEC_ID: &str = "AGDB-CYPHER-OPENCYPHER9@1.0.0";
pub const NEO4J_COMPAT_SPEC_ID: &str = "agdb-cypher-neo4j-compat.v1.0.0";
pub const APOC_MANIFEST_ID: &str = "apoc-procedure-manifest";

#[derive(Debug, Deserialize)]
pub struct TypeMapSpec {
    pub spec_id: String,
    pub protocol_versions: Vec<String>,
    pub canonical_type_system_versions: Vec<String>,
    pub mappings: Vec<TypeMapping>,
}

#[derive(Debug, Deserialize)]
pub struct TypeMapping {
    pub canonical: String,
    pub node: String,
    pub python: String,
    pub lossless: bool,
}

#[derive(Debug, Deserialize)]
pub struct CypherP0GrammarSpec {
    pub spec_id: String,
    pub read_only_clauses: Vec<String>,
    pub write_clauses: Vec<String>,
    pub unsupported_behavior: String,
}

#[derive(Debug, Deserialize)]
pub struct ErrorCodeSpec {
    pub spec_id: String,
    pub codes: Vec<ErrorCodeEntry>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorCodeEntry {
    pub code: String,
    pub category: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
pub struct OpenCypher9TckRequiredSpec {
    pub version: String,
    pub spec_id: String,
    pub source: TckSource,
    pub required_tck_ids: Vec<TckRequiredId>,
    pub tck_selector_map: Vec<TckSelectorEntry>,
}

#[derive(Debug, Deserialize)]
pub struct TckSource {
    pub snapshot_ref: String,
}

#[derive(Debug, Deserialize)]
pub struct TckRequiredId {
    pub id: String,
    pub upstream_file: String,
    pub upstream_scenario: String,
}

#[derive(Debug, Deserialize)]
pub struct TckSelectorEntry {
    pub tck_id: String,
    pub selector: String,
}

#[derive(Debug, Deserialize)]
pub struct ApocProcedureManifest {
    pub version: String,
    pub id: String,
    pub status: String,
    pub id_semantics: String,
    pub allowed_procedures: Vec<ApocProcedure>,
    pub error_policy: ApocErrorPolicy,
}

#[derive(Debug, Deserialize)]
pub struct ApocProcedure {
    pub name: String,
    pub side_effect: bool,
    pub failure_codes: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct ApocErrorPolicy {
    pub unlisted_procedure: String,
    pub partial_execution_forbidden: bool,
}

#[derive(Debug, Deserialize)]
pub struct Neo4jCompatManifestSpec {
    pub manifest_id: String,
    pub baseline_mode: String,
    pub snapshot_ref: String,
    pub required_feature_count: u64,
    pub classified_feature_count: u64,
    pub features: Vec<Neo4jCompatFeatureEntry>,
}

#[derive(Debug, Deserialize)]
pub struct Neo4jCompatFeatureEntry {
    pub feature_id: String,
    pub clause: String,
    pub status: String,
    pub covers_req: Vec<String>,
    pub covers_acceptance: Vec<String>,
}

pub fn load_typemap_spec() -> TypeMapSpec {
    serde_json::from_str(include_str!(
        "../spec/contracts/agdb-typemap-p0.v1.0.0.json"
    ))
    .expect("typemap contract JSON must be valid")
}

pub fn load_cypher_p0_grammar_spec() -> CypherP0GrammarSpec {
    serde_json::from_str(include_str!(
        "../spec/contracts/agdb-cypher-p0-grammar.v1.0.0.json"
    ))
    .expect("cypher contract JSON must be valid")
}

pub fn load_error_code_spec() -> ErrorCodeSpec {
    serde_json::from_str(include_str!(
        "../spec/contracts/agdb-error-codes.v1.0.0.json"
    ))
    .expect("error code contract JSON must be valid")
}

pub fn load_opencypher9_tck_required_spec() -> OpenCypher9TckRequiredSpec {
    serde_yaml::from_str(include_str!(
        "../spec/conformance/opencypher9-tck-required.yaml"
    ))
    .expect("opencypher9 tck required YAML must be valid")
}

pub fn load_apoc_procedure_manifest() -> ApocProcedureManifest {
    serde_yaml::from_str(include_str!(
        "../spec/contracts/apoc-procedure-manifest.v1.0.0.yaml"
    ))
    .expect("apoc procedure manifest YAML must be valid")
}

pub fn load_neo4j_compat_manifest() -> Neo4jCompatManifestSpec {
    serde_json::from_str(include_str!(
        "../spec/contracts/agdb-cypher-neo4j-compat.v1.0.0.json"
    ))
    .expect("neo4j compat manifest JSON must be valid")
}

pub fn build_tck_selector_map(
    spec: &OpenCypher9TckRequiredSpec,
) -> Result<HashMap<String, String>, String> {
    let mut map = HashMap::new();
    for entry in &spec.tck_selector_map {
        if map
            .insert(entry.tck_id.clone(), entry.selector.clone())
            .is_some()
        {
            return Err(format!(
                "duplicate selector mapping for tck_id={}",
                entry.tck_id
            ));
        }
    }

    let mut seen_required = HashSet::new();
    for required in &spec.required_tck_ids {
        if !seen_required.insert(required.id.clone()) {
            return Err(format!("duplicate required_tck_id={}", required.id));
        }
        if !map.contains_key(&required.id) {
            return Err(format!("unresolved tck_id={}", required.id));
        }
    }
    Ok(map)
}

pub fn resolve_tck_selector(
    spec: &OpenCypher9TckRequiredSpec,
    tck_id: &str,
) -> Result<String, String> {
    let map = build_tck_selector_map(spec)?;
    map.get(tck_id)
        .cloned()
        .ok_or_else(|| format!("unresolved tck_id={}", tck_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_typemap_contract() {
        let spec = load_typemap_spec();
        assert_eq!(spec.spec_id, TYPEMAP_SPEC_ID);
        assert!(
            spec.protocol_versions
                .iter()
                .any(|v| v == "protocol-p0@1.0.0")
        );
        assert!(!spec.mappings.is_empty());
    }

    #[test]
    fn loads_cypher_contract() {
        let spec = load_cypher_p0_grammar_spec();
        assert_eq!(spec.spec_id, CYPHER_SPEC_ID);
        assert!(spec.read_only_clauses.iter().any(|c| c == "MATCH"));
        assert!(spec.write_clauses.iter().any(|c| c == "MERGE"));
    }

    #[test]
    fn loads_error_code_contract() {
        let spec = load_error_code_spec();
        assert_eq!(spec.spec_id, ERROR_SPEC_ID);
        assert!(
            spec.codes
                .iter()
                .any(|entry| entry.code == "PROTOCOL_VERSION_MISMATCH")
        );
        assert!(spec.codes.iter().any(|entry| entry.code == "AUTH_FAILED"));
    }

    #[test]
    fn loads_opencypher9_tck_required_contract() {
        let spec = load_opencypher9_tck_required_spec();
        assert_eq!(spec.spec_id, OPENCYPHER9_SPEC_ID);
        assert_eq!(spec.version, "1.0.0");
        assert!(!spec.required_tck_ids.is_empty());
        assert!(!spec.tck_selector_map.is_empty());
    }

    #[test]
    fn builds_selector_map_and_validates_all_required_ids() {
        let spec = load_opencypher9_tck_required_spec();
        let map = build_tck_selector_map(&spec).expect("selector map must be valid");
        assert_eq!(
            map.get("TCK-MATCH-001").map(String::as_str),
            Some("TCK-MATCH-001")
        );
        assert_eq!(map.len(), spec.tck_selector_map.len());
    }

    #[test]
    fn resolves_selector_for_known_tck_id() {
        let spec = load_opencypher9_tck_required_spec();
        let selector = resolve_tck_selector(&spec, "TCK-EXPR-COMPARISON-001")
            .expect("known tck id should resolve");
        assert_eq!(selector, "TCK-EXPR-COMPARISON-001");
    }

    #[test]
    fn fails_when_selector_is_unresolved() {
        let spec = load_opencypher9_tck_required_spec();
        let err = resolve_tck_selector(&spec, "TCK-NOT-EXIST-999")
            .expect_err("missing tck id should fail");
        assert!(err.contains("unresolved tck_id"));
    }
}
