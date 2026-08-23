use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const CONTRACT_DIR: &str = "spec/contracts/bounded-domain";
const PIN_FILE: &str = "bounded-domain-pin.json";
const EXPECTED_PIN_VERSION: &str = "aira-graphdb-bounded-domain-pin@1";
const EXPECTED_SOURCE_REPOSITORY: &str = "https://github.com/Ryuhei-So/aira-synapse";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContractPin {
    pin_version: String,
    source_repository: String,
    source_commit: String,
    artifacts: Vec<PinnedArtifact>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PinnedArtifact {
    local_file: String,
    source_path: String,
    bytes: u64,
    sha256: String,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!(
            "{} is not a regular non-symlink file",
            path.display()
        ));
    }
    fs::read(path).map_err(|error| format!("{}: {error}", path.display()))
}

fn expected_source_paths() -> BTreeMap<&'static str, &'static str> {
    BTreeMap::from([
        (
            "bounded-domain-contract.json",
            "packages/memgraphrag/tests/fixtures/bounded-domain-contract.json",
        ),
        (
            "bounded-domain-fixture.json",
            "packages/memgraphrag/tests/fixtures/bounded-domain-fixture.json",
        ),
        (
            "bounded-domain-fixture.manifest.json",
            "packages/memgraphrag/tests/fixtures/bounded-domain-fixture.manifest.json",
        ),
    ])
}

fn verify_contract_dir(root: &Path) -> Result<(), String> {
    let pin_bytes = read_regular_file(&root.join(PIN_FILE))?;
    let pin: ContractPin = serde_json::from_slice(&pin_bytes)
        .map_err(|error| format!("invalid bounded domain pin: {error}"))?;
    if pin.pin_version != EXPECTED_PIN_VERSION {
        return Err(format!("unsupported pinVersion {}", pin.pin_version));
    }
    if pin.source_repository != EXPECTED_SOURCE_REPOSITORY {
        return Err(format!(
            "unexpected sourceRepository {}",
            pin.source_repository
        ));
    }
    if pin.source_commit.len() != 40
        || !pin
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("sourceCommit must be an exact 40-character Git SHA".into());
    }

    let expected_paths = expected_source_paths();
    let mut seen = BTreeSet::new();
    let mut hashes = BTreeMap::new();
    for artifact in &pin.artifacts {
        let local_path = Path::new(&artifact.local_file);
        if local_path.components().count() != 1
            || !matches!(local_path.components().next(), Some(Component::Normal(_)))
        {
            return Err(format!("invalid localFile {}", artifact.local_file));
        }
        let expected_source = expected_paths
            .get(artifact.local_file.as_str())
            .ok_or_else(|| format!("unexpected artifact {}", artifact.local_file))?;
        if artifact.source_path != *expected_source {
            return Err(format!("sourcePath mismatch for {}", artifact.local_file));
        }
        if !seen.insert(artifact.local_file.as_str()) {
            return Err(format!("duplicate artifact {}", artifact.local_file));
        }
        let bytes = read_regular_file(&root.join(&artifact.local_file))?;
        if bytes.len() as u64 != artifact.bytes {
            return Err(format!("byte length mismatch for {}", artifact.local_file));
        }
        let actual_hash = sha256(&bytes);
        if actual_hash != artifact.sha256 {
            return Err(format!("SHA-256 mismatch for {}", artifact.local_file));
        }
        hashes.insert(artifact.local_file.as_str(), actual_hash);
    }
    if seen != expected_paths.keys().copied().collect() {
        return Err("bounded domain artifact set is incomplete".into());
    }

    let manifest: Value = serde_json::from_slice(&read_regular_file(
        &root.join("bounded-domain-fixture.manifest.json"),
    )?)
    .map_err(|error| format!("invalid bounded domain manifest: {error}"))?;
    let exact_manifest_values = [
        ("manifestVersion", "aira-synapse-bounded-domain-manifest@1"),
        ("contractVersion", "aira-synapse-domain-contract@1"),
        ("contractFile", "bounded-domain-contract.json"),
        ("fixtureVersion", "aira-synapse-bounded-domain-fixture@1"),
        ("fixtureFile", "bounded-domain-fixture.json"),
    ];
    for (field, expected) in exact_manifest_values {
        if manifest.get(field).and_then(Value::as_str) != Some(expected) {
            return Err(format!("manifest {field} mismatch"));
        }
    }
    for (field, file) in [
        ("contractSha256", "bounded-domain-contract.json"),
        ("fixtureSha256", "bounded-domain-fixture.json"),
    ] {
        if manifest.get(field).and_then(Value::as_str) != hashes.get(file).map(String::as_str) {
            return Err(format!("manifest {field} does not match pinned artifact"));
        }
    }

    let contract: Value = serde_json::from_slice(&read_regular_file(
        &root.join("bounded-domain-contract.json"),
    )?)
    .map_err(|error| format!("invalid bounded domain contract: {error}"))?;
    if contract.get("contractVersion").and_then(Value::as_str)
        != Some("aira-synapse-domain-contract@1")
    {
        return Err("contractVersion mismatch".into());
    }
    let fixture: Value = serde_json::from_slice(&read_regular_file(
        &root.join("bounded-domain-fixture.json"),
    )?)
    .map_err(|error| format!("invalid bounded domain fixture: {error}"))?;
    if fixture.get("contractVersion").and_then(Value::as_str)
        != Some("aira-synapse-domain-contract@1")
    {
        return Err("fixture contractVersion mismatch".into());
    }
    Ok(())
}

struct TestDir(PathBuf);

impl TestDir {
    fn copy_from(source: &Path) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "aira-graphdb-bounded-domain-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("create test directory");
        for entry in fs::read_dir(source).expect("read source contract directory") {
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

#[test]
fn copied_synapse_contract_is_exactly_pinned() {
    verify_contract_dir(Path::new(CONTRACT_DIR)).expect("pinned contract must verify");
}

#[test]
fn modified_or_missing_artifact_fails_closed() {
    let source = Path::new(CONTRACT_DIR);
    let modified = TestDir::copy_from(source);
    fs::write(
        modified.0.join("bounded-domain-contract.json"),
        b"{\"contractVersion\":\"aira-synapse-domain-contract@1\"}\n",
    )
    .expect("modify copied contract");
    assert!(
        verify_contract_dir(&modified.0)
            .unwrap_err()
            .contains("mismatch")
    );

    let missing = TestDir::copy_from(source);
    fs::remove_file(missing.0.join("bounded-domain-fixture.json")).expect("remove copied fixture");
    assert!(verify_contract_dir(&missing.0).is_err());
}

#[test]
fn unknown_version_or_partial_manifest_fails_closed() {
    let source = Path::new(CONTRACT_DIR);
    let unknown = TestDir::copy_from(source);
    let pin_path = unknown.0.join(PIN_FILE);
    let mut pin: Value = serde_json::from_slice(&fs::read(&pin_path).expect("read copied pin"))
        .expect("parse copied pin");
    pin["pinVersion"] = Value::String("future-pin@2".into());
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write modified pin");
    assert!(
        verify_contract_dir(&unknown.0)
            .unwrap_err()
            .contains("unsupported pinVersion")
    );

    let partial = TestDir::copy_from(source);
    let manifest_path = partial.0.join("bounded-domain-fixture.manifest.json");
    fs::write(
        &manifest_path,
        b"{\"manifestVersion\":\"aira-synapse-bounded-domain-manifest@1\"}\n",
    )
    .expect("write partial manifest");
    assert!(verify_contract_dir(&partial.0).is_err());
}
