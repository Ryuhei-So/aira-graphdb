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
const EXPECTED_SOURCE_BRANCH: &str = "production-runtime";
const MAX_ARTIFACT_BYTES: u64 = 128 * 1024;
const MAX_TOTAL_ARTIFACT_BYTES: u64 = 256 * 1024;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContractPin {
    pin_version: String,
    source_repository: String,
    source_branch: String,
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureManifest {
    manifest_version: String,
    contract_version: String,
    contract_file: String,
    fixture_version: String,
    fixture_file: String,
    contract_sha256: String,
    fixture_sha256: String,
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn regular_file_size(path: &Path) -> Result<u64, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("{}: {error}", path.display()))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!(
            "{} is not a regular non-symlink file",
            path.display()
        ));
    }
    let length = metadata.len();
    if length > MAX_ARTIFACT_BYTES {
        return Err(format!("{} exceeds per-file byte cap", path.display()));
    }
    Ok(length)
}

fn read_regular_file(path: &Path) -> Result<Vec<u8>, String> {
    regular_file_size(path)?;
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
    let mut expected_files: BTreeSet<&str> = [PIN_FILE].into_iter().collect();
    expected_files.extend(expected_source_paths().keys().copied());
    let actual_files: BTreeSet<String> = fs::read_dir(root)
        .map_err(|error| format!("read contract directory: {error}"))?
        .map(|entry| entry.map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Result<_, _>>()
        .map_err(|error| format!("read contract directory entry: {error}"))?;
    if actual_files.len() != expected_files.len()
        || actual_files
            .iter()
            .any(|file| !expected_files.contains(file.as_str()))
    {
        return Err("bounded domain directory file set mismatch".into());
    }
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
    if pin.source_branch != EXPECTED_SOURCE_BRANCH {
        return Err(format!("unexpected sourceBranch {}", pin.source_branch));
    }
    if pin.source_commit.len() != 40
        || !pin
            .source_commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("sourceCommit must be an exact lowercase 40-character Git SHA".into());
    }

    let expected_paths = expected_source_paths();
    let mut seen = BTreeSet::new();
    let mut hashes = BTreeMap::new();
    let mut total_bytes = 0_u64;
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
        let actual_bytes = regular_file_size(&root.join(&artifact.local_file))?;
        total_bytes = total_bytes
            .checked_add(actual_bytes)
            .ok_or_else(|| "bounded domain aggregate byte count overflow".to_string())?;
        if total_bytes > MAX_TOTAL_ARTIFACT_BYTES {
            return Err("bounded domain aggregate byte cap exceeded".into());
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

    let manifest: FixtureManifest = serde_json::from_slice(&read_regular_file(
        &root.join("bounded-domain-fixture.manifest.json"),
    )?)
    .map_err(|error| format!("invalid bounded domain manifest: {error}"))?;
    if manifest.manifest_version != "aira-synapse-bounded-domain-manifest@1"
        || manifest.contract_version != "aira-synapse-domain-contract@1"
        || manifest.contract_file != "bounded-domain-contract.json"
        || manifest.fixture_version != "aira-synapse-bounded-domain-fixture@1"
        || manifest.fixture_file != "bounded-domain-fixture.json"
    {
        return Err("manifest fixed field mismatch".into());
    }
    if manifest.contract_sha256 != hashes["bounded-domain-contract.json"] {
        return Err("manifest contractSha256 does not match pinned artifact".into());
    }
    if manifest.fixture_sha256 != hashes["bounded-domain-fixture.json"] {
        return Err("manifest fixtureSha256 does not match pinned artifact".into());
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

fn update_pin_artifact(root: &Path, local_file: &str, bytes: &[u8]) {
    let pin_path = root.join(PIN_FILE);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(&pin_path).expect("read pin")).expect("parse pin");
    let artifact = pin["artifacts"]
        .as_array_mut()
        .expect("pin artifacts array")
        .iter_mut()
        .find(|artifact| artifact["localFile"] == local_file)
        .expect("artifact in pin");
    artifact["bytes"] = Value::from(bytes.len() as u64);
    artifact["sha256"] = Value::String(sha256(bytes));
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write pin");
}

#[test]
fn copied_synapse_contract_is_exactly_pinned() {
    verify_contract_dir(Path::new(CONTRACT_DIR)).expect("pinned contract must verify");
}

#[test]
fn modified_or_missing_artifact_fails_closed() {
    let source = Path::new(CONTRACT_DIR);
    let modified = TestDir::copy_from(source);
    let modified_path = modified.0.join("bounded-domain-contract.json");
    let mut modified_bytes = fs::read(&modified_path).expect("read copied contract");
    modified_bytes[0] ^= 1;
    fs::write(&modified_path, modified_bytes).expect("modify copied contract in place");
    assert!(
        verify_contract_dir(&modified.0)
            .unwrap_err()
            .contains("SHA-256 mismatch")
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

    let uppercase = TestDir::copy_from(source);
    let pin_path = uppercase.0.join(PIN_FILE);
    let mut pin: Value = serde_json::from_slice(&fs::read(&pin_path).expect("read copied pin"))
        .expect("parse copied pin");
    pin["sourceCommit"] = Value::String("ABCDEF0123456789ABCDEF0123456789ABCDEF01".into());
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write uppercase pin");
    assert!(
        verify_contract_dir(&uppercase.0)
            .unwrap_err()
            .contains("lowercase")
    );

    let partial = TestDir::copy_from(source);
    let manifest_path = partial.0.join("bounded-domain-fixture.manifest.json");
    fs::write(
        &manifest_path,
        b"{\"manifestVersion\":\"aira-synapse-bounded-domain-manifest@1\"}\n",
    )
    .expect("write partial manifest");
    update_pin_artifact(
        &partial.0,
        "bounded-domain-fixture.manifest.json",
        b"{\"manifestVersion\":\"aira-synapse-bounded-domain-manifest@1\"}\n",
    );
    assert!(
        verify_contract_dir(&partial.0)
            .unwrap_err()
            .contains("missing field")
    );
}

#[test]
fn intended_file_set_and_bounded_allocation_negatives_fail_closed() {
    let source = Path::new(CONTRACT_DIR);

    let extra = TestDir::copy_from(source);
    fs::write(extra.0.join("unexpected.json"), b"{}").expect("write extra artifact");
    assert!(
        verify_contract_dir(&extra.0)
            .unwrap_err()
            .contains("file set")
    );

    let oversized = TestDir::copy_from(source);
    let bytes = vec![b'x'; (MAX_ARTIFACT_BYTES + 1) as usize];
    let path = oversized.0.join("bounded-domain-contract.json");
    fs::write(&path, &bytes).expect("write oversized artifact");
    update_pin_artifact(&oversized.0, "bounded-domain-contract.json", &bytes);
    assert!(
        verify_contract_dir(&oversized.0)
            .unwrap_err()
            .contains("per-file byte cap")
    );

    let aggregate = TestDir::copy_from(source);
    for local_file in [
        "bounded-domain-contract.json",
        "bounded-domain-fixture.json",
        "bounded-domain-fixture.manifest.json",
    ] {
        let bytes = vec![b'x'; 100 * 1024];
        fs::write(aggregate.0.join(local_file), &bytes).expect("write aggregate artifact");
        update_pin_artifact(&aggregate.0, local_file, &bytes);
    }
    assert!(
        verify_contract_dir(&aggregate.0)
            .unwrap_err()
            .contains("aggregate byte cap")
    );
}

#[test]
fn typed_manifest_and_path_shape_negatives_fail_closed() {
    let source = Path::new(CONTRACT_DIR);

    let unknown_manifest = TestDir::copy_from(source);
    let manifest_path = unknown_manifest
        .0
        .join("bounded-domain-fixture.manifest.json");
    let mut manifest: Value =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read manifest"))
            .expect("parse manifest");
    manifest["unexpected"] = Value::String("reject me".into());
    let manifest_bytes = serde_json::to_vec_pretty(&manifest).expect("serialize manifest");
    fs::write(&manifest_path, &manifest_bytes).expect("write manifest");
    update_pin_artifact(
        &unknown_manifest.0,
        "bounded-domain-fixture.manifest.json",
        &manifest_bytes,
    );
    assert!(
        verify_contract_dir(&unknown_manifest.0)
            .unwrap_err()
            .contains("unknown field")
    );

    let traversal = TestDir::copy_from(source);
    let pin_path = traversal.0.join(PIN_FILE);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(&pin_path).expect("read pin")).expect("parse pin");
    pin["artifacts"][0]["localFile"] = Value::String("../escape.json".into());
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write pin");
    assert!(verify_contract_dir(&traversal.0).is_err());

    let mismatch = TestDir::copy_from(source);
    let pin_path = mismatch.0.join(PIN_FILE);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(&pin_path).expect("read pin")).expect("parse pin");
    pin["artifacts"][0]["sourcePath"] = Value::String("wrong/path.json".into());
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write pin");
    assert!(verify_contract_dir(&mismatch.0).is_err());

    let duplicate = TestDir::copy_from(source);
    let pin_path = duplicate.0.join(PIN_FILE);
    let mut pin: Value =
        serde_json::from_slice(&fs::read(&pin_path).expect("read pin")).expect("parse pin");
    let first = pin["artifacts"][0].clone();
    pin["artifacts"]
        .as_array_mut()
        .expect("artifacts")
        .push(first);
    fs::write(
        &pin_path,
        serde_json::to_vec_pretty(&pin).expect("serialize pin"),
    )
    .expect("write pin");
    assert!(verify_contract_dir(&duplicate.0).is_err());
}

#[cfg(unix)]
#[test]
fn symlink_artifact_fails_closed() {
    use std::os::unix::fs::symlink;

    let linked = TestDir::copy_from(Path::new(CONTRACT_DIR));
    let path = linked.0.join("bounded-domain-contract.json");
    fs::remove_file(&path).expect("remove copied artifact");
    symlink("bounded-domain-fixture.json", &path).expect("create artifact symlink");
    assert!(verify_contract_dir(&linked.0).is_err());
}
