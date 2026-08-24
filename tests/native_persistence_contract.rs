use aira_graphdb::native_persistence_contract::{
    COMMIT_EVIDENCE_SCHEMA, CommitEvidence, JSON_SAFE_INTEGER_MAX, NATIVE_PROGRESS_FRAME_SCHEMA,
    NativeProgressFrame, NativeProgressPolicy, PREPARED_COMMIT_EVIDENCE_SCHEMA,
    PreparedCommitEvidence, ProgressCounters,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use sha2::{Digest, Sha256};

const FIXTURE_MANIFEST: &[u8] =
    include_bytes!("../spec/contracts/native-persistence/canonical-fixtures.v1.json");

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureManifest {
    schema: String,
    policy_purpose: String,
    fixtures: Vec<FixtureEntry>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct FixtureEntry {
    name: String,
    canonical_base64: String,
    sha256: String,
}

fn manifest() -> FixtureManifest {
    serde_json::from_slice(FIXTURE_MANIFEST).expect("fixture manifest must be strict JSON")
}

fn fixture(name: &str) -> Vec<u8> {
    let manifest = manifest();
    let entry = manifest
        .fixtures
        .iter()
        .find(|entry| entry.name == name)
        .unwrap_or_else(|| panic!("missing fixture {name}"));
    let raw = STANDARD
        .decode(&entry.canonical_base64)
        .expect("canonical fixture must be base64");
    assert_eq!(
        Sha256::digest(&raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        entry.sha256
    );
    raw
}

fn prepared() -> PreparedCommitEvidence {
    PreparedCommitEvidence::new("11".repeat(32), 7, 8, "ab".repeat(32), 1234, 9).unwrap()
}

#[test]
fn checked_in_manifest_is_the_shared_raw_byte_authority() {
    let manifest = manifest();
    assert_eq!(manifest.schema, "NativePersistenceCanonicalFixtures@1");
    assert_eq!(
        manifest.policy_purpose,
        "contract-only-unmeasured-not-production-policy"
    );
    assert_eq!(manifest.fixtures.len(), 4);
    assert_eq!(
        manifest
            .fixtures
            .iter()
            .map(|entry| entry.name.as_str())
            .collect::<Vec<_>>(),
        [
            "preparedCommitEvidence",
            "commitEvidence",
            "nativeProgressFrame",
            "nativeProgressPolicy",
        ]
    );
    for entry in &manifest.fixtures {
        assert_eq!(entry.sha256.len(), 64);
        assert!(!fixture(&entry.name).ends_with(b"\n"));
    }
}

#[test]
fn prepared_and_commit_evidence_share_one_payload_and_exact_fixtures() {
    let prepared = prepared();
    assert_eq!(
        prepared.canonical_bytes(),
        fixture("preparedCommitEvidence")
    );
    assert_eq!(
        PreparedCommitEvidence::from_canonical_bytes(&fixture("preparedCommitEvidence")).unwrap(),
        prepared
    );
    assert_eq!(prepared.transaction_nonce(), "11".repeat(32));
    assert_eq!(prepared.base_generation(), 7);
    assert_eq!(prepared.generation(), 8);
    assert_eq!(prepared.wal_sha256(), "ab".repeat(32));
    assert_eq!(prepared.wal_bytes(), 1234);
    assert_eq!(prepared.wal_record_count(), 9);

    let commit = prepared.commit_evidence();
    assert_eq!(commit.canonical_bytes(), fixture("commitEvidence"));
    assert_eq!(
        CommitEvidence::from_canonical_bytes(&fixture("commitEvidence")).unwrap(),
        commit
    );
    assert_eq!(commit.transaction_nonce(), prepared.transaction_nonce());
    assert_eq!(commit.base_generation(), prepared.base_generation());
    assert_eq!(commit.generation(), prepared.generation());
    assert_eq!(commit.wal_sha256(), prepared.wal_sha256());
    assert_eq!(commit.wal_bytes(), prepared.wal_bytes());
    assert_eq!(commit.wal_record_count(), prepared.wal_record_count());
}

#[test]
fn evidence_rejects_bypass_shapes_and_safe_integer_boundaries() {
    let canonical = String::from_utf8(fixture("preparedCommitEvidence")).unwrap();
    let reordered = format!(
        "{{\"transactionNonce\":\"{}\",\"schema\":\"PreparedCommitEvidence@1\",\"baseGeneration\":7,\"generation\":8,\"walSha256\":\"{}\",\"walBytes\":1234,\"walRecordCount\":9}}",
        "11".repeat(32),
        "ab".repeat(32),
    );
    for invalid in [
        format!(" {canonical}"),
        reordered,
        canonical.replace("PreparedCommitEvidence@1", "PreparedCommitEvidence\\u00401"),
        canonical.replace("\"walBytes\":1234", "\"extra\":1,\"walBytes\":1234"),
        canonical.replace("\"walBytes\":1234", "\"walBytes\":1234,\"walBytes\":1234"),
    ] {
        assert!(PreparedCommitEvidence::from_canonical_bytes(invalid.as_bytes()).is_err());
    }

    assert!(
        PreparedCommitEvidence::new(
            "11".repeat(32),
            JSON_SAFE_INTEGER_MAX - 1,
            JSON_SAFE_INTEGER_MAX,
            "ab".repeat(32),
            JSON_SAFE_INTEGER_MAX,
            JSON_SAFE_INTEGER_MAX,
        )
        .is_ok()
    );
    assert!(
        PreparedCommitEvidence::new(
            "11".repeat(32),
            0,
            1,
            "ab".repeat(32),
            JSON_SAFE_INTEGER_MAX + 1,
            1,
        )
        .is_err()
    );
    assert!(
        CommitEvidence::from_canonical_bytes(
            String::from_utf8(fixture("commitEvidence"))
                .unwrap()
                .replace(COMMIT_EVIDENCE_SCHEMA, PREPARED_COMMIT_EVIDENCE_SCHEMA)
                .as_bytes()
        )
        .is_err()
    );
    let commit = String::from_utf8(fixture("commitEvidence")).unwrap();
    for invalid in [
        format!("{commit}\n"),
        commit.replace("\"walBytes\":1234", "\"walBytes\":0"),
        commit.replace("\"walRecordCount\":9", "\"extra\":1,\"walRecordCount\":9"),
        commit.replace(
            "\"walRecordCount\":9",
            "\"walRecordCount\":9,\"walRecordCount\":9",
        ),
    ] {
        assert!(CommitEvidence::from_canonical_bytes(invalid.as_bytes()).is_err());
    }
}

#[test]
fn progress_policy_parses_only_the_exact_checked_in_artifact() {
    let raw = fixture("nativeProgressPolicy");
    let policy = NativeProgressPolicy::from_canonical_bytes(&raw).unwrap();
    assert_eq!(
        NativeProgressPolicy::checked_in_candidate().unwrap(),
        policy
    );
    assert_eq!(policy.canonical_bytes(), raw);
    assert_eq!(
        policy.sha256(),
        "61ce9d5474d536d42b624706abe3a989f59a1c0887d9d63ca5a4dd69120a2f07"
    );
}

#[test]
fn progress_policy_rejects_noncanonical_shape_inventory_and_limits() {
    let canonical = String::from_utf8(fixture("nativeProgressPolicy")).unwrap();
    for invalid in [
        format!("\n{canonical}"),
        canonical.replace(
            "\"schema\":\"NativeProgressPolicy@1\",\"protocolVersion\":1",
            "\"protocolVersion\":1,\"schema\":\"NativeProgressPolicy@1\"",
        ),
        canonical.replace(
            "\"protocolVersion\":1",
            "\"protocolVersion\":1,\"protocolVersion\":1",
        ),
        canonical.replace("\"methods\":[", "\"unknown\":1,\"methods\":["),
        canonical.replace("\"protocolVersion\":1", "\"protocolVersion\":1.0"),
        canonical.replace("\"complete\"]", "\"complete\",\"extra\"]"),
        canonical.replacen("\"deadlineMs\":300000", "\"deadlineMs\":0", 1),
        canonical.replace("\"maxFrames\":4096", "\"maxFrames\":9007199254740992"),
    ] {
        assert!(NativeProgressPolicy::from_canonical_bytes(invalid.as_bytes()).is_err());
    }
    let mut duplicated: serde_json::Value = serde_json::from_str(&canonical).unwrap();
    let methods = duplicated["methods"].as_array_mut().unwrap();
    methods.push(methods[0].clone());
    assert!(
        NativeProgressPolicy::from_canonical_bytes(&serde_json::to_vec(&duplicated).unwrap())
            .is_err()
    );
}

#[test]
fn progress_frame_is_opaque_canonical_and_never_terminal() {
    let admitted = NativeProgressFrame::admitted(42).unwrap();
    let raw = fixture("nativeProgressFrame");
    assert_eq!(admitted.canonical_bytes(), raw);
    assert_eq!(
        NativeProgressFrame::from_canonical_bytes(&raw).unwrap(),
        admitted
    );

    let canonical = String::from_utf8(raw).unwrap();
    for invalid in [
        format!(" {canonical}"),
        canonical.replace(
            "\"schema\":\"NativeProgressFrame@1\",\"kind\":\"progress\"",
            "\"kind\":\"progress\",\"schema\":\"NativeProgressFrame@1\"",
        ),
        canonical.replace("\"kind\":\"progress\"", "\"kind\":\"progress\",\"ok\":true"),
        canonical.replace("\"id\":42", "\"id\":42,\"id\":42"),
        canonical.replace(NATIVE_PROGRESS_FRAME_SCHEMA, "WrongFrame@1"),
        canonical.replace("\"kind\":\"progress\"", "\"kind\":\"terminal\""),
        canonical.replace("\"protocolVersion\":1", "\"protocolVersion\":2"),
        canonical.replace("\"method\":\"batch_commit\"", "\"method\":\"ping\""),
        canonical.replace("\"id\":42", "\"id\":9007199254740992"),
        canonical.replace("\"sequence\":1", "\"sequence\":2"),
    ] {
        assert!(NativeProgressFrame::from_canonical_bytes(invalid.as_bytes()).is_err());
    }
    assert!(NativeProgressFrame::from_canonical_bytes(&[0xff, b'\n']).is_err());
    assert!(NativeProgressFrame::from_canonical_bytes(b"{\"schema\":").is_err());
}

#[test]
fn subsequent_progress_requires_sequence_phase_and_counter_validity() {
    let counters = ProgressCounters::new(3, Some(3), 5, Some(5)).unwrap();
    assert!(NativeProgressFrame::progress(7, 2, "wal_verify", counters).is_ok());
    assert!(NativeProgressFrame::progress(7, 1, "wal_verify", counters).is_err());
    assert!(NativeProgressFrame::progress(7, 2, "admitted", counters).is_err());
    assert!(
        ProgressCounters::new(3, Some(2), 5, Some(5)).is_err()
            && ProgressCounters::new(3, Some(3), 6, Some(5)).is_err()
    );
}
