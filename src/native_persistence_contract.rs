use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

pub const JSON_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;
pub const PREPARED_COMMIT_EVIDENCE_SCHEMA: &str = "PreparedCommitEvidence@1";
pub const COMMIT_EVIDENCE_SCHEMA: &str = "CommitEvidence@1";
pub const NATIVE_PROGRESS_POLICY_SCHEMA: &str = "NativeProgressPolicy@1";
pub const NATIVE_PROGRESS_FRAME_SCHEMA: &str = "NativeProgressFrame@1";
pub const NATIVE_PROGRESS_PROTOCOL_VERSION: u64 = 1;

pub const BATCH_COMMIT_PHASES: [&str; 14] = [
    "admitted",
    "wal_verify",
    "prepare_refs",
    "vector_write",
    "vector_sync",
    "vector_publish",
    "vector_dir_sync",
    "json_write",
    "json_sync",
    "json_publish",
    "json_dir_sync",
    "wal_zero",
    "wal_sync",
    "complete",
];

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommitEvidencePayload {
    transaction_nonce: String,
    base_generation: u64,
    generation: u64,
    wal_sha256: String,
    wal_bytes: u64,
    wal_record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Opaque validated evidence. Fields cannot be constructed or serialized
/// outside the canonical contract path.
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::PreparedCommitEvidence;
/// fn expose(value: PreparedCommitEvidence) {
///     let PreparedCommitEvidence(payload) = value;
/// }
/// ```
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::PreparedCommitEvidence;
/// let evidence = PreparedCommitEvidence::new(
///     "11".repeat(32), 0, 1, "ab".repeat(32), 1, 1
/// ).unwrap();
/// let _ = serde_json::to_vec(&evidence);
/// ```
pub struct PreparedCommitEvidence(CommitEvidencePayload);

#[derive(Debug, Clone, PartialEq, Eq)]
/// Opaque committed evidence. Only validated prepared evidence or the exact
/// canonical parser may create it.
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::CommitEvidence;
/// fn expose(value: CommitEvidence) {
///     let CommitEvidence(payload) = value;
/// }
/// ```
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::CommitEvidence;
/// fn require_serialize<T: serde::Serialize>(_: &T) {}
/// fn check(value: CommitEvidence) {
///     require_serialize(&value);
/// }
/// ```
pub struct CommitEvidence(CommitEvidencePayload);

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawCommitEvidence {
    schema: String,
    transaction_nonce: String,
    base_generation: u64,
    generation: u64,
    wal_sha256: String,
    wal_bytes: u64,
    wal_record_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Validated progress policy whose fields cannot be bypassed or serialized.
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::NativeProgressPolicy;
/// fn expose(value: NativeProgressPolicy) {
///     let NativeProgressPolicy { method } = value;
/// }
/// ```
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::NativeProgressPolicy;
/// fn require_serialize<T: serde::Serialize>(_: &T) {}
/// fn check(value: NativeProgressPolicy) {
///     require_serialize(&value);
/// }
/// ```
pub struct NativeProgressPolicy {
    method: NativeProgressMethodPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NativeProgressMethodPolicy {
    initial_frame_deadline_ms: u64,
    inactivity_deadline_ms: u64,
    phase_hard_deadline_ms: Vec<PhaseHardDeadline>,
    absolute_deadline_ms: u64,
    min_frame_interval_ms: u64,
    heartbeat_interval_ms: u64,
    early_byte_delta: u64,
    early_unit_delta: u64,
    max_frames: u64,
    max_frame_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PhaseHardDeadline {
    phase: String,
    deadline_ms: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawNativeProgressPolicy {
    schema: String,
    protocol_version: u64,
    methods: Vec<RawNativeProgressMethodPolicy>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawNativeProgressMethodPolicy {
    method: String,
    phases: Vec<String>,
    initial_frame_deadline_ms: u64,
    inactivity_deadline_ms: u64,
    phase_hard_deadline_ms: Vec<RawPhaseHardDeadline>,
    absolute_deadline_ms: u64,
    min_frame_interval_ms: u64,
    heartbeat_interval_ms: u64,
    early_byte_delta: u64,
    early_unit_delta: u64,
    max_frames: u64,
    max_frame_bytes: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawPhaseHardDeadline {
    phase: String,
    deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Non-terminal progress frame with canonical serialization only.
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::NativeProgressFrame;
/// fn expose(value: NativeProgressFrame) {
///     let NativeProgressFrame { id, .. } = value;
/// }
/// ```
///
/// ```compile_fail
/// use aira_graphdb::native_persistence_contract::NativeProgressFrame;
/// fn require_serialize<T: serde::Serialize>(_: &T) {}
/// fn check(value: NativeProgressFrame) {
///     require_serialize(&value);
/// }
/// ```
pub struct NativeProgressFrame {
    id: u64,
    sequence: u64,
    phase: String,
    completed_units: u64,
    total_units: Option<u64>,
    completed_bytes: u64,
    total_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProgressCounters {
    completed_units: u64,
    total_units: Option<u64>,
    completed_bytes: u64,
    total_bytes: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawNativeProgressFrame {
    schema: String,
    kind: String,
    protocol_version: u64,
    id: u64,
    sequence: u64,
    method: String,
    phase: String,
    completed_units: u64,
    total_units: Option<u64>,
    completed_bytes: u64,
    total_bytes: Option<u64>,
}

fn validate_safe_integer(name: &str, value: u64, allow_zero: bool) -> Result<(), String> {
    if value > JSON_SAFE_INTEGER_MAX || (!allow_zero && value == 0) {
        return Err(format!(
            "{name} is outside the canonical safe-integer range"
        ));
    }
    Ok(())
}

fn validate_lower_hex(name: &str, value: &str, bytes: usize) -> Result<(), String> {
    if value.len() != bytes * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(format!(
            "{name} must be {} lowercase hex characters",
            bytes * 2
        ));
    }
    Ok(())
}

impl CommitEvidencePayload {
    fn new(
        transaction_nonce: String,
        base_generation: u64,
        generation: u64,
        wal_sha256: String,
        wal_bytes: u64,
        wal_record_count: u64,
    ) -> Result<Self, String> {
        validate_lower_hex("transactionNonce", &transaction_nonce, 32)?;
        validate_lower_hex("walSha256", &wal_sha256, 32)?;
        validate_safe_integer("baseGeneration", base_generation, true)?;
        validate_safe_integer("generation", generation, false)?;
        validate_safe_integer("walBytes", wal_bytes, false)?;
        validate_safe_integer("walRecordCount", wal_record_count, false)?;
        if base_generation.checked_add(1) != Some(generation) {
            return Err("generation must equal baseGeneration + 1".to_string());
        }
        Ok(Self {
            transaction_nonce,
            base_generation,
            generation,
            wal_sha256,
            wal_bytes,
            wal_record_count,
        })
    }

    fn canonical_bytes(&self, schema: &str) -> Vec<u8> {
        format!(
            "{{\"schema\":\"{schema}\",\"transactionNonce\":\"{}\",\"baseGeneration\":{},\"generation\":{},\"walSha256\":\"{}\",\"walBytes\":{},\"walRecordCount\":{}}}",
            self.transaction_nonce,
            self.base_generation,
            self.generation,
            self.wal_sha256,
            self.wal_bytes,
            self.wal_record_count,
        )
        .into_bytes()
    }
}

impl PreparedCommitEvidence {
    pub fn new(
        transaction_nonce: String,
        base_generation: u64,
        generation: u64,
        wal_sha256: String,
        wal_bytes: u64,
        wal_record_count: u64,
    ) -> Result<Self, String> {
        Ok(Self(CommitEvidencePayload::new(
            transaction_nonce,
            base_generation,
            generation,
            wal_sha256,
            wal_bytes,
            wal_record_count,
        )?))
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        self.0.canonical_bytes(PREPARED_COMMIT_EVIDENCE_SCHEMA)
    }

    pub fn from_canonical_bytes(raw: &[u8]) -> Result<Self, String> {
        let parsed: RawCommitEvidence =
            serde_json::from_slice(raw).map_err(|error| error.to_string())?;
        if parsed.schema != PREPARED_COMMIT_EVIDENCE_SCHEMA {
            return Err(format!("expected schema {PREPARED_COMMIT_EVIDENCE_SCHEMA}"));
        }
        let validated = Self::new(
            parsed.transaction_nonce,
            parsed.base_generation,
            parsed.generation,
            parsed.wal_sha256,
            parsed.wal_bytes,
            parsed.wal_record_count,
        )?;
        if validated.canonical_bytes() != raw {
            return Err("prepared evidence is not canonical JSON".to_string());
        }
        Ok(validated)
    }

    pub fn commit_evidence(&self) -> CommitEvidence {
        CommitEvidence(self.0.clone())
    }

    pub fn transaction_nonce(&self) -> &str {
        &self.0.transaction_nonce
    }

    pub fn base_generation(&self) -> u64 {
        self.0.base_generation
    }

    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    pub fn wal_sha256(&self) -> &str {
        &self.0.wal_sha256
    }

    pub fn wal_bytes(&self) -> u64 {
        self.0.wal_bytes
    }

    pub fn wal_record_count(&self) -> u64 {
        self.0.wal_record_count
    }
}

impl CommitEvidence {
    pub fn canonical_bytes(&self) -> Vec<u8> {
        self.0.canonical_bytes(COMMIT_EVIDENCE_SCHEMA)
    }

    pub fn from_canonical_bytes(raw: &[u8]) -> Result<Self, String> {
        let parsed: RawCommitEvidence =
            serde_json::from_slice(raw).map_err(|error| error.to_string())?;
        if parsed.schema != COMMIT_EVIDENCE_SCHEMA {
            return Err(format!("expected schema {COMMIT_EVIDENCE_SCHEMA}"));
        }
        let prepared = PreparedCommitEvidence::new(
            parsed.transaction_nonce,
            parsed.base_generation,
            parsed.generation,
            parsed.wal_sha256,
            parsed.wal_bytes,
            parsed.wal_record_count,
        )?;
        let validated = prepared.commit_evidence();
        if validated.canonical_bytes() != raw {
            return Err("commit evidence is not canonical JSON".to_string());
        }
        Ok(validated)
    }

    pub fn transaction_nonce(&self) -> &str {
        &self.0.transaction_nonce
    }

    pub fn base_generation(&self) -> u64 {
        self.0.base_generation
    }

    pub fn generation(&self) -> u64 {
        self.0.generation
    }

    pub fn wal_sha256(&self) -> &str {
        &self.0.wal_sha256
    }

    pub fn wal_bytes(&self) -> u64 {
        self.0.wal_bytes
    }

    pub fn wal_record_count(&self) -> u64 {
        self.0.wal_record_count
    }
}

impl TryFrom<RawNativeProgressPolicy> for NativeProgressPolicy {
    type Error = String;

    fn try_from(raw: RawNativeProgressPolicy) -> Result<Self, Self::Error> {
        if raw.schema != NATIVE_PROGRESS_POLICY_SCHEMA
            || raw.protocol_version != NATIVE_PROGRESS_PROTOCOL_VERSION
            || raw.methods.len() != 1
        {
            return Err("progress policy schema, version, or method set is invalid".to_string());
        }
        let raw_method = raw
            .methods
            .into_iter()
            .next()
            .expect("validated one method");
        if raw_method.method != "batch_commit"
            || raw_method
                .phases
                .iter()
                .map(String::as_str)
                .ne(BATCH_COMMIT_PHASES)
            || raw_method.phase_hard_deadline_ms.len() != BATCH_COMMIT_PHASES.len()
        {
            return Err("batch_commit progress inventory is invalid".to_string());
        }
        let mut phase_hard_deadline_ms = Vec::with_capacity(BATCH_COMMIT_PHASES.len());
        for (deadline, expected_phase) in raw_method
            .phase_hard_deadline_ms
            .into_iter()
            .zip(BATCH_COMMIT_PHASES)
        {
            if deadline.phase != expected_phase {
                return Err("phase deadline order does not match phase inventory".to_string());
            }
            validate_safe_integer(
                "phaseHardDeadlineMs.deadlineMs",
                deadline.deadline_ms,
                false,
            )?;
            phase_hard_deadline_ms.push(PhaseHardDeadline {
                phase: deadline.phase,
                deadline_ms: deadline.deadline_ms,
            });
        }
        for (name, value) in [
            (
                "initialFrameDeadlineMs",
                raw_method.initial_frame_deadline_ms,
            ),
            ("inactivityDeadlineMs", raw_method.inactivity_deadline_ms),
            ("absoluteDeadlineMs", raw_method.absolute_deadline_ms),
            ("minFrameIntervalMs", raw_method.min_frame_interval_ms),
            ("heartbeatIntervalMs", raw_method.heartbeat_interval_ms),
            ("earlyByteDelta", raw_method.early_byte_delta),
            ("earlyUnitDelta", raw_method.early_unit_delta),
            ("maxFrames", raw_method.max_frames),
            ("maxFrameBytes", raw_method.max_frame_bytes),
        ] {
            validate_safe_integer(name, value, false)?;
        }
        if raw_method.heartbeat_interval_ms < raw_method.min_frame_interval_ms {
            return Err("heartbeat interval cannot be below the frame interval".to_string());
        }
        let heartbeat_frames = raw_method
            .absolute_deadline_ms
            .checked_add(raw_method.heartbeat_interval_ms - 1)
            .and_then(|value| value.checked_div(raw_method.heartbeat_interval_ms))
            .ok_or_else(|| "progress frame budget arithmetic overflow".to_string())?;
        let mandatory_frames = (BATCH_COMMIT_PHASES.len() as u64)
            .checked_add(heartbeat_frames)
            .ok_or_else(|| "progress frame budget arithmetic overflow".to_string())?;
        if mandatory_frames > raw_method.max_frames {
            return Err("progress policy cannot reserve all mandatory frames".to_string());
        }
        Ok(Self {
            method: NativeProgressMethodPolicy {
                initial_frame_deadline_ms: raw_method.initial_frame_deadline_ms,
                inactivity_deadline_ms: raw_method.inactivity_deadline_ms,
                phase_hard_deadline_ms,
                absolute_deadline_ms: raw_method.absolute_deadline_ms,
                min_frame_interval_ms: raw_method.min_frame_interval_ms,
                heartbeat_interval_ms: raw_method.heartbeat_interval_ms,
                early_byte_delta: raw_method.early_byte_delta,
                early_unit_delta: raw_method.early_unit_delta,
                max_frames: raw_method.max_frames,
                max_frame_bytes: raw_method.max_frame_bytes,
            },
        })
    }
}

impl NativeProgressPolicy {
    pub fn from_canonical_bytes(raw: &[u8]) -> Result<Self, String> {
        let parsed: RawNativeProgressPolicy =
            serde_json::from_slice(raw).map_err(|error| error.to_string())?;
        let validated = Self::try_from(parsed)?;
        if validated.canonical_bytes() != raw {
            return Err("progress policy is not canonical JSON".to_string());
        }
        Ok(validated)
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let method = &self.method;
        let mut out = String::new();
        write!(
            out,
            "{{\"schema\":\"{NATIVE_PROGRESS_POLICY_SCHEMA}\",\"protocolVersion\":{NATIVE_PROGRESS_PROTOCOL_VERSION},\"methods\":[{{\"method\":\"batch_commit\",\"phases\":["
        )
        .expect("writing to String cannot fail");
        for (index, phase) in BATCH_COMMIT_PHASES.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            write!(out, "\"{phase}\"").expect("writing to String cannot fail");
        }
        out.push_str("],\"initialFrameDeadlineMs\":");
        write!(out, "{}", method.initial_frame_deadline_ms).expect("String write");
        write!(
            out,
            ",\"inactivityDeadlineMs\":{},\"phaseHardDeadlineMs\":[",
            method.inactivity_deadline_ms
        )
        .expect("String write");
        for (index, deadline) in method.phase_hard_deadline_ms.iter().enumerate() {
            if index != 0 {
                out.push(',');
            }
            write!(
                out,
                "{{\"phase\":\"{}\",\"deadlineMs\":{}}}",
                deadline.phase, deadline.deadline_ms
            )
            .expect("String write");
        }
        write!(
            out,
            "],\"absoluteDeadlineMs\":{},\"minFrameIntervalMs\":{},\"heartbeatIntervalMs\":{},\"earlyByteDelta\":{},\"earlyUnitDelta\":{},\"maxFrames\":{},\"maxFrameBytes\":{}}}]}}",
            method.absolute_deadline_ms,
            method.min_frame_interval_ms,
            method.heartbeat_interval_ms,
            method.early_byte_delta,
            method.early_unit_delta,
            method.max_frames,
            method.max_frame_bytes,
        )
        .expect("String write");
        out.into_bytes()
    }

    pub fn sha256(&self) -> String {
        let digest = Sha256::digest(self.canonical_bytes());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }
}

impl NativeProgressFrame {
    pub fn admitted(id: u64) -> Result<Self, String> {
        validate_safe_integer("id", id, true)?;
        Ok(Self {
            id,
            sequence: 1,
            phase: "admitted".to_string(),
            completed_units: 0,
            total_units: None,
            completed_bytes: 0,
            total_bytes: None,
        })
    }

    pub fn progress(
        id: u64,
        sequence: u64,
        phase: &str,
        counters: ProgressCounters,
    ) -> Result<Self, String> {
        if phase == "admitted" || !BATCH_COMMIT_PHASES.contains(&phase) {
            return Err("subsequent progress phase is invalid".to_string());
        }
        validate_safe_integer("id", id, true)?;
        validate_safe_integer("sequence", sequence, false)?;
        if sequence == 1 {
            return Err("sequence 1 is reserved for the admitted frame".to_string());
        }
        Ok(Self {
            id,
            sequence,
            phase: phase.to_string(),
            completed_units: counters.completed_units,
            total_units: counters.total_units,
            completed_bytes: counters.completed_bytes,
            total_bytes: counters.total_bytes,
        })
    }

    pub fn from_canonical_bytes(raw: &[u8]) -> Result<Self, String> {
        let parsed: RawNativeProgressFrame =
            serde_json::from_slice(raw).map_err(|error| error.to_string())?;
        if parsed.schema != NATIVE_PROGRESS_FRAME_SCHEMA
            || parsed.kind != "progress"
            || parsed.protocol_version != NATIVE_PROGRESS_PROTOCOL_VERSION
            || parsed.method != "batch_commit"
        {
            return Err("progress frame schema or inventory is invalid".to_string());
        }
        let validated = if parsed.phase == "admitted" {
            if parsed.sequence != 1
                || parsed.completed_units != 0
                || parsed.total_units.is_some()
                || parsed.completed_bytes != 0
                || parsed.total_bytes.is_some()
            {
                return Err("the admitted frame must be sequence 1 with zero counters".to_string());
            }
            Self::admitted(parsed.id)?
        } else {
            Self::progress(
                parsed.id,
                parsed.sequence,
                &parsed.phase,
                ProgressCounters::new(
                    parsed.completed_units,
                    parsed.total_units,
                    parsed.completed_bytes,
                    parsed.total_bytes,
                )?,
            )?
        };
        if validated.canonical_bytes() != raw {
            return Err("progress frame is not canonical JSON".to_string());
        }
        Ok(validated)
    }

    pub fn canonical_bytes(&self) -> Vec<u8> {
        let total_units = self
            .total_units
            .map_or_else(|| "null".to_string(), |value| value.to_string());
        let total_bytes = self
            .total_bytes
            .map_or_else(|| "null".to_string(), |value| value.to_string());
        format!(
            "{{\"schema\":\"{NATIVE_PROGRESS_FRAME_SCHEMA}\",\"kind\":\"progress\",\"protocolVersion\":{NATIVE_PROGRESS_PROTOCOL_VERSION},\"id\":{},\"sequence\":{},\"method\":\"batch_commit\",\"phase\":\"{}\",\"completedUnits\":{},\"totalUnits\":{total_units},\"completedBytes\":{},\"totalBytes\":{total_bytes}}}",
            self.id, self.sequence, self.phase, self.completed_units, self.completed_bytes,
        )
        .into_bytes()
    }
}

impl ProgressCounters {
    pub fn new(
        completed_units: u64,
        total_units: Option<u64>,
        completed_bytes: u64,
        total_bytes: Option<u64>,
    ) -> Result<Self, String> {
        validate_safe_integer("completedUnits", completed_units, true)?;
        validate_safe_integer("completedBytes", completed_bytes, true)?;
        for (name, completed, total) in [
            ("totalUnits", completed_units, total_units),
            ("totalBytes", completed_bytes, total_bytes),
        ] {
            if let Some(total) = total {
                validate_safe_integer(name, total, true)?;
                if completed > total {
                    return Err(format!("{name} is below its completed counter"));
                }
            }
        }
        Ok(Self {
            completed_units,
            total_units,
            completed_bytes,
            total_bytes,
        })
    }
}
