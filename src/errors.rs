use std::collections::HashMap;

use crate::contracts::{ERROR_SPEC_ID, ErrorCodeSpec, load_error_code_spec};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCode {
    ProtocolVersionMismatch,
    UnsupportedFeature,
    RetryableConflict,
    WriteLockConflict,
    IncompatibleFormat,
    AuthRequired,
    AuthFailed,
    ReferentialIntegrityViolation,
    InvalidArgument,
    InvalidTopK,
    InvalidThreshold,
    InvalidCorpusId,
    InvalidNamespace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorCategory {
    Protocol,
    Query,
    Transaction,
    Storage,
    Auth,
    Integrity,
    Validation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GraphDbError {
    pub code: ErrorCode,
    pub message: String,
    pub details: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct ErrorDefinition {
    pub code: ErrorCode,
    pub category: ErrorCategory,
    pub description: String,
}

#[derive(Debug, Clone)]
pub struct ErrorRegistry {
    definitions: HashMap<ErrorCode, ErrorDefinition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegistryError {
    SpecIdMismatch { expected: String, got: String },
    UnknownCode(String),
    UnknownCategory(String),
    DuplicateCode(String),
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::ProtocolVersionMismatch => "PROTOCOL_VERSION_MISMATCH",
            ErrorCode::UnsupportedFeature => "UNSUPPORTED_FEATURE",
            ErrorCode::RetryableConflict => "RETRYABLE_CONFLICT",
            ErrorCode::WriteLockConflict => "WRITE_LOCK_CONFLICT",
            ErrorCode::IncompatibleFormat => "INCOMPATIBLE_FORMAT",
            ErrorCode::AuthRequired => "AUTH_REQUIRED",
            ErrorCode::AuthFailed => "AUTH_FAILED",
            ErrorCode::ReferentialIntegrityViolation => "REFERENTIAL_INTEGRITY_VIOLATION",
            ErrorCode::InvalidArgument => "INVALID_ARGUMENT",
            ErrorCode::InvalidTopK => "INVALID_TOP_K",
            ErrorCode::InvalidThreshold => "INVALID_THRESHOLD",
            ErrorCode::InvalidCorpusId => "INVALID_CORPUS_ID",
            ErrorCode::InvalidNamespace => "INVALID_NAMESPACE",
        }
    }

    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "PROTOCOL_VERSION_MISMATCH" => Some(ErrorCode::ProtocolVersionMismatch),
            "UNSUPPORTED_FEATURE" => Some(ErrorCode::UnsupportedFeature),
            "RETRYABLE_CONFLICT" => Some(ErrorCode::RetryableConflict),
            "WRITE_LOCK_CONFLICT" => Some(ErrorCode::WriteLockConflict),
            "INCOMPATIBLE_FORMAT" => Some(ErrorCode::IncompatibleFormat),
            "AUTH_REQUIRED" => Some(ErrorCode::AuthRequired),
            "AUTH_FAILED" => Some(ErrorCode::AuthFailed),
            "REFERENTIAL_INTEGRITY_VIOLATION" => Some(ErrorCode::ReferentialIntegrityViolation),
            "INVALID_ARGUMENT" => Some(ErrorCode::InvalidArgument),
            "INVALID_TOP_K" => Some(ErrorCode::InvalidTopK),
            "INVALID_THRESHOLD" => Some(ErrorCode::InvalidThreshold),
            "INVALID_CORPUS_ID" => Some(ErrorCode::InvalidCorpusId),
            "INVALID_NAMESPACE" => Some(ErrorCode::InvalidNamespace),
            _ => None,
        }
    }

    pub fn category(self) -> ErrorCategory {
        match self {
            ErrorCode::ProtocolVersionMismatch => ErrorCategory::Protocol,
            ErrorCode::UnsupportedFeature => ErrorCategory::Query,
            ErrorCode::RetryableConflict => ErrorCategory::Transaction,
            ErrorCode::WriteLockConflict | ErrorCode::IncompatibleFormat => ErrorCategory::Storage,
            ErrorCode::AuthRequired | ErrorCode::AuthFailed => ErrorCategory::Auth,
            ErrorCode::ReferentialIntegrityViolation => ErrorCategory::Integrity,
            ErrorCode::InvalidArgument
            | ErrorCode::InvalidTopK
            | ErrorCode::InvalidThreshold
            | ErrorCode::InvalidCorpusId
            | ErrorCode::InvalidNamespace => ErrorCategory::Validation,
        }
    }
}

impl ErrorCategory {
    pub fn from_str(value: &str) -> Option<Self> {
        match value {
            "protocol" => Some(ErrorCategory::Protocol),
            "query" => Some(ErrorCategory::Query),
            "transaction" => Some(ErrorCategory::Transaction),
            "storage" => Some(ErrorCategory::Storage),
            "auth" => Some(ErrorCategory::Auth),
            "integrity" => Some(ErrorCategory::Integrity),
            "validation" => Some(ErrorCategory::Validation),
            _ => None,
        }
    }
}

impl GraphDbError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_detail(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let mut details = self.details.unwrap_or_default();
        details.insert(key.into(), value.into());
        self.details = Some(details);
        self
    }
}

impl ErrorRegistry {
    pub fn load() -> Result<Self, RegistryError> {
        Self::from_spec(load_error_code_spec())
    }

    pub fn from_spec(spec: ErrorCodeSpec) -> Result<Self, RegistryError> {
        if spec.spec_id != ERROR_SPEC_ID {
            return Err(RegistryError::SpecIdMismatch {
                expected: ERROR_SPEC_ID.to_string(),
                got: spec.spec_id,
            });
        }

        let mut definitions = HashMap::new();
        for entry in spec.codes {
            let code = ErrorCode::from_str(&entry.code)
                .ok_or_else(|| RegistryError::UnknownCode(entry.code.clone()))?;
            let category = ErrorCategory::from_str(&entry.category)
                .ok_or_else(|| RegistryError::UnknownCategory(entry.category.clone()))?;
            if definitions.contains_key(&code) {
                return Err(RegistryError::DuplicateCode(entry.code));
            }

            definitions.insert(
                code,
                ErrorDefinition {
                    code,
                    category,
                    description: entry.description,
                },
            );
        }

        Ok(Self { definitions })
    }

    pub fn definition(&self, code: ErrorCode) -> Option<&ErrorDefinition> {
        self.definitions.get(&code)
    }

    pub fn definitions_by_category(&self, category: ErrorCategory) -> Vec<&ErrorDefinition> {
        self.definitions
            .values()
            .filter(|definition| definition.category == category)
            .collect()
    }

    pub fn is_known_code_str(&self, code: &str) -> bool {
        ErrorCode::from_str(code)
            .and_then(|parsed| self.definitions.get(&parsed).map(|_| parsed))
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::{ErrorCodeEntry, ErrorCodeSpec};

    #[test]
    fn loads_registry_from_contract() {
        let registry = ErrorRegistry::load().expect("registry should load");

        let protocol = registry
            .definition(ErrorCode::ProtocolVersionMismatch)
            .expect("protocol mismatch code exists");
        assert_eq!(protocol.category, ErrorCategory::Protocol);
        assert!(registry.is_known_code_str("AUTH_FAILED"));
    }

    #[test]
    fn filters_definitions_by_category() {
        let registry = ErrorRegistry::load().expect("registry should load");
        let auth_definitions = registry.definitions_by_category(ErrorCategory::Auth);
        assert_eq!(auth_definitions.len(), 2);
    }

    #[test]
    fn rejects_unknown_codes() {
        let spec = ErrorCodeSpec {
            spec_id: ERROR_SPEC_ID.to_string(),
            codes: vec![ErrorCodeEntry {
                code: "NOT_A_REAL_CODE".to_string(),
                category: "auth".to_string(),
                description: "invalid".to_string(),
            }],
        };

        let result = ErrorRegistry::from_spec(spec);
        assert!(matches!(result, Err(RegistryError::UnknownCode(_))));
    }
}
