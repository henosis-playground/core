use crate::domain::ComponentUuid;
use thiserror::Error;

/// Importance of a connector diagnostic.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

/// Stage that produced a structured contract failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractFailureKind {
    Compile,
    Render,
    Validate,
    Resolve,
}

/// Structured evidence for a producer/consumer contract failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContractFailureDetail {
    pub consumer: String,
    pub producer: String,
    pub pinned_sha: Option<String>,
    pub resolved_sha: Option<String>,
    pub outputs_schema_at_pinned_json: Option<Vec<u8>>,
    pub outputs_schema_at_resolved_json: Option<Vec<u8>>,
    pub consumed_paths: Vec<String>,
    pub kind: ContractFailureKind,
    pub excerpt: String,
    pub source_url: Option<String>,
}

/// Borrowed, unvalidated input used to construct contract-failure evidence.
#[derive(Clone, Debug)]
pub struct NewContractFailureDetail<'a> {
    pub consumer: &'a str,
    pub producer: &'a str,
    pub pinned_sha: Option<&'a str>,
    pub resolved_sha: Option<&'a str>,
    pub outputs_schema_at_pinned_json: Option<Vec<u8>>,
    pub outputs_schema_at_resolved_json: Option<Vec<u8>>,
    pub consumed_paths: Vec<&'a str>,
    pub kind: ContractFailureKind,
    pub excerpt: &'a str,
    pub source_url: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ContractFailureDetailError {
    #[error("consumed paths must be unique")]
    DuplicateConsumedPath,
}

impl ContractFailureDetail {
    pub fn new(mut new: NewContractFailureDetail<'_>) -> Result<Self, ContractFailureDetailError> {
        new.consumed_paths.sort_unstable();
        if new.consumed_paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(ContractFailureDetailError::DuplicateConsumedPath);
        }
        Ok(Self {
            consumer: new.consumer.to_owned(),
            producer: new.producer.to_owned(),
            pinned_sha: new.pinned_sha.map(str::to_owned),
            resolved_sha: new.resolved_sha.map(str::to_owned),
            outputs_schema_at_pinned_json: new.outputs_schema_at_pinned_json,
            outputs_schema_at_resolved_json: new.outputs_schema_at_resolved_json,
            consumed_paths: new.consumed_paths.into_iter().map(str::to_owned).collect(),
            kind: new.kind,
            excerpt: new.excerpt.to_owned(),
            source_url: new.source_url.map(str::to_owned),
        })
    }

    #[must_use]
    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    #[must_use]
    pub fn producer(&self) -> &str {
        &self.producer
    }

    #[must_use]
    pub fn pinned_sha(&self) -> Option<&str> {
        self.pinned_sha.as_deref()
    }

    #[must_use]
    pub fn resolved_sha(&self) -> Option<&str> {
        self.resolved_sha.as_deref()
    }

    #[must_use]
    pub fn outputs_schema_at_pinned_json(&self) -> Option<&[u8]> {
        self.outputs_schema_at_pinned_json.as_deref()
    }

    #[must_use]
    pub fn outputs_schema_at_resolved_json(&self) -> Option<&[u8]> {
        self.outputs_schema_at_resolved_json.as_deref()
    }

    #[must_use]
    pub fn consumed_paths(&self) -> &[String] {
        &self.consumed_paths
    }

    #[must_use]
    pub const fn kind(&self) -> ContractFailureKind {
        self.kind
    }

    #[must_use]
    pub fn excerpt(&self) -> &str {
        &self.excerpt
    }

    #[must_use]
    pub fn source_url(&self) -> Option<&str> {
        self.source_url.as_deref()
    }
}

/// One actionable connector diagnostic.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    code: String,
    message: String,
    component_id: Option<ComponentUuid>,
    pointer: String,
    help: String,
    severity: DiagnosticSeverity,
    contract_failure: Option<ContractFailureDetail>,
}

impl Diagnostic {
    /// Start an error diagnostic with a stable code.
    #[must_use]
    pub fn error(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: String::new(),
            component_id: None,
            pointer: String::new(),
            help: String::new(),
            severity: DiagnosticSeverity::Error,
            contract_failure: None,
        }
    }

    /// Construct a diagnostic after decoding and validating its wire fields.
    #[doc(hidden)]
    #[must_use]
    pub fn new(
        code: &str,
        message: &str,
        component_id: Option<ComponentUuid>,
        pointer: &str,
        help: &str,
        severity: DiagnosticSeverity,
        contract_failure: Option<ContractFailureDetail>,
    ) -> Self {
        Self {
            code: code.to_owned(),
            message: message.to_owned(),
            component_id,
            pointer: pointer.to_owned(),
            help: help.to_owned(),
            severity,
            contract_failure,
        }
    }

    #[must_use]
    pub fn for_component(mut self, hash: ComponentUuid) -> Self {
        self.component_id = Some(hash);
        self
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn component_id(&self) -> Option<ComponentUuid> {
        self.component_id
    }

    #[must_use]
    pub fn pointer(&self) -> &str {
        &self.pointer
    }

    #[must_use]
    pub fn help(&self) -> &str {
        &self.help
    }

    #[must_use]
    pub const fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }

    #[must_use]
    pub const fn contract_failure(&self) -> Option<&ContractFailureDetail> {
        self.contract_failure.as_ref()
    }
}
