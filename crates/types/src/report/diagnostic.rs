use crate::ComponentSpecHash;

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

impl ContractFailureDetail {
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
    component_spec_hash: Option<ComponentSpecHash>,
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
            component_spec_hash: None,
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
        code: String,
        message: String,
        component_spec_hash: Option<ComponentSpecHash>,
        pointer: String,
        help: String,
        severity: DiagnosticSeverity,
        contract_failure: Option<ContractFailureDetail>,
    ) -> Self {
        Self {
            code,
            message,
            component_spec_hash,
            pointer,
            help,
            severity,
            contract_failure,
        }
    }

    #[must_use]
    pub fn for_component(mut self, hash: ComponentSpecHash) -> Self {
        self.component_spec_hash = Some(hash);
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
    pub const fn component_spec_hash(&self) -> Option<ComponentSpecHash> {
        self.component_spec_hash
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
