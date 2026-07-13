use anyhow::Error;
use faultline::Error as Fault;
use henosis_journal::JournalError;
use types::domain::Diagnostic;

#[derive(Clone, Debug, thiserror::Error, Eq, PartialEq)]
pub enum OrchestratorError {
    #[error("request is invalid")]
    InvalidArgument { diagnostics: Vec<Diagnostic> },
    #[error("graph or slice was not found")]
    NotFound,
    #[error("resource or request identity already exists")]
    AlreadyExists { diagnostics: Vec<Diagnostic> },
    #[error("optimistic concurrency check failed")]
    Aborted { current_generation: u64 },
    #[error("operation violates graph lifecycle or dependency state")]
    FailedPrecondition { diagnostics: Vec<Diagnostic> },
    #[error("watch cursor is outside retained history")]
    OutOfRange {
        requested: u64,
        earliest: u64,
        current: u64,
    },
}

pub(crate) fn invalid_argument(code: &str) -> Fault<OrchestratorError, Error, Error> {
    Fault::Domain(OrchestratorError::InvalidArgument {
        diagnostics: vec![Diagnostic::error(code)],
    })
}

pub(crate) fn already_exists(code: &str) -> Fault<OrchestratorError, Error, Error> {
    Fault::Domain(OrchestratorError::AlreadyExists {
        diagnostics: vec![Diagnostic::error(code)],
    })
}

pub(crate) fn failed_precondition(code: &str) -> Fault<OrchestratorError, Error, Error> {
    Fault::Domain(OrchestratorError::FailedPrecondition {
        diagnostics: vec![Diagnostic::error(code)],
    })
}

pub(crate) const fn not_found() -> Fault<OrchestratorError, Error, Error> {
    Fault::Domain(OrchestratorError::NotFound)
}

pub(crate) fn invariant(message: &'static str) -> Fault<OrchestratorError, Error, Error> {
    Fault::Invariant(anyhow::anyhow!(message))
}

pub(crate) fn map_journal(
    error: Fault<JournalError, Error, Error>,
) -> Fault<OrchestratorError, Error, Error> {
    match error {
        Fault::Domain(JournalError::NotFound) => not_found(),
        Fault::Domain(JournalError::CasConflict { .. }) => {
            invariant("journal CAS conflict escaped its append boundary")
        }
        Fault::Domain(JournalError::ComponentConflict) => already_exists("component.id.conflict"),
        Fault::Transient(error) => Fault::Transient(error),
        Fault::Invariant(error) => Fault::Invariant(error),
    }
}
