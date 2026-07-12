//! Connector reports and graph state projections.

mod diagnostic;
mod graph_slice;
mod slice;
mod state;

pub use diagnostic::ContractFailureDetail;
pub use diagnostic::ContractFailureKind;
pub use diagnostic::Diagnostic;
pub use diagnostic::DiagnosticSeverity;
pub use graph_slice::GraphSlice;
pub use graph_slice::GraphSliceError;
pub use slice::ComponentDisposition;
pub use slice::ComponentDispositionKind;
pub use slice::NewSliceReport;
pub use slice::PublicationEvidence;
pub use slice::SliceReport;
pub use slice::SliceReportError;
pub use state::DuplicateConnectorReport;
pub use state::DurableGraphState;
pub use state::GraphGenerationState;
pub use state::GraphLifecycle;
pub use state::GraphState;
