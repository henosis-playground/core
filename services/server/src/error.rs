use anyhow::Error;
use connectrpc::ConnectError;
use connectrpc::ErrorCode;
use connectrpc::ErrorDetail;
use faultline::Error as Fault;
use henosis_orchestrator::OrchestratorError;
use henosis_proto::api::ConversionError;
use henosis_proto::protobuf;

#[derive(Clone, Copy)]
pub(crate) enum ErrorSurface {
    Edit,
    Report,
    Watch,
}

pub(crate) fn conversion_error(error: ConversionError) -> ConnectError {
    ConnectError::invalid_argument(error.to_string())
}

pub(crate) fn connect_error(
    error: Fault<OrchestratorError, Error, Error>,
    surface: ErrorSurface,
) -> ConnectError {
    match error {
        Fault::Domain(domain) => domain_error(domain, surface),
        Fault::Transient(_) => ConnectError::unavailable("dependency temporarily unavailable"),
        Fault::Invariant(_) => ConnectError::internal("internal invariant violation"),
    }
}

fn domain_error(error: OrchestratorError, surface: ErrorSurface) -> ConnectError {
    let (code, message, generation, diagnostics, cursor) = match error {
        OrchestratorError::InvalidArgument { diagnostics } => (
            ErrorCode::InvalidArgument,
            "request is invalid",
            0,
            diagnostics,
            None,
        ),
        OrchestratorError::NotFound => (
            ErrorCode::NotFound,
            "graph or slice not found",
            0,
            Vec::new(),
            None,
        ),
        OrchestratorError::AlreadyExists { diagnostics } => (
            ErrorCode::AlreadyExists,
            "resource or request identity already exists",
            0,
            diagnostics,
            None,
        ),
        OrchestratorError::Aborted { current_generation } => (
            ErrorCode::Aborted,
            "generation compare-and-swap failed",
            current_generation,
            Vec::new(),
            None,
        ),
        OrchestratorError::FailedPrecondition { diagnostics } => (
            ErrorCode::FailedPrecondition,
            "operation failed a graph precondition",
            0,
            diagnostics,
            None,
        ),
        OrchestratorError::OutOfRange {
            requested,
            earliest,
            current,
        } => (
            ErrorCode::OutOfRange,
            "watch cursor is outside retained history",
            0,
            Vec::new(),
            Some(protobuf::v1::WatchCursorErrorDetails {
                requested_sequence: Some(requested),
                earliest_available_sequence: Some(earliest),
                current_sequence: Some(current),
                ..Default::default()
            }),
        ),
    };
    let diagnostics = diagnostics.iter().map(Into::into).collect();
    let mut error = ConnectError::new(code, message);
    match surface {
        ErrorSurface::Edit => {
            error = error.with_detail(ErrorDetail::from_message(
                "henosis.v1.EditRejectionDetails",
                &protobuf::v1::EditRejectionDetails {
                    current_generation: (generation > 0).then_some(generation),
                    diagnostics,
                    ..Default::default()
                },
            ));
        }
        ErrorSurface::Report => {
            error = error.with_detail(ErrorDetail::from_message(
                "henosis.v1.ReportRejectionDetails",
                &protobuf::v1::ReportRejectionDetails {
                    diagnostics,
                    ..Default::default()
                },
            ));
        }
        ErrorSurface::Watch => {
            if let Some(cursor) = cursor {
                error = error.with_detail(ErrorDetail::from_message(
                    "henosis.v1.WatchCursorErrorDetails",
                    &cursor,
                ));
            }
        }
    }
    error
}
