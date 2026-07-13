use std::sync::Arc;

use anyhow::Error;
use faultline::Error as Fault;
use henosis_proto::api::publication_hash;
use henosis_proto::api::report_request_hash;
use types::domain::GraphEvent;
use types::domain::GraphHistory;
use types::domain::RecordedSliceReport;
use types::domain::ReportSlice;

use crate::Orchestrator;
use crate::OrchestratorError;
use crate::WatchEvent;
use crate::error::already_exists;
use crate::error::failed_precondition;
use crate::error::invalid_argument;
use crate::error::invariant;
use crate::error::map_journal;
use crate::graph::publish_history;
use crate::slice::compute_slice;
use crate::validation::ReportValidationError;
use crate::validation::validate_report;

impl Orchestrator {
    /// Accept one complete connector observation and optionally publish
    /// outputs.
    pub async fn slice_report(
        self: &Arc<Self>,
        command: ReportSlice,
    ) -> Result<Option<u64>, Fault<OrchestratorError, Error, Error>> {
        let request_hash = report_request_hash(&command);
        let report = command.report();
        let graph_id = report.graph_id();
        let connector = report.connector().clone();
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");

        if let Some(receipt) = history.output_request(&connector, command.request_id()) {
            if receipt.hash() != request_hash {
                return Err(already_exists("request_id.reused"));
            }
            return Ok(receipt.publication_sequence());
        }

        let candidate_publication_hash = command.publication_id().map(|_| publication_hash(report));
        if let (Some(publication_id), Some(hash)) =
            (command.publication_id(), candidate_publication_hash)
            && let Some(receipt) = history.publication(&connector, publication_id)
        {
            if receipt.hash() != hash {
                return Err(already_exists("publication_id.reused"));
            }
            return Ok(Some(receipt.publication_sequence()));
        }

        if history.is_retired() {
            return Err(failed_precondition("graph.retired"));
        }
        let state = current_state(history)?;
        let current_generation = state.graph().generation();
        if report.generation() != current_generation
            || history.head_sequence() != Some(report.sequence())
        {
            return Err(Fault::Domain(OrchestratorError::Aborted {
                current_generation,
            }));
        }
        let slice = {
            let specs = self.specs.read().await;
            compute_slice(history, &specs, report.sequence(), &connector)
                .map_err(|_| invariant("failed to compute current slice"))?
        };
        let publishable = validate_report(report, &slice).map_err(map_report_validation)?;
        match (publishable, command.publication_id()) {
            (true, None) => return Err(invalid_argument("publication_id.required")),
            (false, Some(_)) => {
                return Err(invalid_argument("publication_id.non_publishing"));
            }
            _ => {}
        }
        let publication_id = command.publication_id();
        if let Some(publication_hash) = candidate_publication_hash
            && history
                .latest_publication(&connector)
                .is_some_and(|receipt| receipt.hash() == publication_hash)
        {
            return Err(failed_precondition(
                "publication_id.changed_for_unchanged_level",
            ));
        }
        let event = GraphEvent::SliceReported(RecordedSliceReport {
            report: report.clone(),
            request_id: command.request_id(),
            request_hash,
            publication_id,
            publication_hash: candidate_publication_hash,
        });
        let sequence = self
            .journal
            .graph_append(graph_id, history.next_sequence(), &event)
            .await
            .map_err(map_journal)?;
        let history = self
            .journal
            .graph_load(graph_id)
            .await
            .map_err(map_journal)?;
        *runtime.reports.write().await = history.reports().cloned().collect();
        if publishable {
            publish_history(&runtime, &mut cached, history)?;
        } else {
            *cached = Some(history);
        }
        drop(cached);
        if publishable {
            self.schedule_delivery(graph_id).await;
        }
        let publication_sequence = publishable.then_some(sequence);
        let reports = runtime.reports.read().await.iter().cloned().collect();
        let _ = runtime.events.send(WatchEvent::Volatile { reports });
        Ok(publication_sequence)
    }
}

fn current_state(
    history: &GraphHistory,
) -> Result<&types::domain::DurableGraphState, Fault<OrchestratorError, Error, Error>> {
    history
        .desired_state()
        .ok_or_else(|| invariant("loaded graph has no state"))
}

fn map_report_validation(error: ReportValidationError) -> Fault<OrchestratorError, Error, Error> {
    match error {
        ReportValidationError::Invalid(diagnostics) => {
            Fault::Domain(OrchestratorError::InvalidArgument { diagnostics })
        }
        ReportValidationError::FailedPrecondition(diagnostics) => {
            Fault::Domain(OrchestratorError::FailedPrecondition { diagnostics })
        }
    }
}
