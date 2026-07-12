use anyhow::Error;
use faultline::Error as Fault;
use henosis_types::DurableGraphState;
use henosis_types::FetchSlice;
use henosis_types::GraphSlice;
use henosis_types::SliceReport;
use henosis_types::WatchGraph;
use tokio::sync::broadcast;

use crate::Orchestrator;
use crate::OrchestratorError;
use crate::error::invariant;
use crate::slice::compute_slice;

#[derive(Clone, Debug)]
pub enum WatchEvent {
    Durable {
        sequence: u64,
        state: DurableGraphState,
    },
    Volatile {
        reports: Vec<SliceReport>,
    },
}

pub struct WatchSubscription {
    snapshot_sequence: u64,
    snapshot: DurableGraphState,
    backlog: Vec<(u64, DurableGraphState)>,
    reports: Vec<SliceReport>,
    receiver: broadcast::Receiver<WatchEvent>,
}

pub struct WatchParts {
    pub snapshot_sequence: u64,
    pub snapshot: DurableGraphState,
    pub backlog: Vec<(u64, DurableGraphState)>,
    pub reports: Vec<SliceReport>,
    pub receiver: broadcast::Receiver<WatchEvent>,
}

impl WatchSubscription {
    pub fn into_parts(self) -> WatchParts {
        WatchParts {
            snapshot_sequence: self.snapshot_sequence,
            snapshot: self.snapshot,
            backlog: self.backlog,
            reports: self.reports,
            receiver: self.receiver,
        }
    }
}

impl Orchestrator {
    /// Create an atomic snapshot-to-live handoff at an exact retained sequence.
    pub async fn graph_watch(
        &self,
        command: WatchGraph,
    ) -> Result<WatchSubscription, Fault<OrchestratorError, Error, Error>> {
        let graph_id = command.graph_id();
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        let receiver = runtime.events.subscribe();
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        let current = history
            .head_sequence()
            .ok_or_else(|| invariant("loaded graph has no records"))?;
        let selected = command.after_sequence().unwrap_or(current);
        let earliest = history
            .states()
            .next()
            .map(henosis_types::SequencedGraphState::sequence)
            .unwrap_or(0);
        let snapshot = history.state_at(selected).cloned().ok_or_else(|| {
            Fault::<OrchestratorError, Error, Error>::Domain(OrchestratorError::OutOfRange {
                requested: selected,
                earliest,
                current,
            })
        })?;
        let backlog = history
            .states()
            .filter(|state| state.sequence() > selected)
            .map(|state| (state.sequence(), state.state().clone()))
            .collect();
        let reports = runtime.reports.read().await.iter().cloned().collect();
        Ok(WatchSubscription {
            snapshot_sequence: selected,
            snapshot,
            backlog,
            reports,
            receiver,
        })
    }

    /// Compute one exact-sequence connector slice from domain history.
    pub async fn slice_fetch(
        &self,
        command: FetchSlice,
    ) -> Result<GraphSlice, Fault<OrchestratorError, Error, Error>> {
        let runtime = self.runtime(command.graph_id()).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(command.graph_id(), &mut cached).await?;
        let specs = self.specs.read().await;
        compute_slice(
            cached.as_ref().expect("history was loaded"),
            &specs,
            command.sequence(),
            command.connector(),
        )
        .map_err(|error| match error {
            crate::slice::SliceError::StateNotFound => Fault::Domain(OrchestratorError::NotFound),
            crate::slice::SliceError::SpecNotFound | crate::slice::SliceError::Invalid => {
                Fault::Invariant(Error::new(error))
            }
        })
    }
}
