use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use super::receipt::OutputRequestKey;
use super::receipt::PublicationKey;
use crate::domain::ConnectorKey;
use crate::domain::DurableGraphState;
use crate::domain::Graph;
use crate::domain::GraphEvent;
use crate::domain::GraphLifecycle;
use crate::domain::GraphUuid;
use crate::domain::MutationKind;
use crate::domain::MutationReceipt;
use crate::domain::MutationResponse;
use crate::domain::OutputPublication;
use crate::domain::OutputRequestReceipt;
use crate::domain::PublicationReceipt;
use crate::domain::PublicationUuid;
use crate::domain::PublishedSliceOutputs;
use crate::domain::RecordedSliceReport;
use crate::domain::RequestUuid;
use crate::domain::SequencedGraphEvent;
use crate::domain::SequencedGraphState;
use crate::domain::SliceReport;
use blake3::Hash;

/// Folded state and idempotency indexes for one graph stream.
#[derive(Clone, Debug)]
pub struct GraphHistory {
    graph_id: GraphUuid,
    tail_sequence: Option<u64>,
    desired_sequence: Option<u64>,
    durable: Option<DurableGraphState>,
    generations: IdOrdMap<Graph>,
    states: IdOrdMap<SequencedGraphState>,
    requests: IdHashMap<MutationReceipt>,
    output_requests: IdHashMap<OutputRequestReceipt>,
    publications: IdHashMap<PublicationReceipt>,
    reports: IdOrdMap<SliceReport>,
}

/// Invalid graph-stream history.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum HistoryError {
    #[error("graph stream sequence is not contiguous")]
    NonContiguous,
    #[error("graph creation is not the first record")]
    CreationOrder,
    #[error("graph event identity does not match its stream")]
    GraphUuidentity,
    #[error("created graph generation is not one")]
    InitialGeneration,
    #[error("graph generation is not consecutive")]
    NonConsecutiveGeneration,
    #[error("graph event precedes creation or follows retirement")]
    InvalidLifecycle,
    #[error("a durable request identity was reused")]
    DuplicateRequest,
    #[error("outputs target a stale graph generation")]
    StaleOutputGeneration,
    #[error("output input sequence does not precede its publication")]
    InvalidInputSequence,
    #[error("an output request identity was reused")]
    DuplicateOutputRequest,
    #[error("an output publication identity was reused")]
    DuplicatePublication,
    #[error("retirement generation does not match desired state")]
    RetirementGeneration,
}

impl GraphHistory {
    // === Construction and folding ===

    #[must_use]
    pub fn new(graph_id: GraphUuid) -> Self {
        Self {
            graph_id,
            tail_sequence: None,
            desired_sequence: None,
            durable: None,
            generations: IdOrdMap::new(),
            states: IdOrdMap::new(),
            requests: IdHashMap::new(),
            output_requests: IdHashMap::new(),
            publications: IdHashMap::new(),
            reports: IdOrdMap::new(),
        }
    }

    /// Fold one already-parsed domain event into this history.
    pub fn apply(&mut self, record: SequencedGraphEvent) -> Result<(), HistoryError> {
        let expected = self
            .tail_sequence
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(0);
        if record.sequence != expected {
            return Err(HistoryError::NonContiguous);
        }
        let state_changed = match record.event {
            GraphEvent::Created {
                graph,
                request_id,
                request_hash,
            } => {
                self.apply_created(graph, request_id, request_hash)?;
                true
            }
            GraphEvent::GenerationAccepted {
                graph,
                request_id,
                mutation_kind,
                request_hash,
            } => {
                self.apply_generation(graph, request_id, mutation_kind, request_hash)?;
                true
            }
            GraphEvent::OutputsPublished(publication) => {
                self.apply_outputs(record.sequence, publication)?;
                true
            }
            GraphEvent::SliceReported(report) => self.apply_report(record.sequence, report)?,
            GraphEvent::Retired {
                graph_id,
                last_generation,
                request_id,
                request_hash,
            } => {
                self.apply_retired(graph_id, last_generation, request_id, request_hash)?;
                true
            }
        };
        self.tail_sequence = Some(record.sequence);
        if state_changed {
            self.desired_sequence = Some(record.sequence);
            let state = SequencedGraphState {
                sequence: record.sequence,
                state: self.durable()?.clone(),
            };
            self.states
                .insert_unique(state)
                .map_err(|_| HistoryError::NonContiguous)?;
        }
        Ok(())
    }

    fn apply_report(
        &mut self,
        record_sequence: u64,
        recorded: RecordedSliceReport,
    ) -> Result<bool, HistoryError> {
        self.require_active()?;
        if recorded.report.graph_id() != self.graph_id
            || recorded.report.generation() != self.durable()?.graph().generation()
            || Some(recorded.report.sequence()) != self.desired_sequence
        {
            return Err(HistoryError::StaleOutputGeneration);
        }
        let connector = recorded.report.connector().clone();
        let publishes = match (recorded.publication_id, recorded.publication_hash) {
            (Some(publication_id), Some(publication_hash)) => {
                let outputs = recorded.report.outputs().cloned().collect::<Vec<_>>();
                self.output_requests
                    .insert_unique(OutputRequestReceipt {
                        connector: connector.clone(),
                        request_id: recorded.request_id,
                        hash: recorded.request_hash,
                        publication_sequence: Some(record_sequence),
                    })
                    .map_err(|_| HistoryError::DuplicateOutputRequest)?;
                self.publications
                    .insert_unique(PublicationReceipt {
                        connector: connector.clone(),
                        publication_id,
                        hash: publication_hash,
                        publication_sequence: record_sequence,
                    })
                    .map_err(|_| HistoryError::DuplicatePublication)?;
                self.durable_mut()?
                    .published_outputs_mut()
                    .insert_overwrite(PublishedSliceOutputs::new(
                        recorded.report.generation(),
                        connector,
                        outputs,
                        record_sequence,
                        publication_id,
                        recorded.report.sequence(),
                    ));
                true
            }
            (None, None) => {
                self.output_requests
                    .insert_unique(OutputRequestReceipt {
                        connector,
                        request_id: recorded.request_id,
                        hash: recorded.request_hash,
                        publication_sequence: None,
                    })
                    .map_err(|_| HistoryError::DuplicateOutputRequest)?;
                false
            }
            _ => return Err(HistoryError::DuplicatePublication),
        };
        self.reports.insert_overwrite(recorded.report);
        Ok(publishes)
    }

    fn apply_created(
        &mut self,
        graph: Graph,
        request_id: RequestUuid,
        hash: Hash,
    ) -> Result<(), HistoryError> {
        if self.durable.is_some() || graph.generation() != 1 {
            return Err(if self.durable.is_some() {
                HistoryError::CreationOrder
            } else {
                HistoryError::InitialGeneration
            });
        }
        self.require_identity(&graph)?;
        self.generations
            .insert_unique(graph.clone())
            .map_err(|_| HistoryError::CreationOrder)?;
        self.requests
            .insert_unique(MutationReceipt {
                request_id,
                kind: MutationKind::Create,
                hash,
                response: MutationResponse::Graph(graph.clone()),
            })
            .map_err(|_| HistoryError::DuplicateRequest)?;
        self.durable = Some(DurableGraphState::new(
            graph,
            IdOrdMap::new(),
            GraphLifecycle::Active,
        ));
        Ok(())
    }

    fn apply_generation(
        &mut self,
        graph: Graph,
        request_id: RequestUuid,
        mutation_kind: MutationKind,
        hash: Hash,
    ) -> Result<(), HistoryError> {
        self.require_active()?;
        self.require_identity(&graph)?;
        if graph.generation() != self.durable()?.graph().generation().saturating_add(1) {
            return Err(HistoryError::NonConsecutiveGeneration);
        }
        self.requests
            .insert_unique(MutationReceipt {
                request_id,
                kind: mutation_kind,
                hash,
                response: MutationResponse::Graph(graph.clone()),
            })
            .map_err(|_| HistoryError::DuplicateRequest)?;
        self.generations
            .insert_unique(graph.clone())
            .map_err(|_| HistoryError::NonConsecutiveGeneration)?;
        *self.durable_mut()?.graph_mut() = graph;
        Ok(())
    }

    fn apply_outputs(
        &mut self,
        sequence: u64,
        publication: OutputPublication,
    ) -> Result<(), HistoryError> {
        self.require_active()?;
        if publication.generation != self.durable()?.graph().generation() {
            return Err(HistoryError::StaleOutputGeneration);
        }
        if publication.input_sequence >= sequence {
            return Err(HistoryError::InvalidInputSequence);
        }
        self.output_requests
            .insert_unique(OutputRequestReceipt {
                connector: publication.connector.clone(),
                request_id: publication.request_id,
                hash: publication.request_hash,
                publication_sequence: Some(sequence),
            })
            .map_err(|_| HistoryError::DuplicateOutputRequest)?;
        self.publications
            .insert_unique(PublicationReceipt {
                connector: publication.connector.clone(),
                publication_id: publication.publication_id,
                hash: publication.publication_hash,
                publication_sequence: sequence,
            })
            .map_err(|_| HistoryError::DuplicatePublication)?;
        self.durable_mut()?
            .published_outputs_mut()
            .insert_overwrite(PublishedSliceOutputs::new(
                publication.generation,
                publication.connector,
                publication.outputs,
                sequence,
                publication.publication_id,
                publication.input_sequence,
            ));
        Ok(())
    }

    fn apply_retired(
        &mut self,
        graph_id: GraphUuid,
        last_generation: u64,
        request_id: RequestUuid,
        hash: Hash,
    ) -> Result<(), HistoryError> {
        self.require_active()?;
        if graph_id != self.graph_id {
            return Err(HistoryError::GraphUuidentity);
        }
        if last_generation != self.durable()?.graph().generation() {
            return Err(HistoryError::RetirementGeneration);
        }
        self.requests
            .insert_unique(MutationReceipt {
                request_id,
                kind: MutationKind::Retire,
                hash,
                response: MutationResponse::Retired {
                    graph_id,
                    last_generation,
                },
            })
            .map_err(|_| HistoryError::DuplicateRequest)?;
        self.durable_mut()?.retire();
        Ok(())
    }

    fn require_identity(&self, graph: &Graph) -> Result<(), HistoryError> {
        if graph.id() == self.graph_id {
            Ok(())
        } else {
            Err(HistoryError::GraphUuidentity)
        }
    }

    fn require_active(&self) -> Result<(), HistoryError> {
        match self.durable.as_ref().map(DurableGraphState::lifecycle) {
            Some(GraphLifecycle::Active) => Ok(()),
            Some(GraphLifecycle::Retired) | None => Err(HistoryError::InvalidLifecycle),
        }
    }

    fn durable(&self) -> Result<&DurableGraphState, HistoryError> {
        self.durable.as_ref().ok_or(HistoryError::InvalidLifecycle)
    }

    fn durable_mut(&mut self) -> Result<&mut DurableGraphState, HistoryError> {
        self.durable.as_mut().ok_or(HistoryError::InvalidLifecycle)
    }

    // === History queries ===

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn head_sequence(&self) -> Option<u64> {
        self.desired_sequence
    }

    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.tail_sequence
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(0)
    }

    #[must_use]
    pub fn desired_state(&self) -> Option<&DurableGraphState> {
        self.durable.as_ref()
    }

    #[must_use]
    pub fn generation(&self, generation: u64) -> Option<&Graph> {
        self.generations.get(&generation)
    }

    pub fn generations(&self) -> impl ExactSizeIterator<Item = &Graph> {
        self.generations.iter()
    }

    pub fn states(&self) -> impl ExactSizeIterator<Item = &SequencedGraphState> {
        self.states.iter()
    }

    #[must_use]
    pub fn state_at(&self, sequence: u64) -> Option<&DurableGraphState> {
        self.states.get(&sequence).map(SequencedGraphState::state)
    }

    pub fn reports_for_generation(&self, generation: u64) -> impl Iterator<Item = &SliceReport> {
        self.reports
            .iter()
            .filter(move |report| report.generation() == generation)
    }

    pub fn reports(&self) -> impl ExactSizeIterator<Item = &SliceReport> {
        self.reports.iter()
    }

    #[must_use]
    pub fn last_published_generation(&self) -> Option<u64> {
        self.last_published_generation_at(u64::MAX)
    }

    #[must_use]
    pub fn last_published_generation_at(&self, generation: u64) -> Option<u64> {
        self.states
            .iter()
            .flat_map(|state| state.state().published_outputs())
            .map(PublishedSliceOutputs::generation)
            .filter(|published_generation| *published_generation <= generation)
            .max()
    }

    #[must_use]
    pub fn durable_for_generation(&self, generation: u64) -> Option<DurableGraphState> {
        let graph = self.generation(generation)?.clone();
        let mut selected = None;
        for state in self.states.iter() {
            if state.state().graph().generation() == generation {
                selected = Some(state.state());
            }
        }
        let published_outputs = selected?
            .published_outputs()
            .filter(|output| output.generation() == generation)
            .cloned()
            .collect();
        Some(DurableGraphState::new(
            graph,
            published_outputs,
            self.durable.as_ref()?.lifecycle(),
        ))
    }

    #[must_use]
    pub fn request(&self, request_id: RequestUuid) -> Option<&MutationReceipt> {
        self.requests.get(&request_id)
    }

    #[must_use]
    pub fn output_request(
        &self,
        connector: &ConnectorKey,
        request_id: RequestUuid,
    ) -> Option<OutputRequestReceipt> {
        self.output_requests
            .get(&OutputRequestKey::new(connector, request_id))
            .cloned()
    }

    #[must_use]
    pub fn publication(
        &self,
        connector: &ConnectorKey,
        publication_id: PublicationUuid,
    ) -> Option<PublicationReceipt> {
        self.publications
            .get(&PublicationKey::new(connector, publication_id))
            .cloned()
    }

    #[must_use]
    pub fn latest_publication(&self, connector: &ConnectorKey) -> Option<&PublicationReceipt> {
        self.publications
            .iter()
            .filter(|receipt| &receipt.connector == connector)
            .max_by_key(|receipt| receipt.publication_sequence)
    }

    #[must_use]
    pub fn is_retired(&self) -> bool {
        self.durable
            .as_ref()
            .is_some_and(|state| state.lifecycle() == GraphLifecycle::Retired)
    }
}

impl IdHashItem for GraphHistory {
    type Key<'a> = GraphUuid;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}
