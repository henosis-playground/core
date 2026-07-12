use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use crate::ConnectorKey;
use crate::DurableGraphState;
use crate::Fingerprint;
use crate::Graph;
use crate::GraphId;
use crate::GraphLifecycle;
use crate::OutputPublication;
use crate::PublicationId;
use crate::PublishedSliceOutputs;
use crate::RequestId;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Create,
    AddComponents,
    UpdateComponents,
    RemoveComponents,
    Retire,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationResponse {
    Graph(Graph),
    Retired {
        graph_id: GraphId,
        last_generation: u64,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationReceipt {
    request_id: RequestId,
    kind: MutationKind,
    fingerprint: Fingerprint,
    response: MutationResponse,
}

impl MutationReceipt {
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    #[must_use]
    pub const fn kind(&self) -> MutationKind {
        self.kind
    }

    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn response(&self) -> &MutationResponse {
        &self.response
    }
}

impl IdHashItem for MutationReceipt {
    type Key<'a> = RequestId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.request_id
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OutputRequestKey<'a> {
    connector: &'a ConnectorKey,
    request_id: RequestId,
}

impl<'a> OutputRequestKey<'a> {
    #[must_use]
    pub const fn new(connector: &'a ConnectorKey, request_id: RequestId) -> Self {
        Self {
            connector,
            request_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRequestReceipt {
    connector: ConnectorKey,
    request_id: RequestId,
    fingerprint: Fingerprint,
    publication_sequence: u64,
}

impl OutputRequestReceipt {
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }
}

impl IdHashItem for OutputRequestReceipt {
    type Key<'a> = OutputRequestKey<'a>;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        OutputRequestKey::new(&self.connector, self.request_id)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PublicationKey<'a> {
    connector: &'a ConnectorKey,
    publication_id: PublicationId,
}

impl<'a> PublicationKey<'a> {
    #[must_use]
    pub const fn new(connector: &'a ConnectorKey, publication_id: PublicationId) -> Self {
        Self {
            connector,
            publication_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationReceipt {
    connector: ConnectorKey,
    publication_id: PublicationId,
    fingerprint: Fingerprint,
    publication_sequence: u64,
}

impl PublicationReceipt {
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }
}

impl IdHashItem for PublicationReceipt {
    type Key<'a> = PublicationKey<'a>;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        PublicationKey::new(&self.connector, self.publication_id)
    }
}

#[derive(Clone, Debug)]
pub enum GraphEvent {
    Created {
        graph: Graph,
        request_id: RequestId,
        request_fingerprint: Fingerprint,
    },
    GenerationAccepted {
        graph: Graph,
        request_id: RequestId,
        mutation_kind: MutationKind,
        request_fingerprint: Fingerprint,
    },
    OutputsPublished(OutputPublication),
    Retired {
        graph_id: GraphId,
        last_generation: u64,
        request_id: RequestId,
        request_fingerprint: Fingerprint,
    },
}

#[derive(Clone, Copy, Debug)]
pub enum RegistryEvent {
    Created {
        graph_id: GraphId,
        request_id: RequestId,
    },
    Retired {
        graph_id: GraphId,
        request_id: RequestId,
    },
}

#[derive(Clone, Debug)]
pub struct SequencedGraphEvent {
    sequence: u64,
    event: GraphEvent,
}

impl SequencedGraphEvent {
    #[must_use]
    pub const fn new(sequence: u64, event: GraphEvent) -> Self {
        Self { sequence, event }
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn event(&self) -> &GraphEvent {
        &self.event
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SequencedRegistryEvent {
    sequence: u64,
    event: RegistryEvent,
}

impl SequencedRegistryEvent {
    #[must_use]
    pub const fn new(sequence: u64, event: RegistryEvent) -> Self {
        Self { sequence, event }
    }

    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn event(self) -> RegistryEvent {
        self.event
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequencedGraphState {
    sequence: u64,
    state: DurableGraphState,
}

impl SequencedGraphState {
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn state(&self) -> &DurableGraphState {
        &self.state
    }
}

impl IdOrdItem for SequencedGraphState {
    type Key<'a> = u64;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.sequence
    }
}

#[derive(Clone, Debug)]
pub struct GraphHistory {
    graph_id: GraphId,
    head_sequence: Option<u64>,
    durable: Option<DurableGraphState>,
    generations: IdOrdMap<Graph>,
    states: IdOrdMap<SequencedGraphState>,
    requests: IdHashMap<MutationReceipt>,
    output_requests: IdHashMap<OutputRequestReceipt>,
    publications: IdHashMap<PublicationReceipt>,
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum HistoryError {
    #[error("graph stream sequence is not contiguous")]
    NonContiguous,
    #[error("graph creation is not the first record")]
    CreationOrder,
    #[error("graph event identity does not match its stream")]
    GraphIdentity,
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
    #[must_use]
    pub fn new(graph_id: GraphId) -> Self {
        Self {
            graph_id,
            head_sequence: None,
            durable: None,
            generations: IdOrdMap::new(),
            states: IdOrdMap::new(),
            requests: IdHashMap::new(),
            output_requests: IdHashMap::new(),
            publications: IdHashMap::new(),
        }
    }

    /// Fold one already-parsed domain event into this history.
    pub fn apply(&mut self, record: SequencedGraphEvent) -> Result<(), HistoryError> {
        let expected = self
            .head_sequence
            .map(|sequence| sequence.saturating_add(1))
            .unwrap_or(0);
        if record.sequence != expected {
            return Err(HistoryError::NonContiguous);
        }
        match record.event {
            GraphEvent::Created {
                graph,
                request_id,
                request_fingerprint,
            } => self.apply_created(graph, request_id, request_fingerprint)?,
            GraphEvent::GenerationAccepted {
                graph,
                request_id,
                mutation_kind,
                request_fingerprint,
            } => self.apply_generation(graph, request_id, mutation_kind, request_fingerprint)?,
            GraphEvent::OutputsPublished(publication) => {
                self.apply_outputs(record.sequence, publication)?;
            }
            GraphEvent::Retired {
                graph_id,
                last_generation,
                request_id,
                request_fingerprint,
            } => self.apply_retired(graph_id, last_generation, request_id, request_fingerprint)?,
        }
        self.head_sequence = Some(record.sequence);
        let state = SequencedGraphState {
            sequence: record.sequence,
            state: self.durable()?.clone(),
        };
        self.states
            .insert_unique(state)
            .map_err(|_| HistoryError::NonContiguous)?;
        Ok(())
    }

    fn apply_created(
        &mut self,
        graph: Graph,
        request_id: RequestId,
        fingerprint: Fingerprint,
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
                fingerprint,
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
        request_id: RequestId,
        mutation_kind: MutationKind,
        fingerprint: Fingerprint,
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
                fingerprint,
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
                fingerprint: publication.request_fingerprint,
                publication_sequence: sequence,
            })
            .map_err(|_| HistoryError::DuplicateOutputRequest)?;
        self.publications
            .insert_unique(PublicationReceipt {
                connector: publication.connector.clone(),
                publication_id: publication.publication_id,
                fingerprint: publication.publication_fingerprint,
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
        graph_id: GraphId,
        last_generation: u64,
        request_id: RequestId,
        fingerprint: Fingerprint,
    ) -> Result<(), HistoryError> {
        self.require_active()?;
        if graph_id != self.graph_id {
            return Err(HistoryError::GraphIdentity);
        }
        if last_generation != self.durable()?.graph().generation() {
            return Err(HistoryError::RetirementGeneration);
        }
        self.requests
            .insert_unique(MutationReceipt {
                request_id,
                kind: MutationKind::Retire,
                fingerprint,
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
            Err(HistoryError::GraphIdentity)
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

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn head_sequence(&self) -> Option<u64> {
        self.head_sequence
    }

    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.head_sequence
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

    #[must_use]
    pub fn request(&self, request_id: RequestId) -> Option<&MutationReceipt> {
        self.requests.get(&request_id)
    }

    #[must_use]
    pub fn output_request(
        &self,
        connector: &ConnectorKey,
        request_id: RequestId,
    ) -> Option<OutputRequestReceipt> {
        self.output_requests
            .get(&OutputRequestKey::new(connector, request_id))
            .cloned()
    }

    #[must_use]
    pub fn publication(
        &self,
        connector: &ConnectorKey,
        publication_id: PublicationId,
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
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryGraph {
    graph_id: GraphId,
    retired: bool,
}

impl IdOrdItem for RegistryGraph {
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}

#[derive(Clone, Debug, Default)]
pub struct RegistryHistory {
    graphs: IdOrdMap<RegistryGraph>,
    next_sequence: u64,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum RegistryHistoryError {
    #[error("registry stream sequence is not contiguous")]
    NonContiguous,
    #[error("registry contains duplicate graph creation")]
    DuplicateCreation,
    #[error("registry retires a graph that is not active")]
    InvalidRetirement,
}

impl RegistryHistory {
    pub fn apply(&mut self, record: SequencedRegistryEvent) -> Result<(), RegistryHistoryError> {
        if record.sequence() != self.next_sequence {
            return Err(RegistryHistoryError::NonContiguous);
        }
        match record.event() {
            RegistryEvent::Created { graph_id, .. } => self
                .graphs
                .insert_unique(RegistryGraph {
                    graph_id,
                    retired: false,
                })
                .map_err(|_| RegistryHistoryError::DuplicateCreation)?,
            RegistryEvent::Retired { graph_id, .. } => {
                let mut graph = self
                    .graphs
                    .get_mut(&graph_id)
                    .filter(|graph| !graph.retired)
                    .ok_or(RegistryHistoryError::InvalidRetirement)?;
                graph.retired = true;
            }
        }
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    pub fn graph_ids(&self) -> impl ExactSizeIterator<Item = GraphId> + '_ {
        self.graphs.iter().map(|graph| graph.graph_id)
    }

    #[must_use]
    pub fn retirement(&self, graph_id: GraphId) -> Option<bool> {
        self.graphs.get(&graph_id).map(|graph| graph.retired)
    }
}
