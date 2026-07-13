use iddqd::IdOrdMap;
use thiserror::Error;

use crate::domain::Component;
use crate::domain::Graph;
use crate::domain::PublishedSliceOutputs;
use crate::domain::SliceReport;

/// Durable graph lifecycle level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphLifecycle {
    Active,
    Retired,
}

/// Desired graph and durable connector outputs at one stream sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableGraphState {
    graph: Graph,
    published_outputs: IdOrdMap<PublishedSliceOutputs>,
    lifecycle: GraphLifecycle,
}

impl DurableGraphState {
    /// Construct a state while folding a validated graph history.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        graph: Graph,
        published_outputs: IdOrdMap<PublishedSliceOutputs>,
        lifecycle: GraphLifecycle,
    ) -> Self {
        Self {
            graph,
            published_outputs,
            lifecycle,
        }
    }

    #[must_use]
    pub const fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn published_outputs(&self) -> impl ExactSizeIterator<Item = &PublishedSliceOutputs> {
        self.published_outputs.iter()
    }

    #[must_use]
    pub const fn lifecycle(&self) -> GraphLifecycle {
        self.lifecycle
    }

    pub(crate) fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    pub(crate) fn published_outputs_mut(&mut self) -> &mut IdOrdMap<PublishedSliceOutputs> {
        &mut self.published_outputs
    }

    pub(crate) fn retire(&mut self) {
        self.lifecycle = GraphLifecycle::Retired;
    }
}

/// Current durable and volatile state returned to API readers.
#[derive(Clone, Debug)]
pub struct GraphState {
    durable: DurableGraphState,
    reports: IdOrdMap<SliceReport>,
}

/// Historical generation plus the immutable specs it references.
#[derive(Clone, Debug)]
pub struct GraphGenerationState {
    state: GraphState,
    components: Vec<Component>,
    current_lifecycle: GraphLifecycle,
    last_published_generation: Option<u64>,
}

impl GraphGenerationState {
    #[must_use]
    pub fn new(
        state: GraphState,
        components: Vec<Component>,
        current_lifecycle: GraphLifecycle,
        last_published_generation: Option<u64>,
    ) -> Self {
        Self {
            state,
            components,
            current_lifecycle,
            last_published_generation,
        }
    }

    #[must_use]
    pub const fn state(&self) -> &GraphState {
        &self.state
    }

    #[must_use]
    pub fn components(&self) -> &[Component] {
        &self.components
    }

    #[must_use]
    pub const fn current_lifecycle(&self) -> GraphLifecycle {
        self.current_lifecycle
    }

    #[must_use]
    pub const fn last_published_generation(&self) -> Option<u64> {
        self.last_published_generation
    }
}

impl GraphState {
    pub fn new(
        durable: DurableGraphState,
        reports: Vec<SliceReport>,
    ) -> Result<Self, DuplicateConnectorReport> {
        Ok(Self {
            durable,
            reports: IdOrdMap::from_iter_unique(reports).map_err(|_| DuplicateConnectorReport)?,
        })
    }

    #[must_use]
    pub const fn durable(&self) -> &DurableGraphState {
        &self.durable
    }

    pub fn reports(&self) -> impl ExactSizeIterator<Item = &SliceReport> {
        self.reports.iter()
    }
}

/// More than one report targeted the same connector and generation.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("graph state contains more than one report for a connector and generation")]
pub struct DuplicateConnectorReport;
