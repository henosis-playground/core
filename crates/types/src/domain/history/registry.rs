use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use crate::domain::GraphUuid;
use crate::domain::RegistryEvent;
use crate::domain::SequencedRegistryEvent;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Discovery-registry state for one graph.
pub struct RegistryGraph {
    graph_id: GraphUuid,
    retired: bool,
}

impl RegistryGraph {
    #[must_use]
    pub const fn graph_id(self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn retired(self) -> bool {
        self.retired
    }
}

impl IdOrdItem for RegistryGraph {
    type Key<'a> = GraphUuid;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}

/// Folded graph discovery registry.
#[derive(Clone, Debug, Default)]
pub struct RegistryHistory {
    graphs: IdOrdMap<RegistryGraph>,
    next_sequence: u64,
}

/// Invalid registry-stream history.
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

    pub fn graph_ids(&self) -> impl ExactSizeIterator<Item = GraphUuid> + '_ {
        self.graphs.iter().map(|graph| graph.graph_id)
    }

    #[must_use]
    pub fn retirement(&self, graph_id: GraphUuid) -> Option<bool> {
        self.graphs.get(&graph_id).map(|graph| graph.retired)
    }
}
