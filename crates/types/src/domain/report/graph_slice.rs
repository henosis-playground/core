use iddqd::IdOrdMap;
use thiserror::Error;

use crate::domain::Component;
use crate::domain::ComponentOutputs;
use crate::domain::ConnectorKey;
use crate::domain::GraphUuid;

/// Immutable connector-specific view of one graph sequence.
#[derive(Clone, Debug)]
pub struct GraphSlice {
    graph_id: GraphUuid,
    generation: u64,
    connector: ConnectorKey,
    components: IdOrdMap<Component>,
    upstream_outputs: IdOrdMap<ComponentOutputs>,
    sequence: u64,
}

impl GraphSlice {
    pub fn new(
        graph_id: GraphUuid,
        generation: u64,
        connector: ConnectorKey,
        components: Vec<Component>,
        upstream_outputs: Vec<ComponentOutputs>,
        sequence: u64,
    ) -> Result<Self, GraphSliceError> {
        if generation == 0 {
            return Err(GraphSliceError::InvalidGeneration);
        }
        Ok(Self {
            graph_id,
            generation,
            connector,
            components: IdOrdMap::from_iter_unique(components)
                .map_err(|_| GraphSliceError::DuplicateComponent)?,
            upstream_outputs: IdOrdMap::from_iter_unique(upstream_outputs)
                .map_err(|_| GraphSliceError::DuplicateUpstreamOutput)?,
            sequence,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    pub fn components(&self) -> impl ExactSizeIterator<Item = &Component> {
        self.components.iter()
    }

    pub fn upstream_outputs(&self) -> impl ExactSizeIterator<Item = &ComponentOutputs> {
        self.upstream_outputs.iter()
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

/// Invalid connector slice input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GraphSliceError {
    #[error("graph slice generation must be greater than zero")]
    InvalidGeneration,
    #[error("graph slice components must have unique identities")]
    DuplicateComponent,
    #[error("graph slice upstream outputs must have unique component identities")]
    DuplicateUpstreamOutput,
}
