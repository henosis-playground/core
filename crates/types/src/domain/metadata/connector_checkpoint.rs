use crate::domain::ConnectorKey;
use crate::domain::GraphUuid;

/// Last graph sequence accepted by a connector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCheckpoint {
    graph_id: GraphUuid,
    connector: ConnectorKey,
    accepted_sequence: u64,
}

impl ConnectorCheckpoint {
    /// Construct a value loaded and validated by the datastore boundary.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(graph_id: GraphUuid, connector: ConnectorKey, accepted_sequence: u64) -> Self {
        Self {
            graph_id,
            connector,
            accepted_sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub const fn accepted_sequence(&self) -> u64 {
        self.accepted_sequence
    }
}

/// Input used to persist a connector checkpoint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewConnectorCheckpoint {
    pub graph_id: GraphUuid,
    pub connector: ConnectorKey,
    pub accepted_sequence: u64,
}
