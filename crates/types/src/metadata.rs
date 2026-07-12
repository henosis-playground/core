use crate::ConnectorKey;
use crate::GraphId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorCheckpoint {
    graph_id: GraphId,
    connector: ConnectorKey,
    accepted_sequence: u64,
}

impl ConnectorCheckpoint {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        connector: ConnectorKey,
        accepted_sequence: u64,
    ) -> Self {
        Self {
            graph_id,
            connector,
            accepted_sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewConnectorCheckpoint {
    pub graph_id: GraphId,
    pub connector: ConnectorKey,
    pub accepted_sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLabel {
    graph_id: GraphId,
    display_label: String,
}

impl GraphLabel {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(graph_id: GraphId, display_label: String) -> Self {
        Self {
            graph_id,
            display_label,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub fn display_label(&self) -> &str {
        &self.display_label
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewGraphLabel {
    pub graph_id: GraphId,
    pub display_label: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthMaterial {
    key: String,
    token_hash: Vec<u8>,
    enabled: bool,
}

impl AuthMaterial {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(key: String, token_hash: Vec<u8>, enabled: bool) -> Self {
        Self {
            key,
            token_hash,
            enabled,
        }
    }

    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    #[must_use]
    pub fn token_hash(&self) -> &[u8] {
        &self.token_hash
    }

    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }
}
