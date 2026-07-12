use henosis_types::ConnectorKey;
use iddqd::IdOrdItem;
use iddqd::id_upcast;
use serde::Deserialize;

/// Endpoint and bearer material for one configured connector.
#[derive(Clone, Debug, Deserialize)]
pub struct ConnectorConfig {
    key: ConnectorKey,
    endpoint: String,
    token: String,
}

impl ConnectorConfig {
    #[must_use]
    pub const fn new(key: ConnectorKey, endpoint: String, token: String) -> Self {
        Self {
            key,
            endpoint,
            token,
        }
    }

    #[must_use]
    pub const fn key(&self) -> &ConnectorKey {
        &self.key
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl IdOrdItem for ConnectorConfig {
    type Key<'a> = &'a ConnectorKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.key
    }
}

/// Connector configuration contains the same key more than once.
#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
#[error("connector configuration contains a duplicate key")]
pub struct DuplicateConnector;
