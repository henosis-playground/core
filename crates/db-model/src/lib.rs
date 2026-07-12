//! Diesel row models for non-log-shaped Henosis metadata.

use diesel::HasQuery;
use diesel::Insertable;
use henosis_db_schema::auth_material;
use henosis_db_schema::connector_checkpoints;
use henosis_db_schema::graph_labels;
use henosis_types as domain;
use thiserror::Error;
use uuid::Uuid;

#[derive(Clone, Debug, HasQuery, Insertable, Eq, PartialEq)]
#[diesel(table_name = connector_checkpoints)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbConnectorCheckpoint {
    pub graph_id: Uuid,
    pub connector: String,
    pub accepted_sequence: i64,
}

impl DbConnectorCheckpoint {
    pub fn try_from_new(
        value: domain::NewConnectorCheckpoint,
    ) -> Result<Self, SequenceOutOfRange> {
        Ok(Self {
            graph_id: value.graph_id.as_uuid(),
            connector: value.connector.to_string(),
            accepted_sequence: i64::try_from(value.accepted_sequence)
                .map_err(|_| SequenceOutOfRange)?,
        })
    }
}

impl TryFrom<DbConnectorCheckpoint> for domain::ConnectorCheckpoint {
    type Error = InvalidConnectorCheckpoint;

    fn try_from(value: DbConnectorCheckpoint) -> Result<Self, Self::Error> {
        Ok(Self::new(
            domain::GraphId::from_uuid(value.graph_id),
            value
                .connector
                .parse()
                .map_err(|_| InvalidConnectorCheckpoint)?,
            u64::try_from(value.accepted_sequence).map_err(|_| InvalidConnectorCheckpoint)?,
        ))
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("sequence does not fit PostgreSQL BIGINT")]
pub struct SequenceOutOfRange;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("stored connector checkpoint violates domain invariants")]
pub struct InvalidConnectorCheckpoint;

#[derive(Clone, Debug, HasQuery, Insertable, Eq, PartialEq)]
#[diesel(table_name = graph_labels)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbGraphLabel {
    pub graph_id: Uuid,
    pub display_label: String,
}

impl From<domain::NewGraphLabel> for DbGraphLabel {
    fn from(value: domain::NewGraphLabel) -> Self {
        Self {
            graph_id: value.graph_id.as_uuid(),
            display_label: value.display_label,
        }
    }
}

impl From<DbGraphLabel> for domain::GraphLabel {
    fn from(value: DbGraphLabel) -> Self {
        Self::new(
            domain::GraphId::from_uuid(value.graph_id),
            value.display_label,
        )
    }
}

#[derive(Clone, Debug, HasQuery, Eq, PartialEq)]
#[diesel(table_name = auth_material)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbAuthMaterial {
    pub key: String,
    pub token_hash: Vec<u8>,
    pub enabled: bool,
}

impl From<DbAuthMaterial> for domain::AuthMaterial {
    fn from(value: DbAuthMaterial) -> Self {
        Self::new(value.key, value.token_hash, value.enabled)
    }
}
