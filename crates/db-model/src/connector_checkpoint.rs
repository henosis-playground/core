use diesel::HasQuery;
use diesel::Insertable;
use henosis_db_schema::connector_checkpoints;
use henosis_types as domain;
use thiserror::Error;
use uuid::Uuid;

/// Stored connector delivery checkpoint.
#[derive(Clone, Debug, HasQuery, Insertable, Eq, PartialEq)]
#[diesel(table_name = connector_checkpoints)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct DbConnectorCheckpoint {
    pub graph_id: Uuid,
    pub connector: String,
    pub accepted_sequence: i64,
}

impl DbConnectorCheckpoint {
    pub fn try_from_new(value: domain::NewConnectorCheckpoint) -> Result<Self, SequenceOutOfRange> {
        Ok(Self {
            graph_id: Uuid::from_bytes(value.graph_id.into_bytes()),
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
            domain::GraphUuid::from_bytes(value.graph_id.into_bytes()),
            value
                .connector
                .parse()
                .map_err(|_| InvalidConnectorCheckpoint)?,
            u64::try_from(value.accepted_sequence).map_err(|_| InvalidConnectorCheckpoint)?,
        ))
    }
}

/// A domain sequence cannot be represented by `PostgreSQL` `BIGINT`.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("sequence does not fit PostgreSQL BIGINT")]
pub struct SequenceOutOfRange;

/// A stored connector checkpoint violates domain invariants.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("stored connector checkpoint violates domain invariants")]
pub struct InvalidConnectorCheckpoint;
