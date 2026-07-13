use std::future::Future;

use diesel::prelude::*;
use diesel::upsert::excluded;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use faultline::Error as Fault;
use faultline::Never;
use henosis_db_model::DbConnectorCheckpoint;
use henosis_db_schema::connector_checkpoints;
use henosis_types::ConnectorCheckpoint;
use henosis_types::ConnectorKey;
use henosis_types::GraphUuid;
use henosis_types::NewConnectorCheckpoint;
use thiserror::Error;
use uuid::Uuid;

use crate::error::diesel_fault;
use crate::error::invariant;

/// Domain failure while advancing a connector checkpoint.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CheckpointUpsertError {
    /// The S2 sequence cannot be represented by `PostgreSQL` `BIGINT`.
    #[error("sequence does not fit PostgreSQL BIGINT")]
    SequenceOutOfRange,
}

/// Persists monotonic connector delivery checkpoints.
pub trait ConnectorCheckpointStore {
    /// Return the current connector delivery checkpoint, if one exists.
    fn connector_checkpoint_get(
        &self,
        graph_id: GraphUuid,
        connector: &ConnectorKey,
    ) -> impl Future<
        Output = Result<Option<ConnectorCheckpoint>, Fault<Never, anyhow::Error, anyhow::Error>>,
    > + Send;

    /// Advance a checkpoint monotonically; an older resend cannot regress it.
    fn connector_checkpoint_upsert(
        &self,
        checkpoint: NewConnectorCheckpoint,
    ) -> impl Future<Output = Result<(), Fault<CheckpointUpsertError, anyhow::Error, anyhow::Error>>>
    + Send;
}

// === AsyncPgConnection ===

impl ConnectorCheckpointStore for AsyncPgConnection {
    async fn connector_checkpoint_get(
        &self,
        graph_id: GraphUuid,
        connector: &ConnectorKey,
    ) -> Result<Option<ConnectorCheckpoint>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let mut connection = self;
        let row = DbConnectorCheckpoint::query()
            .filter(checkpoint_filter(
                Uuid::from_bytes(graph_id.into_bytes()),
                connector.as_str(),
            ))
            .first::<DbConnectorCheckpoint>(&mut connection)
            .await
            .optional()
            .map_err(diesel_fault)?;
        row.map(TryInto::try_into).transpose().map_err(invariant)
    }

    async fn connector_checkpoint_upsert(
        &self,
        checkpoint: NewConnectorCheckpoint,
    ) -> Result<(), Fault<CheckpointUpsertError, anyhow::Error, anyhow::Error>> {
        let row = DbConnectorCheckpoint::try_from_new(checkpoint).map_err(|_| {
            Fault::<CheckpointUpsertError, anyhow::Error, anyhow::Error>::Domain(
                CheckpointUpsertError::SequenceOutOfRange,
            )
        })?;
        let mut connection = self;
        let upsert = diesel::insert_into(connector_checkpoints::table)
            .values(&row)
            .on_conflict((
                connector_checkpoints::graph_id,
                connector_checkpoints::connector,
            ))
            .do_update()
            .set(
                connector_checkpoints::accepted_sequence
                    .eq(excluded(connector_checkpoints::accepted_sequence)),
            );
        diesel::query_dsl::methods::FilterDsl::filter(
            upsert,
            connector_checkpoints::accepted_sequence
                .lt(excluded(connector_checkpoints::accepted_sequence)),
        )
        .execute(&mut connection)
        .await
        .map_err(|error| diesel_fault(error).squash())?;
        Ok(())
    }
}

#[diesel::dsl::auto_type(no_type_alias)]
fn checkpoint_filter(graph_id: Uuid, connector: &str) -> _ {
    connector_checkpoints::graph_id
        .eq(graph_id)
        .and(connector_checkpoints::connector.eq(connector))
}
