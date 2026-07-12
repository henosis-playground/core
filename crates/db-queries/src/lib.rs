//! Async Diesel stores for non-log-shaped Henosis metadata.
//!
//! Function names use the singular `{resource}_{operation}` convention. Each
//! store exposes domain types; Diesel row conversion remains in `db-model`.

use std::future::Future;

use diesel::prelude::*;
use diesel::result::DatabaseErrorKind;
use diesel::upsert::excluded;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use faultline::Error as Fault;
use faultline::Never;
use henosis_db_model::DbAuthMaterial;
use henosis_db_model::DbConnectorCheckpoint;
use henosis_db_model::DbGraphLabel;
use henosis_db_schema::auth_material;
use henosis_db_schema::connector_checkpoints;
use henosis_db_schema::graph_labels;
use henosis_types::AuthMaterial;
use henosis_types::ConnectorCheckpoint;
use henosis_types::ConnectorKey;
use henosis_types::GraphId;
use henosis_types::GraphLabel;
use henosis_types::NewConnectorCheckpoint;
use henosis_types::NewGraphLabel;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum CheckpointUpsertError {
    #[error("sequence does not fit PostgreSQL BIGINT")]
    SequenceOutOfRange,
}

#[derive(Clone, Debug)]
pub struct MetadataDb {
    pool: Pool<AsyncPgConnection>,
}

impl MetadataDb {
    /// Connect to a database whose schema has already been migrated by Diesel
    /// CLI.
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let manager = AsyncDieselConnectionManager::<AsyncPgConnection>::new(database_url);
        let pool = Pool::builder().max_size(8).build(manager).await?;
        Ok(Self { pool })
    }
}

pub trait ConnectorCheckpointStore {
    /// Return the current connector delivery checkpoint, if one exists.
    fn connector_checkpoint_get(
        &self,
        graph_id: GraphId,
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

impl ConnectorCheckpointStore for MetadataDb {
    async fn connector_checkpoint_get(
        &self,
        graph_id: GraphId,
        connector: &ConnectorKey,
    ) -> Result<Option<ConnectorCheckpoint>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let mut connection = self.pool.get().await.map_err(pool_transient)?;
        let row = DbConnectorCheckpoint::query()
            .filter(checkpoint_filter(graph_id.as_uuid(), connector.as_str()))
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
        let row =
            DbConnectorCheckpoint::try_from_new(checkpoint).map_err(|_| checkpoint_domain())?;
        let mut connection = self.pool.get().await.map_err(checkpoint_transient)?;
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
        .map_err(diesel_fault_with_domain)?;
        Ok(())
    }
}

pub trait GraphLabelStore {
    /// Create or replace a graph's user-facing label.
    fn graph_label_upsert(
        &self,
        label: NewGraphLabel,
    ) -> impl Future<Output = Result<GraphLabel, Fault<Never, anyhow::Error, anyhow::Error>>> + Send;
}

impl GraphLabelStore for MetadataDb {
    async fn graph_label_upsert(
        &self,
        label: NewGraphLabel,
    ) -> Result<GraphLabel, Fault<Never, anyhow::Error, anyhow::Error>> {
        let row = DbGraphLabel::from(label);
        let mut connection = self.pool.get().await.map_err(pool_transient)?;
        diesel::insert_into(graph_labels::table)
            .values(&row)
            .on_conflict(graph_labels::graph_id)
            .do_update()
            .set(graph_labels::display_label.eq(excluded(graph_labels::display_label)))
            .returning(DbGraphLabel::as_returning())
            .get_result(&mut connection)
            .await
            .map(Into::into)
            .map_err(diesel_fault)
    }
}

pub trait AuthMaterialStore {
    /// Fetch enabled authentication material by administrative key.
    fn auth_material_get(
        &self,
        key: &str,
    ) -> impl Future<
        Output = Result<Option<AuthMaterial>, Fault<Never, anyhow::Error, anyhow::Error>>,
    > + Send;
}

impl AuthMaterialStore for MetadataDb {
    async fn auth_material_get(
        &self,
        key: &str,
    ) -> Result<Option<AuthMaterial>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let mut connection = self.pool.get().await.map_err(pool_transient)?;
        DbAuthMaterial::query()
            .filter(auth_material::key.eq(key))
            .filter(auth_material::enabled.eq(true))
            .first(&mut connection)
            .await
            .optional()
            .map(|row| row.map(Into::into))
            .map_err(diesel_fault)
    }
}

#[diesel::dsl::auto_type(no_type_alias)]
fn checkpoint_filter(graph_id: uuid::Uuid, connector: &str) -> _ {
    connector_checkpoints::graph_id
        .eq(graph_id)
        .and(connector_checkpoints::connector.eq(connector))
}

fn pool_transient(
    error: diesel_async::pooled_connection::bb8::RunError,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Transient(anyhow::Error::new(error))
}

fn diesel_fault(error: diesel::result::Error) -> Fault<Never, anyhow::Error, anyhow::Error> {
    match &error {
        diesel::result::Error::DatabaseError(DatabaseErrorKind::SerializationFailure, _)
        | diesel::result::Error::DatabaseError(DatabaseErrorKind::ClosedConnection, _) => {
            Fault::Transient(anyhow::Error::new(error))
        }
        _ => Fault::Invariant(anyhow::Error::new(error)),
    }
}

fn diesel_fault_with_domain(
    error: diesel::result::Error,
) -> Fault<CheckpointUpsertError, anyhow::Error, anyhow::Error> {
    match diesel_fault(error) {
        Fault::Transient(error) => Fault::Transient(error),
        Fault::Invariant(error) => Fault::Invariant(error),
        Fault::Domain(never) => match never {},
    }
}

fn invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}

fn checkpoint_domain() -> Fault<CheckpointUpsertError, anyhow::Error, anyhow::Error> {
    Fault::Domain(CheckpointUpsertError::SequenceOutOfRange)
}

fn checkpoint_transient(
    error: diesel_async::pooled_connection::bb8::RunError,
) -> Fault<CheckpointUpsertError, anyhow::Error, anyhow::Error> {
    Fault::Transient(anyhow::Error::new(error))
}
