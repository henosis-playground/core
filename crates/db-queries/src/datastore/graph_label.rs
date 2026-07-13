use std::future::Future;

use diesel::prelude::*;
use diesel::upsert::excluded;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use faultline::Error as Fault;
use faultline::Never;
use henosis_db_model::DbGraphLabel;
use henosis_db_schema::graph_labels;
use types::domain::GraphLabel;
use types::domain::NewGraphLabel;

use crate::error::diesel_fault;

/// Persists user-facing graph labels.
pub trait GraphLabelStore {
    /// Create or replace a graph's user-facing label.
    fn graph_label_upsert(
        &self,
        label: NewGraphLabel,
    ) -> impl Future<Output = Result<GraphLabel, Fault<Never, anyhow::Error, anyhow::Error>>> + Send;
}

// === AsyncPgConnection ===

impl GraphLabelStore for AsyncPgConnection {
    async fn graph_label_upsert(
        &self,
        label: NewGraphLabel,
    ) -> Result<GraphLabel, Fault<Never, anyhow::Error, anyhow::Error>> {
        let row = DbGraphLabel::from(label);
        let mut connection = self;
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
