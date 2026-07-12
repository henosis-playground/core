use std::future::Future;

use diesel::prelude::*;
use diesel_async::AsyncPgConnection;
use diesel_async::RunQueryDsl;
use faultline::Error as Fault;
use faultline::Never;
use henosis_db_model::DbAuthMaterial;
use henosis_db_schema::auth_material;
use henosis_types::AuthMaterial;

use crate::error::diesel_fault;

/// Reads administrative authentication material.
pub trait AuthMaterialStore {
    /// Fetch enabled authentication material by administrative key.
    fn auth_material_get(
        &self,
        key: &str,
    ) -> impl Future<
        Output = Result<Option<AuthMaterial>, Fault<Never, anyhow::Error, anyhow::Error>>,
    > + Send;
}

// === AsyncPgConnection ===

impl AuthMaterialStore for AsyncPgConnection {
    async fn auth_material_get(
        &self,
        key: &str,
    ) -> Result<Option<AuthMaterial>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let mut connection = self;
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
