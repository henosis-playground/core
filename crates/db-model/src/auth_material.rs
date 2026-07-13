use diesel::HasQuery;
use henosis_db_schema::auth_material;
use types::domain;

/// Stored administrative authentication material.
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
