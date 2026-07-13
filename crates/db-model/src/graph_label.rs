use diesel::HasQuery;
use diesel::Insertable;
use henosis_db_schema::graph_labels;
use types::domain;
use uuid::Uuid;

/// Stored user-facing graph label.
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
            graph_id: Uuid::from_bytes(value.graph_id.into_bytes()),
            display_label: value.display_label,
        }
    }
}

impl From<DbGraphLabel> for domain::GraphLabel {
    fn from(value: DbGraphLabel) -> Self {
        Self::new(
            domain::GraphUuid::from_bytes(value.graph_id.into_bytes()),
            value.display_label,
        )
    }
}
