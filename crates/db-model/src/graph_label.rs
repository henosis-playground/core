use diesel::HasQuery;
use diesel::Insertable;
use henosis_db_schema::graph_labels;
use henosis_types as domain;
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
