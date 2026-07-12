//! Relational metadata kept outside the append-only graph journal.

mod auth_material;
mod connector_checkpoint;
mod graph_label;

pub use auth_material::AuthMaterial;
pub use connector_checkpoint::ConnectorCheckpoint;
pub use connector_checkpoint::NewConnectorCheckpoint;
pub use graph_label::GraphLabel;
pub use graph_label::NewGraphLabel;
