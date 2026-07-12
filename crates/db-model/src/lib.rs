//! Diesel row models for non-log-shaped Henosis metadata.
//!
//! Each module owns one table's row types and its conversions to or from the
//! domain crate.

mod auth_material;
mod connector_checkpoint;
mod graph_label;

pub use auth_material::DbAuthMaterial;
pub use connector_checkpoint::DbConnectorCheckpoint;
pub use connector_checkpoint::InvalidConnectorCheckpoint;
pub use connector_checkpoint::SequenceOutOfRange;
pub use graph_label::DbGraphLabel;
