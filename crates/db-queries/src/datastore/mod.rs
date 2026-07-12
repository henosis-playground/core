//! Domain-facing metadata datastore operations.
//!
//! Methods in this module do not acquire connections or start transactions;
//! callers compose them inside [`crate::DbPool::transaction`].

mod auth_material;
mod connector_checkpoint;
mod graph_label;

pub use auth_material::AuthMaterialStore;
pub use connector_checkpoint::CheckpointUpsertError;
pub use connector_checkpoint::ConnectorCheckpointStore;
pub use graph_label::GraphLabelStore;
