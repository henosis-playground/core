//! Async Diesel stores for non-log-shaped Henosis metadata.
//!
//! [`DbPool`] owns connection acquisition and serializable transaction
//! boundaries. Datastore traits are implemented for
//! [`diesel_async::AsyncPgConnection`], so their methods can be composed inside
//! one transaction without exposing database rows outside this crate.

mod error;
mod pool;

pub mod datastore;

pub use datastore::AuthMaterialStore;
pub use datastore::CheckpointUpsertError;
pub use datastore::ConnectorCheckpointStore;
pub use datastore::GraphLabelStore;
pub use pool::DbPool;
