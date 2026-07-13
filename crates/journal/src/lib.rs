//! S2-backed durable stores for graph, registry, and component streams.
//!
//! Wire records are parsed by `henosis-proto` into validated domain events.
//! Domain histories own all folding rules and never observe protobuf values.

mod client;
mod component;
mod error;
mod graph;
mod registry;
mod stream;

use s2_sdk::S2Basin;

pub use error::JournalError;

/// Durable Henosis journal backed by one S2 basin.
#[derive(Clone, Debug)]
pub struct Journal {
    basin: S2Basin,
}
