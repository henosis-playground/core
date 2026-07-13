//! Domain types contain no protobuf, database, transport, or storage
//! representation.

/*
Candidate types describe untrusted input. Accepted domain types can only be constructed by the
storage or replay boundary after durable acceptance. Their public constructors are hidden from
documentation and language-server suggestions so normal application code acquires them from the
datastore instead of manufacturing trusted values.
*/

mod command;
mod component;
mod connector;
mod graph;
mod history;
mod metadata;
mod output;
mod report;
mod uuid;

pub use command::*;
pub use component::*;
pub use connector::*;
pub use graph::*;
pub use history::*;
pub use metadata::*;
pub use output::*;
pub use report::*;
pub use uuid::*;
