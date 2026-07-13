//! Committed Henosis protocol bindings.

pub mod api;
pub mod journal;
mod parsing;

#[allow(
    clippy::doc_markdown,
    clippy::semicolon_if_nothing_returned,
    clippy::str_to_string,
    elided_lifetimes_in_paths
)]
#[path = "generated/buffa/mod.rs"]
pub mod proto;

pub use proto::henosis as protobuf;

pub(crate) mod oneof {
    pub(crate) use crate::protobuf::v1::__buffa::view::oneof::*;
}

#[allow(clippy::doc_markdown)]
#[path = "generated/connect/mod.rs"]
pub mod connect;
