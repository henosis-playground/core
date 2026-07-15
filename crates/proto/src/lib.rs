//! Committed `buffa` and `ConnectRPC` bindings for the graph-facing API.

#![allow(
    elided_lifetimes_in_paths,
    clippy::doc_markdown,
    clippy::inefficient_to_string,
    clippy::semicolon_if_nothing_returned,
    clippy::str_to_string,
    reason = "generated buffa/connect code is committed verbatim"
)]

#[path = "generated/buffa/mod.rs"]
pub mod proto;

#[path = "generated/connect/mod.rs"]
pub mod connect;
