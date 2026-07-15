//! Committed `buffa` and `ConnectRPC` bindings for the graph-facing API.

#![allow(
    elided_lifetimes_in_paths,
    reason = "generated buffa views omit explicit lifetimes"
)]

#[path = "generated/buffa/mod.rs"]
pub mod proto;

#[path = "generated/connect/mod.rs"]
pub mod connect;
