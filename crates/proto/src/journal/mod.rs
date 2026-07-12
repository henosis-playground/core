//! Durable wire encoding and fail-closed stream parsing.
//!
//! This module is the only place that knows both storage protobufs and domain
//! journal events. Parsing is a stream operator; callers fold only validated
//! domain events.

mod decode;
mod encode;

pub use decode::*;
pub use encode::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WireRecord {
    sequence: u64,
    body: Vec<u8>,
}

impl WireRecord {
    #[must_use]
    pub const fn new(sequence: u64, body: Vec<u8>) -> Self {
        Self { sequence, body }
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
