use thiserror::Error;

/// Domain failures exposed by graph-stream operations.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum JournalError {
    /// The graph stream does not exist or contains no records.
    #[error("graph does not exist")]
    NotFound,
    /// Another writer advanced the stream before this append.
    #[error("journal compare-and-append failed; current tail is {current_tail}")]
    CasConflict { current_tail: u64 },
    /// A component identity already names a different immutable specification.
    #[error("component identity is already registered with another specification")]
    ComponentConflict,
}
