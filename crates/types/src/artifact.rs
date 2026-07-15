use std::sync::Arc;

use futures::future::BoxFuture;
use thiserror::Error;

use crate::BundleRef;

/// Verified access to one native file in a content-addressed component closure.
///
/// Implementations must resolve `path` inside `bundle`'s manifest, read exactly
/// that entry from the shared bundle store, and verify its declared SHA-256
/// before returning bytes. Controllers must never fall back to a checkout path.
pub trait BundleArtifactReader: Send + Sync {
    fn read<'a>(
        &'a self,
        bundle: BundleRef,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Arc<[u8]>, BundleArtifactError>>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum BundleArtifactError {
    #[error("bundle {bundle} does not contain native file {path:?}")]
    Missing { bundle: BundleRef, path: String },
    #[error("bundle {bundle} manifest is invalid: {message}")]
    InvalidManifest { bundle: BundleRef, message: String },
    #[error(
        "bundle {bundle} native file {path:?} failed digest verification: expected {expected}, got {actual}"
    )]
    DigestMismatch {
        bundle: BundleRef,
        path: String,
        expected: String,
        actual: String,
    },
    #[error("cannot read bundle {bundle} native file {path:?}: {message}")]
    Unavailable {
        bundle: BundleRef,
        path: String,
        message: String,
    },
}
