use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use thiserror::Error;

use crate::BundleRef;

// === CONFIG CLOSURE ===

/// Verified access to one configuration file in a content-addressed component
/// evaluation closure.
///
/// Implementations must resolve `path` inside `bundle`'s manifest, read exactly
/// that entry from the shared bundle store, and verify its declared SHA-256
/// before returning bytes. Controllers must never fall back to a checkout path.
pub trait ConfigClosureReader: Send + Sync {
    fn read<'a>(
        &'a self,
        bundle: BundleRef,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Arc<[u8]>, ConfigClosureError>>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigClosureError {
    #[error("bundle {bundle} does not contain configuration file {path:?}")]
    Missing { bundle: BundleRef, path: String },
    #[error("bundle {bundle} manifest is invalid: {message}")]
    InvalidManifest { bundle: BundleRef, message: String },
    #[error(
        "bundle {bundle} configuration file {path:?} failed digest verification: expected \
         {expected}, got {actual}"
    )]
    DigestMismatch {
        bundle: BundleRef,
        path: String,
        expected: String,
        actual: String,
    },
    #[error("cannot read bundle {bundle} configuration file {path:?}: {message}")]
    Unavailable {
        bundle: BundleRef,
        path: String,
        message: String,
    },
}

// === WORKLOAD ARTIFACT STORE ===

/// SHA-256 identity of workload bytes held outside component bundles and core.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ArtifactDigest([u8; 32]);

impl ArtifactDigest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Display for ArtifactDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("sha256:")?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ArtifactDigest {
    type Err = ArtifactDigestError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hexadecimal = value.strip_prefix("sha256:").ok_or(ArtifactDigestError)?;
        if hexadecimal.len() != 64
            || !hexadecimal
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ArtifactDigestError);
        }
        let bytes = hex::decode(hexadecimal).map_err(|_| ArtifactDigestError)?;
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| ArtifactDigestError)?;
        Ok(Self(bytes))
    }
}

impl Serialize for ArtifactDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ArtifactDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("artifact digest must be sha256 followed by 64 lowercase hexadecimal digits")]
pub struct ArtifactDigestError;

/// Read side of the shared content-addressed workload artifact store.
///
/// Implementations fetch exactly the object named by `digest`, recompute its
/// SHA-256, and fail closed on missing bytes or mismatch. Core and controllers
/// treat returned bytes as opaque.
pub trait ArtifactStore: Send + Sync {
    fn fetch(&self, digest: ArtifactDigest)
    -> BoxFuture<'_, Result<Arc<[u8]>, ArtifactStoreError>>;
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ArtifactStoreError {
    #[error("workload artifact {digest} is missing from the artifact store")]
    Missing { digest: ArtifactDigest },
    #[error(
        "workload artifact {digest} failed digest verification: fetched bytes hash to {actual}"
    )]
    DigestMismatch {
        digest: ArtifactDigest,
        actual: ArtifactDigest,
    },
    #[error("cannot fetch workload artifact {digest}: {message}")]
    Unavailable {
        digest: ArtifactDigest,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::ArtifactDigest;

    #[test]
    fn artifact_digest_round_trips_as_prefixed_lowercase_sha256() {
        let text = format!("sha256:{}", "ab".repeat(32));
        let digest: ArtifactDigest = text.parse().unwrap();
        assert_eq!(digest.to_string(), text);
        assert_eq!(serde_json::to_string(&digest).unwrap(), format!("{text:?}"));
        assert_eq!(
            serde_json::from_str::<ArtifactDigest>(&format!("{text:?}")).unwrap(),
            digest
        );
    }

    #[test]
    fn artifact_digest_rejects_noncanonical_text() {
        assert!("ab".repeat(32).parse::<ArtifactDigest>().is_err());
        assert!(
            format!("sha256:{}", "AB".repeat(32))
                .parse::<ArtifactDigest>()
                .is_err()
        );
        assert!(
            format!("sha256:{}", "ab".repeat(31))
                .parse::<ArtifactDigest>()
                .is_err()
        );
    }
}
