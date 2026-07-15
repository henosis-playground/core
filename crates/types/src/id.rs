use std::fmt;
use std::str::FromStr;

use newtype_uuid::TypedUuid;
use newtype_uuid::TypedUuidKind;
use newtype_uuid::TypedUuidTag;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;
use uuid::Timestamp;

macro_rules! domain_id {
    ($name:ident, $kind:ident, $tag:literal) => {
        #[derive(Debug)]
        enum $kind {}

        impl TypedUuidKind for $kind {
            fn tag() -> TypedUuidTag {
                const TAG: TypedUuidTag = TypedUuidTag::new($tag);
                TAG
            }
        }

        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(TypedUuid<$kind>);

        impl $name {
            #[must_use]
            pub fn new_v7(timestamp: Timestamp) -> Self {
                Self(TypedUuid::new_v7(timestamp))
            }

            #[doc(hidden)]
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(TypedUuid::from_bytes(bytes))
            }

            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                self.0.into_bytes()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = ParseDomainIdError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value
                    .parse::<TypedUuid<$kind>>()
                    .map(Self)
                    .map_err(|error| ParseDomainIdError(error.to_string()))
            }
        }
    };
}

domain_id!(GraphId, GraphKind, "graph");
domain_id!(ResourceId, ResourceKind, "resource");
domain_id!(PublicationId, PublicationKind, "publication");

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("invalid TypeID: {0}")]
pub struct ParseDomainIdError(String);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Generation(u64);

impl Generation {
    pub fn new(ordinal: u64) -> Result<Self, GenerationError> {
        if ordinal == 0 {
            return Err(GenerationError);
        }
        Ok(Self(ordinal))
    }

    #[must_use]
    pub const fn ordinal(self) -> u64 {
        self.0
    }

    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0.checked_add(1).expect("generation ordinal overflow"))
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("generation ordinal must be greater than zero")]
pub struct GenerationError;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentDigest([u8; 32]);

impl ContentDigest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    #[must_use]
    pub fn digest(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}
