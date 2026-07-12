use std::fmt;
use std::str::FromStr;

use newtype_uuid::GenericUuid;
use newtype_uuid::TypedUuid;
use newtype_uuid::TypedUuidKind;
use newtype_uuid::TypedUuidTag;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

macro_rules! domain_uuid {
    ($kind:ident, $name:ident, $prefix:literal) => {
        #[derive(Debug)]
        enum $kind {}

        impl TypedUuidKind for $kind {
            fn tag() -> TypedUuidTag {
                const TAG: TypedUuidTag = TypedUuidTag::new($prefix);
                TAG
            }
        }

        #[derive(
            Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(TypedUuid<$kind>);

        impl $name {
            #[must_use]
            pub fn from_uuid(value: uuid::Uuid) -> Self {
                Self(TypedUuid::from_untyped_uuid(value))
            }

            #[must_use]
            pub fn as_uuid(self) -> uuid::Uuid {
                *self.0.as_untyped_uuid()
            }

            #[must_use]
            pub fn from_bytes(value: [u8; 16]) -> Self {
                Self::from_uuid(uuid::Uuid::from_bytes(value))
            }

            #[must_use]
            pub fn to_bytes(self) -> [u8; 16] {
                *self.as_uuid().as_bytes()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, formatter)
            }
        }

        impl FromStr for $name {
            type Err = newtype_uuid::ParseError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                value.parse().map(Self)
            }
        }
    };
}

domain_uuid!(GraphKind, GraphId, "graph");
domain_uuid!(RequestKind, RequestId, "request");
domain_uuid!(PublicationKind, PublicationId, "publication");

/// A malformed fixed-width identifier at an external boundary.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
#[error("{field} must contain exactly {expected} bytes")]
pub struct IdentifierBytesError {
    field: &'static str,
    expected: usize,
}

impl IdentifierBytesError {
    #[must_use]
    pub const fn new(field: &'static str, expected: usize) -> Self {
        Self { field, expected }
    }
}

/// The content-addressed identity of a component spec.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ComponentSpecHash([u8; 32]);

impl ComponentSpecHash {
    pub const LENGTH: usize = 32;

    #[must_use]
    pub const fn from_bytes(value: [u8; Self::LENGTH]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; Self::LENGTH] {
        self.0
    }
}

impl fmt::Display for ComponentSpecHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A canonical semantic request or publication digest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Fingerprint([u8; 32]);

impl Fingerprint {
    pub const LENGTH: usize = 32;

    #[must_use]
    pub const fn from_bytes(value: [u8; Self::LENGTH]) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::LENGTH] {
        &self.0
    }
}

pub fn uuid_from_bytes(
    value: &[u8],
    field: &'static str,
) -> Result<uuid::Uuid, IdentifierBytesError> {
    let bytes = <[u8; 16]>::try_from(value).map_err(|_| IdentifierBytesError::new(field, 16))?;
    Ok(uuid::Uuid::from_bytes(bytes))
}

pub fn spec_hash_from_bytes(
    value: &[u8],
    field: &'static str,
) -> Result<ComponentSpecHash, IdentifierBytesError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| IdentifierBytesError::new(field, 32))?;
    Ok(ComponentSpecHash::from_bytes(bytes))
}

pub fn fingerprint_from_bytes(
    value: &[u8],
    field: &'static str,
) -> Result<Fingerprint, IdentifierBytesError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| IdentifierBytesError::new(field, 32))?;
    Ok(Fingerprint::from_bytes(bytes))
}
