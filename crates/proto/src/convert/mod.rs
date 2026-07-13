//! Protobuf/domain boundary conversions.

mod fingerprint;
mod request;
mod value;

use std::fmt;

use henosis_types as domain;
use newtype_uuid::TypedUuid;
use newtype_uuid::TypedUuidKind;
use thiserror::Error;

pub use fingerprint::*;
pub use value::reconcile_slice_request;
pub use value::register_component_spec;
pub use value::retire_slice_request;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConversionError {
    #[error("missing required field {0}")]
    Missing(&'static str),
    #[error("invalid field {field}: {message}")]
    Invalid {
        field: &'static str,
        message: String,
    },
}

pub(crate) const fn missing(field: &'static str) -> ConversionError {
    ConversionError::Missing(field)
}

pub(crate) fn invalid(field: &'static str, error: impl fmt::Display) -> ConversionError {
    ConversionError::Invalid {
        field,
        message: error.to_string(),
    }
}

pub(crate) fn uuid<K: TypedUuidKind>(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<TypedUuid<K>, ConversionError> {
    fixed_bytes(value, field).map(TypedUuid::from_bytes)
}

pub(crate) fn spec_hash(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::ComponentSpecHash, ConversionError> {
    fixed_bytes(value, field).map(domain::ComponentSpecHash::from_bytes)
}

pub(crate) fn fingerprint(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::Fingerprint, ConversionError> {
    fixed_bytes(value, field).map(domain::Fingerprint::from_bytes)
}

fn fixed_bytes<const LENGTH: usize>(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<[u8; LENGTH], ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    value
        .try_into()
        .map_err(|_| invalid(field, format_args!("must contain exactly {LENGTH} bytes")))
}
