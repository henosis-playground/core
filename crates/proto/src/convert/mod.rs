//! Protobuf/domain boundary conversions.

mod fingerprint;
mod request;
mod value;

use std::fmt;

use henosis_types as domain;
use thiserror::Error;

pub use fingerprint::*;
pub use value::register_component_spec;

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

pub(crate) fn graph_id(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::GraphId, ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    domain::uuid_from_bytes(value, field)
        .map(domain::GraphId::from_uuid)
        .map_err(|error| invalid(field, error))
}

pub(crate) fn request_id(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::RequestId, ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    domain::uuid_from_bytes(value, field)
        .map(domain::RequestId::from_uuid)
        .map_err(|error| invalid(field, error))
}

pub(crate) fn publication_id(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::PublicationId, ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    domain::uuid_from_bytes(value, field)
        .map(domain::PublicationId::from_uuid)
        .map_err(|error| invalid(field, error))
}

pub(crate) fn spec_hash(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::ComponentSpecHash, ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    domain::spec_hash_from_bytes(value, field).map_err(|error| invalid(field, error))
}

pub(crate) fn fingerprint(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<domain::Fingerprint, ConversionError> {
    let value = value.ok_or_else(|| missing(field))?;
    domain::fingerprint_from_bytes(value, field).map_err(|error| invalid(field, error))
}
