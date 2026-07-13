use buffa::EnumValue;
use buffa::MessageView;
use futures::Stream;
use futures::StreamExt;
use henosis_types as domain;
use thiserror::Error;

use super::WireRecord;
use crate::ConversionError;
use crate::convert::register_component_spec;
use crate::convert::wire_field;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;
use crate::proto::henosis::v1::__buffa::view::oneof;

const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum JournalDecodeError {
    #[error("unsupported durable format version {0}")]
    UnsupportedVersion(u32),
    #[error("invalid durable protobuf: {0}")]
    InvalidProtobuf(String),
    #[error("missing or unknown durable {0}")]
    Missing(&'static str),
    #[error("invalid durable domain value: {0}")]
    InvalidDomain(ConversionError),
    #[error("component spec hash does not match its canonical content")]
    SpecHashMismatch,
}

impl From<ConversionError> for JournalDecodeError {
    fn from(error: ConversionError) -> Self {
        Self::InvalidDomain(error)
    }
}

#[derive(Debug)]
pub enum DecodeStreamError<E> {
    Source(E),
    Invalid {
        sequence: u64,
        error: JournalDecodeError,
    },
}

pub fn decode_graph_stream<S, E>(
    stream: S,
) -> impl Stream<Item = Result<domain::SequencedGraphEvent, DecodeStreamError<E>>>
where
    S: Stream<Item = Result<WireRecord, E>>,
{
    stream.map(|item| parse_stream_item(item, decode_graph_record))
}

pub fn decode_registry_stream<S, E>(
    stream: S,
) -> impl Stream<Item = Result<domain::SequencedRegistryEvent, DecodeStreamError<E>>>
where
    S: Stream<Item = Result<WireRecord, E>>,
{
    stream.map(|item| parse_stream_item(item, decode_registry_record))
}

pub fn decode_spec_stream<S, E>(
    stream: S,
) -> impl Stream<Item = Result<domain::SequencedSpecEvent, DecodeStreamError<E>>>
where
    S: Stream<Item = Result<WireRecord, E>>,
{
    stream.map(|item| parse_stream_item(item, decode_spec_record))
}

fn parse_stream_item<T, E>(
    item: Result<WireRecord, E>,
    parse: impl FnOnce(&WireRecord) -> Result<T, JournalDecodeError>,
) -> Result<T, DecodeStreamError<E>> {
    match item {
        Ok(record) => parse(&record).map_err(|error| DecodeStreamError::Invalid {
            sequence: record.sequence(),
            error,
        }),
        Err(error) => Err(DecodeStreamError::Source(error)),
    }
}

pub fn decode_graph_record(
    record: &WireRecord,
) -> Result<domain::SequencedGraphEvent, JournalDecodeError> {
    let envelope = view::GraphStreamEnvelopeView::decode_view(record.body())
        .map_err(|error| JournalDecodeError::InvalidProtobuf(error.to_string()))?;
    require_version(envelope.format_version)?;
    let version = match envelope.version.as_ref() {
        Some(oneof::graph_stream_envelope::Version::V1(value)) => value,
        None => return Err(JournalDecodeError::Missing("graph record version")),
    };
    let event = match version
        .event
        .as_ref()
        .ok_or(JournalDecodeError::Missing("graph event"))?
    {
        oneof::graph_stream_record_v1::Event::GraphCreated(value) => domain::GraphEvent::Created {
            graph: wire_field!(value.graph).required()?.convert()?,
            request_id: wire_field!(value.request_id).required()?.uuid()?,
            request_fingerprint: wire_field!(value.request_fingerprint)
                .required()?
                .fingerprint()?,
        },
        oneof::graph_stream_record_v1::Event::GenerationAccepted(value) => {
            let mutation_kind = match wire_field!(value.mutation_kind).required()?.into_inner() {
                EnumValue::Known(pb::GraphMutationKindV1::AddComponents) => {
                    domain::MutationKind::AddComponents
                }
                EnumValue::Known(pb::GraphMutationKindV1::UpdateComponents) => {
                    domain::MutationKind::UpdateComponents
                }
                EnumValue::Known(pb::GraphMutationKindV1::RemoveComponents) => {
                    domain::MutationKind::RemoveComponents
                }
                _ => {
                    return Err(wire_field!(value.mutation_kind)
                        .invalid("must be specified")
                        .into());
                }
            };
            domain::GraphEvent::GenerationAccepted {
                graph: wire_field!(value.graph).required()?.convert()?,
                request_id: wire_field!(value.request_id).required()?.uuid()?,
                mutation_kind,
                request_fingerprint: wire_field!(value.request_fingerprint)
                    .required()?
                    .fingerprint()?,
            }
        }
        oneof::graph_stream_record_v1::Event::OutputsPublished(value) => {
            domain::GraphEvent::OutputsPublished(domain::OutputPublication {
                generation: wire_field!(value.generation)
                    .required()?
                    .validate(|generation| *generation > 0, "must be greater than zero")?,
                input_sequence: wire_field!(value.input_sequence).required()?.into_inner(),
                connector: wire_field!(value.connector).required()?.parse()?,
                outputs: wire_field!(value.outputs)
                    .iter()
                    .map(|item| item.convert())
                    .collect::<Result<Vec<_>, _>>()?,
                request_id: wire_field!(value.request_id).required()?.uuid()?,
                request_fingerprint: wire_field!(value.request_fingerprint)
                    .required()?
                    .fingerprint()?,
                publication_id: wire_field!(value.publication_id).required()?.uuid()?,
                publication_fingerprint: wire_field!(value.publication_fingerprint)
                    .required()?
                    .fingerprint()?,
            })
        }
        oneof::graph_stream_record_v1::Event::SliceReported(value) => {
            domain::GraphEvent::SliceReported(domain::RecordedSliceReport {
                report: wire_field!(value.report).required()?.convert()?,
                request_id: wire_field!(value.request_id).required()?.uuid()?,
                request_fingerprint: wire_field!(value.request_fingerprint)
                    .required()?
                    .fingerprint()?,
                publication_id: wire_field!(value.publication_id)
                    .optional()
                    .map(|value| value.uuid())
                    .transpose()
                    .map_err(JournalDecodeError::InvalidDomain)?,
                publication_fingerprint: wire_field!(value.publication_fingerprint)
                    .optional()
                    .map(|value| value.fingerprint())
                    .transpose()
                    .map_err(JournalDecodeError::InvalidDomain)?,
            })
        }
        oneof::graph_stream_record_v1::Event::GraphRetired(value) => domain::GraphEvent::Retired {
            graph_id: wire_field!(value.graph_id).required()?.uuid()?,
            last_generation: wire_field!(value.last_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            request_id: wire_field!(value.request_id).required()?.uuid()?,
            request_fingerprint: wire_field!(value.request_fingerprint)
                .required()?
                .fingerprint()?,
        },
    };
    Ok(domain::SequencedGraphEvent::new(record.sequence(), event))
}

pub fn decode_registry_record(
    record: &WireRecord,
) -> Result<domain::SequencedRegistryEvent, JournalDecodeError> {
    let envelope = view::RegistryStreamEnvelopeView::decode_view(record.body())
        .map_err(|error| JournalDecodeError::InvalidProtobuf(error.to_string()))?;
    require_version(envelope.format_version)?;
    let version = match envelope.version.as_ref() {
        Some(oneof::registry_stream_envelope::Version::V1(value)) => value,
        None => return Err(JournalDecodeError::Missing("registry record version")),
    };
    let event = match version
        .event
        .as_ref()
        .ok_or(JournalDecodeError::Missing("registry event"))?
    {
        oneof::registry_stream_record_v1::Event::GraphCreated(value) => {
            domain::RegistryEvent::Created {
                graph_id: wire_field!(value.graph_id).required()?.uuid()?,
                request_id: wire_field!(value.request_id).required()?.uuid()?,
            }
        }
        oneof::registry_stream_record_v1::Event::GraphRetired(value) => {
            domain::RegistryEvent::Retired {
                graph_id: wire_field!(value.graph_id).required()?.uuid()?,
                request_id: wire_field!(value.request_id).required()?.uuid()?,
            }
        }
    };
    Ok(domain::SequencedRegistryEvent::new(
        record.sequence(),
        event,
    ))
}

pub fn decode_spec_record(
    record: &WireRecord,
) -> Result<domain::SequencedSpecEvent, JournalDecodeError> {
    let envelope = view::SpecStreamEnvelopeView::decode_view(record.body())
        .map_err(|error| JournalDecodeError::InvalidProtobuf(error.to_string()))?;
    require_version(envelope.format_version)?;
    let version = match envelope.version.as_ref() {
        Some(oneof::spec_stream_envelope::Version::V1(value)) => value,
        None => return Err(JournalDecodeError::Missing("spec record version")),
    };
    let oneof::spec_stream_record_v1::Event::ComponentSpecRegistered(value) = version
        .event
        .as_ref()
        .ok_or(JournalDecodeError::Missing("spec event"))?;
    let expected = wire_field!(value.hash).required()?.spec_hash()?;
    let spec = wire_field!(value.spec).required()?.convert()?;
    let registered = register_component_spec(spec);
    if registered.hash() != expected {
        return Err(JournalDecodeError::SpecHashMismatch);
    }
    Ok(domain::SequencedSpecEvent::new(
        record.sequence(),
        registered,
    ))
}

fn require_version(version: Option<u32>) -> Result<(), JournalDecodeError> {
    match version {
        Some(FORMAT_VERSION) => Ok(()),
        Some(other) => Err(JournalDecodeError::UnsupportedVersion(other)),
        None => Err(JournalDecodeError::Missing("format version")),
    }
}

#[cfg(test)]
mod tests {
    use buffa::Message;

    use super::*;

    #[test]
    fn unknown_version_fails_closed() {
        let body = pb::GraphStreamEnvelope {
            format_version: Some(99),
            ..Default::default()
        }
        .encode_to_vec();
        let result = decode_graph_record(&WireRecord::new(0, body));
        assert!(matches!(
            result,
            Err(JournalDecodeError::UnsupportedVersion(99))
        ));
    }
}
