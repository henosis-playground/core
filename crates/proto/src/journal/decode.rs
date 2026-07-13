use std::time::Duration;
use std::time::UNIX_EPOCH;

use buffa::EnumValue;
use buffa::MessageView;
use futures::Stream;
use futures::StreamExt;
use thiserror::Error;
use types::domain;

use super::WireRecord;
use crate::oneof;
use crate::parsing::ConversionError;
use crate::parsing::field;
use crate::protobuf;

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
    #[error("durable timestamp is outside the supported system-time range")]
    InvalidTimestamp,
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
) -> impl Stream<Item = Result<domain::ComponentSpecEvent, DecodeStreamError<E>>>
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
    let envelope = protobuf::v1::GraphStreamEnvelopeView::decode_view(record.body())
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
            graph: field!(value.graph).required()?.convert()?,
            request_id: field!(value.request_id).required()?.uuid()?,
            request_hash: field!(value.request_hash).required()?.hash()?,
        },
        oneof::graph_stream_record_v1::Event::GenerationAccepted(value) => {
            let mutation_kind = match field!(value.mutation_kind).required()?.into_inner() {
                EnumValue::Known(protobuf::v1::GraphMutationKindV1::AddComponents) => {
                    domain::MutationKind::AddComponents
                }
                EnumValue::Known(protobuf::v1::GraphMutationKindV1::UpdateComponents) => {
                    domain::MutationKind::UpdateComponents
                }
                EnumValue::Known(protobuf::v1::GraphMutationKindV1::RemoveComponents) => {
                    domain::MutationKind::RemoveComponents
                }
                _ => {
                    return Err(field!(value.mutation_kind)
                        .invalid("must be specified")
                        .into());
                }
            };
            domain::GraphEvent::GenerationAccepted {
                graph: field!(value.graph).required()?.convert()?,
                request_id: field!(value.request_id).required()?.uuid()?,
                mutation_kind,
                request_hash: field!(value.request_hash).required()?.hash()?,
            }
        }
        oneof::graph_stream_record_v1::Event::OutputsPublished(value) => {
            domain::GraphEvent::OutputsPublished(domain::OutputPublication {
                generation: field!(value.generation)
                    .required()?
                    .validate(|generation| *generation > 0, "must be greater than zero")?,
                input_sequence: field!(value.input_sequence).required()?.into_inner(),
                connector: field!(value.connector).required()?.parse()?,
                outputs: field!(value.outputs)
                    .iter()
                    .map(|item| item.convert())
                    .collect::<Result<Vec<_>, _>>()?,
                request_id: field!(value.request_id).required()?.uuid()?,
                request_hash: field!(value.request_hash).required()?.hash()?,
                publication_id: field!(value.publication_id).required()?.uuid()?,
                publication_hash: field!(value.publication_hash).required()?.hash()?,
            })
        }
        oneof::graph_stream_record_v1::Event::SliceReported(value) => {
            domain::GraphEvent::SliceReported(domain::RecordedSliceReport {
                report: field!(value.report).required()?.convert()?,
                request_id: field!(value.request_id).required()?.uuid()?,
                request_hash: field!(value.request_hash).required()?.hash()?,
                publication_id: field!(value.publication_id)
                    .optional()
                    .map(|value| value.uuid())
                    .transpose()
                    .map_err(JournalDecodeError::InvalidDomain)?,
                publication_hash: field!(value.publication_hash)
                    .optional()
                    .map(|value| value.hash())
                    .transpose()
                    .map_err(JournalDecodeError::InvalidDomain)?,
            })
        }
        oneof::graph_stream_record_v1::Event::GraphRetired(value) => domain::GraphEvent::Retired {
            graph_id: field!(value.graph_id).required()?.uuid()?,
            last_generation: field!(value.last_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            request_id: field!(value.request_id).required()?.uuid()?,
            request_hash: field!(value.request_hash).required()?.hash()?,
        },
    };
    Ok(domain::SequencedGraphEvent::new(record.sequence(), event))
}

pub fn decode_registry_record(
    record: &WireRecord,
) -> Result<domain::SequencedRegistryEvent, JournalDecodeError> {
    let envelope = protobuf::v1::RegistryStreamEnvelopeView::decode_view(record.body())
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
                graph_id: field!(value.graph_id).required()?.uuid()?,
                request_id: field!(value.request_id).required()?.uuid()?,
            }
        }
        oneof::registry_stream_record_v1::Event::GraphRetired(value) => {
            domain::RegistryEvent::Retired {
                graph_id: field!(value.graph_id).required()?.uuid()?,
                request_id: field!(value.request_id).required()?.uuid()?,
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
) -> Result<domain::ComponentSpecEvent, JournalDecodeError> {
    let envelope = protobuf::v1::SpecStreamEnvelopeView::decode_view(record.body())
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
    let component_id = field!(value.component_id).required()?.uuid()?;
    let value = field!(value.spec).required()?.into_inner();
    let spec = domain::NewComponentSpec::new(
        field!(value.name).required()?.into_inner(),
        field!(value.connector).required()?.parse()?,
        field!(value.outputs_schema).or_default().into_inner(),
        field!(value.depends_on_component_ids)
            .iter()
            .map(|item| item.component_id())
            .collect::<Result<Vec<_>, _>>()?,
        field!(value.connector_context).or_default().into_inner(),
    )
    .map_err(ConversionError::from)?;
    let time_recorded = UNIX_EPOCH
        .checked_add(Duration::from_millis(record.timestamp()))
        .ok_or(JournalDecodeError::InvalidTimestamp)?;
    Ok(domain::ComponentSpecEvent::new(
        component_id,
        record.sequence(),
        time_recorded,
        spec,
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
        let body = protobuf::v1::GraphStreamEnvelope {
            format_version: Some(99),
            ..Default::default()
        }
        .encode_to_vec();
        let result = decode_graph_record(&WireRecord::new(0, 0, body));
        assert!(matches!(
            result,
            Err(JournalDecodeError::UnsupportedVersion(99))
        ));
    }
}
