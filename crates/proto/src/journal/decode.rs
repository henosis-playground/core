use buffa::EnumValue;
use buffa::MessageView;
use futures::Stream;
use futures::StreamExt;
use henosis_types as domain;
use thiserror::Error;

use super::WireRecord;
use crate::ConversionError;
use crate::convert::fingerprint;
use crate::convert::graph_id;
use crate::convert::invalid;
use crate::convert::publication_id;
use crate::convert::register_component_spec;
use crate::convert::request_id;
use crate::convert::spec_hash;
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
            graph: value
                .graph
                .as_option()
                .ok_or(JournalDecodeError::Missing("created graph"))?
                .try_into()
                .map_err(JournalDecodeError::InvalidDomain)?,
            request_id: request_id(value.request_id, "created.request_id")
                .map_err(JournalDecodeError::InvalidDomain)?,
            request_fingerprint: fingerprint(
                value.request_fingerprint,
                "created.request_fingerprint",
            )
            .map_err(JournalDecodeError::InvalidDomain)?,
        },
        oneof::graph_stream_record_v1::Event::GenerationAccepted(value) => {
            let mutation_kind = match value.mutation_kind {
                Some(EnumValue::Known(pb::GraphMutationKindV1::AddComponents)) => {
                    domain::MutationKind::AddComponents
                }
                Some(EnumValue::Known(pb::GraphMutationKindV1::UpdateComponents)) => {
                    domain::MutationKind::UpdateComponents
                }
                Some(EnumValue::Known(pb::GraphMutationKindV1::RemoveComponents)) => {
                    domain::MutationKind::RemoveComponents
                }
                _ => return Err(JournalDecodeError::Missing("mutation kind")),
            };
            domain::GraphEvent::GenerationAccepted {
                graph: value
                    .graph
                    .as_option()
                    .ok_or(JournalDecodeError::Missing("accepted graph"))?
                    .try_into()
                    .map_err(JournalDecodeError::InvalidDomain)?,
                request_id: request_id(value.request_id, "accepted.request_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
                mutation_kind,
                request_fingerprint: fingerprint(
                    value.request_fingerprint,
                    "accepted.request_fingerprint",
                )
                .map_err(JournalDecodeError::InvalidDomain)?,
            }
        }
        oneof::graph_stream_record_v1::Event::OutputsPublished(value) => {
            domain::GraphEvent::OutputsPublished(domain::OutputPublication {
                generation: value
                    .generation
                    .ok_or(JournalDecodeError::Missing("output generation"))?,
                input_sequence: value
                    .input_sequence
                    .ok_or(JournalDecodeError::Missing("output input sequence"))?,
                connector: value
                    .connector
                    .ok_or(JournalDecodeError::Missing("output connector"))?
                    .parse()
                    .map_err(|error| {
                        JournalDecodeError::InvalidDomain(invalid("output connector", error))
                    })?,
                outputs: value
                    .outputs
                    .iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(JournalDecodeError::InvalidDomain)?,
                request_id: request_id(value.request_id, "output.request_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
                request_fingerprint: fingerprint(
                    value.request_fingerprint,
                    "output.request_fingerprint",
                )
                .map_err(JournalDecodeError::InvalidDomain)?,
                publication_id: publication_id(value.publication_id, "output.publication_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
                publication_fingerprint: fingerprint(
                    value.publication_fingerprint,
                    "output.publication_fingerprint",
                )
                .map_err(JournalDecodeError::InvalidDomain)?,
            })
        }
        oneof::graph_stream_record_v1::Event::GraphRetired(value) => domain::GraphEvent::Retired {
            graph_id: graph_id(value.graph_id, "retired.graph_id")
                .map_err(JournalDecodeError::InvalidDomain)?,
            last_generation: value
                .last_generation
                .ok_or(JournalDecodeError::Missing("retired generation"))?,
            request_id: request_id(value.request_id, "retired.request_id")
                .map_err(JournalDecodeError::InvalidDomain)?,
            request_fingerprint: fingerprint(
                value.request_fingerprint,
                "retired.request_fingerprint",
            )
            .map_err(JournalDecodeError::InvalidDomain)?,
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
                graph_id: graph_id(value.graph_id, "registry.graph_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
                request_id: request_id(value.request_id, "registry.request_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
            }
        }
        oneof::registry_stream_record_v1::Event::GraphRetired(value) => {
            domain::RegistryEvent::Retired {
                graph_id: graph_id(value.graph_id, "registry.graph_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
                request_id: request_id(value.request_id, "registry.request_id")
                    .map_err(JournalDecodeError::InvalidDomain)?,
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
    let value = match version
        .event
        .as_ref()
        .ok_or(JournalDecodeError::Missing("spec event"))?
    {
        oneof::spec_stream_record_v1::Event::ComponentSpecRegistered(value) => value,
    };
    let expected =
        spec_hash(value.hash, "registered spec hash").map_err(JournalDecodeError::InvalidDomain)?;
    let spec = value
        .spec
        .as_option()
        .ok_or(JournalDecodeError::Missing("registered spec"))?
        .try_into()
        .map_err(JournalDecodeError::InvalidDomain)?;
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
