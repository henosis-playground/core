use buffa::Message;
use buffa::MessageField;
use types::domain;

use crate::protobuf;

const FORMAT_VERSION: u32 = 1;

#[must_use]
pub fn encode_spec_record(value: &domain::NewComponent) -> Vec<u8> {
    protobuf::v1::SpecStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: protobuf::v1::SpecStreamRecordV1 {
            event: Some(
                protobuf::v1::ComponentSpecRegisteredV1 {
                    component_id: Some(value.id().into_bytes().to_vec()),
                    spec: MessageField::some(spec_to_record(value.spec())),
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
    .encode_to_vec()
}

#[must_use]
pub fn encode_graph_event(event: &domain::GraphEvent) -> Vec<u8> {
    let event = match event {
        domain::GraphEvent::Created {
            graph,
            request_id,
            request_hash,
        } => protobuf::v1::GraphCreatedV1 {
            graph: MessageField::some(graph_to_record(graph)),
            request_id: Some(request_id.into_bytes().to_vec()),
            request_hash: Some(request_hash.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::GenerationAccepted {
            graph,
            request_id,
            mutation_kind,
            request_hash,
        } => protobuf::v1::GenerationAcceptedV1 {
            graph: MessageField::some(graph_to_record(graph)),
            request_id: Some(request_id.into_bytes().to_vec()),
            mutation_kind: Some(edit_kind(*mutation_kind).into()),
            request_hash: Some(request_hash.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::OutputsPublished(value) => protobuf::v1::OutputsPublishedV1 {
            generation: Some(value.generation),
            connector: Some(value.connector.to_string()),
            outputs: value.outputs.iter().map(output_to_record).collect(),
            request_id: Some(value.request_id.into_bytes().to_vec()),
            request_hash: Some(value.request_hash.as_bytes().to_vec()),
            publication_id: Some(value.publication_id.into_bytes().to_vec()),
            publication_hash: Some(value.publication_hash.as_bytes().to_vec()),
            input_sequence: Some(value.input_sequence),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::SliceReported(value) => protobuf::v1::SliceReportedV1 {
            report: MessageField::some((&value.report).into()),
            request_id: Some(value.request_id.into_bytes().to_vec()),
            request_hash: Some(value.request_hash.as_bytes().to_vec()),
            publication_id: value
                .publication_id
                .map(|publication_id| publication_id.into_bytes().to_vec()),
            publication_hash: value.publication_hash.map(|hash| hash.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::Retired {
            graph_id,
            last_generation,
            request_id,
            request_hash,
        } => protobuf::v1::GraphRetiredV1 {
            graph_id: Some(graph_id.into_bytes().to_vec()),
            last_generation: Some(*last_generation),
            request_id: Some(request_id.into_bytes().to_vec()),
            request_hash: Some(request_hash.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
    };
    protobuf::v1::GraphStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: protobuf::v1::GraphStreamRecordV1 {
            event: Some(event),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
    .encode_to_vec()
}

#[must_use]
pub fn encode_registry_event(event: domain::RegistryEvent) -> Vec<u8> {
    let event = match event {
        domain::RegistryEvent::Created {
            graph_id,
            request_id,
        } => protobuf::v1::RegistryGraphCreatedV1 {
            graph_id: Some(graph_id.into_bytes().to_vec()),
            request_id: Some(request_id.into_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::RegistryEvent::Retired {
            graph_id,
            request_id,
        } => protobuf::v1::RegistryGraphRetiredV1 {
            graph_id: Some(graph_id.into_bytes().to_vec()),
            request_id: Some(request_id.into_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
    };
    protobuf::v1::RegistryStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: protobuf::v1::RegistryStreamRecordV1 {
            event: Some(event),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
    .encode_to_vec()
}

fn edit_kind(kind: domain::MutationKind) -> protobuf::v1::GraphMutationKindV1 {
    match kind {
        domain::MutationKind::AddComponents => protobuf::v1::GraphMutationKindV1::AddComponents,
        domain::MutationKind::UpdateComponents => {
            protobuf::v1::GraphMutationKindV1::UpdateComponents
        }
        domain::MutationKind::RemoveComponents => {
            protobuf::v1::GraphMutationKindV1::RemoveComponents
        }
        domain::MutationKind::Create | domain::MutationKind::Retire => {
            unreachable!("creation and retirement have dedicated durable events")
        }
    }
}

fn graph_to_record(graph: &domain::Graph) -> protobuf::v1::GraphSnapshotV1 {
    protobuf::v1::GraphSnapshotV1 {
        id: Some(graph.id().into_bytes().to_vec()),
        generation: Some(graph.generation()),
        component_ids: graph
            .components()
            .map(|component| component.component_id().as_bytes().to_vec())
            .collect(),
        ..Default::default()
    }
}

fn spec_to_record(value: &domain::NewComponentSpec) -> protobuf::v1::ComponentSpecRecordV1 {
    protobuf::v1::ComponentSpecRecordV1 {
        name: Some(value.name().to_owned()),
        connector: Some(value.connector().to_string()),
        outputs_schema: Some(value.outputs_schema().to_vec()),
        depends_on_component_ids: value
            .depends_on()
            .iter()
            .map(|item| item.as_bytes().to_vec())
            .collect(),
        connector_context: Some(value.connector_context().to_vec()),
        ..Default::default()
    }
}

fn output_to_record(value: &domain::ComponentOutputs) -> protobuf::v1::ComponentOutputsRecordV1 {
    protobuf::v1::ComponentOutputsRecordV1 {
        component_id: Some(value.component_id().as_bytes().to_vec()),
        values_json: Some(value.values_json().to_vec()),
        ..Default::default()
    }
}
