use buffa::Message;
use buffa::MessageField;
use henosis_types as domain;

use crate::proto::henosis::v1 as pb;

const FORMAT_VERSION: u32 = 1;

#[must_use]
pub fn encode_spec_record(value: &domain::RegisteredComponentSpec) -> Vec<u8> {
    pb::SpecStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: pb::SpecStreamRecordV1 {
            event: Some(
                pb::ComponentSpecRegisteredV1 {
                    hash: Some(value.hash().as_bytes().to_vec()),
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
            request_fingerprint,
        } => pb::GraphCreatedV1 {
            graph: MessageField::some(graph_to_record(graph)),
            request_id: Some(request_id.to_bytes().to_vec()),
            request_fingerprint: Some(request_fingerprint.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::GenerationAccepted {
            graph,
            request_id,
            mutation_kind,
            request_fingerprint,
        } => pb::GenerationAcceptedV1 {
            graph: MessageField::some(graph_to_record(graph)),
            request_id: Some(request_id.to_bytes().to_vec()),
            mutation_kind: Some(edit_kind(*mutation_kind).into()),
            request_fingerprint: Some(request_fingerprint.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::OutputsPublished(value) => pb::OutputsPublishedV1 {
            generation: Some(value.generation),
            connector: Some(value.connector.to_string()),
            outputs: value.outputs.iter().map(output_to_record).collect(),
            request_id: Some(value.request_id.to_bytes().to_vec()),
            request_fingerprint: Some(value.request_fingerprint.as_bytes().to_vec()),
            publication_id: Some(value.publication_id.to_bytes().to_vec()),
            publication_fingerprint: Some(value.publication_fingerprint.as_bytes().to_vec()),
            input_sequence: Some(value.input_sequence),
            ..Default::default()
        }
        .into(),
        domain::GraphEvent::Retired {
            graph_id,
            last_generation,
            request_id,
            request_fingerprint,
        } => pb::GraphRetiredV1 {
            graph_id: Some(graph_id.to_bytes().to_vec()),
            last_generation: Some(*last_generation),
            request_id: Some(request_id.to_bytes().to_vec()),
            request_fingerprint: Some(request_fingerprint.as_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
    };
    pb::GraphStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: pb::GraphStreamRecordV1 {
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
        } => pb::RegistryGraphCreatedV1 {
            graph_id: Some(graph_id.to_bytes().to_vec()),
            request_id: Some(request_id.to_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
        domain::RegistryEvent::Retired {
            graph_id,
            request_id,
        } => pb::RegistryGraphRetiredV1 {
            graph_id: Some(graph_id.to_bytes().to_vec()),
            request_id: Some(request_id.to_bytes().to_vec()),
            ..Default::default()
        }
        .into(),
    };
    pb::RegistryStreamEnvelope {
        format_version: Some(FORMAT_VERSION),
        version: pb::RegistryStreamRecordV1 {
            event: Some(event),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
    .encode_to_vec()
}

fn edit_kind(kind: domain::MutationKind) -> pb::GraphMutationKindV1 {
    match kind {
        domain::MutationKind::AddComponents => pb::GraphMutationKindV1::AddComponents,
        domain::MutationKind::UpdateComponents => pb::GraphMutationKindV1::UpdateComponents,
        domain::MutationKind::RemoveComponents => pb::GraphMutationKindV1::RemoveComponents,
        domain::MutationKind::Create | domain::MutationKind::Retire => {
            unreachable!("creation and retirement have dedicated durable events")
        }
    }
}

fn graph_to_record(graph: &domain::Graph) -> pb::GraphSnapshotV1 {
    pb::GraphSnapshotV1 {
        id: Some(graph.id().to_bytes().to_vec()),
        generation: Some(graph.generation()),
        component_spec_hashes: graph
            .components()
            .map(|component| component.spec_hash().as_bytes().to_vec())
            .collect(),
        ..Default::default()
    }
}

fn spec_to_record(value: &domain::ComponentSpec) -> pb::ComponentSpecRecordV1 {
    pb::ComponentSpecRecordV1 {
        name: Some(value.name().to_owned()),
        connector: Some(value.connector().to_string()),
        outputs_schema: Some(value.outputs_schema().to_vec()),
        depends_on: value
            .depends_on()
            .iter()
            .map(|item| item.as_bytes().to_vec())
            .collect(),
        connector_context: Some(value.connector_context().to_vec()),
        ..Default::default()
    }
}

fn output_to_record(value: &domain::ComponentOutputs) -> pb::ComponentOutputsRecordV1 {
    pb::ComponentOutputsRecordV1 {
        component_spec_hash: Some(value.component_spec_hash().as_bytes().to_vec()),
        values_json: Some(value.values_json().to_vec()),
        ..Default::default()
    }
}
