use blake3::Hasher;
use buffa::Message;
use buffa::MessageField;
use types::domain;

use crate::protobuf;

fn digest(kind: &[u8], message: &impl Message) -> blake3::Hash {
    let mut hasher = Hasher::new();
    hasher.update(kind);
    hasher.update(&[0]);
    hasher.update(&message.encode_to_vec());
    hasher.finalize()
}

#[must_use]
pub fn create_request_hash(command: &domain::CreateGraph) -> blake3::Hash {
    digest(
        b"create",
        &protobuf::v1::CreateGraphRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            component_ids: command
                .component_ids()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn add_request_hash(command: &domain::AddComponents) -> blake3::Hash {
    digest(
        b"add",
        &protobuf::v1::AddComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            component_ids: command
                .component_ids()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn update_request_hash(command: &domain::UpdateComponents) -> blake3::Hash {
    let mut replacements = command.replacements().to_vec();
    replacements.sort_by_key(|item| item.current());
    digest(
        b"update",
        &protobuf::v1::UpdateComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            replacements: replacements
                .into_iter()
                .map(|item| protobuf::v1::ComponentReplacement {
                    current_component_id: Some(item.current().as_bytes().to_vec()),
                    replacement_component_id: Some(item.replacement().as_bytes().to_vec()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn remove_request_hash(command: &domain::RemoveComponents) -> blake3::Hash {
    digest(
        b"remove",
        &protobuf::v1::RemoveComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            component_ids: command
                .component_ids()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn retire_request_hash(command: domain::RetireGraph) -> blake3::Hash {
    digest(
        b"retire",
        &protobuf::v1::RetireGraphRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn report_request_hash(command: &domain::ReportSlice) -> blake3::Hash {
    digest(
        b"report",
        &protobuf::v1::ReportSliceRequest {
            report: MessageField::some(command.report().into()),
            publication_id: command
                .publication_id()
                .map(|item| item.into_bytes().to_vec()),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn publication_hash(report: &domain::SliceReport) -> blake3::Hash {
    let mut hasher = Hasher::new();
    hasher.update(b"publication\0");
    hasher.update(&report.graph_id().into_bytes());
    hasher.update(&report.generation().to_be_bytes());
    hasher.update(report.connector().as_str().as_bytes());
    for output in report.outputs() {
        hasher.update(output.component_id().as_bytes());
        hasher.update(&(output.values_json().len() as u64).to_be_bytes());
        hasher.update(output.values_json());
    }
    hasher.finalize()
}
