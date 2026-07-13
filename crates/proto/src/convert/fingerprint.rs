use blake3::Hasher;
use buffa::Message;
use buffa::MessageField;
use henosis_types as domain;

use crate::proto::henosis::v1 as pb;

fn digest(kind: &[u8], message: &impl Message) -> domain::Fingerprint {
    let mut hasher = Hasher::new();
    hasher.update(kind);
    hasher.update(&[0]);
    hasher.update(&message.encode_to_vec());
    domain::Fingerprint::from_bytes(*hasher.finalize().as_bytes())
}

#[must_use]
pub fn create_fingerprint(command: &domain::CreateGraph) -> domain::Fingerprint {
    digest(
        b"create",
        &pb::CreateGraphRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            component_spec_hashes: command
                .component_spec_hashes()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn add_fingerprint(command: &domain::AddComponents) -> domain::Fingerprint {
    digest(
        b"add",
        &pb::AddComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            component_spec_hashes: command
                .component_spec_hashes()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn update_fingerprint(command: &domain::UpdateComponents) -> domain::Fingerprint {
    let mut replacements = command.replacements().to_vec();
    replacements.sort_by_key(|item| item.current());
    digest(
        b"update",
        &pb::UpdateComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            replacements: replacements
                .into_iter()
                .map(|item| pb::ComponentReplacement {
                    current_spec_hash: Some(item.current().as_bytes().to_vec()),
                    replacement_spec_hash: Some(item.replacement().as_bytes().to_vec()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn remove_fingerprint(command: &domain::RemoveComponents) -> domain::Fingerprint {
    digest(
        b"remove",
        &pb::RemoveComponentsRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            component_spec_hashes: command
                .component_spec_hashes()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn retire_fingerprint(command: domain::RetireGraph) -> domain::Fingerprint {
    digest(
        b"retire",
        &pb::RetireGraphRequest {
            graph_id: Some(command.graph_id().into_bytes().to_vec()),
            expected_generation: Some(command.expected_generation()),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn report_fingerprint(command: &domain::ReportSlice) -> domain::Fingerprint {
    digest(
        b"report",
        &pb::ReportSliceRequest {
            report: MessageField::some(command.report().into()),
            publication_id: command
                .publication_id()
                .map(|item| item.into_bytes().to_vec()),
            ..Default::default()
        },
    )
}

#[must_use]
pub fn publication_fingerprint(report: &domain::SliceReport) -> domain::Fingerprint {
    let mut hasher = Hasher::new();
    hasher.update(b"publication\0");
    hasher.update(&report.graph_id().into_bytes());
    hasher.update(&report.generation().to_be_bytes());
    hasher.update(report.connector().as_str().as_bytes());
    for output in report.outputs() {
        hasher.update(output.component_spec_hash().as_bytes());
        hasher.update(&(output.values_json().len() as u64).to_be_bytes());
        hasher.update(output.values_json());
    }
    domain::Fingerprint::from_bytes(*hasher.finalize().as_bytes())
}
