use buffa::EnumValue;
use buffa::MessageField;
use henosis_types as domain;

use super::super::ConversionError;
use super::super::invalid;
use super::super::missing;
use super::super::spec_hash;
use super::output::normalize_json;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::DiagnosticView<'_>> for domain::Diagnostic {
    type Error = ConversionError;

    fn try_from(value: &view::DiagnosticView<'_>) -> Result<Self, Self::Error> {
        let severity = match value.severity {
            Some(EnumValue::Known(pb::DiagnosticSeverity::Error)) => {
                domain::DiagnosticSeverity::Error
            }
            Some(EnumValue::Known(pb::DiagnosticSeverity::Warning)) => {
                domain::DiagnosticSeverity::Warning
            }
            Some(EnumValue::Known(pb::DiagnosticSeverity::Info)) => {
                domain::DiagnosticSeverity::Info
            }
            _ => return Err(invalid("diagnostic.severity", "must be specified")),
        };
        let contract_failure = value
            .contract_failure
            .as_option()
            .map(contract_failure)
            .transpose()?;
        Ok(domain::Diagnostic::new(
            value
                .code
                .ok_or_else(|| missing("diagnostic.code"))?
                .to_owned(),
            value.message.unwrap_or_default().to_owned(),
            value
                .component_spec_hash
                .map(|item| spec_hash(Some(item), "diagnostic.component_spec_hash"))
                .transpose()?,
            value.pointer.unwrap_or_default().to_owned(),
            value.help.unwrap_or_default().to_owned(),
            severity,
            contract_failure,
        ))
    }
}

fn contract_failure(
    value: &view::ContractFailureDetailView<'_>,
) -> Result<domain::ContractFailureDetail, ConversionError> {
    let kind = match value.kind {
        Some(EnumValue::Known(pb::ContractFailureKind::Compile)) => {
            domain::ContractFailureKind::Compile
        }
        Some(EnumValue::Known(pb::ContractFailureKind::Render)) => {
            domain::ContractFailureKind::Render
        }
        Some(EnumValue::Known(pb::ContractFailureKind::Validate)) => {
            domain::ContractFailureKind::Validate
        }
        Some(EnumValue::Known(pb::ContractFailureKind::Resolve)) => {
            domain::ContractFailureKind::Resolve
        }
        _ => {
            return Err(invalid(
                "diagnostic.contract_failure.kind",
                "must be specified",
            ));
        }
    };
    let mut consumed_paths = value
        .consumed_paths
        .iter()
        .map(|path| path.to_string())
        .collect::<Vec<_>>();
    consumed_paths.sort();
    if consumed_paths.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(invalid(
            "diagnostic.contract_failure.consumed_paths",
            "must be unique",
        ));
    }
    Ok(domain::ContractFailureDetail {
        consumer: value
            .consumer
            .ok_or_else(|| missing("diagnostic.contract_failure.consumer"))?
            .to_owned(),
        producer: value
            .producer
            .ok_or_else(|| missing("diagnostic.contract_failure.producer"))?
            .to_owned(),
        pinned_sha: nonempty(value.pinned_sha),
        resolved_sha: nonempty(value.resolved_sha),
        outputs_schema_at_pinned_json: optional_json(
            value.outputs_schema_at_pinned_json,
            "diagnostic.contract_failure.outputs_schema_at_pinned_json",
        )?,
        outputs_schema_at_resolved_json: optional_json(
            value.outputs_schema_at_resolved_json,
            "diagnostic.contract_failure.outputs_schema_at_resolved_json",
        )?,
        consumed_paths,
        kind,
        excerpt: value.excerpt.unwrap_or_default().to_owned(),
        source_url: nonempty(value.source_url),
    })
}

fn optional_json(
    value: Option<&[u8]>,
    field: &'static str,
) -> Result<Option<Vec<u8>>, ConversionError> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| normalize_json(value, field))
        .transpose()
}

fn nonempty(value: Option<&str>) -> Option<String> {
    value.filter(|value| !value.is_empty()).map(str::to_owned)
}

impl From<&domain::Diagnostic> for pb::Diagnostic {
    fn from(value: &domain::Diagnostic) -> Self {
        let severity = match value.severity() {
            domain::DiagnosticSeverity::Error => pb::DiagnosticSeverity::Error,
            domain::DiagnosticSeverity::Warning => pb::DiagnosticSeverity::Warning,
            domain::DiagnosticSeverity::Info => pb::DiagnosticSeverity::Info,
        };
        Self {
            code: Some(value.code().to_owned()),
            message: Some(value.message().to_owned()),
            component_spec_hash: value
                .component_spec_hash()
                .map(|item| item.as_bytes().to_vec()),
            pointer: Some(value.pointer().to_owned()),
            help: Some(value.help().to_owned()),
            severity: Some(severity.into()),
            contract_failure: value
                .contract_failure()
                .map(|detail| MessageField::some(detail.into()))
                .unwrap_or_default(),
            ..Self::default()
        }
    }
}

impl From<&domain::ContractFailureDetail> for pb::ContractFailureDetail {
    fn from(value: &domain::ContractFailureDetail) -> Self {
        let kind = match value.kind {
            domain::ContractFailureKind::Compile => pb::ContractFailureKind::Compile,
            domain::ContractFailureKind::Render => pb::ContractFailureKind::Render,
            domain::ContractFailureKind::Validate => pb::ContractFailureKind::Validate,
            domain::ContractFailureKind::Resolve => pb::ContractFailureKind::Resolve,
        };
        Self {
            consumer: Some(value.consumer.clone()),
            producer: Some(value.producer.clone()),
            pinned_sha: value.pinned_sha.clone(),
            resolved_sha: value.resolved_sha.clone(),
            outputs_schema_at_pinned_json: value.outputs_schema_at_pinned_json.clone(),
            outputs_schema_at_resolved_json: value.outputs_schema_at_resolved_json.clone(),
            consumed_paths: value.consumed_paths.clone(),
            kind: Some(kind.into()),
            excerpt: Some(value.excerpt.clone()),
            source_url: value.source_url.clone(),
            ..Self::default()
        }
    }
}
