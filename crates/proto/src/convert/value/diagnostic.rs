use buffa::EnumValue;
use buffa::MessageField;
use henosis_types as domain;

use crate::convert::ConversionError;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::DiagnosticView<'_>> for domain::Diagnostic {
    type Error = ConversionError;

    fn try_from(value: &view::DiagnosticView<'_>) -> Result<Self, Self::Error> {
        let severity = match wire_field!(value.severity).required()?.into_inner() {
            EnumValue::Known(pb::DiagnosticSeverity::Error) => domain::DiagnosticSeverity::Error,
            EnumValue::Known(pb::DiagnosticSeverity::Warning) => {
                domain::DiagnosticSeverity::Warning
            }
            EnumValue::Known(pb::DiagnosticSeverity::Info) => domain::DiagnosticSeverity::Info,
            _ => return Err(wire_field!(value.severity).invalid("must be specified")),
        };
        let contract_failure = wire_field!(value.contract_failure)
            .optional()
            .map(|detail| detail.convert())
            .transpose()?;
        Ok(domain::Diagnostic::new(
            wire_field!(value.code).required()?.owned(),
            wire_field!(value.message).or_default().owned(),
            wire_field!(value.component_spec_hash)
                .optional()
                .map(|item| item.spec_hash())
                .transpose()?,
            wire_field!(value.pointer).or_default().owned(),
            wire_field!(value.help).or_default().owned(),
            severity,
            contract_failure,
        ))
    }
}

impl TryFrom<&view::ContractFailureDetailView<'_>> for domain::ContractFailureDetail {
    type Error = ConversionError;

    fn try_from(value: &view::ContractFailureDetailView<'_>) -> Result<Self, Self::Error> {
        let kind = match wire_field!(value.kind).required()?.into_inner() {
            EnumValue::Known(pb::ContractFailureKind::Compile) => {
                domain::ContractFailureKind::Compile
            }
            EnumValue::Known(pb::ContractFailureKind::Render) => {
                domain::ContractFailureKind::Render
            }
            EnumValue::Known(pb::ContractFailureKind::Validate) => {
                domain::ContractFailureKind::Validate
            }
            EnumValue::Known(pb::ContractFailureKind::Resolve) => {
                domain::ContractFailureKind::Resolve
            }
            _ => return Err(wire_field!(value.kind).invalid("must be specified")),
        };
        let mut consumed_paths = wire_field!(value.consumed_paths)
            .iter()
            .map(|path| path.into_inner().to_string())
            .collect::<Vec<_>>();
        consumed_paths.sort();
        if consumed_paths.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(wire_field!(value.consumed_paths).invalid("must be unique"));
        }
        Ok(Self {
            consumer: wire_field!(value.consumer).required()?.owned(),
            producer: wire_field!(value.producer).required()?.owned(),
            pinned_sha: wire_field!(value.pinned_sha)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.owned()),
            resolved_sha: wire_field!(value.resolved_sha)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.owned()),
            outputs_schema_at_pinned_json: wire_field!(value.outputs_schema_at_pinned_json)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.json())
                .transpose()?,
            outputs_schema_at_resolved_json: wire_field!(value.outputs_schema_at_resolved_json)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.json())
                .transpose()?,
            consumed_paths,
            kind,
            excerpt: wire_field!(value.excerpt).or_default().owned(),
            source_url: wire_field!(value.source_url)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.owned()),
        })
    }
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
