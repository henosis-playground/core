use buffa::EnumValue;
use buffa::MessageField;
use types::domain;

use crate::parsing::field;

use crate::parsing::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::DiagnosticView<'_>> for domain::Diagnostic {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::DiagnosticView<'_>) -> Result<Self, Self::Error> {
        let severity = match field!(value.severity).required()?.into_inner() {
            EnumValue::Known(protobuf::v1::DiagnosticSeverity::Error) => {
                domain::DiagnosticSeverity::Error
            }
            EnumValue::Known(protobuf::v1::DiagnosticSeverity::Warning) => {
                domain::DiagnosticSeverity::Warning
            }
            EnumValue::Known(protobuf::v1::DiagnosticSeverity::Info) => {
                domain::DiagnosticSeverity::Info
            }
            _ => return Err(field!(value.severity).invalid("must be specified")),
        };
        let contract_failure = field!(value.contract_failure)
            .optional()
            .map(|detail| detail.convert())
            .transpose()?;
        Ok(domain::Diagnostic::new(
            field!(value.code).required()?.into_inner(),
            field!(value.message).or_default().into_inner(),
            field!(value.component_id)
                .optional()
                .map(|item| item.component_id())
                .transpose()?,
            field!(value.pointer).or_default().into_inner(),
            field!(value.help).or_default().into_inner(),
            severity,
            contract_failure,
        ))
    }
}

impl TryFrom<&protobuf::v1::ContractFailureDetailView<'_>> for domain::ContractFailureDetail {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::ContractFailureDetailView<'_>) -> Result<Self, Self::Error> {
        let kind = match field!(value.kind).required()?.into_inner() {
            EnumValue::Known(protobuf::v1::ContractFailureKind::Compile) => {
                domain::ContractFailureKind::Compile
            }
            EnumValue::Known(protobuf::v1::ContractFailureKind::Render) => {
                domain::ContractFailureKind::Render
            }
            EnumValue::Known(protobuf::v1::ContractFailureKind::Validate) => {
                domain::ContractFailureKind::Validate
            }
            EnumValue::Known(protobuf::v1::ContractFailureKind::Resolve) => {
                domain::ContractFailureKind::Resolve
            }
            _ => return Err(field!(value.kind).invalid("must be specified")),
        };
        let consumed_paths = field!(value.consumed_paths)
            .iter()
            .map(|path| *path.into_inner())
            .collect::<Vec<_>>();
        let detail = domain::NewContractFailureDetail {
            consumer: field!(value.consumer).required()?.into_inner(),
            producer: field!(value.producer).required()?.into_inner(),
            pinned_sha: field!(value.pinned_sha)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.into_inner()),
            resolved_sha: field!(value.resolved_sha)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.into_inner()),
            outputs_schema_at_pinned_json: field!(value.outputs_schema_at_pinned_json)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.json())
                .transpose()?,
            outputs_schema_at_resolved_json: field!(value.outputs_schema_at_resolved_json)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.json())
                .transpose()?,
            consumed_paths,
            kind,
            excerpt: field!(value.excerpt).or_default().into_inner(),
            source_url: field!(value.source_url)
                .optional()
                .filter(|value| !value.value().is_empty())
                .map(|value| value.into_inner()),
        };
        Self::new(detail).map_err(ConversionError::from)
    }
}

impl From<&domain::Diagnostic> for protobuf::v1::Diagnostic {
    fn from(value: &domain::Diagnostic) -> Self {
        let severity = match value.severity() {
            domain::DiagnosticSeverity::Error => protobuf::v1::DiagnosticSeverity::Error,
            domain::DiagnosticSeverity::Warning => protobuf::v1::DiagnosticSeverity::Warning,
            domain::DiagnosticSeverity::Info => protobuf::v1::DiagnosticSeverity::Info,
        };
        Self {
            code: Some(value.code().to_owned()),
            message: Some(value.message().to_owned()),
            component_id: value.component_id().map(|item| item.as_bytes().to_vec()),
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

impl From<&domain::ContractFailureDetail> for protobuf::v1::ContractFailureDetail {
    fn from(value: &domain::ContractFailureDetail) -> Self {
        let kind = match value.kind {
            domain::ContractFailureKind::Compile => protobuf::v1::ContractFailureKind::Compile,
            domain::ContractFailureKind::Render => protobuf::v1::ContractFailureKind::Render,
            domain::ContractFailureKind::Validate => protobuf::v1::ContractFailureKind::Validate,
            domain::ContractFailureKind::Resolve => protobuf::v1::ContractFailureKind::Resolve,
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
