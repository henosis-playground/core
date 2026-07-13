use buffa::EnumValue;
use buffa::MessageField;
use types::domain;

use crate::parsing::field;

use crate::parsing::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::ComponentDispositionView<'_>> for domain::ComponentDisposition {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::ComponentDispositionView<'_>) -> Result<Self, Self::Error> {
        let kind = match field!(value.kind).required()?.into_inner() {
            EnumValue::Known(protobuf::v1::ComponentDispositionKind::Pending) => {
                domain::ComponentDispositionKind::Pending
            }
            EnumValue::Known(protobuf::v1::ComponentDispositionKind::Reconciling) => {
                domain::ComponentDispositionKind::Reconciling
            }
            EnumValue::Known(protobuf::v1::ComponentDispositionKind::Ready) => {
                domain::ComponentDispositionKind::Ready
            }
            EnumValue::Known(protobuf::v1::ComponentDispositionKind::Failed) => {
                domain::ComponentDispositionKind::Failed
            }
            _ => return Err(field!(value.kind).invalid("must be specified")),
        };
        Ok(Self::new(
            field!(value.component_id).required()?.component_id()?,
            kind,
        ))
    }
}

impl From<&domain::ComponentDisposition> for protobuf::v1::ComponentDisposition {
    fn from(value: &domain::ComponentDisposition) -> Self {
        let kind = match value.kind() {
            domain::ComponentDispositionKind::Pending => {
                protobuf::v1::ComponentDispositionKind::Pending
            }
            domain::ComponentDispositionKind::Reconciling => {
                protobuf::v1::ComponentDispositionKind::Reconciling
            }
            domain::ComponentDispositionKind::Ready => {
                protobuf::v1::ComponentDispositionKind::Ready
            }
            domain::ComponentDispositionKind::Failed => {
                protobuf::v1::ComponentDispositionKind::Failed
            }
        };
        Self {
            component_id: Some(value.component_id().as_bytes().to_vec()),
            kind: Some(kind.into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&protobuf::v1::SliceReportView<'_>> for domain::SliceReport {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::SliceReportView<'_>) -> Result<Self, Self::Error> {
        domain::SliceReport::new(domain::NewSliceReport {
            graph_id: field!(value.graph_id).required()?.uuid()?,
            generation: field!(value.generation).required()?.into_inner(),
            connector: field!(value.connector).required()?.parse()?,
            dispositions: field!(value.dispositions)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            outputs: field!(value.outputs)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            diagnostics: field!(value.diagnostics)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            sequence: field!(value.sequence).required()?.into_inner(),
            publication: field!(value.publication)
                .optional()
                .map(|publication| {
                    let publication = publication.into_inner();
                    let revision = field!(publication.revision).required()?.into_inner();
                    let uri = field!(publication.uri).required()?.into_inner();
                    Ok::<_, ConversionError>(domain::PublicationEvidence::new(revision, uri))
                })
                .transpose()?,
        })
        .map_err(ConversionError::from)
    }
}

impl From<&domain::SliceReport> for protobuf::v1::SliceReport {
    fn from(value: &domain::SliceReport) -> Self {
        Self {
            graph_id: Some(value.graph_id().into_bytes().to_vec()),
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            dispositions: value.dispositions().map(Into::into).collect(),
            outputs: value.outputs().map(Into::into).collect(),
            diagnostics: value.diagnostics().iter().map(Into::into).collect(),
            sequence: Some(value.sequence()),
            publication: value
                .publication()
                .map(|publication| {
                    MessageField::some(protobuf::v1::PublicationEvidence {
                        revision: Some(publication.revision.clone()),
                        uri: Some(publication.uri.clone()),
                        ..Default::default()
                    })
                })
                .unwrap_or_default(),
            ..Self::default()
        }
    }
}
