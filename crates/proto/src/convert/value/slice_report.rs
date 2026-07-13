use buffa::EnumValue;
use buffa::MessageField;
use henosis_types as domain;

use crate::convert::ConversionError;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::ComponentDispositionView<'_>> for domain::ComponentDisposition {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentDispositionView<'_>) -> Result<Self, Self::Error> {
        let kind = match wire_field!(value.kind).required()?.into_inner() {
            EnumValue::Known(pb::ComponentDispositionKind::Pending) => {
                domain::ComponentDispositionKind::Pending
            }
            EnumValue::Known(pb::ComponentDispositionKind::Reconciling) => {
                domain::ComponentDispositionKind::Reconciling
            }
            EnumValue::Known(pb::ComponentDispositionKind::Ready) => {
                domain::ComponentDispositionKind::Ready
            }
            EnumValue::Known(pb::ComponentDispositionKind::Failed) => {
                domain::ComponentDispositionKind::Failed
            }
            _ => return Err(wire_field!(value.kind).invalid("must be specified")),
        };
        Ok(Self::new(
            wire_field!(value.component_spec_hash)
                .required()?
                .spec_hash()?,
            kind,
        ))
    }
}

impl From<&domain::ComponentDisposition> for pb::ComponentDisposition {
    fn from(value: &domain::ComponentDisposition) -> Self {
        let kind = match value.kind() {
            domain::ComponentDispositionKind::Pending => pb::ComponentDispositionKind::Pending,
            domain::ComponentDispositionKind::Reconciling => {
                pb::ComponentDispositionKind::Reconciling
            }
            domain::ComponentDispositionKind::Ready => pb::ComponentDispositionKind::Ready,
            domain::ComponentDispositionKind::Failed => pb::ComponentDispositionKind::Failed,
        };
        Self {
            component_spec_hash: Some(value.component_spec_hash().as_bytes().to_vec()),
            kind: Some(kind.into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&view::SliceReportView<'_>> for domain::SliceReport {
    type Error = ConversionError;

    fn try_from(value: &view::SliceReportView<'_>) -> Result<Self, Self::Error> {
        domain::SliceReport::new(domain::NewSliceReport {
            graph_id: wire_field!(value.graph_id).required()?.uuid()?,
            generation: wire_field!(value.generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            connector: wire_field!(value.connector).required()?.parse()?,
            dispositions: wire_field!(value.dispositions)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            outputs: wire_field!(value.outputs)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            diagnostics: wire_field!(value.diagnostics)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            sequence: wire_field!(value.sequence).required()?.into_inner(),
            publication: wire_field!(value.publication)
                .optional()
                .map(|publication| {
                    let publication = publication.into_inner();
                    Ok(domain::PublicationEvidence {
                        revision: wire_field!(publication.revision).required()?.owned(),
                        uri: wire_field!(publication.uri).required()?.owned(),
                    })
                })
                .transpose()?,
        })
        .map_err(|error| match error {
            domain::SliceReportError::InvalidGeneration => {
                wire_field!(value.generation).invalid(error)
            }
            domain::SliceReportError::DuplicateDisposition => {
                wire_field!(value.dispositions).invalid(error)
            }
            domain::SliceReportError::DuplicateOutput => wire_field!(value.outputs).invalid(error),
        })
    }
}

impl From<&domain::SliceReport> for pb::SliceReport {
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
                    MessageField::some(pb::PublicationEvidence {
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
