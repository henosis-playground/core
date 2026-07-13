use buffa::EnumValue;
use buffa::MessageField;
use henosis_types as domain;

use super::super::ConversionError;
use super::super::invalid;
use super::super::missing;
use super::super::spec_hash;
use super::super::uuid;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::ComponentDispositionView<'_>> for domain::ComponentDisposition {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentDispositionView<'_>) -> Result<Self, Self::Error> {
        let kind = match value.kind {
            Some(EnumValue::Known(pb::ComponentDispositionKind::Pending)) => {
                domain::ComponentDispositionKind::Pending
            }
            Some(EnumValue::Known(pb::ComponentDispositionKind::Reconciling)) => {
                domain::ComponentDispositionKind::Reconciling
            }
            Some(EnumValue::Known(pb::ComponentDispositionKind::Ready)) => {
                domain::ComponentDispositionKind::Ready
            }
            Some(EnumValue::Known(pb::ComponentDispositionKind::Failed)) => {
                domain::ComponentDispositionKind::Failed
            }
            _ => return Err(invalid("disposition.kind", "must be specified")),
        };
        Ok(Self::new(
            spec_hash(value.component_spec_hash, "disposition.component_spec_hash")?,
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
            graph_id: uuid(value.graph_id, "report.graph_id")?,
            generation: value
                .generation
                .ok_or_else(|| missing("report.generation"))?,
            connector: value
                .connector
                .ok_or_else(|| missing("report.connector"))?
                .parse()
                .map_err(|error| invalid("report.connector", error))?,
            dispositions: value
                .dispositions
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            outputs: value
                .outputs
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            diagnostics: value
                .diagnostics
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            sequence: value.sequence.ok_or_else(|| missing("report.sequence"))?,
            publication: value
                .publication
                .as_option()
                .map(|publication| {
                    Ok(domain::PublicationEvidence {
                        revision: publication
                            .revision
                            .ok_or_else(|| missing("report.publication.revision"))?
                            .to_owned(),
                        uri: publication
                            .uri
                            .ok_or_else(|| missing("report.publication.uri"))?
                            .to_owned(),
                    })
                })
                .transpose()?,
        })
        .map_err(|error| invalid("report", error))
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
