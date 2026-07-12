use blake3::hash;
use buffa::EnumValue;
use buffa::Message;
use buffa::MessageField;
use henosis_types as domain;

use super::ConversionError;
use super::graph_id;
use super::invalid;
use super::missing;
use super::spec_hash;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::ComponentSpecView<'_>> for domain::ComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentSpecView<'_>) -> Result<Self, Self::Error> {
        domain::ComponentSpec::new(domain::NewComponentSpec {
            name: value.name.ok_or_else(|| missing("spec.name"))?.to_owned(),
            connector: value
                .connector
                .ok_or_else(|| missing("spec.connector"))?
                .parse()
                .map_err(|error| invalid("spec.connector", error))?,
            outputs_schema: value.outputs_schema.unwrap_or_default().to_vec(),
            depends_on: value
                .depends_on
                .iter()
                .map(|item| spec_hash(Some(item), "spec.depends_on"))
                .collect::<Result<Vec<_>, _>>()?,
            connector_context: value.connector_context.unwrap_or_default().to_vec(),
        })
        .map_err(|error| invalid("spec", error))
    }
}

impl TryFrom<&view::ComponentSpecRecordV1View<'_>> for domain::ComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentSpecRecordV1View<'_>) -> Result<Self, Self::Error> {
        domain::ComponentSpec::new(domain::NewComponentSpec {
            name: value.name.ok_or_else(|| missing("spec.name"))?.to_owned(),
            connector: value
                .connector
                .ok_or_else(|| missing("spec.connector"))?
                .parse()
                .map_err(|error| invalid("spec.connector", error))?,
            outputs_schema: value.outputs_schema.unwrap_or_default().to_vec(),
            depends_on: value
                .depends_on
                .iter()
                .map(|item| spec_hash(Some(item), "spec.depends_on"))
                .collect::<Result<Vec<_>, _>>()?,
            connector_context: value.connector_context.unwrap_or_default().to_vec(),
        })
        .map_err(|error| invalid("spec", error))
    }
}

impl From<&domain::ComponentSpec> for pb::ComponentSpec {
    fn from(value: &domain::ComponentSpec) -> Self {
        Self {
            name: Some(value.name().to_owned()),
            connector: Some(value.connector().to_string()),
            outputs_schema: Some(value.outputs_schema().to_vec()),
            depends_on: value
                .depends_on()
                .iter()
                .map(|item| item.as_bytes().to_vec())
                .collect(),
            connector_context: Some(value.connector_context().to_vec()),
            ..Self::default()
        }
    }
}

#[must_use]
pub fn register_component_spec(spec: domain::ComponentSpec) -> domain::RegisteredComponentSpec {
    let wire = pb::ComponentSpec::from(&spec);
    domain::RegisteredComponentSpec::new(
        domain::ComponentSpecHash::from_bytes(*hash(&wire.encode_to_vec()).as_bytes()),
        spec,
    )
}

impl TryFrom<&view::RegisteredComponentSpecView<'_>> for domain::RegisteredComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::RegisteredComponentSpecView<'_>) -> Result<Self, Self::Error> {
        let expected = spec_hash(value.hash, "component.hash")?;
        let spec = value
            .spec
            .as_option()
            .ok_or_else(|| missing("component.spec"))?
            .try_into()?;
        let registered = register_component_spec(spec);
        if registered.hash() != expected {
            return Err(invalid(
                "component.hash",
                "does not match canonical spec content",
            ));
        }
        Ok(registered)
    }
}

impl From<&domain::RegisteredComponentSpec> for pb::RegisteredComponentSpec {
    fn from(value: &domain::RegisteredComponentSpec) -> Self {
        Self {
            hash: Some(value.hash().as_bytes().to_vec()),
            spec: MessageField::some(value.spec().into()),
            ..Self::default()
        }
    }
}

impl TryFrom<&view::GraphView<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphView<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: graph_id(value.id, "graph.id")?,
            generation: value
                .generation
                .ok_or_else(|| missing("graph.generation"))?,
            component_spec_hashes: value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "graph.component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| invalid("graph", error))
    }
}

impl TryFrom<&view::GraphSnapshotV1View<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphSnapshotV1View<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: graph_id(value.id, "graph.id")?,
            generation: value
                .generation
                .ok_or_else(|| missing("graph.generation"))?,
            component_spec_hashes: value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "graph.component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| invalid("graph", error))
    }
}

impl From<&domain::Graph> for pb::Graph {
    fn from(value: &domain::Graph) -> Self {
        Self {
            id: Some(value.id().to_bytes().to_vec()),
            generation: Some(value.generation()),
            component_spec_hashes: value
                .components()
                .map(|component| component.spec_hash().as_bytes().to_vec())
                .collect(),
            ..Self::default()
        }
    }
}

impl TryFrom<&view::ComponentOutputsView<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentOutputsView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            spec_hash(value.component_spec_hash, "outputs.component_spec_hash")?,
            normalize_json(value.values_json.unwrap_or_default(), "outputs.values_json")?,
        ))
    }
}

impl TryFrom<&view::ComponentOutputsRecordV1View<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentOutputsRecordV1View<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            spec_hash(value.component_spec_hash, "outputs.component_spec_hash")?,
            normalize_json(value.values_json.unwrap_or_default(), "outputs.values_json")?,
        ))
    }
}

impl From<&domain::ComponentOutputs> for pb::ComponentOutputs {
    fn from(value: &domain::ComponentOutputs) -> Self {
        Self {
            component_spec_hash: Some(value.component_spec_hash().as_bytes().to_vec()),
            values_json: Some(value.values_json().to_vec()),
            ..Self::default()
        }
    }
}

fn normalize_json(value: &[u8], field: &'static str) -> Result<Vec<u8>, ConversionError> {
    let value = serde_json::from_slice::<serde_json::Value>(value)
        .map_err(|error| invalid(field, error))?;
    serde_json::to_vec(&value).map_err(|error| invalid(field, error))
}

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
        ))
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
            ..Self::default()
        }
    }
}

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
            graph_id: graph_id(value.graph_id, "report.graph_id")?,
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
        })
        .map_err(|error| invalid("report", error))
    }
}

impl From<&domain::SliceReport> for pb::SliceReport {
    fn from(value: &domain::SliceReport) -> Self {
        Self {
            graph_id: Some(value.graph_id().to_bytes().to_vec()),
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            dispositions: value.dispositions().map(Into::into).collect(),
            outputs: value.outputs().map(Into::into).collect(),
            diagnostics: value.diagnostics().iter().map(Into::into).collect(),
            sequence: Some(value.sequence()),
            ..Self::default()
        }
    }
}

impl From<&domain::PublishedSliceOutputs> for pb::PublishedSliceOutputs {
    fn from(value: &domain::PublishedSliceOutputs) -> Self {
        Self {
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            outputs: value.outputs().iter().map(Into::into).collect(),
            publication_sequence: Some(value.publication_sequence()),
            publication_id: Some(value.publication_id().to_bytes().to_vec()),
            input_sequence: Some(value.input_sequence()),
            ..Self::default()
        }
    }
}

impl From<&domain::DurableGraphState> for pb::DurableGraphState {
    fn from(value: &domain::DurableGraphState) -> Self {
        let lifecycle = match value.lifecycle() {
            domain::GraphLifecycle::Active => pb::GraphLifecycle::Active,
            domain::GraphLifecycle::Retired => pb::GraphLifecycle::Retired,
        };
        Self {
            graph: MessageField::some(value.graph().into()),
            published_outputs: value.published_outputs().map(Into::into).collect(),
            lifecycle: Some(lifecycle.into()),
            ..Self::default()
        }
    }
}

impl From<&domain::GraphState> for pb::GraphState {
    fn from(value: &domain::GraphState) -> Self {
        Self {
            durable: MessageField::some(value.durable().into()),
            reports: value.reports().map(Into::into).collect(),
            ..Self::default()
        }
    }
}

impl From<&domain::GraphSlice> for pb::GraphSlice {
    fn from(value: &domain::GraphSlice) -> Self {
        Self {
            graph_id: Some(value.graph_id().to_bytes().to_vec()),
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            components: value.components().map(Into::into).collect(),
            upstream_outputs: value.upstream_outputs().map(Into::into).collect(),
            sequence: Some(value.sequence()),
            ..Self::default()
        }
    }
}
