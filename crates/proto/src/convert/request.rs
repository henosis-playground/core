use henosis_types as domain;

use super::ConversionError;
use super::graph_id;
use super::invalid;
use super::missing;
use super::publication_id;
use super::request_id;
use super::spec_hash;
use crate::convert::register_component_spec;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::RegisterComponentSpecRequestView<'_>> for domain::RegisterComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::RegisterComponentSpecRequestView<'_>) -> Result<Self, Self::Error> {
        let spec = value.spec.as_option().ok_or_else(|| missing("spec"))?;
        Ok(Self::new(register_component_spec(spec.try_into()?)))
    }
}

impl TryFrom<&view::CreateGraphRequestView<'_>> for domain::CreateGraph {
    type Error = ConversionError;

    fn try_from(value: &view::CreateGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
            request_id(value.request_id, "request_id")?,
        ))
    }
}

impl TryFrom<&view::AddComponentsRequestView<'_>> for domain::AddComponents {
    type Error = ConversionError;

    fn try_from(value: &view::AddComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            required_generation(value.expected_generation, "expected_generation")?,
            value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
            request_id(value.request_id, "request_id")?,
        ))
    }
}

impl TryFrom<&view::ComponentReplacementView<'_>> for domain::ComponentReplacement {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentReplacementView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            spec_hash(value.current_spec_hash, "replacement.current_spec_hash")?,
            spec_hash(
                value.replacement_spec_hash,
                "replacement.replacement_spec_hash",
            )?,
        ))
    }
}

impl TryFrom<&view::UpdateComponentsRequestView<'_>> for domain::UpdateComponents {
    type Error = ConversionError;

    fn try_from(value: &view::UpdateComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            required_generation(value.expected_generation, "expected_generation")?,
            value
                .replacements
                .iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            request_id(value.request_id, "request_id")?,
        ))
    }
}

impl TryFrom<&view::RemoveComponentsRequestView<'_>> for domain::RemoveComponents {
    type Error = ConversionError;

    fn try_from(value: &view::RemoveComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            required_generation(value.expected_generation, "expected_generation")?,
            value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
            request_id(value.request_id, "request_id")?,
        ))
    }
}

impl TryFrom<&view::RetireGraphRequestView<'_>> for domain::RetireGraph {
    type Error = ConversionError;

    fn try_from(value: &view::RetireGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            required_generation(value.expected_generation, "expected_generation")?,
            request_id(value.request_id, "request_id")?,
        ))
    }
}

impl TryFrom<&view::GetGraphRequestView<'_>> for domain::GetGraph {
    type Error = ConversionError;

    fn try_from(value: &view::GetGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(graph_id(value.graph_id, "graph_id")?))
    }
}

impl TryFrom<&view::WatchGraphRequestView<'_>> for domain::WatchGraph {
    type Error = ConversionError;

    fn try_from(value: &view::WatchGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            value.after_sequence,
        ))
    }
}

impl TryFrom<&view::FetchSliceRequestView<'_>> for domain::FetchSlice {
    type Error = ConversionError;

    fn try_from(value: &view::FetchSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            graph_id(value.graph_id, "graph_id")?,
            value
                .connector
                .ok_or_else(|| missing("connector"))?
                .parse()
                .map_err(|error| invalid("connector", error))?,
            value.sequence.ok_or_else(|| missing("sequence"))?,
        ))
    }
}

impl TryFrom<&view::ReportSliceRequestView<'_>> for domain::ReportSlice {
    type Error = ConversionError;

    fn try_from(value: &view::ReportSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            request_id(value.request_id, "request_id")?,
            value
                .report
                .as_option()
                .ok_or_else(|| missing("report"))?
                .try_into()?,
            value
                .publication_id
                .map(|item| publication_id(Some(item), "publication_id"))
                .transpose()?,
        ))
    }
}

fn required_generation(value: Option<u64>, field: &'static str) -> Result<u64, ConversionError> {
    value
        .filter(|generation| *generation > 0)
        .ok_or_else(|| invalid(field, "must be greater than zero"))
}
