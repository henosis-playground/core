use henosis_types as domain;

use super::ConversionError;
use crate::convert::register_component_spec;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::RegisterComponentSpecRequestView<'_>> for domain::RegisterComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::RegisterComponentSpecRequestView<'_>) -> Result<Self, Self::Error> {
        let spec = wire_field!(value.spec).required()?.convert()?;
        Ok(Self::new(register_component_spec(spec)))
    }
}

impl TryFrom<&view::CreateGraphRequestView<'_>> for domain::CreateGraph {
    type Error = ConversionError;

    fn try_from(value: &view::CreateGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.component_spec_hashes)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
            wire_field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&view::AddComponentsRequestView<'_>> for domain::AddComponents {
    type Error = ConversionError;

    fn try_from(value: &view::AddComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            wire_field!(value.component_spec_hashes)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
            wire_field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&view::ComponentReplacementView<'_>> for domain::ComponentReplacement {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentReplacementView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.current_spec_hash)
                .required()?
                .spec_hash()?,
            wire_field!(value.replacement_spec_hash)
                .required()?
                .spec_hash()?,
        ))
    }
}

impl TryFrom<&view::UpdateComponentsRequestView<'_>> for domain::UpdateComponents {
    type Error = ConversionError;

    fn try_from(value: &view::UpdateComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            wire_field!(value.replacements)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            wire_field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&view::RemoveComponentsRequestView<'_>> for domain::RemoveComponents {
    type Error = ConversionError;

    fn try_from(value: &view::RemoveComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            wire_field!(value.component_spec_hashes)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
            wire_field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&view::RetireGraphRequestView<'_>> for domain::RetireGraph {
    type Error = ConversionError;

    fn try_from(value: &view::RetireGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            wire_field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&view::GetGraphRequestView<'_>> for domain::GetGraph {
    type Error = ConversionError;

    fn try_from(value: &view::GetGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(wire_field!(value.graph_id).required()?.uuid()?))
    }
}

impl TryFrom<&view::GetGraphGenerationRequestView<'_>> for domain::GetGraphGeneration {
    type Error = ConversionError;

    fn try_from(value: &view::GetGraphGenerationRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
        ))
    }
}

impl TryFrom<&view::WatchGraphRequestView<'_>> for domain::WatchGraph {
    type Error = ConversionError;

    fn try_from(value: &view::WatchGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.after_sequence)
                .optional()
                .map(|value| value.into_inner()),
        ))
    }
}

impl TryFrom<&view::FetchSliceRequestView<'_>> for domain::FetchSlice {
    type Error = ConversionError;

    fn try_from(value: &view::FetchSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.graph_id).required()?.uuid()?,
            wire_field!(value.connector).required()?.parse()?,
            wire_field!(value.sequence).required()?.into_inner(),
        ))
    }
}

impl TryFrom<&view::ReportSliceRequestView<'_>> for domain::ReportSlice {
    type Error = ConversionError;

    fn try_from(value: &view::ReportSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.request_id).required()?.uuid()?,
            wire_field!(value.report).required()?.convert()?,
            wire_field!(value.publication_id)
                .optional()
                .map(|item| item.uuid())
                .transpose()?,
        ))
    }
}
