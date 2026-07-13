use types::domain;

use crate::parsing::field;

use super::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::CreateComponentRequestView<'_>> for domain::NewComponent {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::CreateComponentRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.component_id).required()?.uuid()?,
            field!(value.spec).required()?.convert()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::CreateGraphRequestView<'_>> for domain::CreateGraph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::CreateGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
            field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::AddComponentsRequestView<'_>> for domain::AddComponents {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::AddComponentsRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            field!(value.component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
            field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::ComponentReplacementView<'_>> for domain::ComponentReplacement {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::ComponentReplacementView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.current_component_id)
                .required()?
                .component_id()?,
            field!(value.replacement_component_id)
                .required()?
                .component_id()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::UpdateComponentsRequestView<'_>> for domain::UpdateComponents {
    type Error = ConversionError;

    fn try_from(
        value: &protobuf::v1::UpdateComponentsRequestView<'_>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            field!(value.replacements)
                .iter()
                .map(|item| item.convert())
                .collect::<Result<Vec<_>, _>>()?,
            field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::RemoveComponentsRequestView<'_>> for domain::RemoveComponents {
    type Error = ConversionError;

    fn try_from(
        value: &protobuf::v1::RemoveComponentsRequestView<'_>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            field!(value.component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
            field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::RetireGraphRequestView<'_>> for domain::RetireGraph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::RetireGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.expected_generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            field!(value.request_id).required()?.uuid()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::GetGraphRequestView<'_>> for domain::GetGraph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::GetGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(field!(value.graph_id).required()?.uuid()?))
    }
}

impl TryFrom<&protobuf::v1::GetGraphGenerationRequestView<'_>> for domain::GetGraphGeneration {
    type Error = ConversionError;

    fn try_from(
        value: &protobuf::v1::GetGraphGenerationRequestView<'_>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
        ))
    }
}

impl TryFrom<&protobuf::v1::WatchGraphRequestView<'_>> for domain::WatchGraph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::WatchGraphRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.after_sequence)
                .optional()
                .map(|value| value.into_inner()),
        ))
    }
}

impl TryFrom<&protobuf::v1::FetchSliceRequestView<'_>> for domain::FetchSlice {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::FetchSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.graph_id).required()?.uuid()?,
            field!(value.connector).required()?.parse()?,
            field!(value.sequence).required()?.into_inner(),
        ))
    }
}

impl TryFrom<&protobuf::v1::ReportSliceRequestView<'_>> for domain::ReportSlice {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::ReportSliceRequestView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.request_id).required()?.uuid()?,
            field!(value.report).required()?.convert()?,
            field!(value.publication_id)
                .optional()
                .map(|item| item.uuid())
                .transpose()?,
        ))
    }
}
