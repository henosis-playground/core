use types::domain;

use crate::parsing::field;

use crate::parsing::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::ComponentOutputsView<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::ComponentOutputsView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.component_id).required()?.component_id()?,
            field!(value.values_json).or_default().json()?,
        ))
    }
}

impl TryFrom<&protobuf::v1::ComponentOutputsRecordV1View<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(
        value: &protobuf::v1::ComponentOutputsRecordV1View<'_>,
    ) -> Result<Self, Self::Error> {
        Ok(Self::new(
            field!(value.component_id).required()?.component_id()?,
            field!(value.values_json).or_default().json()?,
        ))
    }
}

impl From<&domain::ComponentOutputs> for protobuf::v1::ComponentOutputs {
    fn from(value: &domain::ComponentOutputs) -> Self {
        Self {
            component_id: Some(value.component_id().as_bytes().to_vec()),
            values_json: Some(value.values_json().to_vec()),
            ..Self::default()
        }
    }
}
