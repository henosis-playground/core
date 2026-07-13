use henosis_types as domain;

use crate::convert::ConversionError;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::ComponentOutputsView<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentOutputsView<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.component_spec_hash)
                .required()?
                .spec_hash()?,
            wire_field!(value.values_json).or_default().json()?,
        ))
    }
}

impl TryFrom<&view::ComponentOutputsRecordV1View<'_>> for domain::ComponentOutputs {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentOutputsRecordV1View<'_>) -> Result<Self, Self::Error> {
        Ok(Self::new(
            wire_field!(value.component_spec_hash)
                .required()?
                .spec_hash()?,
            wire_field!(value.values_json).or_default().json()?,
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
