use henosis_types as domain;

use super::super::ConversionError;
use super::super::invalid;
use super::super::spec_hash;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

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

pub(super) fn normalize_json(
    value: &[u8],
    field: &'static str,
) -> Result<Vec<u8>, ConversionError> {
    let value = serde_json::from_slice::<serde_json::Value>(value)
        .map_err(|error| invalid(field, error))?;
    serde_json::to_vec(&value).map_err(|error| invalid(field, error))
}
