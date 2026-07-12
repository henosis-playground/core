use blake3::hash;
use buffa::Message;
use buffa::MessageField;
use henosis_types as domain;

use super::super::ConversionError;
use super::super::invalid;
use super::super::missing;
use super::super::spec_hash;
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
