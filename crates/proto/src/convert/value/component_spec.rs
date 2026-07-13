use blake3::hash;
use buffa::Message;
use buffa::MessageField;
use henosis_types as domain;

use crate::convert::ConversionError;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::ComponentSpecView<'_>> for domain::ComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentSpecView<'_>) -> Result<Self, Self::Error> {
        domain::ComponentSpec::new(domain::NewComponentSpec {
            name: wire_field!(value.name).required()?.owned(),
            connector: wire_field!(value.connector).required()?.parse()?,
            outputs_schema: wire_field!(value.outputs_schema).or_default().owned(),
            depends_on: wire_field!(value.depends_on)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
            connector_context: wire_field!(value.connector_context).or_default().owned(),
        })
        .map_err(|error| match error {
            domain::ComponentSpecError::EmptyName => wire_field!(value.name).invalid(error),
            domain::ComponentSpecError::DuplicateDependency => {
                wire_field!(value.depends_on).invalid(error)
            }
        })
    }
}

impl TryFrom<&view::ComponentSpecRecordV1View<'_>> for domain::ComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &view::ComponentSpecRecordV1View<'_>) -> Result<Self, Self::Error> {
        domain::ComponentSpec::new(domain::NewComponentSpec {
            name: wire_field!(value.name).required()?.owned(),
            connector: wire_field!(value.connector).required()?.parse()?,
            outputs_schema: wire_field!(value.outputs_schema).or_default().owned(),
            depends_on: wire_field!(value.depends_on)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
            connector_context: wire_field!(value.connector_context).or_default().owned(),
        })
        .map_err(|error| match error {
            domain::ComponentSpecError::EmptyName => wire_field!(value.name).invalid(error),
            domain::ComponentSpecError::DuplicateDependency => {
                wire_field!(value.depends_on).invalid(error)
            }
        })
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
        let expected = wire_field!(value.hash).required()?.spec_hash()?;
        let spec = wire_field!(value.spec).required()?.convert()?;
        let registered = register_component_spec(spec);
        if registered.hash() != expected {
            return Err(wire_field!(value.hash).invalid("does not match canonical spec content"));
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
