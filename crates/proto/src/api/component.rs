use buffa::MessageField;
use buffa_types::google::protobuf::Timestamp;
use types::domain;

use crate::parsing::field;

use crate::parsing::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::NewComponentSpecView<'_>> for domain::NewComponentSpec {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::NewComponentSpecView<'_>) -> Result<Self, Self::Error> {
        domain::NewComponentSpec::new(
            field!(value.name).required()?.into_inner(),
            field!(value.connector).required()?.parse()?,
            field!(value.outputs_schema).or_default().into_inner(),
            field!(value.depends_on_component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
            field!(value.connector_context).or_default().into_inner(),
        )
        .map_err(ConversionError::from)
    }
}

impl From<&domain::ComponentSpec> for protobuf::v1::ComponentSpec {
    fn from(value: &domain::ComponentSpec) -> Self {
        Self {
            name: Some(value.name().to_owned()),
            connector: Some(value.connector().to_string()),
            outputs_schema: Some(value.outputs_schema().to_vec()),
            depends_on_component_ids: value
                .depends_on()
                .iter()
                .map(|item| item.into_bytes().to_vec())
                .collect(),
            connector_context: Some(value.connector_context().to_vec()),
            ..Self::default()
        }
    }
}

impl From<&domain::Component> for protobuf::v1::Component {
    fn from(value: &domain::Component) -> Self {
        Self {
            id: Some(value.id().into_bytes().to_vec()),
            spec: MessageField::some(value.spec().into()),
            generation: Some(value.generation()),
            time_created: MessageField::some(Timestamp::from(value.time_created())),
            time_modified: MessageField::some(Timestamp::from(value.time_modified())),
            ..Self::default()
        }
    }
}
