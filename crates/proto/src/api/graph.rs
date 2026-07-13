use types::domain;

use crate::parsing::field;

use crate::parsing::ConversionError;
use crate::protobuf;

impl TryFrom<&protobuf::v1::GraphView<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::GraphView<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: field!(value.id).required()?.uuid()?,
            generation: field!(value.generation).required()?.into_inner(),
            component_ids: field!(value.component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(ConversionError::from)
    }
}

impl TryFrom<&protobuf::v1::GraphSnapshotV1View<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &protobuf::v1::GraphSnapshotV1View<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: field!(value.id).required()?.uuid()?,
            generation: field!(value.generation).required()?.into_inner(),
            component_ids: field!(value.component_ids)
                .iter()
                .map(|item| item.component_id())
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(ConversionError::from)
    }
}

impl From<&domain::Graph> for protobuf::v1::Graph {
    fn from(value: &domain::Graph) -> Self {
        Self {
            id: Some(value.id().into_bytes().to_vec()),
            generation: Some(value.generation()),
            component_ids: value
                .components()
                .map(|component| component.component_id().as_bytes().to_vec())
                .collect(),
            ..Self::default()
        }
    }
}
