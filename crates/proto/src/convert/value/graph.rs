use henosis_types as domain;

use crate::convert::ConversionError;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::GraphView<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphView<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: wire_field!(value.id).required()?.uuid()?,
            generation: wire_field!(value.generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            component_spec_hashes: wire_field!(value.component_spec_hashes)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| wire_field!(value.component_spec_hashes).invalid(error))
    }
}

impl TryFrom<&view::GraphSnapshotV1View<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphSnapshotV1View<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: wire_field!(value.id).required()?.uuid()?,
            generation: wire_field!(value.generation)
                .required()?
                .validate(|generation| *generation > 0, "must be greater than zero")?,
            component_spec_hashes: wire_field!(value.component_spec_hashes)
                .iter()
                .map(|item| item.spec_hash())
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| wire_field!(value.component_spec_hashes).invalid(error))
    }
}

impl From<&domain::Graph> for pb::Graph {
    fn from(value: &domain::Graph) -> Self {
        Self {
            id: Some(value.id().into_bytes().to_vec()),
            generation: Some(value.generation()),
            component_spec_hashes: value
                .components()
                .map(|component| component.spec_hash().as_bytes().to_vec())
                .collect(),
            ..Self::default()
        }
    }
}
