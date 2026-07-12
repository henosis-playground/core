use henosis_types as domain;

use super::super::ConversionError;
use super::super::graph_id;
use super::super::invalid;
use super::super::missing;
use super::super::spec_hash;
use crate::proto::henosis::v1 as pb;
use crate::proto::henosis::v1::__buffa::view;

impl TryFrom<&view::GraphView<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphView<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: graph_id(value.id, "graph.id")?,
            generation: value
                .generation
                .ok_or_else(|| missing("graph.generation"))?,
            component_spec_hashes: value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "graph.component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| invalid("graph", error))
    }
}

impl TryFrom<&view::GraphSnapshotV1View<'_>> for domain::Graph {
    type Error = ConversionError;

    fn try_from(value: &view::GraphSnapshotV1View<'_>) -> Result<Self, Self::Error> {
        domain::Graph::new(domain::NewGraph {
            id: graph_id(value.id, "graph.id")?,
            generation: value
                .generation
                .ok_or_else(|| missing("graph.generation"))?,
            component_spec_hashes: value
                .component_spec_hashes
                .iter()
                .map(|item| spec_hash(Some(item), "graph.component_spec_hashes"))
                .collect::<Result<Vec<_>, _>>()?,
        })
        .map_err(|error| invalid("graph", error))
    }
}

impl From<&domain::Graph> for pb::Graph {
    fn from(value: &domain::Graph) -> Self {
        Self {
            id: Some(value.id().to_bytes().to_vec()),
            generation: Some(value.generation()),
            component_spec_hashes: value
                .components()
                .map(|component| component.spec_hash().as_bytes().to_vec())
                .collect(),
            ..Self::default()
        }
    }
}
