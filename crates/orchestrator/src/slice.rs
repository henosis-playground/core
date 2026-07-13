use std::collections::BTreeSet;

use thiserror::Error;
use types::domain::Component;
use types::domain::ComponentCatalog;
use types::domain::ComponentOutputs;
use types::domain::ConnectorKey;
use types::domain::GraphHistory;
use types::domain::GraphSlice;

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub(crate) enum SliceError {
    #[error("requested graph state is not retained")]
    StateNotFound,
    #[error("graph references an unregistered component spec")]
    SpecNotFound,
    #[error("computed graph slice violates domain invariants")]
    Invalid,
}

pub(crate) fn compute_slice(
    history: &GraphHistory,
    specs: &ComponentCatalog,
    sequence: u64,
    connector: &ConnectorKey,
) -> Result<GraphSlice, SliceError> {
    let state = history
        .state_at(sequence)
        .ok_or(SliceError::StateNotFound)?;
    let graph = state.graph();
    let components = graph
        .components()
        .map(|component| component.component_id())
        .filter_map(|hash| specs.get(hash))
        .filter(|registered| registered.spec().connector() == connector)
        .cloned()
        .collect::<Vec<_>>();
    let owned = components
        .iter()
        .map(Component::id)
        .collect::<BTreeSet<_>>();
    let mut upstream = BTreeSet::new();
    let mut pending = components
        .iter()
        .flat_map(|component| component.spec().depends_on().iter().copied())
        .collect::<Vec<_>>();
    while let Some(hash) = pending.pop() {
        if !upstream.insert(hash) {
            continue;
        }
        let dependency = specs.get(hash).ok_or(SliceError::SpecNotFound)?;
        pending.extend(dependency.spec().depends_on().iter().copied());
    }
    upstream.retain(|hash| !owned.contains(hash));
    let upstream_outputs = state
        .published_outputs()
        .filter(|published| published.generation() == graph.generation())
        .flat_map(|published| published.outputs().iter())
        .filter(|output| upstream.contains(&output.component_id()))
        .cloned()
        .collect::<Vec<ComponentOutputs>>();
    GraphSlice::new(
        graph.id(),
        graph.generation(),
        connector.clone(),
        components,
        upstream_outputs,
        sequence,
    )
    .map_err(|_| SliceError::Invalid)
}

pub(crate) fn connectors_for_sequence(
    history: &GraphHistory,
    specs: &ComponentCatalog,
    sequence: u64,
) -> Result<BTreeSet<ConnectorKey>, SliceError> {
    let state = history
        .state_at(sequence)
        .ok_or(SliceError::StateNotFound)?;
    let generation = state.graph().generation();
    let mut connectors = BTreeSet::new();
    for selected in [generation.checked_sub(1), Some(generation)]
        .into_iter()
        .flatten()
    {
        if let Some(graph) = history.generation(selected) {
            for component in graph.components() {
                let registered = specs
                    .get(component.component_id())
                    .ok_or(SliceError::SpecNotFound)?;
                connectors.insert(registered.spec().connector().clone());
            }
        }
    }
    Ok(connectors)
}

pub(crate) fn superseded_components(
    history: &GraphHistory,
    specs: &ComponentCatalog,
    sequence: u64,
    connector: &ConnectorKey,
) -> Result<Vec<Component>, SliceError> {
    let current = history
        .state_at(sequence)
        .ok_or(SliceError::StateNotFound)?
        .graph();
    let mut superseded = Vec::new();
    for graph in history.generations() {
        if graph.generation() >= current.generation() {
            continue;
        }
        for component in graph.components() {
            if current.contains(component.component_id()) {
                continue;
            }
            let registered = specs
                .get(component.component_id())
                .ok_or(SliceError::SpecNotFound)?;
            if registered.spec().connector() == connector
                && !superseded
                    .iter()
                    .any(|item: &Component| item.id() == registered.id())
            {
                superseded.push(registered.clone());
            }
        }
    }
    Ok(superseded)
}

#[cfg(test)]
mod tests {
    use std::time::UNIX_EPOCH;

    use blake3::Hash;
    use types::domain::ComponentCatalog;
    use types::domain::ComponentOutputs;
    use types::domain::ComponentUuid;
    use types::domain::Graph;
    use types::domain::GraphEvent;
    use types::domain::GraphHistory;
    use types::domain::GraphUuid;
    use types::domain::MutationKind;
    use types::domain::NewComponentSpec;
    use types::domain::NewGraph;
    use types::domain::OutputPublication;
    use types::domain::PublicationUuid;
    use types::domain::RequestUuid;
    use types::domain::SequencedGraphEvent;

    use super::*;

    fn registered(name: &str, connector: &str, depends_on: Vec<ComponentUuid>) -> Component {
        Component::new(
            ComponentUuid::from_bytes([name.as_bytes()[0]; 16]),
            0,
            UNIX_EPOCH,
            UNIX_EPOCH,
            NewComponentSpec::new(name, connector.parse().unwrap(), &[], depends_on, &[]).unwrap(),
        )
    }

    #[test]
    fn slices_use_transitive_outputs_only_from_the_current_generation() {
        let source = registered("source", "source", Vec::new());
        let middle = registered("middle", "middle", vec![source.id()]);
        let target = registered("target", "target", vec![middle.id()]);
        let mut specs = ComponentCatalog::default();
        specs.apply(source.clone()).unwrap();
        specs.apply(middle.clone()).unwrap();
        specs.apply(target.clone()).unwrap();
        let graph_id = GraphUuid::from_bytes(1_u128.to_be_bytes());
        let graph = Graph::new(NewGraph {
            id: graph_id,
            generation: 1,
            component_ids: vec![source.id(), middle.id(), target.id()],
        })
        .unwrap();
        let mut history = GraphHistory::new(graph_id);
        history
            .apply(SequencedGraphEvent::new(
                0,
                GraphEvent::Created {
                    graph: graph.clone(),
                    request_id: RequestUuid::from_bytes(2_u128.to_be_bytes()),
                    request_hash: Hash::from_bytes([2; 32]),
                },
            ))
            .unwrap();
        history
            .apply(SequencedGraphEvent::new(
                1,
                GraphEvent::OutputsPublished(OutputPublication {
                    generation: 1,
                    input_sequence: 0,
                    connector: "source".parse().unwrap(),
                    outputs: vec![ComponentOutputs::new(
                        source.id(),
                        br#"{"url":"https://example.test"}"#.to_vec(),
                    )],
                    request_id: RequestUuid::from_bytes(3_u128.to_be_bytes()),
                    request_hash: Hash::from_bytes([3; 32]),
                    publication_id: PublicationUuid::from_bytes(4_u128.to_be_bytes()),
                    publication_hash: Hash::from_bytes([4; 32]),
                }),
            ))
            .unwrap();
        let target_connector = "target".parse().unwrap();
        let slice = compute_slice(&history, &specs, 1, &target_connector).unwrap();
        assert_eq!(slice.upstream_outputs().len(), 1);

        let graph_v2 = Graph::new(NewGraph {
            id: graph_id,
            generation: 2,
            component_ids: graph.component_ids(),
        })
        .unwrap();
        history
            .apply(SequencedGraphEvent::new(
                2,
                GraphEvent::GenerationAccepted {
                    graph: graph_v2,
                    request_id: RequestUuid::from_bytes(5_u128.to_be_bytes()),
                    mutation_kind: MutationKind::UpdateComponents,
                    request_hash: Hash::from_bytes([5; 32]),
                },
            ))
            .unwrap();
        let slice = compute_slice(&history, &specs, 2, &target_connector).unwrap();
        assert_eq!(slice.upstream_outputs().len(), 0);
    }
}
