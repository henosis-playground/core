use std::collections::BTreeSet;

use henosis_types::ComponentOutputs;
use henosis_types::ConnectorKey;
use henosis_types::GraphHistory;
use henosis_types::GraphSlice;
use henosis_types::RegisteredComponentSpec;
use henosis_types::SpecCatalog;
use thiserror::Error;

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
    specs: &SpecCatalog,
    sequence: u64,
    connector: &ConnectorKey,
) -> Result<GraphSlice, SliceError> {
    let state = history
        .state_at(sequence)
        .ok_or(SliceError::StateNotFound)?;
    let graph = state.graph();
    let components = graph
        .components()
        .map(|component| component.spec_hash())
        .filter_map(|hash| specs.get(hash))
        .filter(|registered| registered.spec().connector() == connector)
        .cloned()
        .collect::<Vec<_>>();
    let owned = components
        .iter()
        .map(RegisteredComponentSpec::hash)
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
        .filter(|output| upstream.contains(&output.component_spec_hash()))
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
    specs: &SpecCatalog,
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
                    .get(component.spec_hash())
                    .ok_or(SliceError::SpecNotFound)?;
                connectors.insert(registered.spec().connector().clone());
            }
        }
    }
    Ok(connectors)
}

pub(crate) fn superseded_components(
    history: &GraphHistory,
    specs: &SpecCatalog,
    sequence: u64,
    connector: &ConnectorKey,
) -> Result<Vec<RegisteredComponentSpec>, SliceError> {
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
            if current.contains(component.spec_hash()) {
                continue;
            }
            let registered = specs
                .get(component.spec_hash())
                .ok_or(SliceError::SpecNotFound)?;
            if registered.spec().connector() == connector
                && !superseded
                    .iter()
                    .any(|item: &RegisteredComponentSpec| item.hash() == registered.hash())
            {
                superseded.push(registered.clone());
            }
        }
    }
    Ok(superseded)
}

#[cfg(test)]
mod tests {
    use henosis_proto::register_component_spec;
    use henosis_types::ComponentOutputs;
    use henosis_types::ComponentSpec;
    use henosis_types::ComponentSpecHash;
    use henosis_types::Fingerprint;
    use henosis_types::Graph;
    use henosis_types::GraphEvent;
    use henosis_types::GraphHistory;
    use henosis_types::GraphId;
    use henosis_types::MutationKind;
    use henosis_types::NewComponentSpec;
    use henosis_types::NewGraph;
    use henosis_types::OutputPublication;
    use henosis_types::PublicationId;
    use henosis_types::RequestId;
    use henosis_types::SequencedGraphEvent;
    use henosis_types::SpecCatalog;

    use super::*;

    fn registered(
        name: &str,
        connector: &str,
        depends_on: Vec<ComponentSpecHash>,
    ) -> RegisteredComponentSpec {
        register_component_spec(
            ComponentSpec::new(NewComponentSpec {
                name: name.to_owned(),
                connector: connector.parse().unwrap(),
                outputs_schema: Vec::new(),
                depends_on,
                connector_context: Vec::new(),
            })
            .unwrap(),
        )
    }

    #[test]
    fn slices_use_transitive_outputs_only_from_the_current_generation() {
        let source = registered("source", "source", Vec::new());
        let middle = registered("middle", "middle", vec![source.hash()]);
        let target = registered("target", "target", vec![middle.hash()]);
        let mut specs = SpecCatalog::default();
        specs.apply(source.clone()).unwrap();
        specs.apply(middle.clone()).unwrap();
        specs.apply(target.clone()).unwrap();
        let graph_id = GraphId::from_bytes(1_u128.to_be_bytes());
        let graph = Graph::new(NewGraph {
            id: graph_id,
            generation: 1,
            component_spec_hashes: vec![source.hash(), middle.hash(), target.hash()],
        })
        .unwrap();
        let mut history = GraphHistory::new(graph_id);
        history
            .apply(SequencedGraphEvent::new(
                0,
                GraphEvent::Created {
                    graph: graph.clone(),
                    request_id: RequestId::from_bytes(2_u128.to_be_bytes()),
                    request_fingerprint: Fingerprint::from_bytes([2; 32]),
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
                        source.hash(),
                        br#"{"url":"https://example.test"}"#.to_vec(),
                    )],
                    request_id: RequestId::from_bytes(3_u128.to_be_bytes()),
                    request_fingerprint: Fingerprint::from_bytes([3; 32]),
                    publication_id: PublicationId::from_bytes(4_u128.to_be_bytes()),
                    publication_fingerprint: Fingerprint::from_bytes([4; 32]),
                }),
            ))
            .unwrap();
        let target_connector = "target".parse().unwrap();
        let slice = compute_slice(&history, &specs, 1, &target_connector).unwrap();
        assert_eq!(slice.upstream_outputs().len(), 1);

        let graph_v2 = Graph::new(NewGraph {
            id: graph_id,
            generation: 2,
            component_spec_hashes: graph.component_spec_hashes(),
        })
        .unwrap();
        history
            .apply(SequencedGraphEvent::new(
                2,
                GraphEvent::GenerationAccepted {
                    graph: graph_v2,
                    request_id: RequestId::from_bytes(5_u128.to_be_bytes()),
                    mutation_kind: MutationKind::UpdateComponents,
                    request_fingerprint: Fingerprint::from_bytes([5; 32]),
                },
            ))
            .unwrap();
        let slice = compute_slice(&history, &specs, 2, &target_connector).unwrap();
        assert_eq!(slice.upstream_outputs().len(), 0);
    }
}
