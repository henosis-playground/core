use std::sync::Arc;

use anyhow::Error;
use faultline::Error as Fault;
use henosis_proto::add_fingerprint;
use henosis_proto::create_fingerprint;
use henosis_proto::remove_fingerprint;
use henosis_proto::retire_fingerprint;
use henosis_proto::update_fingerprint;
use henosis_types::AddComponents;
use henosis_types::CreateGraph;
use henosis_types::DurableGraphState;
use henosis_types::Graph;
use henosis_types::GraphEditError;
use henosis_types::GraphEvent;
use henosis_types::GraphHistory;
use henosis_types::GraphState;
use henosis_types::GraphGenerationState;
use henosis_types::MutationKind;
use henosis_types::MutationResponse;
use henosis_types::NewGraph;
use henosis_types::RegisterComponentSpec;
use henosis_types::RegisteredComponentSpec;
use henosis_types::RemoveComponents;
use henosis_types::RequestId;
use henosis_types::RetireGraph;
use henosis_types::UpdateComponents;

use crate::Orchestrator;
use crate::OrchestratorError;
use crate::WatchEvent;
use crate::error::already_exists;
use crate::error::failed_precondition;
use crate::error::invalid_argument;
use crate::error::invariant;
use crate::error::map_journal;
use crate::error::not_found;
use crate::validation::GraphValidationError;
use crate::validation::validate_graph;

impl Orchestrator {
    /// Rebuild immutable specs and graph histories, then resume delivery.
    pub async fn initialize(
        self: &Arc<Self>,
    ) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        let catalog = self
            .journal
            .component_spec_catalog()
            .await
            .map_err(|error| error.squash())?;
        *self.specs.write().await = catalog;
        let graph_ids = self
            .journal
            .graph_list()
            .await
            .map_err(|error| error.squash())?;
        for graph_id in graph_ids {
            let runtime = self.runtime(graph_id).await;
            let history = self
                .journal
                .graph_load(graph_id)
                .await
                .map_err(map_journal)?;
            *runtime.reports.write().await = history.reports().cloned().collect();
            *runtime.history.lock().await = Some(history);
            self.schedule_delivery(graph_id).await;
        }
        Ok(())
    }

    /// Durably register a content-addressed component spec before graph use.
    pub async fn component_spec_register(
        &self,
        command: RegisterComponentSpec,
    ) -> Result<RegisteredComponentSpec, Fault<OrchestratorError, Error, Error>> {
        let registered = self
            .journal
            .component_spec_register(command.into_component())
            .await
            .map_err(|error| error.squash())?;
        let catalog = self
            .journal
            .component_spec_catalog()
            .await
            .map_err(|error| error.squash())?;
        *self.specs.write().await = catalog;
        Ok(registered)
    }

    /// Accept generation one and add the graph to the discovery registry.
    pub async fn graph_create(
        self: &Arc<Self>,
        command: CreateGraph,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let fingerprint = create_fingerprint(&command);
        let graph_id = command.graph_id();
        let request_id = command.request_id();
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        match self.ensure_loaded(graph_id, &mut cached).await {
            Ok(()) => {
                let graph = replay_graph(
                    cached.as_ref().expect("history was loaded"),
                    request_id,
                    MutationKind::Create,
                    fingerprint,
                )?;
                self.registry_ensure(graph_id, request_id, false).await?;
                return Ok(graph);
            }
            Err(Fault::Domain(OrchestratorError::NotFound)) => {}
            Err(error) => return Err(error),
        }
        let graph = Graph::new(NewGraph {
            id: graph_id,
            generation: 1,
            component_spec_hashes: command.component_spec_hashes().to_vec(),
        })
        .map_err(|_| invalid_argument("graph.invalid"))?;
        self.validate(&graph).await?;
        let event = GraphEvent::Created {
            graph: graph.clone(),
            request_id,
            request_fingerprint: fingerprint,
        };
        self.journal
            .graph_append(graph_id, 0, &event)
            .await
            .map_err(map_journal)?;
        self.registry_ensure(graph_id, request_id, false).await?;
        let history = self
            .journal
            .graph_load(graph_id)
            .await
            .map_err(map_journal)?;
        publish_history(&runtime, &mut cached, history)?;
        drop(cached);
        self.schedule_delivery(graph_id).await;
        Ok(graph)
    }

    pub async fn graph_add_components(
        self: &Arc<Self>,
        command: AddComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let fingerprint = add_fingerprint(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::AddComponents,
            fingerprint,
            |graph| graph.add(command.component_spec_hashes()),
        )
        .await
    }

    pub async fn graph_update_components(
        self: &Arc<Self>,
        command: UpdateComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let fingerprint = update_fingerprint(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::UpdateComponents,
            fingerprint,
            |graph| graph.replace(command.replacements()),
        )
        .await
    }

    pub async fn graph_remove_components(
        self: &Arc<Self>,
        command: RemoveComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let fingerprint = remove_fingerprint(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::RemoveComponents,
            fingerprint,
            |graph| graph.remove(command.component_spec_hashes()),
        )
        .await
    }

    pub async fn graph_get(
        &self,
        graph_id: henosis_types::GraphId,
    ) -> Result<GraphState, Fault<OrchestratorError, Error, Error>> {
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let durable = cached
            .as_ref()
            .and_then(GraphHistory::desired_state)
            .cloned()
            .ok_or_else(|| invariant("loaded graph has no state"))?;
        let reports = runtime.reports.read().await.iter().cloned().collect();
        GraphState::new(durable, reports).map_err(|error| Fault::Invariant(Error::new(error)))
    }

    pub async fn graph_generation_get(
        &self,
        command: henosis_types::GetGraphGeneration,
    ) -> Result<GraphGenerationState, Fault<OrchestratorError, Error, Error>> {
        let runtime = self.runtime(command.graph_id()).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(command.graph_id(), &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        let durable = history
            .durable_for_generation(command.generation())
            .ok_or(Fault::<OrchestratorError, Error, Error>::Domain(
                OrchestratorError::NotFound,
            ))?;
        let reports = history
            .reports_for_generation(command.generation())
            .cloned()
            .collect();
        let graph = durable.graph();
        let specs = self.specs.read().await;
        let components = graph
            .components()
            .map(|component| {
                specs
                    .get(component.spec_hash())
                    .cloned()
                    .ok_or_else(|| invariant("generation references an unregistered spec"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let current_lifecycle = history
            .desired_state()
            .ok_or_else(|| invariant("loaded graph has no state"))?
            .lifecycle();
        let state = GraphState::new(durable, reports).map_err(|error| {
            Fault::<OrchestratorError, Error, Error>::Invariant(Error::new(error))
        })?;
        Ok(GraphGenerationState::new(
            state,
            components,
            current_lifecycle,
            history.last_published_generation(),
        ))
    }

    pub async fn graph_retire(
        self: &Arc<Self>,
        command: RetireGraph,
    ) -> Result<(henosis_types::GraphId, u64), Fault<OrchestratorError, Error, Error>> {
        let fingerprint = retire_fingerprint(command);
        let graph_id = command.graph_id();
        let request_id = command.request_id();
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        if let Some(receipt) = history.request(request_id) {
            if receipt.kind() != MutationKind::Retire || receipt.fingerprint() != fingerprint {
                return Err(already_exists("request_id.reused"));
            }
            if let MutationResponse::Retired {
                graph_id,
                last_generation,
            } = receipt.response()
            {
                return Ok((*graph_id, *last_generation));
            }
        }
        if history.is_retired() {
            return Err(failed_precondition("graph.retired"));
        }
        let generation = current_state(history)?.graph().generation();
        if command.expected_generation() != generation {
            return Err(Fault::Domain(OrchestratorError::Aborted {
                current_generation: generation,
            }));
        }
        let event = GraphEvent::Retired {
            graph_id,
            last_generation: generation,
            request_id,
            request_fingerprint: fingerprint,
        };
        self.journal
            .graph_append(graph_id, history.next_sequence(), &event)
            .await
            .map_err(map_journal)?;
        self.registry_ensure(graph_id, request_id, true).await?;
        let history = self
            .journal
            .graph_load(graph_id)
            .await
            .map_err(map_journal)?;
        publish_history(&runtime, &mut cached, history)?;
        drop(cached);
        self.schedule_delivery(graph_id).await;
        Ok((graph_id, generation))
    }

    async fn graph_edit(
        self: &Arc<Self>,
        graph_id: henosis_types::GraphId,
        request_id: RequestId,
        expected_generation: u64,
        kind: MutationKind,
        fingerprint: henosis_types::Fingerprint,
        apply: impl FnOnce(&mut Graph) -> Result<(), GraphEditError> + Send,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        if history.request(request_id).is_some() {
            return replay_graph(history, request_id, kind, fingerprint);
        }
        if history.is_retired() {
            return Err(failed_precondition("graph.retired"));
        }
        let current = current_state(history)?.graph();
        if current.generation() != expected_generation {
            return Err(Fault::Domain(OrchestratorError::Aborted {
                current_generation: current.generation(),
            }));
        }
        let mut graph = current.clone();
        apply(&mut graph).map_err(map_edit_error)?;
        graph.advance_generation();
        self.validate(&graph).await?;
        let event = GraphEvent::GenerationAccepted {
            graph: graph.clone(),
            request_id,
            mutation_kind: kind,
            request_fingerprint: fingerprint,
        };
        self.journal
            .graph_append(graph_id, history.next_sequence(), &event)
            .await
            .map_err(map_journal)?;
        let history = self
            .journal
            .graph_load(graph_id)
            .await
            .map_err(map_journal)?;
        publish_history(&runtime, &mut cached, history)?;
        drop(cached);
        self.schedule_delivery(graph_id).await;
        Ok(graph)
    }

    async fn validate(&self, graph: &Graph) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        let specs = self.specs.read().await;
        validate_graph(graph, &specs, self).map_err(|error| match error {
            GraphValidationError::Invalid(diagnostics) => {
                Fault::Domain(OrchestratorError::InvalidArgument { diagnostics })
            }
            GraphValidationError::FailedPrecondition(diagnostics) => {
                Fault::Domain(OrchestratorError::FailedPrecondition { diagnostics })
            }
        })
    }

    pub(crate) async fn ensure_loaded(
        &self,
        graph_id: henosis_types::GraphId,
        cached: &mut Option<GraphHistory>,
    ) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        if cached.is_none() {
            *cached = Some(
                self.journal
                    .graph_load(graph_id)
                    .await
                    .map_err(map_journal)?,
            );
        }
        Ok(())
    }

    async fn registry_ensure(
        &self,
        graph_id: henosis_types::GraphId,
        request_id: RequestId,
        retired: bool,
    ) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        self.journal
            .graph_registry_ensure(graph_id, request_id, retired)
            .await
            .map_err(|error| error.squash())
    }
}

fn replay_graph(
    history: &GraphHistory,
    request_id: RequestId,
    kind: MutationKind,
    fingerprint: henosis_types::Fingerprint,
) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
    let receipt = history
        .request(request_id)
        .ok_or_else(|| already_exists("graph.already_exists"))?;
    if receipt.kind() != kind || receipt.fingerprint() != fingerprint {
        return Err(already_exists("request_id.reused"));
    }
    match receipt.response() {
        MutationResponse::Graph(graph) => Ok(graph.clone()),
        MutationResponse::Retired { .. } => Err(already_exists("request_id.reused")),
    }
}

fn current_state(
    history: &GraphHistory,
) -> Result<&DurableGraphState, Fault<OrchestratorError, Error, Error>> {
    history
        .desired_state()
        .ok_or_else(|| invariant("loaded graph has no state"))
}

pub(crate) fn publish_history(
    runtime: &crate::GraphRuntime,
    cached: &mut Option<GraphHistory>,
    history: GraphHistory,
) -> Result<(), Fault<OrchestratorError, Error, Error>> {
    let sequence = history
        .head_sequence()
        .ok_or_else(|| invariant("loaded graph has no records"))?;
    let state = history
        .desired_state()
        .cloned()
        .ok_or_else(|| invariant("loaded graph has no state"))?;
    *cached = Some(history);
    let _ = runtime.events.send(WatchEvent::Durable { sequence, state });
    Ok(())
}

fn map_edit_error(error: GraphEditError) -> Fault<OrchestratorError, Error, Error> {
    match error {
        GraphEditError::NotFound => not_found(),
        GraphEditError::EmptyEdit => invalid_argument("edit.empty"),
        GraphEditError::DuplicateInput => invalid_argument("edit.component.duplicate"),
        GraphEditError::AlreadyPresent => invalid_argument("edit.component.already_present"),
    }
}
