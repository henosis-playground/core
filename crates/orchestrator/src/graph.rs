use std::sync::Arc;

use anyhow::Error;
use faultline::Error as Fault;
use henosis_proto::api::add_request_hash;
use henosis_proto::api::create_request_hash;
use henosis_proto::api::remove_request_hash;
use henosis_proto::api::retire_request_hash;
use henosis_proto::api::update_request_hash;
use types::domain::AddComponents;
use types::domain::Component;
use types::domain::CreateGraph;
use types::domain::DurableGraphState;
use types::domain::Graph;
use types::domain::GraphEditError;
use types::domain::GraphEvent;
use types::domain::GraphGenerationState;
use types::domain::GraphHistory;
use types::domain::GraphState;
use types::domain::MutationKind;
use types::domain::MutationResponse;
use types::domain::NewComponent;
use types::domain::NewGraph;
use types::domain::RemoveComponents;
use types::domain::RequestUuid;
use types::domain::RetireGraph;
use types::domain::UpdateComponents;

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
    // === Initialization ===

    /// Rebuild immutable specs and graph histories, then resume delivery.
    pub async fn initialize(
        self: &Arc<Self>,
    ) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        let catalog = self
            .journal
            .component_catalog()
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

    // === CreateComponent ===

    /// Durably create a component and its initial specification before graph
    /// use.
    pub async fn component_create(
        &self,
        command: NewComponent,
    ) -> Result<Component, Fault<OrchestratorError, Error, Error>> {
        let registered = self
            .journal
            .component_register(command)
            .await
            .map_err(|error| error.squash())?;
        let catalog = self
            .journal
            .component_catalog()
            .await
            .map_err(|error| error.squash())?;
        *self.specs.write().await = catalog;
        Ok(registered)
    }

    // === CreateGraph ===

    /// Accept generation one and add the graph to the discovery registry.
    pub async fn graph_create(
        self: &Arc<Self>,
        command: CreateGraph,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let hash = create_request_hash(&command);
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
                    hash,
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
            component_ids: command.component_ids().to_vec(),
        })
        .map_err(|_| invalid_argument("graph.invalid"))?;
        self.validate(&graph).await?;
        let event = GraphEvent::Created {
            graph: graph.clone(),
            request_id,
            request_hash: hash,
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

    // === EditGraph ===

    pub async fn graph_add_components(
        self: &Arc<Self>,
        command: AddComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let hash = add_request_hash(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::AddComponents,
            hash,
            |graph| graph.add(command.component_ids()),
        )
        .await
    }

    pub async fn graph_update_components(
        self: &Arc<Self>,
        command: UpdateComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let hash = update_request_hash(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::UpdateComponents,
            hash,
            |graph| graph.replace(command.replacements()),
        )
        .await
    }

    pub async fn graph_remove_components(
        self: &Arc<Self>,
        command: RemoveComponents,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let hash = remove_request_hash(&command);
        self.graph_edit(
            command.graph_id(),
            command.request_id(),
            command.expected_generation(),
            MutationKind::RemoveComponents,
            hash,
            |graph| graph.remove(command.component_ids()),
        )
        .await
    }

    // === ReadGraph ===

    pub async fn graph_get(
        &self,
        graph_id: types::domain::GraphUuid,
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
        command: types::domain::GetGraphGeneration,
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
                    .get(component.component_id())
                    .cloned()
                    .ok_or_else(|| invariant("generation references an unregistered spec"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let generation_lifecycle = durable.lifecycle();
        let state = GraphState::new(durable, reports).map_err(|error| {
            Fault::<OrchestratorError, Error, Error>::Invariant(Error::new(error))
        })?;
        Ok(GraphGenerationState::new(
            state,
            components,
            generation_lifecycle,
            history.last_published_generation_at(command.generation()),
        ))
    }

    // === RetireGraph ===

    pub async fn graph_retire(
        self: &Arc<Self>,
        command: RetireGraph,
    ) -> Result<(types::domain::GraphUuid, u64), Fault<OrchestratorError, Error, Error>> {
        let hash = retire_request_hash(command);
        let graph_id = command.graph_id();
        let request_id = command.request_id();
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        if let Some(receipt) = history.request(request_id) {
            if receipt.kind() != MutationKind::Retire || receipt.hash() != hash {
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
            request_hash: hash,
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

    // === Internal operations ===

    async fn graph_edit(
        self: &Arc<Self>,
        graph_id: types::domain::GraphUuid,
        request_id: RequestUuid,
        expected_generation: u64,
        kind: MutationKind,
        hash: blake3::Hash,
        apply: impl FnOnce(&mut Graph) -> Result<(), GraphEditError> + Send,
    ) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
        let runtime = self.runtime(graph_id).await;
        let mut cached = runtime.history.lock().await;
        self.ensure_loaded(graph_id, &mut cached).await?;
        let history = cached.as_ref().expect("history was loaded");
        if history.request(request_id).is_some() {
            return replay_graph(history, request_id, kind, hash);
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
            request_hash: hash,
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
        graph_id: types::domain::GraphUuid,
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
        graph_id: types::domain::GraphUuid,
        request_id: RequestUuid,
        retired: bool,
    ) -> Result<(), Fault<OrchestratorError, Error, Error>> {
        self.journal
            .graph_registry_ensure(graph_id, request_id, retired)
            .await
            .map_err(|error| error.squash())
    }
}

// === Replay helpers ===

fn replay_graph(
    history: &GraphHistory,
    request_id: RequestUuid,
    kind: MutationKind,
    hash: blake3::Hash,
) -> Result<Graph, Fault<OrchestratorError, Error, Error>> {
    let receipt = history
        .request(request_id)
        .ok_or_else(|| already_exists("graph.already_exists"))?;
    if receipt.kind() != kind || receipt.hash() != hash {
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
