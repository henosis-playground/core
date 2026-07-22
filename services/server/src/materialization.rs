use std::collections::BTreeMap;
use std::sync::Arc;

use henosis_journal::Journal;
use henosis_journal::RegistryEvent;
use henosis_journal::is_durable;
use henosis_orchestrator::Command;
use henosis_orchestrator::ControllerEffect;
use henosis_orchestrator::Core;
use henosis_orchestrator::MaterializedCore;
use henosis_orchestrator::Transition;
use henosis_storage::StorageDomainError;
use henosis_storage::StreamPosition;
use henosis_types::CoreEvent;
use henosis_types::Evaluator;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphIntent;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

#[derive(Clone)]
pub(crate) struct MaterializedGraphs {
    evaluator: Arc<dyn Evaluator>,
    journal: Journal,
    registry: Arc<Mutex<RegistryState>>,
    actors: Arc<RwLock<BTreeMap<GraphId, Arc<GraphActor>>>>,
}

pub(crate) struct ApplyResult {
    pub graph_id: GraphId,
    pub transition: Transition,
    pub state: MaterializedCore,
}

impl MaterializedGraphs {
    pub(crate) async fn boot(
        evaluator: Arc<dyn Evaluator>,
        journal: Journal,
    ) -> anyhow::Result<(Self, Vec<ControllerEffect>)> {
        let (registry_events, tail) = journal.load_registry().await.map_err(storage_error)?;
        let registry = RegistryState::fold(tail, &registry_events);
        let registered = registry.graphs.values().cloned().collect::<Vec<_>>();
        let materialized = Self {
            evaluator,
            journal,
            registry: Arc::new(Mutex::new(registry)),
            actors: Arc::new(RwLock::new(BTreeMap::new())),
        };

        let mut effects = Vec::new();
        for registration in registered {
            let Some((actor, resumed)) =
                materialized.load_actor(registration.intent.clone()).await?
            else {
                continue;
            };
            effects.extend(resumed);
            let state = actor.snapshot().await;
            let retired_generation = state
                .graph(registration.intent.id())
                .filter(|graph| graph.is_retired())
                .map(|graph| graph.intent().generation());
            materialized
                .actors
                .write()
                .await
                .insert(registration.intent.id(), Arc::new(actor));
            if let Some(generation) = retired_generation {
                materialized
                    .record_retirement(registration.intent.id(), generation)
                    .await?;
            }
        }
        Ok((materialized, effects))
    }

    pub(crate) async fn apply(&self, command: Command) -> anyhow::Result<ApplyResult> {
        match command {
            Command::CreateGraph(new) => self.create_graph(new).await,
            Command::RetireGraph {
                graph_id,
                expected_generation,
            } => {
                let result = self
                    .apply_existing(
                        graph_id,
                        Command::RetireGraph {
                            graph_id,
                            expected_generation,
                        },
                    )
                    .await?;
                if result
                    .state
                    .graph(graph_id)
                    .is_some_and(henosis_orchestrator::GraphState::is_retired)
                {
                    self.record_retirement(graph_id, expected_generation)
                        .await?;
                }
                Ok(result)
            }
            Command::UpdateGraph {
                graph_id,
                expected_generation,
                components,
            } => {
                self.apply_existing(
                    graph_id,
                    Command::UpdateGraph {
                        graph_id,
                        expected_generation,
                        components,
                    },
                )
                .await
            }
            Command::ReportController(report) => {
                let graph_id = report.graph_id();
                self.apply_existing(graph_id, Command::ReportController(report))
                    .await
            }
            Command::CheckQuiescence(graph_id) => {
                self.apply_existing(graph_id, Command::CheckQuiescence(graph_id))
                    .await
            }
        }
    }

    pub(crate) async fn snapshot(&self, graph_id: GraphId) -> Option<MaterializedCore> {
        let actor = self.actors.read().await.get(&graph_id).cloned()?;
        Some(actor.snapshot().await)
    }

    pub(crate) async fn snapshots(&self) -> Vec<MaterializedCore> {
        let actors = self
            .actors
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut states = Vec::with_capacity(actors.len());
        for actor in actors {
            states.push(actor.snapshot().await);
        }
        states
    }

    async fn create_graph(
        &self,
        new: henosis_types::NewGraphIntent,
    ) -> anyhow::Result<ApplyResult> {
        let graph_id = new.id;
        if let Some(actor) = self.actors.read().await.get(&graph_id).cloned() {
            return actor.apply(Command::CreateGraph(new)).await;
        }

        let actor = GraphActor::new(
            graph_id,
            Arc::clone(&self.evaluator),
            Core::new(Arc::clone(&self.evaluator)),
            self.journal.clone(),
            StreamPosition::default(),
        );
        let command = Command::CreateGraph(new);
        let prepared = actor.prepare(command.clone()).await?;
        let intent = prepared
            .transition
            .events()
            .iter()
            .find_map(|event| match event {
                CoreEvent::GraphCreated(intent) => Some(intent.clone()),
                _ => None,
            })
            .expect("create transition contains the accepted graph intent");

        {
            let mut registry = self.registry.lock().await;
            loop {
                let event = if let Some(registration) = registry.graphs.get(&graph_id).cloned() {
                    if let Some((loaded, _)) = self.load_actor(registration.intent.clone()).await? {
                        self.actors.write().await.insert(graph_id, Arc::new(loaded));
                        return Err(anyhow::anyhow!("graph already exists"));
                    }
                    if registration.intent == intent {
                        break;
                    }
                    RegistryEvent::GraphRegistrationSuperseded(intent.clone())
                } else {
                    RegistryEvent::GraphRegistered(intent.clone())
                };
                match self.journal.append_registry(registry.tail, &event).await {
                    Ok(ack) => {
                        registry.tail = ack.tail();
                        registry.graphs.insert(
                            graph_id,
                            RegistryGraph {
                                intent: intent.clone(),
                                retired: false,
                            },
                        );
                        break;
                    }
                    Err(error) if is_occ_conflict(&error) => {
                        let (events, tail) =
                            self.journal.load_registry().await.map_err(storage_error)?;
                        *registry = RegistryState::fold(tail, &events);
                    }
                    Err(error) => return Err(storage_error(error)),
                }
            }
        }

        let result = actor.apply(command).await?;
        self.actors.write().await.insert(graph_id, Arc::new(actor));
        Ok(result)
    }

    async fn apply_existing(
        &self,
        graph_id: GraphId,
        command: Command,
    ) -> anyhow::Result<ApplyResult> {
        let actor = self
            .actors
            .read()
            .await
            .get(&graph_id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("graph does not exist"))?;
        actor.apply(command).await
    }

    async fn record_retirement(
        &self,
        graph_id: GraphId,
        last_generation: Generation,
    ) -> anyhow::Result<()> {
        let mut registry = self.registry.lock().await;
        loop {
            let Some(registration) = registry.graphs.get(&graph_id) else {
                return Err(anyhow::anyhow!("retired graph is absent from the registry"));
            };
            if registration.retired {
                return Ok(());
            }
            let event = RegistryEvent::GraphRetired {
                graph_id,
                last_generation,
            };
            match self.journal.append_registry(registry.tail, &event).await {
                Ok(ack) => {
                    registry.tail = ack.tail();
                    registry
                        .graphs
                        .get_mut(&graph_id)
                        .expect("registration was checked")
                        .retired = true;
                    return Ok(());
                }
                Err(error) if is_occ_conflict(&error) => {
                    let (events, tail) =
                        self.journal.load_registry().await.map_err(storage_error)?;
                    *registry = RegistryState::fold(tail, &events);
                }
                Err(error) => return Err(storage_error(error)),
            }
        }
    }

    async fn load_actor(
        &self,
        registered_intent: GraphIntent,
    ) -> anyhow::Result<Option<(GraphActor, Vec<ControllerEffect>)>> {
        let graph_id = registered_intent.id();
        let (events, tail) = self
            .journal
            .load_with_tail(graph_id)
            .await
            .map_err(storage_error)?;
        if events.is_empty() {
            return Ok(None);
        }
        let state = MaterializedCore::fold(&events);
        let actor = GraphActor::new(
            graph_id,
            Arc::clone(&self.evaluator),
            Core::from_materialized(Arc::clone(&self.evaluator), state),
            self.journal.clone(),
            tail,
        );
        let effects = actor.resume().await?;
        Ok(Some((actor, effects)))
    }
}

struct GraphActor {
    graph_id: GraphId,
    evaluator: Arc<dyn Evaluator>,
    journal: Journal,
    state: Mutex<ActorState>,
}

struct ActorState {
    core: Core,
    tail: StreamPosition,
    needs_reload: bool,
}

struct PreparedTransition {
    candidate: Core,
    transition: Transition,
}

enum CommitFailure {
    Occ,
    Other(anyhow::Error),
}

impl GraphActor {
    fn new(
        graph_id: GraphId,
        evaluator: Arc<dyn Evaluator>,
        core: Core,
        journal: Journal,
        tail: StreamPosition,
    ) -> Self {
        Self {
            graph_id,
            evaluator,
            journal,
            state: Mutex::new(ActorState {
                core,
                tail,
                needs_reload: false,
            }),
        }
    }

    async fn apply(&self, command: Command) -> anyhow::Result<ApplyResult> {
        let mut state = self.state.lock().await;
        loop {
            self.ensure_authoritative(&mut state).await?;
            let mut candidate = state.core.clone();
            let transition = candidate
                .handle(command.clone())
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            match self
                .commit_locked(
                    &mut state,
                    PreparedTransition {
                        candidate,
                        transition,
                    },
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(CommitFailure::Occ) => {}
                Err(CommitFailure::Other(error)) => return Err(error),
            }
        }
    }

    async fn prepare(&self, command: Command) -> anyhow::Result<PreparedTransition> {
        let state = self.state.lock().await;
        let mut candidate = state.core.clone();
        drop(state);
        let transition = candidate
            .handle(command)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        Ok(PreparedTransition {
            candidate,
            transition,
        })
    }

    async fn commit_locked(
        &self,
        state: &mut ActorState,
        prepared: PreparedTransition,
    ) -> Result<ApplyResult, CommitFailure> {
        let durable = prepared
            .transition
            .events()
            .iter()
            .filter(|event| is_durable(event))
            .cloned()
            .collect::<Vec<_>>();
        if !durable.is_empty() {
            match self
                .journal
                .append_all(self.graph_id, state.tail, &durable)
                .await
            {
                Ok(ack) => state.tail = ack.tail(),
                Err(error) => {
                    state.needs_reload = true;
                    let occ = is_occ_conflict(&error);
                    let original = storage_error(error);
                    if let Err(reload) = self.ensure_authoritative(state).await {
                        return Err(CommitFailure::Other(original.context(format!(
                            "authoritative graph reload also failed: {reload}"
                        ))));
                    }
                    if occ {
                        return Err(CommitFailure::Occ);
                    }
                    return Err(CommitFailure::Other(original));
                }
            }
        }
        state.core = prepared.candidate;
        Ok(ApplyResult {
            graph_id: self.graph_id,
            transition: prepared.transition,
            state: state.core.state().clone(),
        })
    }

    async fn resume(&self) -> anyhow::Result<Vec<ControllerEffect>> {
        let mut state = self.state.lock().await;
        loop {
            self.ensure_authoritative(&mut state).await?;
            let mut candidate = state.core.clone();
            let transition = candidate
                .resume_graph(self.graph_id)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            let durable = transition
                .events()
                .iter()
                .filter(|event| is_durable(event))
                .cloned()
                .collect::<Vec<_>>();
            if !durable.is_empty() {
                match self
                    .journal
                    .append_all(self.graph_id, state.tail, &durable)
                    .await
                {
                    Ok(ack) => state.tail = ack.tail(),
                    Err(error) => {
                        state.needs_reload = true;
                        let occ = is_occ_conflict(&error);
                        let original = storage_error(error);
                        if let Err(reload) = self.ensure_authoritative(&mut state).await {
                            return Err(original.context(format!(
                                "authoritative graph reload also failed: {reload}"
                            )));
                        }
                        if occ {
                            continue;
                        }
                        return Err(original);
                    }
                }
            }
            state.core = candidate;
            return Ok(transition.effects().to_vec());
        }
    }

    async fn ensure_authoritative(&self, state: &mut ActorState) -> anyhow::Result<()> {
        if !state.needs_reload {
            return Ok(());
        }
        let (events, tail) = self
            .journal
            .load_with_tail(self.graph_id)
            .await
            .map_err(storage_error)?;
        let materialized = MaterializedCore::fold(&events);
        state.core = Core::from_materialized(Arc::clone(&self.evaluator), materialized);
        state.tail = tail;
        state.needs_reload = false;
        Ok(())
    }

    async fn snapshot(&self) -> MaterializedCore {
        self.state.lock().await.core.state().clone()
    }
}

#[derive(Clone)]
struct RegistryGraph {
    intent: GraphIntent,
    retired: bool,
}

struct RegistryState {
    tail: StreamPosition,
    graphs: BTreeMap<GraphId, RegistryGraph>,
}

impl RegistryState {
    fn fold(tail: StreamPosition, events: &[RegistryEvent]) -> Self {
        let mut graphs = BTreeMap::new();
        for event in events {
            match event {
                RegistryEvent::GraphRegistered(intent) => {
                    graphs.insert(
                        intent.id(),
                        RegistryGraph {
                            intent: intent.clone(),
                            retired: false,
                        },
                    );
                }
                RegistryEvent::GraphRegistrationSuperseded(intent) => {
                    let registration = graphs
                        .get_mut(&intent.id())
                        .expect("superseded registry graph was registered first");
                    registration.intent = intent.clone();
                    registration.retired = false;
                }
                RegistryEvent::GraphRetired { graph_id, .. } => {
                    graphs
                        .get_mut(graph_id)
                        .expect("retired registry graph was registered first")
                        .retired = true;
                }
            }
        }
        Self { tail, graphs }
    }
}

fn is_occ_conflict(
    error: &faultline::Error<StorageDomainError, anyhow::Error, anyhow::Error>,
) -> bool {
    matches!(
        error,
        faultline::Error::Domain(StorageDomainError::CasConflict { .. })
    )
}

fn storage_error(error: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use henosis_testkit::AppendFault;
    use henosis_testkit::ComponentProgram;
    use henosis_testkit::MemS2;
    use henosis_testkit::ProgramEvaluator;
    use henosis_testkit::ReadFault;
    use henosis_types::BundleRef;
    use henosis_types::ComponentIntent;
    use henosis_types::ComponentName;
    use henosis_types::ComponentRevision;
    use henosis_types::GraphSourcePolicy;
    use henosis_types::NewComponentIntent;
    use henosis_types::NewGraphIntent;

    use super::*;

    #[tokio::test]
    async fn actor_reloads_after_commit_unknown_resolution_read_fails() {
        let storage = MemS2::default();
        let (graphs, _evaluator, bundle) = materialized(storage.clone()).await;
        let graph_id = GraphId::from_bytes([41; 16]);
        graphs
            .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
            .await
            .expect("initial graph is created");

        let sessions_before_fault = storage.opened_sessions();
        storage.script([AppendFault::CommitThenTimeout]);
        storage.script_reads([ReadFault::Reject]);
        let first = graphs
            .apply(Command::UpdateGraph {
                graph_id,
                expected_generation: Generation::new(1).expect("one is non-zero"),
                components: graph(graph_id, bundle, 2).components,
            })
            .await;
        assert!(
            first.is_err(),
            "the failed resolution remains visible to the caller"
        );

        let result = graphs
            .apply(Command::UpdateGraph {
                graph_id,
                expected_generation: Generation::new(2).expect("two is non-zero"),
                components: graph(graph_id, bundle, 3).components,
            })
            .await
            .expect("the reloaded actor accepts the next command without restart");
        assert_eq!(
            result
                .state
                .graph(graph_id)
                .expect("graph exists")
                .intent()
                .generation()
                .ordinal(),
            3
        );
        assert_eq!(storage.opened_sessions(), sessions_before_fault + 1);
    }

    #[tokio::test]
    async fn stale_actor_discards_its_poisoned_session_before_reappend() {
        let storage = MemS2::default();
        let evaluator = evaluator();
        let bundle = evaluator.register(ComponentProgram {
            resources: Vec::new(),
            static_outputs: BTreeMap::new(),
        });
        let (first, _) =
            MaterializedGraphs::boot(evaluator.clone(), Journal::new(Arc::new(storage.clone())))
                .await
                .expect("first materializer boots");
        let graph_id = GraphId::from_bytes([45; 16]);
        first
            .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
            .await
            .expect("graph is created");
        let (second, _) =
            MaterializedGraphs::boot(evaluator, Journal::new(Arc::new(storage.clone())))
                .await
                .expect("second materializer boots from the graph stream");

        first
            .apply(Command::UpdateGraph {
                graph_id,
                expected_generation: Generation::new(1).expect("one is non-zero"),
                components: graph(graph_id, bundle, 2).components,
            })
            .await
            .expect("first actor advances the graph");
        let sessions_before_conflict = storage.opened_sessions();
        assert!(
            second
                .apply(Command::UpdateGraph {
                    graph_id,
                    expected_generation: Generation::new(1).expect("one is non-zero"),
                    components: graph(graph_id, bundle, 3).components,
                })
                .await
                .is_err(),
            "the re-derived stale command is rejected after authoritative reload"
        );
        assert_eq!(storage.opened_sessions(), sessions_before_conflict + 1);

        let result = second
            .apply(Command::UpdateGraph {
                graph_id,
                expected_generation: Generation::new(2).expect("two is non-zero"),
                components: graph(graph_id, bundle, 3).components,
            })
            .await
            .expect("a fresh session accepts the re-derived update");
        assert_eq!(component_revision(&result.state, graph_id), revision(3));
        assert_eq!(storage.opened_sessions(), sessions_before_conflict + 2);
    }

    #[tokio::test]
    async fn stale_registry_reloads_before_rederiving_on_a_fresh_session() {
        let storage = MemS2::default();
        let evaluator = evaluator();
        let bundle = evaluator.register(ComponentProgram {
            resources: Vec::new(),
            static_outputs: BTreeMap::new(),
        });
        let (first, _) =
            MaterializedGraphs::boot(evaluator.clone(), Journal::new(Arc::new(storage.clone())))
                .await
                .expect("first materializer boots");
        let (second, _) =
            MaterializedGraphs::boot(evaluator, Journal::new(Arc::new(storage.clone())))
                .await
                .expect("second materializer boots with the same empty registry");

        first
            .apply(Command::CreateGraph(graph(
                GraphId::from_bytes([46; 16]),
                bundle,
                1,
            )))
            .await
            .expect("first materializer advances the registry");
        let sessions_before_conflict = storage.opened_sessions();
        let graph_id = GraphId::from_bytes([47; 16]);
        let result = second
            .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
            .await
            .expect("stale registry reloads and appends through a fresh session");
        assert_eq!(component_revision(&result.state, graph_id), revision(1));
        assert_eq!(storage.opened_sessions(), sessions_before_conflict + 3);
    }

    #[tokio::test]
    async fn failed_registry_append_does_not_reserve_graph_id() {
        let storage = MemS2::default();
        let (graphs, _evaluator, bundle) = materialized(storage.clone()).await;
        let graph_id = GraphId::from_bytes([42; 16]);
        storage.script([AppendFault::RejectBeforeCommit]);
        assert!(
            graphs
                .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
                .await
                .is_err()
        );

        let result = graphs
            .apply(Command::CreateGraph(graph(graph_id, bundle, 2)))
            .await
            .expect("a different create succeeds after registry append rejection");
        assert_eq!(component_revision(&result.state, graph_id), revision(2));
    }

    #[tokio::test]
    async fn abandoned_registration_is_superseded_by_different_create() {
        let storage = MemS2::default();
        let (graphs, evaluator, bundle) = materialized(storage.clone()).await;
        let graph_id = GraphId::from_bytes([43; 16]);
        storage.script([AppendFault::Acknowledge, AppendFault::RejectBeforeCommit]);
        assert!(
            graphs
                .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
                .await
                .is_err()
        );

        let result = graphs
            .apply(Command::CreateGraph(graph(graph_id, bundle, 2)))
            .await
            .expect("a different create supersedes the abandoned registration");
        assert_eq!(component_revision(&result.state, graph_id), revision(2));

        let (rebooted, _) = MaterializedGraphs::boot(evaluator, Journal::new(Arc::new(storage)))
            .await
            .expect("registry supersession replays");
        let snapshot = rebooted.snapshot(graph_id).await.expect("graph replays");
        assert_eq!(component_revision(&snapshot, graph_id), revision(2));
    }

    #[tokio::test]
    async fn committed_graph_stream_wins_over_different_follow_up_create() {
        let storage = MemS2::default();
        let (graphs, _evaluator, bundle) = materialized(storage.clone()).await;
        let graph_id = GraphId::from_bytes([44; 16]);
        storage.script([AppendFault::Acknowledge, AppendFault::CommitThenTimeout]);
        storage.script_reads([ReadFault::Reject]);
        assert!(
            graphs
                .apply(Command::CreateGraph(graph(graph_id, bundle, 1)))
                .await
                .is_err()
        );

        assert!(
            graphs
                .apply(Command::CreateGraph(graph(graph_id, bundle, 2)))
                .await
                .is_err(),
            "the accepted graph stream cannot be superseded by registry intent"
        );
        let snapshot = graphs
            .snapshot(graph_id)
            .await
            .expect("accepted graph is loaded");
        assert_eq!(component_revision(&snapshot, graph_id), revision(1));
    }

    async fn materialized(
        storage: MemS2,
    ) -> (MaterializedGraphs, Arc<ProgramEvaluator>, BundleRef) {
        let evaluator = evaluator();
        let bundle = evaluator.register(ComponentProgram {
            resources: Vec::new(),
            static_outputs: BTreeMap::new(),
        });
        let (graphs, effects) =
            MaterializedGraphs::boot(evaluator.clone(), Journal::new(Arc::new(storage)))
                .await
                .expect("empty materialization boots");
        assert!(effects.is_empty());
        (graphs, evaluator, bundle)
    }

    fn evaluator() -> Arc<ProgramEvaluator> {
        Arc::new(ProgramEvaluator::default())
    }

    fn graph(graph_id: GraphId, bundle: BundleRef, revision_byte: u8) -> NewGraphIntent {
        let component = ComponentIntent::new(NewComponentIntent {
            name: ComponentName::new("api").expect("component name is valid"),
            revision: revision(revision_byte),
            bundle,
            inputs: Vec::new(),
            outputs: Vec::new(),
            compiled_dependencies: Vec::new(),
            source: None,
        })
        .expect("component intent is valid");
        NewGraphIntent {
            id: graph_id,
            components: vec![component],
            source_policy: GraphSourcePolicy::AcceptLocal,
        }
    }

    fn component_revision(state: &MaterializedCore, graph_id: GraphId) -> ComponentRevision {
        state
            .graph(graph_id)
            .expect("graph exists")
            .intent()
            .components()
            .next()
            .expect("graph has a component")
            .revision()
            .clone()
    }

    fn revision(byte: u8) -> ComponentRevision {
        ComponentRevision::new(format!("{byte:02x}").repeat(32))
            .expect("revision is a SHA-256 string")
    }
}
