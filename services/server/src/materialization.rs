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
    pub(crate) async fnboot(
        evaluator: Arc<dyn Evaluator>,
        journal: Journal,
    ) -> anyhow::Result<(Self, Vec<ControllerEffect>)> {
        let (registry_events, tail) = journal
            .load_registry()
            .await
            .map_err(storage_error)?;
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
            let (actor, resumed) = materialized
                .load_actor(registration.intent.clone())
                .await?;
            effects.extend(resumed);
            materialized
                .actors
                .write()
                .await
                .insert(registration.intent.id(), Arc::new(actor));
        }
        Ok((materialized, effects))
    }

    pub(crate) async fnapply(&self, command: Command) -> anyhow::Result<ApplyResult> {
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
                    self.record_retirement(graph_id, expected_generation).await?;
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

    pub(crate) async fnsnapshot(&self, graph_id: GraphId) -> Option<MaterializedCore> {
        let actor = self.actors.read().await.get(&graph_id).cloned()?;
        Some(actor.snapshot().await)
    }

    pub(crate) async fnsnapshots(&self) -> Vec<MaterializedCore> {
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
            Core::new(Arc::clone(&self.evaluator)),
            self.journal.clone(),
            StreamPosition::default(),
        );
        let prepared = actor.prepare(Command::CreateGraph(new)).await?;
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
            if registry.graphs.contains_key(&graph_id) {
                drop(registry);
                let registration = self
                    .registry
                    .lock()
                    .await
                    .graphs
                    .get(&graph_id)
                    .cloned()
                    .expect("registration still exists");
                let (loaded, _) = self.load_actor(registration.intent).await?;
                self.actors.write().await.insert(graph_id, Arc::new(loaded));
                return Err(anyhow::anyhow!("graph already exists"));
            }
            let event = RegistryEvent::GraphRegistered(intent.clone());
            let ack = self
                .journal
                .append_registry(registry.tail, &event)
                .await
                .map_err(storage_error)?;
            registry.tail = ack.tail();
            registry.graphs.insert(
                graph_id,
                RegistryGraph {
                    intent: intent.clone(),
                    retired: false,
                },
            );
        }

        let result = actor.commit(prepared).await?;
        self.actors
            .write()
            .await
            .insert(graph_id, Arc::new(actor));
        Ok(result)
    }

    async fn apply_existing(&self, graph_id: GraphId, command: Command) -> anyhow::Result<ApplyResult> {
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
        let ack = self
            .journal
            .append_registry(registry.tail, &event)
            .await
            .map_err(storage_error)?;
        registry.tail = ack.tail();
        registry
            .graphs
            .get_mut(&graph_id)
            .expect("registration was checked")
            .retired = true;
        Ok(())
    }

    async fn load_actor(
        &self,
        registered_intent: GraphIntent,
    ) -> anyhow::Result<(GraphActor, Vec<ControllerEffect>)> {
        let graph_id = registered_intent.id();
        let (mut events, mut tail) = self
            .journal
            .load_with_tail(graph_id)
            .await
            .map_err(storage_error)?;
        if events.is_empty() {
            let created = CoreEvent::GraphCreated(registered_intent);
            let ack = self
                .journal
                .append(graph_id, tail, &created)
                .await
                .map_err(storage_error)?;
            tail = ack.tail();
            events.push(created);
        }
        let state = MaterializedCore::fold(&events);
        let actor = GraphActor::new(
            graph_id,
            Core::from_materialized(Arc::clone(&self.evaluator), state),
            self.journal.clone(),
            tail,
        );
        let effects = actor.resume().await?;
        Ok((actor, effects))
    }
}

struct GraphActor {
    graph_id: GraphId,
    journal: Journal,
    state: Mutex<ActorState>,
}

struct ActorState {
    core: Core,
    tail: StreamPosition,
}

struct PreparedTransition {
    candidate: Core,
    transition: Transition,
}

impl GraphActor {
    fn new(graph_id: GraphId, core: Core, journal: Journal, tail: StreamPosition) -> Self {
        Self {
            graph_id,
            journal,
            state: Mutex::new(ActorState { core, tail }),
        }
    }

    async fn apply(&self, command: Command) -> anyhow::Result<ApplyResult> {
        let mut state = self.state.lock().await;
        let mut candidate = state.core.clone();
        let transition = candidate
            .handle(command)
            .await
            .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        self.commit_locked(
            &mut state,
            PreparedTransition {
                candidate,
                transition,
            },
        )
        .await
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

    async fn commit(&self, prepared: PreparedTransition) -> anyhow::Result<ApplyResult> {
        let mut state = self.state.lock().await;
        self.commit_locked(&mut state, prepared).await
    }

    async fn commit_locked(
        &self,
        state: &mut ActorState,
        prepared: PreparedTransition,
    ) -> anyhow::Result<ApplyResult> {
        let durable = prepared
            .transition
            .events()
            .iter()
            .filter(|event| is_durable(event))
            .cloned()
            .collect::<Vec<_>>();
        if !durable.is_empty() {
            let ack = self
                .journal
                .append_all(self.graph_id, state.tail, &durable)
                .await
                .map_err(storage_error)?;
            state.tail = ack.tail();
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
            let ack = self
                .journal
                .append_all(self.graph_id, state.tail, &durable)
                .await
                .map_err(storage_error)?;
            state.tail = ack.tail();
        }
        state.core = candidate;
        Ok(transition.effects().to_vec())
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

fn storage_error(error: impl std::fmt::Display) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}
