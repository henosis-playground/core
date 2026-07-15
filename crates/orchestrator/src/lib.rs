//! Deterministic coordination state machine for the D26 plan/controller model.

mod telemetry;

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use faultline::Error;
use faultline::Never;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error as ThisError;
use tracing::instrument;

use henosis_types::ComponentIntent;
use henosis_types::ComponentName;
use henosis_types::ControllerCommand;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::CoreEvent;
use henosis_types::EvaluationRequest;
use henosis_types::Evaluator;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphIntent;
use henosis_types::NewGraphIntent;
use henosis_types::NewPlan;
use henosis_types::OutputKey;
use henosis_types::OutputMode;
use henosis_types::OutputPublication;
use henosis_types::OutputRecord;
use henosis_types::OutputRef;
use henosis_types::OutputSnapshot;
use henosis_types::OutputSource;
use henosis_types::Plan;
use henosis_types::PublicationId;
use henosis_types::Resource;
use henosis_types::ResourceDisposition;
use henosis_types::ResourceId;
use henosis_types::Retirement;
use henosis_types::Stall;
use henosis_types::Supersession;

// === Commands and effects ===

#[derive(Clone, Debug)]
pub enum Command {
    CreateGraph(NewGraphIntent),
    UpdateGraph {
        graph_id: GraphId,
        expected_generation: Generation,
        components: Vec<ComponentIntent>,
    },
    ReportController(ControllerReport),
    CheckQuiescence(GraphId),
    RetireGraph {
        graph_id: GraphId,
        expected_generation: Generation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControllerEffect {
    controller: ControllerName,
    command: ControllerCommand,
}

impl ControllerEffect {
    #[must_use]
    pub const fn new(controller: ControllerName, command: ControllerCommand) -> Self {
        Self {
            controller,
            command,
        }
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub const fn command(&self) -> &ControllerCommand {
        &self.command
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Transition {
    events: Vec<CoreEvent>,
    effects: Vec<ControllerEffect>,
}

impl Transition {
    #[must_use]
    pub fn events(&self) -> &[CoreEvent] {
        &self.events
    }

    #[must_use]
    pub fn effects(&self) -> &[ControllerEffect] {
        &self.effects
    }

    fn extend(&mut self, other: Self) {
        self.events.extend(other.events);
        self.effects.extend(other.effects);
    }
}

#[derive(Clone, Debug, ThisError, Eq, PartialEq)]
pub enum CommandError {
    #[error("graph does not exist")]
    GraphNotFound,
    #[error("graph already exists")]
    GraphAlreadyExists,
    #[error("graph has been retired")]
    GraphRetired,
    #[error("expected generation {expected}, current generation is {actual}")]
    GenerationConflict {
        expected: Generation,
        actual: Generation,
    },
    #[error("controller report targets a stale plan")]
    StaleControllerReport,
    #[error("controller report does not cover exactly its current holistic slice")]
    IncompleteControllerReport,
    #[error("controller published an undeclared or non-observed output")]
    InvalidObservedOutput,
    #[error("component evaluation failed: {0}")]
    Evaluation(String),
    #[error("graph intent is invalid: {0}")]
    InvalidIntent(String),
}

// === Live state machine ===

pub struct Core {
    evaluator: Arc<dyn Evaluator>,
    state: MaterializedCore,
}

impl Core {
    #[must_use]
    pub fn new(evaluator: Arc<dyn Evaluator>) -> Self {
        Self {
            evaluator,
            state: MaterializedCore::default(),
        }
    }

    #[must_use]
    pub const fn state(&self) -> &MaterializedCore {
        &self.state
    }

    #[instrument(
        name = "handle core command",
        skip_all,
        err(level = "warn"),
        fields(
            { telemetry::COMMAND_TYPE } = command_type(&command),
            { telemetry::GRAPH_ID } = command_graph_id(&command).map(|id| id.to_string()),
        )
    )]
    pub async fn handle(
        &mut self,
        command: Command,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        match command {
            Command::CreateGraph(new) => self.create_graph(new).await,
            Command::UpdateGraph {
                graph_id,
                expected_generation,
                components,
            } => {
                self.update_graph(graph_id, expected_generation, components)
                    .await
            }
            Command::ReportController(report) => self.report_controller(report).await,
            Command::CheckQuiescence(graph_id) => self.check_quiescence(graph_id),
            Command::RetireGraph {
                graph_id,
                expected_generation,
            } => self.retire_graph(graph_id, expected_generation),
        }
    }

    async fn create_graph(
        &mut self,
        new: NewGraphIntent,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        if self.state.graphs.contains_key(&new.id) {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GraphAlreadyExists));
        }
        let graph = GraphIntent::new(new)
            .map_err(|error| {
                Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidIntent(
                    error.to_string(),
                ))
            })?;
        let graph_id = graph.id();
        self.state
            .graphs
            .insert_unique(GraphState::new(graph.clone()))
            .expect("graph absence was checked");
        let mut transition = Transition {
            events: vec![CoreEvent::GraphCreated(graph)],
            effects: Vec::new(),
        };
        transition.extend(self.evaluate_graph(graph_id).await?);
        Ok(transition)
    }

    async fn update_graph(
        &mut self,
        graph_id: GraphId,
        expected_generation: Generation,
        components: Vec<ComponentIntent>,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        let graph = self.graph(graph_id)?;
        if graph.retired {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GraphRetired));
        }
        if graph.intent.generation() != expected_generation {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GenerationConflict {
                expected: expected_generation,
                actual: graph.intent.generation(),
            }));
        }
        let updated = graph
            .intent
            .replace_components(components)
            .map_err(|error| {
                Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidIntent(
                    error.to_string(),
                ))
            })?;
        {
            let mut graph = self.graph_mut(graph_id)?;
            graph.intent = updated.clone();
            graph.pending.clear();
        }
        let mut transition = Transition {
            events: vec![CoreEvent::GraphUpdated(updated)],
            effects: Vec::new(),
        };
        transition.extend(self.evaluate_graph(graph_id).await?);
        Ok(transition)
    }

    async fn report_controller(
        &mut self,
        report: ControllerReport,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        let graph_id = report.graph_id();
        let graph = self.graph(graph_id)?;
        let plan = graph.plan.as_ref().ok_or_else(|| {
            Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::anyhow!("controller report arrived before a plan"))
        })?;
        if report.generation() != graph.intent.generation()
            || report.plan_digest() != plan.digest()
        {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::StaleControllerReport));
        }
        let expected = plan
            .resources()
            .filter(|resource| resource.controller() == report.controller())
            .map(Resource::id)
            .collect::<BTreeSet<_>>();
        let actual = report
            .dispositions()
            .map(ResourceDisposition::resource_id)
            .collect::<BTreeSet<_>>();
        if expected != actual {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::IncompleteControllerReport));
        }
        if let Some(publication_id) = report.publication_id()
            && graph.publications.contains(&publication_id)
        {
            return Ok(Transition::default());
        }

        let mut published = Vec::new();
        for output in report.outputs() {
            let resource = plan
                .resource(output.key_value().resource_id())
                .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidObservedOutput))?;
            if resource.controller() != report.controller() {
                return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidObservedOutput));
            }
            let declaration = resource
                .output(output.key_value().output())
                .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidObservedOutput))?;
            if !matches!(declaration.mode(), OutputMode::Observed) {
                return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidObservedOutput));
            }
            published.push(OutputRecord::new(
                OutputKey::new(
                    report.generation(),
                    OutputRef::new(
                        resource.path().instance().clone(),
                        output.key_value().output().clone(),
                    ),
                ),
                output.value().clone(),
                OutputSource::Observed {
                    resource_id: resource.id(),
                },
            ));
        }

        let publication = report.publication_id().map(|publication_id| {
            OutputPublication::new(
                graph_id,
                report.generation(),
                report.controller().clone(),
                publication_id,
                published.clone(),
            )
        });
        {
            let mut graph = self.graph_mut(graph_id)?;
            graph
                .reports
                .insert_overwrite(LatestControllerReport(report.clone()));
            graph.pending.remove(report.controller());
            if let Some(publication_id) = report.publication_id() {
                graph.publications.insert(publication_id);
            }
            for output in published {
                graph.outputs.insert_overwrite(output);
            }
        }

        let mut transition = Transition {
            events: vec![CoreEvent::ControllerReported(report)],
            effects: Vec::new(),
        };
        if let Some(publication) = publication {
            transition
                .events
                .push(CoreEvent::OutputsPublished(publication));
        }
        transition.extend(self.evaluate_graph(graph_id).await?);
        Ok(transition)
    }

    fn check_quiescence(
        &mut self,
        graph_id: GraphId,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        let graph = self.graph(graph_id)?;
        if graph.pending.is_empty()
            && let Some(plan) = &graph.plan
            && !plan.is_complete()
            && let Some(cycle) = blocked_cycle(plan)
        {
            let stall = Stall::new(graph_id, plan.generation(), cycle);
            self.graph_mut(graph_id)?.stall = Some(stall.clone());
            return Ok(Transition {
                events: vec![CoreEvent::StallDetected(stall)],
                effects: Vec::new(),
            });
        }
        Ok(Transition::default())
    }

    fn retire_graph(
        &mut self,
        graph_id: GraphId,
        expected_generation: Generation,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        let graph = self.graph(graph_id)?;
        if graph.intent.generation() != expected_generation {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GenerationConflict {
                expected: expected_generation,
                actual: graph.intent.generation(),
            }));
        }
        if graph.retired {
            return Ok(Transition::default());
        }
        let resources = graph
            .plan
            .as_ref()
            .into_iter()
            .flat_map(Plan::resources)
            .fold(
                BTreeMap::<ControllerName, Vec<ResourceId>>::new(),
                |mut grouped, resource| {
                    grouped
                        .entry(resource.controller().clone())
                        .or_default()
                        .push(resource.id());
                    grouped
                },
            );
        let effects = resources
            .into_iter()
            .map(|(controller, resources)| {
                ControllerEffect::new(
                    controller.clone(),
                    ControllerCommand::Retire(Retirement {
                        graph_id,
                        last_generation: expected_generation,
                        controller,
                        resources,
                    }),
                )
            })
            .collect();
        self.graph_mut(graph_id)?.retired = true;
        Ok(Transition {
            events: vec![CoreEvent::GraphRetired {
                graph_id,
                last_generation: expected_generation,
            }],
            effects,
        })
    }

    async fn evaluate_graph(
        &mut self,
        graph_id: GraphId,
    ) -> Result<Transition, Error<CommandError, Never, anyhow::Error>> {
        let intent = self.graph(graph_id)?.intent.clone();
        let generation = intent.generation();
        let mut rounds = 0_usize;
        let plan = loop {
            rounds = rounds.saturating_add(1);
            if rounds > intent.components().len().saturating_add(1) {
                return Err(Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::anyhow!(
                    "static output fixed point exceeded component count"
                )));
            }
            let snapshot = OutputSnapshot::for_generation(&self.graph(graph_id)?.outputs, generation);
            let mut resources = Vec::new();
            let mut blocked = Vec::new();
            for component in intent.components() {
                let evaluated = self
                    .evaluator
                    .evaluate(EvaluationRequest::new(
                        graph_id,
                        generation,
                        component.name().clone(),
                        component.bundle(),
                        snapshot.clone(),
                    ))
                    .await
                    .map_err(|error| Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::Evaluation(error.to_string())))?;
                resources.extend_from_slice(evaluated.resources());
                if let Some(marker) = evaluated.blocked_marker() {
                    blocked.push(marker);
                }
            }
            let mut inserted_static = false;
            {
                let mut graph = self.graph_mut(graph_id)?;
                for resource in &resources {
                    for declaration in resource.outputs() {
                        if let OutputMode::Static(value) = declaration.mode() {
                            let record = OutputRecord::new(
                                OutputKey::new(
                                    generation,
                                    OutputRef::new(
                                        resource.path().instance().clone(),
                                        declaration.name().clone(),
                                    ),
                                ),
                                value.clone(),
                                OutputSource::Static,
                            );
                            let key = record.key_value().clone();
                            if graph.outputs.get(&key) != Some(&record) {
                                graph.outputs.insert_overwrite(record);
                                inserted_static = true;
                            }
                        }
                    }
                }
            }
            let plan = Plan::new(NewPlan {
                generation,
                resources,
                blocked,
            })
            .map_err(|error| Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::Error::new(error)))?;
            if !inserted_static {
                break plan;
            }
        };

        let previous = self.graph(graph_id)?.plan.clone();
        if previous
            .as_ref()
            .is_some_and(|old| old.generation() == generation && old.digest() == plan.digest())
        {
            return self.check_quiescence(graph_id);
        }
        let effects = dispatch_effects(graph_id, previous.as_ref(), &plan);
        let pending = effects
            .iter()
            .filter_map(|effect| match effect.command() {
                ControllerCommand::Reconcile(_) => Some(effect.controller().clone()),
                ControllerCommand::Supersede(_) | ControllerCommand::Retire(_) => None,
            })
            .collect();
        {
            let mut graph = self.graph_mut(graph_id)?;
            graph.plan = Some(plan.clone());
            graph.pending = pending;
            graph.stall = None;
        }
        let mut transition = Transition {
            events: vec![CoreEvent::PlanAccepted { graph_id, plan }],
            effects,
        };
        transition.extend(self.check_quiescence(graph_id)?);
        Ok(transition)
    }

    fn graph(
        &self,
        graph_id: GraphId,
    ) -> Result<&GraphState, Error<CommandError, Never, anyhow::Error>> {
        self.state
            .graphs
            .get(&graph_id)
            .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GraphNotFound))
    }

    fn graph_mut(
        &mut self,
        graph_id: GraphId,
    ) -> Result<
        iddqd::id_ord_map::RefMut<'_, GraphState>,
        Error<CommandError, Never, anyhow::Error>,
    > {
        self.state
            .graphs
            .get_mut(&graph_id)
            .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GraphNotFound))
    }
}

fn dispatch_effects(
    graph_id: GraphId,
    previous: Option<&Plan>,
    plan: &Plan,
) -> Vec<ControllerEffect> {
    let mut current = BTreeMap::<ControllerName, Vec<Resource>>::new();
    for resource in plan.resources() {
        current
            .entry(resource.controller().clone())
            .or_default()
            .push(resource.clone());
    }
    let mut removed = BTreeMap::<ControllerName, Vec<ResourceId>>::new();
    if let Some(previous) = previous {
        for resource in previous.resources() {
            if plan.resource(resource.id()).is_none() {
                removed
                    .entry(resource.controller().clone())
                    .or_default()
                    .push(resource.id());
            }
        }
    }
    let mut effects = Vec::new();
    for (controller, resources) in current {
        let superseded = removed.remove(&controller).unwrap_or_default();
        effects.push(ControllerEffect::new(
            controller.clone(),
            ControllerCommand::Reconcile(ControllerSlice::new(
                graph_id,
                plan.generation(),
                plan.digest(),
                controller,
                resources,
                superseded,
            )),
        ));
    }
    for (controller, resources) in removed {
        effects.push(ControllerEffect::new(
            controller.clone(),
            ControllerCommand::Supersede(Supersession {
                graph_id,
                generation: plan.generation(),
                controller,
                resources,
            }),
        ));
    }
    effects
}

fn blocked_cycle(plan: &Plan) -> Option<Vec<ComponentName>> {
    let blocked = plan
        .blocked()
        .map(|marker| (marker.component().clone(), marker.blocked_on().to_vec()))
        .collect::<BTreeMap<_, _>>();
    for start in blocked.keys() {
        let mut path = Vec::new();
        let mut positions = BTreeMap::<ComponentName, usize>::new();
        if let Some(cycle) = visit_blocked(start, &blocked, &mut path, &mut positions) {
            return Some(cycle);
        }
    }
    None
}

fn visit_blocked(
    component: &ComponentName,
    blocked: &BTreeMap<ComponentName, Vec<OutputRef>>,
    path: &mut Vec<ComponentName>,
    positions: &mut BTreeMap<ComponentName, usize>,
) -> Option<Vec<ComponentName>> {
    if let Some(position) = positions.get(component).copied() {
        let mut cycle = path[position..].to_vec();
        cycle.push(component.clone());
        return Some(cycle);
    }
    let dependencies = blocked.get(component)?;
    positions.insert(component.clone(), path.len());
    path.push(component.clone());
    for dependency in dependencies {
        if blocked.contains_key(dependency.component())
            && let Some(cycle) = visit_blocked(dependency.component(), blocked, path, positions)
        {
            return Some(cycle);
        }
    }
    path.pop();
    positions.remove(component);
    None
}

// === Replay materialization ===

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MaterializedCore {
    graphs: IdOrdMap<GraphState>,
}

impl MaterializedCore {
    #[must_use]
    pub fn fold(events: &[CoreEvent]) -> Self {
        let mut state = Self::default();
        for event in events {
            state.apply(event);
        }
        state
    }

    pub fn apply(&mut self, event: &CoreEvent) {
        match event {
            CoreEvent::GraphCreated(intent) => {
                self.graphs
                    .insert_unique(GraphState::new(intent.clone()))
                    .expect("a graph may be created only once");
            }
            CoreEvent::GraphUpdated(intent) => {
                let mut graph = self
                    .graphs
                    .get_mut(&intent.id())
                    .expect("updated graph must already exist");
                assert_eq!(
                    intent.generation(),
                    graph.intent.generation().next(),
                    "generation ordinals must be monotonic while folding"
                );
                graph.intent = intent.clone();
            }
            CoreEvent::PlanAccepted { graph_id, plan } => {
                let mut graph = self
                    .graphs
                    .get_mut(graph_id)
                    .expect("planned graph must already exist");
                assert_eq!(
                    graph.intent.generation(),
                    plan.generation(),
                    "accepted plan must match current generation"
                );
                graph.plan = Some(plan.clone());
            }
            CoreEvent::ControllerReported(report) => {
                self.graphs
                    .get_mut(&report.graph_id())
                    .expect("reported graph must already exist")
                    .reports
                    .insert_overwrite(LatestControllerReport(report.clone()));
            }
            CoreEvent::OutputsPublished(publication) => {
                let mut graph = self
                    .graphs
                    .get_mut(&publication.graph_id())
                    .expect("published graph must already exist");
                assert_eq!(
                    graph.intent.generation(),
                    publication.generation(),
                    "generation fencing must hold while folding"
                );
                graph.publications.insert(publication.publication_id());
                for output in publication.outputs() {
                    graph.outputs.insert_overwrite(output.clone());
                }
            }
            CoreEvent::StallDetected(stall) => {
                self.graphs
                    .get_mut(&stall.graph_id())
                    .expect("stalled graph must already exist")
                    .stall = Some(stall.clone());
            }
            CoreEvent::GraphRetired {
                graph_id,
                last_generation,
            } => {
                let mut graph = self
                    .graphs
                    .get_mut(graph_id)
                    .expect("retired graph must already exist");
                assert_eq!(graph.intent.generation(), *last_generation);
                graph.retired = true;
            }
        }
    }

    #[must_use]
    pub fn graph(&self, graph_id: GraphId) -> Option<&GraphState> {
        self.graphs.get(&graph_id)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphState {
    intent: GraphIntent,
    plan: Option<Plan>,
    outputs: IdOrdMap<OutputRecord>,
    reports: IdOrdMap<LatestControllerReport>,
    publications: BTreeSet<PublicationId>,
    pending: BTreeSet<ControllerName>,
    stall: Option<Stall>,
    retired: bool,
}

impl GraphState {
    fn new(intent: GraphIntent) -> Self {
        Self {
            intent,
            plan: None,
            outputs: IdOrdMap::new(),
            reports: IdOrdMap::new(),
            publications: BTreeSet::new(),
            pending: BTreeSet::new(),
            stall: None,
            retired: false,
        }
    }

    #[must_use]
    pub const fn intent(&self) -> &GraphIntent {
        &self.intent
    }

    #[must_use]
    pub const fn plan(&self) -> Option<&Plan> {
        self.plan.as_ref()
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &OutputRecord> {
        self.outputs.iter()
    }

    #[must_use]
    pub const fn stall(&self) -> Option<&Stall> {
        self.stall.as_ref()
    }

    #[must_use]
    pub const fn is_retired(&self) -> bool {
        self.retired
    }
}

impl IdOrdItem for GraphState {
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.intent.id()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LatestControllerReport(ControllerReport);

impl IdOrdItem for LatestControllerReport {
    type Key<'a> = &'a ControllerName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.0.controller()
    }
}

fn command_type(command: &Command) -> &'static str {
    match command {
        Command::CreateGraph(_) => "create_graph",
        Command::UpdateGraph { .. } => "update_graph",
        Command::ReportController(_) => "report_controller",
        Command::CheckQuiescence(_) => "check_quiescence",
        Command::RetireGraph { .. } => "retire_graph",
    }
}

fn command_graph_id(command: &Command) -> Option<GraphId> {
    match command {
        Command::CreateGraph(new) => Some(new.id),
        Command::UpdateGraph { graph_id, .. }
        | Command::CheckQuiescence(graph_id)
        | Command::RetireGraph { graph_id, .. } => Some(*graph_id),
        Command::ReportController(report) => Some(report.graph_id()),
    }
}
