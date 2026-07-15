//! Deterministic command/state/effect core for the D26 plan/controller model.

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

use henosis_types::BlockedMarker;
use henosis_types::ComponentInputSource;
use henosis_types::ComponentIntent;
use henosis_types::ComponentName;
use henosis_types::ComponentOutputs;
use henosis_types::ControllerCommand;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::CoreEvent;
use henosis_types::EvaluationAttempt;
use henosis_types::EvaluationRequest;
use henosis_types::EvaluationSnapshot;
use henosis_types::Evaluator;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphIntent;
use henosis_types::InputCell;
use henosis_types::InputCellState;
use henosis_types::NewComponentOutputs;
use henosis_types::NewGraphIntent;
use henosis_types::NewPlan;
use henosis_types::ObservedOutputBinding;
use henosis_types::ObservedOutputKey;
use henosis_types::OutputAvailability;
use henosis_types::OutputKey;
use henosis_types::OutputPublication;
use henosis_types::OutputRecord;
use henosis_types::OutputRef;
use henosis_types::OutputSource;
use henosis_types::Plan;
use henosis_types::PublicationId;
use henosis_types::Resource;
use henosis_types::ResourceDisposition;
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
    #[error("controller published an undeclared observed output")]
    InvalidObservedOutput,
    #[error("component evaluation failed: {0}")]
    Evaluation(String),
    #[error("component result violates the host protocol: {0}")]
    EvaluationProtocol(String),
    #[error("graph intent is invalid: {0}")]
    InvalidIntent(String),
}

// === Live state machine ===

pub struct Core {
    evaluator: Arc<dyn Evaluator>,
    state: MaterializedCore,
    runtime: BTreeMap<GraphId, GraphRuntime>,
}

impl Core {
    #[must_use]
    pub fn new(evaluator: Arc<dyn Evaluator>) -> Self {
        Self {
            evaluator,
            state: MaterializedCore::default(),
            runtime: BTreeMap::new(),
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
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::GraphAlreadyExists,
            ));
        }
        let graph = GraphIntent::new(new).map_err(|error| {
            Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidIntent(
                error.to_string(),
            ))
        })?;
        let graph_id = graph.id();
        self.state
            .graphs
            .insert_unique(GraphState::new(graph.clone()))
            .expect("graph absence was checked");
        self.runtime.insert(graph_id, GraphRuntime::default());
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
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::GraphRetired,
            ));
        }
        if graph.intent.generation() != expected_generation {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::GenerationConflict {
                    expected: expected_generation,
                    actual: graph.intent.generation(),
                },
            ));
        }
        let updated = graph
            .intent
            .replace_components(components)
            .map_err(|error| {
                Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::InvalidIntent(
                    error.to_string(),
                ))
            })?;
        self.graph_mut(graph_id)?.accept_intent(updated.clone());
        self.runtime.insert(graph_id, GraphRuntime::default());
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
            Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::anyhow!(
                "controller report arrived before a plan"
            ))
        })?;
        if report.generation() != graph.intent.generation() || report.plan_digest() != plan.digest()
        {
            // Re-evaluation can replace a plan while controller work for its predecessor is
            // already in flight. The late level report is expected and has no current
            // effect.
            return Ok(Transition::default());
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
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::IncompleteControllerReport,
            ));
        }
        if let Some(publication_id) = report.publication_id()
            && graph.publications.contains(&publication_id)
        {
            return Ok(Transition::default());
        }

        let bindings = self
            .runtime
            .get(&graph_id)
            .expect("runtime exists for every graph")
            .bindings
            .clone();
        let mut published = Vec::new();
        for output in report.outputs() {
            let resource = plan
                .resource(output.key_value().resource_id())
                .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(
                    CommandError::InvalidObservedOutput,
                ))?;
            if resource.controller() != report.controller() {
                return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                    CommandError::InvalidObservedOutput,
                ));
            }
            let declaration = resource.output(output.key_value().output()).ok_or(Error::<
                CommandError,
                Never,
                anyhow::Error,
            >::Domain(
                CommandError::InvalidObservedOutput,
            ))?;
            if declaration.availability() != OutputAvailability::Observed {
                return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                    CommandError::InvalidObservedOutput,
                ));
            }
            let Some(component_output) = bindings.get(output.key_value()).cloned() else {
                continue;
            };
            published.push(OutputRecord::new(
                OutputKey::new(report.generation(), component_output),
                output.value().clone(),
                OutputSource::Observed {
                    resource_id: resource.id(),
                    resource_output: output.key_value().output().clone(),
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
            if let Some(publication_id) = report.publication_id() {
                graph.publications.insert(publication_id);
            }
            for output in published {
                graph.outputs.insert_overwrite(output);
            }
        }
        self.runtime
            .get_mut(&graph_id)
            .expect("runtime exists for every graph")
            .pending
            .remove(report.controller());

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
        let pending = &self
            .runtime
            .get(&graph_id)
            .expect("runtime exists for every graph")
            .pending;
        if pending.is_empty()
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
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::GenerationConflict {
                    expected: expected_generation,
                    actual: graph.intent.generation(),
                },
            ));
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
                BTreeMap::<ControllerName, Vec<Resource>>::new(),
                |mut grouped, resource| {
                    grouped
                        .entry(resource.controller().clone())
                        .or_default()
                        .push(resource.clone());
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
        let max_rounds = intent.components().len().saturating_add(1);
        let mut final_plan = None;
        let mut evaluation_events = Vec::new();

        for _ in 0..max_rounds {
            let before = self
                .runtime
                .get(&graph_id)
                .expect("runtime exists for every graph")
                .interpretations
                .clone();
            for component in intent.components() {
                let snapshot = self.snapshot_for(graph_id, component)?;
                let attempt = self
                    .evaluator
                    .evaluate(EvaluationRequest::new(
                        graph_id,
                        generation,
                        component.name().clone(),
                        component.bundle(),
                        snapshot,
                    ))
                    .await
                    .map_err(|error| {
                        Error::<CommandError, Never, anyhow::Error>::Domain(
                            CommandError::Evaluation(error.to_string()),
                        )
                    })?;
                if let Some(outputs) = self.accept_interpretation(graph_id, component, attempt)? {
                    evaluation_events.push(CoreEvent::ComponentOutputsReplaced(outputs));
                }
            }
            let plan = self.plan_from_interpretations(graph_id, generation)?;
            final_plan = Some(plan);
            let after = &self
                .runtime
                .get(&graph_id)
                .expect("runtime exists for every graph")
                .interpretations;
            if &before == after {
                break;
            }
        }

        let plan = final_plan.expect("graphs contain at least one component");
        let previous = self.graph(graph_id)?.plan.clone();
        if previous
            .as_ref()
            .is_some_and(|old| old.generation() == generation && old.digest() == plan.digest())
        {
            let mut transition = Transition {
                events: evaluation_events,
                effects: Vec::new(),
            };
            transition.extend(self.check_quiescence(graph_id)?);
            return Ok(transition);
        }
        let effects = dispatch_effects(graph_id, previous.as_ref(), &plan);
        let pending = effects
            .iter()
            .filter_map(|effect| match effect.command() {
                ControllerCommand::Reconcile(_) => Some(effect.controller().clone()),
                ControllerCommand::Supersede(_) | ControllerCommand::Retire(_) => None,
            })
            .collect();
        self.runtime
            .get_mut(&graph_id)
            .expect("runtime exists for every graph")
            .pending = pending;
        self.graph_mut(graph_id)?.accept_plan(plan.clone());
        evaluation_events.push(CoreEvent::PlanAccepted { graph_id, plan });
        let mut transition = Transition {
            events: evaluation_events,
            effects,
        };
        transition.extend(self.check_quiescence(graph_id)?);
        Ok(transition)
    }

    fn snapshot_for(
        &self,
        graph_id: GraphId,
        component: &ComponentIntent,
    ) -> Result<EvaluationSnapshot, Error<CommandError, Never, anyhow::Error>> {
        let graph = self.graph(graph_id)?;
        let runtime = self
            .runtime
            .get(&graph_id)
            .expect("runtime exists for every graph");
        let generation = graph.intent.generation();
        let mut cells = Vec::new();
        for input in component.inputs() {
            match input.source() {
                ComponentInputSource::Config { default, .. } => {
                    let value = component
                        .input_binding(input.name())
                        .map(|binding| binding.value().clone())
                        .or_else(|| default.clone())
                        .expect("graph validation proved config input availability");
                    cells.push(InputCell::config(input.name().clone(), value));
                }
                ComponentInputSource::Output { source, optional } => {
                    let key = OutputKey::new(generation, source.clone());
                    let state = if let Some(record) = graph.outputs.get(&key) {
                        InputCellState::Available(record.value().clone())
                    } else {
                        let producer = graph
                            .intent
                            .component(source.component())
                            .expect("graph validation proved producer existence");
                        let declaration = producer
                            .output(source.output())
                            .expect("graph validation proved output existence");
                        match runtime.interpretations.get(source.component()) {
                            Some(interpretation) if interpretation.complete => {
                                if *optional
                                    && declaration.is_optional()
                                    && !interpretation.declared_outputs.contains(source.output())
                                {
                                    InputCellState::Absent
                                } else {
                                    InputCellState::Blocked
                                }
                            }
                            Some(_) | None => InputCellState::Blocked,
                        }
                    };
                    cells.push(
                        InputCell::new(input.name().clone(), source.clone(), *optional, state)
                            .map_err(|error| {
                                Error::<CommandError, Never, anyhow::Error>::Invariant(
                                    anyhow::Error::new(error),
                                )
                            })?,
                    );
                }
            }
        }
        EvaluationSnapshot::new(cells).map_err(|error| {
            Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::Error::new(error))
        })
    }

    fn accept_interpretation(
        &mut self,
        graph_id: GraphId,
        component: &ComponentIntent,
        attempt: EvaluationAttempt,
    ) -> Result<Option<ComponentOutputs>, Error<CommandError, Never, anyhow::Error>> {
        if attempt
            .resources()
            .iter()
            .any(|resource| resource.resource().path().instance() != component.name())
        {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::EvaluationProtocol(
                    "resource belongs to another component".to_owned(),
                ),
            ));
        }

        let mut interpretation = ComponentInterpretation {
            resources: attempt
                .resources()
                .iter()
                .map(|resource| resource.resource().clone())
                .collect(),
            complete: attempt.complete_result().is_some(),
            blocked_on: attempt
                .blocked_result()
                .map(|blocked| blocked.blocked().source().clone()),
            declared_outputs: BTreeSet::new(),
            bindings: BTreeMap::new(),
        };

        let generation = self.graph(graph_id)?.intent.generation();
        let old_outputs = self
            .graph(graph_id)?
            .outputs
            .iter()
            .filter(|record| {
                record.key_value().generation() == generation
                    && record.key_value().reference().component() == component.name()
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut replacement_outputs = Vec::new();

        if let Some(complete) = attempt.complete_result() {
            for output in complete.outputs() {
                let declaration = component.output(output.name()).ok_or_else(|| {
                    Error::<CommandError, Never, anyhow::Error>::Domain(
                        CommandError::EvaluationProtocol(format!(
                            "undeclared static output {}",
                            output.name()
                        )),
                    )
                })?;
                if declaration.availability() != OutputAvailability::Static {
                    return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                        CommandError::EvaluationProtocol(format!(
                            "observed output {} returned as static",
                            output.name()
                        )),
                    ));
                }
                interpretation
                    .declared_outputs
                    .insert(output.name().clone());
                replacement_outputs.push(OutputRecord::new(
                    OutputKey::new(
                        generation,
                        OutputRef::new(component.name().clone(), output.name().clone()),
                    ),
                    output.value().clone(),
                    OutputSource::Static,
                ));
            }
            for binding in complete.observed_outputs() {
                self.validate_binding(component, &interpretation.resources, binding)?;
                interpretation
                    .declared_outputs
                    .insert(binding.name().clone());
                let resource = interpretation
                    .resources
                    .iter()
                    .find(|resource| resource.path().address() == binding.resource())
                    .expect("binding validation found the resource");
                interpretation.bindings.insert(
                    ObservedOutputKey::new(resource.id(), binding.output().clone()),
                    OutputRef::new(component.name().clone(), binding.name().clone()),
                );
            }
            for declaration in component.outputs() {
                if !declaration.is_optional()
                    && !interpretation.declared_outputs.contains(declaration.name())
                {
                    return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                        CommandError::EvaluationProtocol(format!(
                            "required output {} was omitted",
                            declaration.name()
                        )),
                    ));
                }
            }
            for output in &old_outputs {
                if let OutputSource::Observed {
                    resource_id,
                    resource_output,
                } = output.source()
                {
                    let observation = ObservedOutputKey::new(*resource_id, resource_output.clone());
                    if interpretation.bindings.get(&observation)
                        == Some(output.key_value().reference())
                    {
                        replacement_outputs.push(output.clone());
                    }
                }
            }
        }

        replacement_outputs.sort_by(|left, right| left.key_value().cmp(right.key_value()));
        {
            let keys = old_outputs
                .iter()
                .map(|output| output.key_value().clone())
                .collect::<Vec<_>>();
            let mut graph = self.graph_mut(graph_id)?;
            for key in keys {
                graph.outputs.remove(&key);
            }
            for output in &replacement_outputs {
                graph.outputs.insert_overwrite(output.clone());
            }
        }

        let runtime = self
            .runtime
            .get_mut(&graph_id)
            .expect("runtime exists for every graph");
        runtime
            .interpretations
            .insert(component.name().clone(), interpretation);
        runtime.bindings = runtime
            .interpretations
            .values()
            .flat_map(|value| value.bindings.clone())
            .collect();

        if old_outputs == replacement_outputs {
            Ok(None)
        } else {
            ComponentOutputs::new(NewComponentOutputs {
                graph_id,
                generation,
                component: component.name().clone(),
                outputs: replacement_outputs,
            })
            .map(Some)
            .map_err(|error| {
                Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::Error::new(error))
            })
        }
    }

    fn validate_binding(
        &self,
        component: &ComponentIntent,
        resources: &[Resource],
        binding: &ObservedOutputBinding,
    ) -> Result<(), Error<CommandError, Never, anyhow::Error>> {
        let declaration = component.output(binding.name()).ok_or_else(|| {
            Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::EvaluationProtocol(
                format!("undeclared observed output {}", binding.name()),
            ))
        })?;
        if declaration.availability() != OutputAvailability::Observed {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::EvaluationProtocol(format!(
                    "static output {} returned as observed",
                    binding.name()
                )),
            ));
        }
        let resource = resources
            .iter()
            .find(|resource| resource.path().address() == binding.resource())
            .ok_or_else(|| {
                Error::<CommandError, Never, anyhow::Error>::Domain(
                    CommandError::EvaluationProtocol(
                        "observed binding points outside the component result".to_owned(),
                    ),
                )
            })?;
        let output = resource.output(binding.output()).ok_or_else(|| {
            Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::EvaluationProtocol(
                format!(
                    "resource does not declare observed output {}",
                    binding.output()
                ),
            ))
        })?;
        if output.availability() != OutputAvailability::Observed {
            return Err(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::EvaluationProtocol(format!(
                    "resource output {} is not observed",
                    binding.output()
                )),
            ));
        }
        Ok(())
    }

    fn plan_from_interpretations(
        &self,
        graph_id: GraphId,
        generation: Generation,
    ) -> Result<Plan, Error<CommandError, Never, anyhow::Error>> {
        let runtime = self
            .runtime
            .get(&graph_id)
            .expect("runtime exists for every graph");
        let resources = runtime
            .interpretations
            .values()
            .flat_map(|interpretation| interpretation.resources.clone())
            .collect();
        let blocked = runtime
            .interpretations
            .iter()
            .filter_map(|(component, interpretation)| {
                interpretation.blocked_on.as_ref().map(|source| {
                    BlockedMarker::new(component.clone(), vec![source.clone()])
                        .expect("blocked interpretation has one source")
                })
            })
            .collect();
        Plan::new(NewPlan {
            generation,
            resources,
            blocked,
        })
        .map_err(|error| {
            Error::<CommandError, Never, anyhow::Error>::Invariant(anyhow::Error::new(error))
        })
    }

    fn graph(
        &self,
        graph_id: GraphId,
    ) -> Result<&GraphState, Error<CommandError, Never, anyhow::Error>> {
        self.state
            .graphs
            .get(&graph_id)
            .ok_or(Error::<CommandError, Never, anyhow::Error>::Domain(
                CommandError::GraphNotFound,
            ))
    }

    fn graph_mut(
        &mut self,
        graph_id: GraphId,
    ) -> Result<iddqd::id_ord_map::RefMut<'_, GraphState>, Error<CommandError, Never, anyhow::Error>>
    {
        self.state.graphs.get_mut(&graph_id).ok_or(
            Error::<CommandError, Never, anyhow::Error>::Domain(CommandError::GraphNotFound),
        )
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct GraphRuntime {
    interpretations: BTreeMap<ComponentName, ComponentInterpretation>,
    bindings: BTreeMap<ObservedOutputKey, OutputRef>,
    pending: BTreeSet<ControllerName>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ComponentInterpretation {
    resources: Vec<Resource>,
    complete: bool,
    blocked_on: Option<OutputRef>,
    declared_outputs: BTreeSet<henosis_types::OutputName>,
    bindings: BTreeMap<ObservedOutputKey, OutputRef>,
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
    let mut removed = BTreeMap::<ControllerName, Vec<Resource>>::new();
    if let Some(previous) = previous {
        for resource in previous.resources() {
            if plan.resource(resource.id()).is_none() {
                removed
                    .entry(resource.controller().clone())
                    .or_default()
                    .push(resource.clone());
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
                graph.accept_intent(intent.clone());
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
                graph.accept_plan(plan.clone());
            }
            CoreEvent::ControllerReported(report) => {
                self.graphs
                    .get_mut(&report.graph_id())
                    .expect("reported graph must already exist")
                    .reports
                    .insert_overwrite(LatestControllerReport(report.clone()));
            }
            CoreEvent::ComponentOutputsReplaced(replacement) => {
                let mut graph = self
                    .graphs
                    .get_mut(&replacement.graph_id())
                    .expect("output graph must already exist");
                assert_eq!(graph.intent.generation(), replacement.generation());
                let keys = graph
                    .outputs
                    .iter()
                    .filter(|output| {
                        output.key_value().generation() == replacement.generation()
                            && output.key_value().reference().component() == replacement.component()
                    })
                    .map(|output| output.key_value().clone())
                    .collect::<Vec<_>>();
                for key in keys {
                    graph.outputs.remove(&key);
                }
                for output in replacement.outputs() {
                    graph.outputs.insert_overwrite(output.clone());
                }
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
            stall: None,
            retired: false,
        }
    }

    fn accept_intent(&mut self, intent: GraphIntent) {
        self.intent = intent;
        self.stall = None;
    }

    fn accept_plan(&mut self, plan: Plan) {
        self.plan = Some(plan);
        self.stall = None;
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

    pub fn reports(&self) -> impl ExactSizeIterator<Item = &ControllerReport> {
        self.reports.iter().map(|report| &report.0)
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
