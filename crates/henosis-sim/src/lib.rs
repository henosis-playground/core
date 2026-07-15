//! Seeded, explicit-transition simulation around the real Henosis core loop.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use faultline::Error;
use henosis_orchestrator::{Command, ControllerEffect, Core, MaterializedCore, Transition};
use henosis_testkit::{NamedRng, NormalizedTrace, Seed, TraceRecorder};
use henosis_types::{
    BundleRef, ComponentInput, ComponentIntent, ComponentName, ComponentOutput, ContentDigest,
    ControllerCommand, ControllerName, ControllerReport, GraphId, GraphName, InputName,
    NativeValue, NewComponentIntent, NewControllerReport, NewGraphIntent, ObservedOutput,
    ObservedOutputKey, OutputAvailability, OutputName, OutputRef, Plan, PublicationId,
    ResourceDisposition, ResourceDispositionKind, ResourceId,
};

pub use henosis_testkit::{ComponentProgram, ProgramEvaluator, Quiescence, ResourceProgram, StallReport, WaitGraph, WaitNode};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scenario {
    pub source_count: u8,
}

impl Scenario {
    #[must_use]
    pub const fn bounded(source_count: u8) -> Self {
        Self {
            source_count: if source_count == 0 { 1 } else { source_count },
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SimAction {
    CompleteTarget(ControllerName),
    DeliverOutput(ControllerName),
    DeliverDuplicate(ControllerName),
    DeliverStale(ControllerName),
    FireRetry(ControllerName),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Converged,
    Stalled,
    BudgetExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunResult {
    pub status: RunStatus,
    pub trace: NormalizedTrace,
    pub plan: Plan,
    pub events: Vec<henosis_types::CoreEvent>,
    pub presence_history: BTreeMap<ResourceId, Vec<bool>>,
}

pub struct SimWorld {
    scheduler: NamedRng,
    core: Core,
    graph_id: GraphId,
    pending: BTreeMap<ControllerName, ControllerReport>,
    ready: BTreeMap<ControllerName, ControllerReport>,
    delivered: BTreeMap<ControllerName, ControllerReport>,
    stale: BTreeMap<ControllerName, ControllerReport>,
    events: Vec<henosis_types::CoreEvent>,
    trace: TraceRecorder,
    presence_history: BTreeMap<ResourceId, Vec<bool>>,
    publication_counter: u8,
}

impl SimWorld {
    pub async fn new(seed: Seed, scenario: &Scenario) -> Self {
        let evaluator = Arc::new(ProgramEvaluator::default());
        let graph = build_graph(evaluator.as_ref(), scenario);
        let graph_id = graph.id;
        let mut core = Core::new(evaluator);
        let transition = core
            .handle(Command::CreateGraph(graph))
            .await
            .expect("generated graph must be accepted");
        let mut world = Self {
            scheduler: NamedRng::new(seed, "scheduler"),
            core,
            graph_id,
            pending: BTreeMap::new(),
            ready: BTreeMap::new(),
            delivered: BTreeMap::new(),
            stale: BTreeMap::new(),
            events: Vec::new(),
            trace: TraceRecorder::new(seed),
            presence_history: BTreeMap::new(),
            publication_counter: 1,
        };
        world.accept_transition(transition);
        world
    }

    #[must_use]
    pub fn enabled_actions(&self) -> Vec<SimAction> {
        let mut actions = self
            .pending
            .keys()
            .cloned()
            .map(SimAction::CompleteTarget)
            .collect::<Vec<_>>();
        actions.extend(self.ready.keys().cloned().map(SimAction::DeliverOutput));
        actions.extend(
            self.delivered
                .keys()
                .cloned()
                .map(SimAction::DeliverDuplicate),
        );
        actions.extend(self.stale.keys().cloned().map(SimAction::DeliverStale));
        actions.sort();
        actions
    }

    pub async fn run(mut self, step_budget: usize) -> RunResult {
        let mut duplicate_budget = self.delivered.len().saturating_add(2);
        for _ in 0..step_budget {
            let enabled = self.enabled_actions();
            let progress_actions = enabled
                .iter()
                .filter(|action| {
                    matches!(action, SimAction::CompleteTarget(_) | SimAction::DeliverOutput(_))
                })
                .cloned()
                .collect::<Vec<_>>();
            if progress_actions.is_empty() {
                let plan = self.plan().clone();
                let status = if plan.is_complete() {
                    RunStatus::Converged
                } else {
                    RunStatus::Stalled
                };
                return self.finish(status);
            }
            let index = self
                .scheduler
                .choose_index(progress_actions.len())
                .expect("progress actions are non-empty");
            self.apply(progress_actions[index].clone()).await;
            if duplicate_budget > 0 && self.scheduler.next_u64().is_multiple_of(4) {
                if let Some(controller) = self.delivered.keys().next().cloned() {
                    self.apply(SimAction::DeliverDuplicate(controller)).await;
                    duplicate_budget -= 1;
                }
            }
        }
        self.finish(RunStatus::BudgetExhausted)
    }

    pub async fn apply(&mut self, action: SimAction) {
        let action_name = format!("{action:?}");
        let outcome = match action {
            SimAction::CompleteTarget(controller) => {
                let report = self
                    .pending
                    .remove(&controller)
                    .expect("enabled target operation exists");
                self.ready.insert(controller, report);
                "target applied".to_owned()
            }
            SimAction::DeliverOutput(controller) => {
                let report = self
                    .ready
                    .remove(&controller)
                    .expect("enabled output delivery exists");
                self.delivered.insert(controller, report.clone());
                self.handle_report(report).await
            }
            SimAction::DeliverDuplicate(controller) => {
                let report = self
                    .delivered
                    .get(&controller)
                    .expect("enabled duplicate exists")
                    .clone();
                self.handle_report(report).await
            }
            SimAction::DeliverStale(controller) => {
                let report = self
                    .stale
                    .remove(&controller)
                    .expect("enabled stale report exists");
                match self.core.handle(Command::ReportController(report)).await {
                    Ok(transition) => {
                        self.accept_transition(transition);
                        "unexpectedly accepted stale report".to_owned()
                    }
                    Err(Error::Domain(error)) => format!("rejected: {error}"),
                    Err(error) => format!("failed: {error}"),
                }
            }
            SimAction::FireRetry(controller) => format!("retry fired for {controller}"),
        };
        self.record_presence();
        let state = self.canonical_state();
        self.trace.record(action_name, outcome, &state);
        self.assert_invariants();
    }

    pub async fn start_next_generation(&mut self) {
        self.stale.append(&mut self.pending);
        self.stale.append(&mut self.ready);
        let graph = self
            .core
            .state()
            .graph(self.graph_id)
            .expect("graph exists")
            .intent();
        let components = graph.components().cloned().collect();
        let transition = self
            .core
            .handle(Command::UpdateGraph {
                graph_id: self.graph_id,
                expected_generation: graph.generation(),
                components,
            })
            .await
            .expect("generated update is valid");
        self.accept_transition(transition);
    }

    #[must_use]
    pub fn plan(&self) -> &Plan {
        self.core
            .state()
            .graph(self.graph_id)
            .and_then(|graph| graph.plan())
            .expect("simulation always has a plan")
    }

    #[must_use]
    pub fn canonical_state(&self) -> Vec<u8> {
        let plan = self.plan();
        let resources = plan
            .resources()
            .map(|resource| {
                (
                    resource.id().to_string(),
                    resource.path().to_string(),
                    resource.body().canonical().to_owned(),
                )
            })
            .collect::<Vec<_>>();
        let blocked = plan
            .blocked()
            .map(|marker| {
                (
                    marker.component().as_str().to_owned(),
                    marker
                        .blocked_on()
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        serde_json::to_vec(&(plan.generation().ordinal(), resources, blocked))
            .expect("canonical simulation observation serializes")
    }

    fn finish(self, status: RunStatus) -> RunResult {
        let plan = self.plan().clone();
        let state = self.canonical_state();
        RunResult {
            status,
            trace: self.trace.finish(&state),
            plan,
            events: self.events,
            presence_history: self.presence_history,
        }
    }

    async fn handle_report(&mut self, report: ControllerReport) -> String {
        match self.core.handle(Command::ReportController(report)).await {
            Ok(transition) => {
                self.accept_transition(transition);
                "accepted".to_owned()
            }
            Err(Error::Domain(error)) => format!("rejected: {error}"),
            Err(error) => format!("failed: {error}"),
        }
    }

    fn accept_transition(&mut self, transition: Transition) {
        self.events.extend(transition.events().iter().cloned());
        for effect in transition.effects() {
            if let Some(report) = self.report_for_effect(effect) {
                self.pending.insert(effect.controller().clone(), report);
            }
        }
        self.record_presence();
        self.assert_invariants();
    }

    fn report_for_effect(&mut self, effect: &ControllerEffect) -> Option<ControllerReport> {
        let ControllerCommand::Reconcile(slice) = effect.command() else {
            return None;
        };
        let outputs = slice
            .resources()
            .iter()
            .flat_map(|resource| {
                resource.outputs().filter_map(|output| {
                    (output.availability() == OutputAvailability::Observed).then(|| {
                        ObservedOutput::new(
                            ObservedOutputKey::new(resource.id(), output.name().clone()),
                            NativeValue::new(serde_json::json!(format!(
                                "value-{}-{}",
                                slice.generation().ordinal(),
                                resource.id()
                            )))
                            .expect("generated output is finite JSON"),
                        )
                    })
                })
            })
            .collect();
        let dispositions = slice
            .resources()
            .iter()
            .map(|resource| {
                ResourceDisposition::new(resource.id(), ResourceDispositionKind::Ready)
            })
            .collect();
        let publication_id = PublicationId::from_bytes([self.publication_counter; 16]);
        self.publication_counter = self.publication_counter.wrapping_add(1).max(1);
        Some(
            ControllerReport::new(NewControllerReport {
                graph_id: slice.graph_id(),
                generation: slice.generation(),
                plan_digest: slice.plan_digest(),
                controller: slice.controller().clone(),
                publication_id: Some(publication_id),
                dispositions,
                outputs,
            })
            .expect("generated report is holistic"),
        )
    }

    fn record_presence(&mut self) {
        let present = self.plan().resources().map(|resource| resource.id()).collect::<BTreeSet<_>>();
        let known = self
            .presence_history
            .keys()
            .copied()
            .chain(present.iter().copied())
            .collect::<BTreeSet<_>>();
        for resource in known {
            let history = self.presence_history.entry(resource).or_default();
            let value = present.contains(&resource);
            if history.last() != Some(&value) {
                history.push(value);
            }
        }
    }

    fn assert_invariants(&self) {
        let folded = MaterializedCore::fold(&self.events);
        assert_eq!(
            folded,
            self.core.state().clone(),
            "live state must equal event replay after every transition"
        );
        for history in self.presence_history.values() {
            assert!(
                !history.windows(3).any(|window| window == [true, false, true]),
                "resource presence must not flap within a generation"
            );
        }
    }
}

#[must_use]
pub async fn run_seed(seed: Seed, scenario: &Scenario, step_budget: usize) -> RunResult {
    SimWorld::new(seed, scenario).await.run(step_budget).await
}

#[must_use]
pub fn replay_fold(events: &[henosis_types::CoreEvent]) -> MaterializedCore {
    MaterializedCore::fold(events)
}

fn build_graph(evaluator: &ProgramEvaluator, scenario: &Scenario) -> NewGraphIntent {
    let mut components = Vec::new();
    for index in 0..scenario.source_count {
        let component = component_name(&format!("source-{index}"));
        let output = output_name("value");
        let bundle = evaluator.register(ComponentProgram {
            resources: vec![ResourceProgram {
                id: resource_id(index.saturating_add(1)),
                name: resource_name(&format!("source-{index}")),
                controller: controller_name(&format!("controller-{index}")),
                required_values: Vec::new(),
                observed_component_output: Some(output.clone()),
            }],
            static_outputs: BTreeMap::new(),
        });
        components.push(component_intent(
            component,
            bundle,
            Vec::new(),
            vec![ComponentOutput::new(output, OutputAvailability::Observed, false)],
        ));
    }
    let inputs = (0..scenario.source_count)
        .map(|index| {
            ComponentInput::new(
                input_name(&format!("input-{index}")),
                OutputRef::new(component_name(&format!("source-{index}")), output_name("value")),
                false,
            )
        })
        .collect::<Vec<_>>();
    let bundle = evaluator.register(ComponentProgram {
        resources: vec![ResourceProgram {
            id: resource_id(200),
            name: resource_name("sink"),
            controller: controller_name("controller-sink"),
            required_values: inputs.iter().map(|input| input.name().clone()).collect(),
            observed_component_output: None,
        }],
        static_outputs: BTreeMap::new(),
    });
    components.push(component_intent(
        component_name("sink"),
        bundle,
        inputs,
        Vec::new(),
    ));
    NewGraphIntent {
        id: GraphId::from_bytes([7; 16]),
        name: GraphName::new("simulated-graph").expect("fixture graph name is valid"),
        components,
    }
}

fn component_intent(
    name: ComponentName,
    bundle: BundleRef,
    inputs: Vec<ComponentInput>,
    outputs: Vec<ComponentOutput>,
) -> ComponentIntent {
    ComponentIntent::new(NewComponentIntent {
        name,
        bundle,
        inputs,
        outputs,
    })
    .expect("generated component is valid")
}

fn component_name(value: &str) -> ComponentName {
    ComponentName::new(value).expect("generated component name is valid")
}

fn controller_name(value: &str) -> ControllerName {
    ControllerName::new(value).expect("generated controller name is valid")
}

fn input_name(value: &str) -> InputName {
    InputName::new(value).expect("generated input name is valid")
}

fn output_name(value: &str) -> OutputName {
    OutputName::new(value).expect("generated output name is valid")
}

fn resource_name(value: &str) -> henosis_types::ResourceName {
    henosis_types::ResourceName::new(value).expect("generated resource name is valid")
}

fn resource_id(value: u8) -> ResourceId {
    ResourceId::from_bytes([value; 16])
}

#[allow(dead_code)]
fn _content_digest_is_canonical(_: ContentDigest) {}
