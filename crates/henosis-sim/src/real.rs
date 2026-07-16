use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::RwLock;

use faultline::Error;
use futures::future::BoxFuture;
use henosis_controller_cloudflare::CloudflareController;
use henosis_controller_k8s::K8sController;
use henosis_controller_runtime::PerResourceReconciler;
use henosis_controller_runtime::ResourceConvergence;
use henosis_controller_runtime::ResourceGoal;
use henosis_controller_runtime::ResourcePass;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_controller_runtime::reconcile_resource_once;
use henosis_controller_supabase::ComponentBundleResolver;
use henosis_controller_supabase::SupabaseController;
use henosis_controller_supabase::SupabaseError;
use henosis_orchestrator::Command;
use henosis_orchestrator::Core;
use henosis_orchestrator::MaterializedCore;
use henosis_orchestrator::Transition;
use henosis_testkit::FakeCloudflareTransport;
use henosis_testkit::FakeK8sTarget;
use henosis_testkit::FakeSupabaseTarget;
use henosis_testkit::NamedRng;
use henosis_testkit::NormalizedTrace;
use henosis_testkit::Seed;
use henosis_testkit::TargetFault;
use henosis_testkit::TraceRecorder;
use henosis_types::ArtifactDigest;
use henosis_types::ArtifactStoreError;
use henosis_types::BundleRef;
use henosis_types::ComponentIntent;
use henosis_types::ComponentName;
use henosis_types::ComponentOutput;
use henosis_types::ConfigClosureError;
use henosis_types::ConfigClosureReader;
use henosis_types::ContentDigest;
use henosis_types::ControllerCommand;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::EvaluationAttempt;
use henosis_types::EvaluationError;
use henosis_types::EvaluationRequest;
use henosis_types::EvaluationResource;
use henosis_types::Evaluator;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphSourcePolicy;
use henosis_types::KindName;
use henosis_types::KindVersion;
use henosis_types::NativeValue;
use henosis_types::NewCompleteEvaluation;
use henosis_types::NewComponentIntent;
use henosis_types::NewEvaluationResource;
use henosis_types::NewGraphIntent;
use henosis_types::ObservedOutput;
use henosis_types::ObservedOutputBinding;
use henosis_types::OutputAvailability;
use henosis_types::OutputDeclaration;
use henosis_types::OutputName;
use henosis_types::Resource;
use henosis_types::ResourceAddress;
use henosis_types::ResourceId;
use henosis_types::ResourceName;
use henosis_types::StaticOutput;

const GRAPH_BYTES: [u8; 16] = [81; 16];

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RealControllerAction {
    ControllerPass {
        controller: ControllerName,
        resource: ResourceId,
    },
    DeliverReport {
        controller: ControllerName,
        generation: Generation,
    },
    DeliverDuplicate {
        controller: ControllerName,
        generation: Generation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RealControllerRun {
    pub trace: NormalizedTrace,
    pub generation: Generation,
    pub complete: bool,
    pub target_actions: usize,
}

#[derive(Clone, Debug)]
struct ResourceWork {
    resource: Resource,
    goal: ResourceGoal,
    convergence: Option<ResourceConvergence>,
}

#[derive(Clone, Debug)]
struct ControllerWork {
    graph_id: GraphId,
    desired: Vec<Resource>,
    resources: BTreeMap<ResourceId, ResourceWork>,
    report_slice: Option<ControllerSlice>,
}

impl ControllerWork {
    fn from_command(command: &ControllerCommand) -> Self {
        match command {
            ControllerCommand::Reconcile(slice) => {
                let resources = slice
                    .resources()
                    .iter()
                    .cloned()
                    .map(|resource| {
                        (
                            resource.id(),
                            ResourceWork {
                                resource,
                                goal: ResourceGoal::Present,
                                convergence: None,
                            },
                        )
                    })
                    .chain(slice.superseded().iter().cloned().map(|resource| {
                        (
                            resource.id(),
                            ResourceWork {
                                resource,
                                goal: ResourceGoal::Absent,
                                convergence: None,
                            },
                        )
                    }))
                    .collect();
                Self {
                    graph_id: slice.graph_id(),
                    desired: slice.resources().to_vec(),
                    resources,
                    report_slice: Some(slice.clone()),
                }
            }
            ControllerCommand::Supersede(supersession) => Self {
                graph_id: supersession.graph_id,
                desired: Vec::new(),
                resources: supersession
                    .resources
                    .iter()
                    .cloned()
                    .map(|resource| {
                        (
                            resource.id(),
                            ResourceWork {
                                resource,
                                goal: ResourceGoal::Absent,
                                convergence: None,
                            },
                        )
                    })
                    .collect(),
                report_slice: None,
            },
            ControllerCommand::Retire(retirement) => Self {
                graph_id: retirement.graph_id,
                desired: Vec::new(),
                resources: retirement
                    .resources
                    .iter()
                    .cloned()
                    .map(|resource| {
                        (
                            resource.id(),
                            ResourceWork {
                                resource,
                                goal: ResourceGoal::Absent,
                                convergence: None,
                            },
                        )
                    })
                    .collect(),
                report_slice: None,
            },
        }
    }

    fn is_complete(&self) -> bool {
        self.resources
            .values()
            .all(|resource| resource.convergence.is_some())
    }

    fn report(&self) -> Option<ControllerReport> {
        let slice = self.report_slice.as_ref()?;
        if !self.is_complete() {
            return None;
        }
        let outputs = self
            .resources
            .values()
            .filter(|work| work.goal == ResourceGoal::Present)
            .flat_map(|work| {
                work.convergence
                    .as_ref()
                    .expect("complete work has convergence")
                    .outputs
                    .clone()
            })
            .collect::<Vec<_>>();
        let evidence = self
            .resources
            .values()
            .filter(|work| work.goal == ResourceGoal::Present)
            .flat_map(|work| {
                let mut evidence = work.resource.id().to_string().into_bytes();
                evidence.push(b'=');
                evidence.extend_from_slice(
                    &work
                        .convergence
                        .as_ref()
                        .expect("complete work has convergence")
                        .evidence,
                );
                evidence.push(b';');
                evidence
            })
            .collect::<Vec<_>>();
        Some(
            ready_report(slice, Some(publication_id(&evidence)), outputs)
                .expect("real controller convergence builds a holistic report"),
        )
    }
}

pub struct RealControllerWorld {
    scheduler: NamedRng,
    evaluator: Arc<RealControllerEvaluator>,
    core: Core,
    graph_id: GraphId,
    component: ComponentName,
    work: BTreeMap<ControllerName, ControllerWork>,
    ready: BTreeMap<(ControllerName, Generation), ControllerReport>,
    delivered: BTreeMap<(ControllerName, Generation), ControllerReport>,
    events: Vec<henosis_types::CoreEvent>,
    trace: TraceRecorder,
    k8s_target: FakeK8sTarget,
    cloudflare_target: FakeCloudflareTransport,
    supabase_target: FakeSupabaseTarget,
    k8s: K8sController<FakeK8sTarget>,
    cloudflare: CloudflareController<FakeCloudflareTransport>,
    supabase: SupabaseController<FakeSupabaseTarget>,
}

impl RealControllerWorld {
    pub async fn new(seed: Seed, resources_per_controller: u8) -> Self {
        let evaluator = Arc::new(RealControllerEvaluator::default());
        let component = ComponentName::new("platform").expect("component name is valid");
        let bundle = evaluator.register(1, resources_per_controller.max(1));
        let graph_id = GraphId::from_bytes(GRAPH_BYTES);
        let graph = NewGraphIntent {
            id: graph_id,
            components: vec![component_intent(
                component.clone(),
                bundle,
                resources_per_controller,
            )],
            source_policy: GraphSourcePolicy::AcceptLocal,
        };
        let mut core = Core::new(evaluator.clone());
        let transition = core
            .handle(Command::CreateGraph(graph))
            .await
            .expect("real-controller fixture graph is valid");
        let k8s_target = FakeK8sTarget::default();
        let cloudflare_target = FakeCloudflareTransport::default();
        let supabase_target = FakeSupabaseTarget::default();
        let mut world = Self {
            scheduler: NamedRng::new(seed, "real-controller-scheduler"),
            evaluator,
            core,
            graph_id,
            component,
            work: BTreeMap::new(),
            ready: BTreeMap::new(),
            delivered: BTreeMap::new(),
            events: Vec::new(),
            trace: TraceRecorder::new(seed),
            k8s: K8sController::new(k8s_target.clone()),
            cloudflare: CloudflareController::new(cloudflare_target.clone()),
            supabase: SupabaseController::new(
                supabase_target.clone(),
                Arc::new(NoConfigFiles),
                Arc::new(NoBundles),
            ),
            k8s_target,
            cloudflare_target,
            supabase_target,
        };
        world.accept_transition(transition);
        world
    }

    pub fn script_k8s(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.k8s_target.script(faults);
    }

    pub fn script_cloudflare(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.cloudflare_target.script(faults);
    }

    pub fn script_supabase(&self, faults: impl IntoIterator<Item = TargetFault>) {
        self.supabase_target.script(faults);
    }

    #[must_use]
    pub fn generation(&self) -> Generation {
        self.core
            .state()
            .graph(self.graph_id)
            .expect("fixture graph exists")
            .intent()
            .generation()
    }

    #[must_use]
    pub fn enabled_actions(&self) -> Vec<RealControllerAction> {
        let mut actions = Vec::new();
        for (controller, work) in &self.work {
            actions.extend(
                work.resources
                    .values()
                    .filter(|resource| resource.convergence.is_none())
                    .map(|resource| RealControllerAction::ControllerPass {
                        controller: controller.clone(),
                        resource: resource.resource.id(),
                    }),
            );
        }
        actions.extend(self.ready.keys().cloned().map(|(controller, generation)| {
            RealControllerAction::DeliverReport {
                controller,
                generation,
            }
        }));
        actions.extend(
            self.delivered
                .keys()
                .cloned()
                .map(
                    |(controller, generation)| RealControllerAction::DeliverDuplicate {
                        controller,
                        generation,
                    },
                ),
        );
        actions.sort();
        actions
    }

    pub async fn apply(&mut self, action: RealControllerAction) {
        let before_actions = self.target_action_count();
        let action_text = format!("{action:?}");
        let outcome = match action {
            RealControllerAction::ControllerPass {
                controller,
                resource,
            } => self.apply_controller_pass(&controller, resource).await,
            RealControllerAction::DeliverReport {
                controller,
                generation,
            } => self.deliver_report(controller, generation, false).await,
            RealControllerAction::DeliverDuplicate {
                controller,
                generation,
            } => self.deliver_report(controller, generation, true).await,
        };
        let after_actions = self.target_action_count();
        assert!(
            after_actions.saturating_sub(before_actions) <= 1,
            "one scheduler transition may perform at most one target action"
        );
        let state = self.canonical_state();
        self.trace.record(action_text, outcome, &state);
        self.assert_invariants();
    }

    pub async fn start_next_generation(&mut self, resources_per_controller: u8) {
        let bundle = self.evaluator.register(2, resources_per_controller.max(1));
        let transition = self
            .core
            .handle(Command::UpdateGraph {
                graph_id: self.graph_id,
                expected_generation: self.generation(),
                components: vec![component_intent(
                    self.component.clone(),
                    bundle,
                    resources_per_controller,
                )],
            })
            .await
            .expect("fixture generation update is valid");
        self.accept_transition(transition);
    }

    pub async fn retire(&mut self) {
        let transition = self
            .core
            .handle(Command::RetireGraph {
                graph_id: self.graph_id,
                expected_generation: self.generation(),
            })
            .await
            .expect("fixture graph retirement is valid");
        self.accept_transition(transition);
    }

    pub fn restart_controller(&mut self, controller: &str) {
        match controller {
            "k8s" => self.k8s = K8sController::new(self.k8s_target.clone()),
            "cloudflare" => {
                self.cloudflare = CloudflareController::new(self.cloudflare_target.clone());
            }
            "supabase" => {
                self.supabase = SupabaseController::new(
                    self.supabase_target.clone(),
                    Arc::new(NoConfigFiles),
                    Arc::new(NoBundles),
                );
            }
            _ => panic!("unknown fixture controller {controller}"),
        }
    }

    pub async fn run(mut self, step_budget: usize) -> RealControllerRun {
        for _ in 0..step_budget {
            let actions = self
                .enabled_actions()
                .into_iter()
                .filter(|action| !matches!(action, RealControllerAction::DeliverDuplicate { .. }))
                .collect::<Vec<_>>();
            if actions.is_empty() {
                break;
            }
            let selected = self
                .scheduler
                .choose_index(actions.len())
                .expect("enabled action list is non-empty");
            self.apply(actions[selected].clone()).await;
        }
        let state = self.canonical_state();
        let generation = self.generation();
        let complete = self
            .core
            .state()
            .graph(self.graph_id)
            .and_then(|graph| graph.plan())
            .is_some_and(henosis_types::Plan::is_complete);
        let target_actions = self.target_action_count();
        RealControllerRun {
            trace: self.trace.finish(&state),
            generation,
            complete,
            target_actions,
        }
    }

    #[must_use]
    pub fn target_action_count(&self) -> usize {
        self.core
            .state()
            .graph(self.graph_id)
            .and_then(|graph| graph.plan())
            .map(|plan| {
                plan.resources()
                    .map(|resource| match resource.controller().as_str() {
                        "k8s" => self.k8s_target.action_count(resource.id()),
                        "cloudflare" => self.cloudflare_target.action_count(resource.id()),
                        "supabase" => self.supabase_target.action_count(resource.id()),
                        _ => 0,
                    })
                    .sum()
            })
            .unwrap_or(0)
    }

    #[must_use]
    pub fn plan_complete(&self) -> bool {
        self.core
            .state()
            .graph(self.graph_id)
            .and_then(|graph| graph.plan())
            .is_some_and(henosis_types::Plan::is_complete)
    }

    #[must_use]
    pub fn all_current_resources_exist(&self) -> bool {
        self.core
            .state()
            .graph(self.graph_id)
            .and_then(|graph| graph.plan())
            .is_some_and(|plan| {
                plan.resources()
                    .all(|resource| match resource.controller().as_str() {
                        "k8s" => self.k8s_target.contains(self.graph_id, resource.id()),
                        "cloudflare" => self
                            .cloudflare_target
                            .contains(self.graph_id, resource.id()),
                        "supabase" => self.supabase_target.contains(self.graph_id, resource.id()),
                        _ => false,
                    })
            })
    }

    #[must_use]
    pub fn no_resources_exist(&self) -> bool {
        self.events
            .iter()
            .filter_map(|event| match event {
                henosis_types::CoreEvent::PlanAccepted { plan, .. } => Some(plan),
                _ => None,
            })
            .flat_map(|plan| plan.resources())
            .all(|resource| match resource.controller().as_str() {
                "k8s" => !self.k8s_target.contains(self.graph_id, resource.id()),
                "cloudflare" => !self
                    .cloudflare_target
                    .contains(self.graph_id, resource.id()),
                "supabase" => !self.supabase_target.contains(self.graph_id, resource.id()),
                _ => true,
            })
    }

    #[must_use]
    pub fn canonical_state(&self) -> Vec<u8> {
        let graph = self
            .core
            .state()
            .graph(self.graph_id)
            .expect("fixture graph exists");
        let resources = graph
            .plan()
            .map(|plan| {
                plan.resources()
                    .map(|resource| {
                        (
                            resource.id().to_string(),
                            resource.controller().as_str().to_owned(),
                            resource.body().canonical().to_owned(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        serde_json::to_vec(&(
            graph.intent().generation().ordinal(),
            graph.is_retired(),
            resources,
        ))
        .expect("canonical real-controller state serializes")
    }

    async fn apply_controller_pass(
        &mut self,
        controller: &ControllerName,
        resource_id: ResourceId,
    ) -> String {
        let Some(work) = self.work.get(controller) else {
            return "cancelled stale work".to_owned();
        };
        let graph_id = work.graph_id;
        let desired = work.desired.clone();
        let resource = work
            .resources
            .get(&resource_id)
            .expect("enabled resource work exists")
            .resource
            .clone();
        let goal = work
            .resources
            .get(&resource_id)
            .expect("enabled resource work exists")
            .goal;
        let result = match controller.as_str() {
            "k8s" => pass(&self.k8s, graph_id, &desired, &resource, goal).await,
            "cloudflare" => pass(&self.cloudflare, graph_id, &desired, &resource, goal).await,
            "supabase" => pass(&self.supabase, graph_id, &desired, &resource, goal).await,
            _ => Err(format!("unknown controller {controller}")),
        };
        match result {
            Ok(ResourcePass::Acted) => "acted".to_owned(),
            Ok(ResourcePass::Converged(convergence)) => {
                self.work
                    .get_mut(controller)
                    .expect("work remains while pass executes")
                    .resources
                    .get_mut(&resource_id)
                    .expect("resource work remains while pass executes")
                    .convergence = Some(convergence);
                self.promote_report(controller);
                "converged".to_owned()
            }
            Err(error) => format!("retryable failure: {error}"),
        }
    }

    fn promote_report(&mut self, controller: &ControllerName) {
        let Some(work) = self.work.get(controller) else {
            return;
        };
        if !work.is_complete() {
            return;
        }
        if let Some(report) = work.report() {
            self.ready
                .insert((controller.clone(), report.generation()), report);
        }
        self.work.remove(controller);
    }

    async fn deliver_report(
        &mut self,
        controller: ControllerName,
        generation: Generation,
        duplicate: bool,
    ) -> String {
        let key = (controller.clone(), generation);
        let report = if duplicate {
            self.delivered
                .get(&key)
                .expect("enabled duplicate report exists")
                .clone()
        } else {
            let report = self
                .ready
                .remove(&key)
                .expect("enabled report delivery exists");
            self.delivered.insert(key, report.clone());
            report
        };
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
            let controller = effect.controller().clone();
            self.work
                .insert(controller, ControllerWork::from_command(effect.command()));
        }
        self.assert_invariants();
    }

    fn assert_invariants(&self) {
        assert_eq!(
            MaterializedCore::fold(&self.events),
            self.core.state().clone(),
            "real-controller live state must equal replay after every transition"
        );
        let generation = self.generation();
        assert!(
            self.work.values().all(|work| {
                work.report_slice
                    .as_ref()
                    .is_none_or(|slice| slice.generation() == generation)
            }),
            "superseding a generation cancels unfinished stale controller work"
        );
    }
}

async fn pass<R>(
    reconciler: &R,
    graph_id: GraphId,
    desired: &[Resource],
    resource: &Resource,
    goal: ResourceGoal,
) -> Result<ResourcePass, String>
where
    R: PerResourceReconciler,
    R::Error: std::fmt::Display,
{
    reconcile_resource_once(reconciler, graph_id, desired, resource, goal)
        .await
        .map_err(|error| error.to_string())
}

#[derive(Clone, Debug)]
struct RealProgram {
    revision: u8,
    resources_per_controller: u8,
}

#[derive(Debug, Default)]
struct RealControllerEvaluator {
    programs: RwLock<BTreeMap<ContentDigest, RealProgram>>,
}

impl RealControllerEvaluator {
    fn register(&self, revision: u8, resources_per_controller: u8) -> BundleRef {
        let program = RealProgram {
            revision,
            resources_per_controller,
        };
        let digest = ContentDigest::digest(format!("{program:?}").as_bytes());
        self.programs
            .write()
            .expect("real evaluator lock is not poisoned")
            .insert(digest, program);
        BundleRef::new(digest)
    }
}

impl Evaluator for RealControllerEvaluator {
    fn evaluate<'a>(
        &'a self,
        request: EvaluationRequest,
    ) -> BoxFuture<'a, Result<EvaluationAttempt, EvaluationError>> {
        Box::pin(async move {
            let program = self
                .programs
                .read()
                .expect("real evaluator lock is not poisoned")
                .get(&request.bundle().digest())
                .cloned()
                .ok_or_else(|| EvaluationError::new("unregistered real-controller program"))?;
            let (resources, bindings) = real_resources(request.component(), &program)?;
            EvaluationAttempt::complete(
                request.snapshot(),
                NewCompleteEvaluation {
                    resources,
                    outputs: Vec::<StaticOutput>::new(),
                    observed_outputs: bindings,
                    reads: Vec::new(),
                },
            )
            .map_err(|error| EvaluationError::new(error.to_string()))
        })
    }
}

fn real_resources(
    component: &ComponentName,
    program: &RealProgram,
) -> Result<(Vec<EvaluationResource>, Vec<ObservedOutputBinding>), EvaluationError> {
    let mut resources = Vec::new();
    let mut bindings = Vec::new();
    for index in 0..program.resources_per_controller {
        resources.push(evaluation_resource(
            component,
            resource_id(10, index),
            "k8s/object",
            &format!("k8s-{index}"),
            "k8s",
            serde_json::json!({
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": { "name": format!("config-{index}") },
                "data": { "revision": program.revision.to_string() },
            }),
            Vec::new(),
        )?);

        let cloudflare_output = OutputName::new(format!("worker{index}Url"))
            .expect("generated component output name is valid");
        resources.push(evaluation_resource(
            component,
            resource_id(20, index),
            "cloudflare/worker",
            &format!("worker-{index}"),
            "cloudflare",
            serde_json::json!({
                "source": {
                    "entry": {
                        "kind": "cloudflare-worker",
                        "digest": ArtifactDigest::from_bytes([program.revision; 32]),
                    },
                    "assets": null,
                },
                "compatibilityDate": "2026-07-16",
                "compatibilityFlags": [],
                "vars": { "REVISION": program.revision },
                "services": {},
            }),
            vec![OutputDeclaration::new(
                OutputName::new("url").expect("output name is valid"),
                OutputAvailability::Observed,
            )],
        )?);
        bindings.push(ObservedOutputBinding::new(
            cloudflare_output,
            ResourceAddress::new(
                kind("cloudflare/worker"),
                resource_name(&format!("worker-{index}")),
            ),
            OutputName::new("url").expect("output name is valid"),
        ));

        let supabase_output = OutputName::new(format!("schema{index}"))
            .expect("generated component output name is valid");
        resources.push(evaluation_resource(
            component,
            resource_id(30, index),
            "supabase/schema",
            &format!("schema-{index}"),
            "supabase",
            serde_json::json!({
                "stack": "local",
                "project": "henosis-local",
                "database": "postgres",
                "schema": format!("app_{index}"),
                "migrations": [],
                "api": { "expose": false, "anonAccess": "none" },
            }),
            vec![OutputDeclaration::new(
                OutputName::new("schema").expect("output name is valid"),
                OutputAvailability::Observed,
            )],
        )?);
        bindings.push(ObservedOutputBinding::new(
            supabase_output,
            ResourceAddress::new(
                kind("supabase/schema"),
                resource_name(&format!("schema-{index}")),
            ),
            OutputName::new("schema").expect("output name is valid"),
        ));
    }
    Ok((resources, bindings))
}

fn evaluation_resource(
    component: &ComponentName,
    id: ResourceId,
    kind_name: &str,
    name: &str,
    controller: &str,
    body: serde_json::Value,
    outputs: Vec<OutputDeclaration>,
) -> Result<EvaluationResource, EvaluationError> {
    let native =
        NativeValue::new(body.clone()).map_err(|error| EvaluationError::new(error.to_string()))?;
    EvaluationResource::new(NewEvaluationResource {
        id,
        component: component.clone(),
        kind: kind(kind_name),
        name: resource_name(name),
        controller: ControllerName::new(controller).expect("controller name is valid"),
        body,
        canonical: native.canonical().to_owned(),
        outputs,
    })
    .map_err(|error| EvaluationError::new(error.to_string()))
}

fn component_intent(
    component: ComponentName,
    bundle: BundleRef,
    resources_per_controller: u8,
) -> ComponentIntent {
    let outputs = (0..resources_per_controller.max(1))
        .flat_map(|index| {
            [
                ComponentOutput::new(
                    OutputName::new(format!("worker{index}Url"))
                        .expect("component output name is valid"),
                    OutputAvailability::Observed,
                    false,
                ),
                ComponentOutput::new(
                    OutputName::new(format!("schema{index}"))
                        .expect("component output name is valid"),
                    OutputAvailability::Observed,
                    false,
                ),
            ]
        })
        .collect();
    ComponentIntent::new(NewComponentIntent {
        name: component,
        bundle,
        inputs: Vec::new(),
        outputs,
        source: None,
    })
    .expect("real-controller component intent is valid")
}

fn kind(name: &str) -> KindVersion {
    KindVersion::new(
        KindName::new(name).expect("generated kind name is valid"),
        NonZeroU32::new(1).expect("one is non-zero"),
    )
}

fn resource_name(name: &str) -> ResourceName {
    ResourceName::new(name).expect("generated resource name is valid")
}

fn resource_id(namespace: u8, index: u8) -> ResourceId {
    let mut bytes = [namespace; 16];
    bytes[15] = index;
    ResourceId::from_bytes(bytes)
}

struct NoConfigFiles;

impl ConfigClosureReader for NoConfigFiles {
    fn read<'a>(
        &'a self,
        bundle: BundleRef,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Arc<[u8]>, ConfigClosureError>> {
        Box::pin(async move {
            Err(ConfigClosureError::Missing {
                bundle,
                path: path.to_owned(),
            })
        })
    }
}

struct NoBundles;

impl ComponentBundleResolver for NoBundles {
    fn resolve(&self, _component: &ComponentName) -> Result<BundleRef, SupabaseError> {
        Err(SupabaseError::Plan(
            "fixture has no migration bundles".to_owned(),
        ))
    }
}

#[allow(dead_code)]
fn _artifact_error_is_not_a_target_fault(_: ArtifactStoreError) {}

#[allow(dead_code)]
fn _observed_outputs_are_atomic(_: Vec<ObservedOutput>) {}

#[allow(dead_code)]
fn _sets_are_ordered(_: BTreeSet<String>) {}
