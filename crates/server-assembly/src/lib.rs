//! Composition root for the Henosis server process.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_app::VerifiedBundleDirectory;
use henosis_controller_cloudflare::CloudflareAction;
use henosis_controller_cloudflare::CloudflareError;
use henosis_controller_cloudflare::CloudflareObservation;
use henosis_controller_cloudflare::CloudflareTransport;
use henosis_controller_cloudflare::LiveCloudflareConfig;
use henosis_controller_cloudflare::LiveCloudflareTransport;
use henosis_controller_cloudflare::RouteObservation;
use henosis_controller_cloudflare::TunnelObservation;
use henosis_controller_cloudflare::WorkerObservation;
use henosis_controller_k8s::K8sController;
use henosis_controller_runtime::ControllerSchedule;
use henosis_controller_runtime::ControllerScheduleCompletion;
use henosis_controller_runtime::DirectoryArtifactStore;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::ScheduledControllerPass;
use henosis_controller_runtime::ScheduledControllerReport;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::output;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_controller_supabase::LocalSupabaseConfig;
use henosis_controller_supabase::LocalSupabaseTarget;
use henosis_controller_supabase::SupabaseController;
use henosis_evaluation_engine::BundleSource;
use henosis_evaluation_engine::EngineConfig;
use henosis_evaluation_engine::EvaluationEngine;
use henosis_evaluation_engine::ResourceContract;
use henosis_evaluation_engine::ResourceRegistry;
use henosis_orchestrator::ControllerEffect;
use henosis_types::BundleRef;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerPass;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::KindVersion;
use henosis_types::OutputName;
use henosis_types::Resource;
use henosis_types::ResourceId;
use tokio::sync::mpsc;
use tracing::error;
use tracing::info;

pub trait ControllerReportHandler: Send + Sync {
    fn report(
        &self,
        report: ControllerReport,
    ) -> BoxFuture<'_, anyhow::Result<Vec<ControllerEffect>>>;
}

#[derive(Clone)]
pub struct ControllerDispatcher {
    effects: mpsc::UnboundedSender<Vec<ControllerEffect>>,
}

impl ControllerDispatcher {
    #[must_use]
    pub fn start(
        controllers: BTreeMap<ControllerName, Arc<dyn Controller>>,
        reports: Arc<dyn ControllerReportHandler>,
    ) -> Self {
        let (effects, mut effect_receiver) = mpsc::unbounded_channel::<Vec<ControllerEffect>>();
        let (pass_completions, mut pass_completion_receiver) =
            mpsc::unbounded_channel::<(ScheduledControllerPass, ControllerPass)>();
        let (report_completions, mut report_completion_receiver) =
            mpsc::unbounded_channel::<ReportCompletion>();
        tokio::spawn(async move {
            let controllers = Arc::new(controllers);
            let mut schedule = ControllerSchedule::default();
            loop {
                tokio::select! {
                    Some(incoming) = effect_receiver.recv() => {
                        submit_effects(
                            incoming,
                            &mut schedule,
                            &controllers,
                            &pass_completions,
                        );
                    }
                    Some((pass, outcome)) = pass_completion_receiver.recv() => {
                        match schedule.complete(&pass, outcome) {
                            ControllerScheduleCompletion::Continue => {
                                spawn_next_pass(
                                    &mut schedule,
                                    pass.key(),
                                    &controllers,
                                    &pass_completions,
                                    Duration::ZERO,
                                );
                            }
                            ControllerScheduleCompletion::Report(pending) => {
                                spawn_report_delivery(
                                    Arc::clone(&reports),
                                    pending,
                                    report_completions.clone(),
                                );
                            }
                            ControllerScheduleCompletion::Retry { attempt, message } => {
                                let delay = retry_delay(attempt);
                                error!(
                                    controller = %pass.key().controller(),
                                    graph = %pass.key().graph_id(),
                                    %message,
                                    ?delay,
                                    "controller pass failed; retrying from fresh observation",
                                );
                                spawn_next_pass(
                                    &mut schedule,
                                    pass.key(),
                                    &controllers,
                                    &pass_completions,
                                    delay,
                                );
                            }
                            ControllerScheduleCompletion::Complete => {}
                        }
                    }
                    Some(delivery) = report_completion_receiver.recv() => {
                        let key = delivery.pending.key().clone();
                        match schedule.acknowledge_report(&delivery.pending) {
                            ControllerScheduleCompletion::Continue => {
                                spawn_next_pass(
                                    &mut schedule,
                                    &key,
                                    &controllers,
                                    &pass_completions,
                                    Duration::ZERO,
                                );
                            }
                            ControllerScheduleCompletion::Complete => {}
                            ControllerScheduleCompletion::Report(_)
                            | ControllerScheduleCompletion::Retry { .. } => {
                                unreachable!("report acknowledgement cannot produce work")
                            }
                        }
                        submit_effects(
                            delivery.follow_up,
                            &mut schedule,
                            &controllers,
                            &pass_completions,
                        );
                    }
                    else => break,
                }
            }
        });
        Self { effects }
    }

    pub fn dispatch(&self, effects: Vec<ControllerEffect>) {
        if self.effects.send(effects).is_err() {
            error!("controller dispatcher stopped");
        }
    }
}

struct ReportCompletion {
    pending: ScheduledControllerReport,
    follow_up: Vec<ControllerEffect>,
}

fn submit_effects(
    effects: Vec<ControllerEffect>,
    schedule: &mut ControllerSchedule,
    controllers: &Arc<BTreeMap<ControllerName, Arc<dyn Controller>>>,
    completions: &mpsc::UnboundedSender<(ScheduledControllerPass, ControllerPass)>,
) {
    for effect in effects {
        if let Some(key) = schedule.submit(
            effect.controller().clone(),
            effect.command().clone(),
        ) {
            spawn_next_pass(schedule, &key, controllers, completions, Duration::ZERO);
        }
    }
}

fn spawn_next_pass(
    schedule: &mut ControllerSchedule,
    key: &henosis_controller_runtime::ControllerWorkKey,
    controllers: &Arc<BTreeMap<ControllerName, Arc<dyn Controller>>>,
    completions: &mpsc::UnboundedSender<(ScheduledControllerPass, ControllerPass)>,
    delay: Duration,
) {
    if let Some(pass) = schedule.pass(key) {
        spawn_controller_pass(Arc::clone(controllers), pass, completions.clone(), delay);
    }
}

fn spawn_controller_pass(
    controllers: Arc<BTreeMap<ControllerName, Arc<dyn Controller>>>,
    pass: ScheduledControllerPass,
    completions: mpsc::UnboundedSender<(ScheduledControllerPass, ControllerPass)>,
    delay: Duration,
) {
    tokio::spawn(async move {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let outcome = AssertUnwindSafe(async {
            let Some(controller) = controllers.get(pass.key().controller()) else {
                return ControllerPass::Retryable(format!(
                    "no controller named {}",
                    pass.key().controller()
                ));
            };
            match controller.execute(pass.command()).await {
                Ok(outcome) => outcome,
                Err(error) => ControllerPass::Retryable(error.to_string()),
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| ControllerPass::Retryable("controller pass panicked".to_owned()));
        let _ = completions.send((pass, outcome));
    });
}

fn spawn_report_delivery(
    reports: Arc<dyn ControllerReportHandler>,
    pending: ScheduledControllerReport,
    completions: mpsc::UnboundedSender<ReportCompletion>,
) {
    tokio::spawn(async move {
        let mut attempt = 0;
        loop {
            match reports.report(pending.report().clone()).await {
                Ok(follow_up) => {
                    let _ = completions.send(ReportCompletion { pending, follow_up });
                    break;
                }
                Err(error) => {
                    attempt += 1;
                    let delay = retry_delay(attempt);
                    error!(
                        graph = %pending.key().graph_id(),
                        controller = %pending.key().controller(),
                        %error,
                        ?delay,
                        "controller report was not acknowledged; retaining and retrying",
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    });
}

fn retry_delay(attempt: u32) -> Duration {
    Duration::from_secs(1_u64.checked_shl(attempt.saturating_sub(1).min(5)).unwrap_or(32).min(30))
}

pub struct ServerAssembly {
    pub bind: String,
    pub bundle_root: PathBuf,
    pub engine_config: EngineConfig,
    pub evaluator: Arc<dyn henosis_types::Evaluator>,
    pub controllers: BTreeMap<ControllerName, Arc<dyn Controller>>,
}

pub fn from_environment() -> anyhow::Result<ServerAssembly> {
    require_live_target("HENOSIS_CLOUDFLARE_LIVE", "Cloudflare")?;
    require_live_target("HENOSIS_SUPABASE_LIVE", "Supabase")?;
    assemble_from_environment(TargetAssembly::Live)
}

/// Explicit fake composition for tests and local demonstrations. The server
/// binary never selects this assembly implicitly.
pub fn demo_from_environment() -> anyhow::Result<ServerAssembly> {
    assemble_from_environment(TargetAssembly::Demo)
}

#[derive(Clone, Copy)]
enum TargetAssembly {
    Live,
    Demo,
}

fn assemble_from_environment(targets: TargetAssembly) -> anyhow::Result<ServerAssembly> {
    let bind = std::env::var("HENOSIS_BIND").unwrap_or_else(|_| "127.0.0.1:4481".into());
    let bundle_root = PathBuf::from(
        std::env::var("HENOSIS_BUNDLE_ROOT").unwrap_or_else(|_| ".henosis/bundles".into()),
    );
    let deploy_remote = PathBuf::from(
        std::env::var("HENOSIS_DEPLOY_REMOTE")
            .map_err(|_| anyhow::anyhow!("HENOSIS_DEPLOY_REMOTE is required"))?,
    );
    let engine_config = EngineConfig::default();
    let bundles = Arc::new(VerifiedBundleDirectory::new(bundle_root.clone()));
    let evaluator: Arc<dyn henosis_types::Evaluator> = Arc::new(EvaluationEngine::new(
        Arc::new(FileBundleSource {
            bundles: Arc::clone(&bundles),
        }),
        Arc::new(BuiltinResourceRegistry),
        engine_config.clone(),
    )?);

    let mut controllers: BTreeMap<ControllerName, Arc<dyn Controller>> = BTreeMap::new();
    let k8s: Arc<dyn Controller> = Arc::new(K8sController::new(GitRepository::new(deploy_remote)));
    controllers.insert(k8s.name().clone(), k8s);
    let cloudflare: Arc<dyn Controller> = match targets {
        TargetAssembly::Live => {
            let artifact_root = std::env::var("HENOSIS_ARTIFACT_ROOT").map_err(|_| {
                anyhow::anyhow!("HENOSIS_ARTIFACT_ROOT is required for live Cloudflare")
            })?;
            let transport = LiveCloudflareTransport::connect(
                &LiveCloudflareConfig::default(),
                Arc::new(DirectoryArtifactStore::new(artifact_root)),
            )?;
            info!("Cloudflare controller uses live transport");
            Arc::new(henosis_controller_cloudflare::CloudflareController::new(
                transport,
            ))
        }
        TargetAssembly::Demo => Arc::new(
            henosis_controller_cloudflare::CloudflareController::new(
                RecordedCloudflareTransport::default(),
            ),
        ),
    };
    controllers.insert(cloudflare.name().clone(), cloudflare);
    let supabase: Arc<dyn Controller> = match targets {
        TargetAssembly::Live => {
            let target = LocalSupabaseTarget::new(LocalSupabaseConfig {
                host: required_string_env("HENOSIS_SUPABASE_HOST")?,
                port: required_string_env("HENOSIS_SUPABASE_PORT")?.parse()?,
                user: required_string_env("HENOSIS_SUPABASE_USER")?,
                database: required_string_env("HENOSIS_SUPABASE_DATABASE")?,
                password_file: PathBuf::from(required_string_env(
                    "HENOSIS_SUPABASE_PASSWORD_FILE",
                )?),
                api_url: required_string_env("HENOSIS_SUPABASE_API_URL")?,
                database_url_ref: required_string_env("HENOSIS_SUPABASE_DATABASE_URL_REF")?,
                anon_key_ref: required_string_env("HENOSIS_SUPABASE_ANON_KEY_REF")?,
            });
            info!("Supabase controller uses live local Postgres/PostgREST target");
            Arc::new(SupabaseController::new(target, bundles))
        }
        TargetAssembly::Demo => Arc::new(DemoSupabaseController::new()),
    };
    controllers.insert(supabase.name().clone(), supabase);

    Ok(ServerAssembly {
        bind,
        bundle_root,
        engine_config,
        evaluator,
        controllers,
    })
}

fn require_live_target(variable: &str, target: &str) -> anyhow::Result<()> {
    if std::env::var(variable).as_deref() == Ok("1") {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "{variable}=1 is required for the normal server assembly; use the explicit demo \
             assembly for fake {target} wiring"
        ))
    }
}

fn required_string_env(name: &str) -> anyhow::Result<String> {
    std::env::var(name).map_err(|_| anyhow::anyhow!("{name} is required for the live assembly"))
}

struct FileBundleSource {
    bundles: Arc<VerifiedBundleDirectory>,
}

impl BundleSource for FileBundleSource {
    fn load(
        &self,
        bundle: BundleRef,
    ) -> BoxFuture<'_, Result<Arc<[u8]>, henosis_types::EvaluationError>> {
        Box::pin(async move {
            self.bundles
                .verify(bundle)
                .map(|verified| Arc::<[u8]>::from(verified.module))
                .map_err(|error| henosis_types::EvaluationError::new(error.to_string()))
        })
    }
}

struct BuiltinResourceRegistry;

impl ResourceRegistry for BuiltinResourceRegistry {
    fn validate(
        &self,
        kind: &KindVersion,
        body: &serde_json::Value,
    ) -> Result<ResourceContract, String> {
        if !body.is_object() {
            return Err(format!("{kind} body must be an object"));
        }
        let (controller, outputs): (&str, &[&str]) = match kind.to_string().as_str() {
            "k8s/object@1" => ("k8s", &[]),
            "supabase/schema@1" => (
                "supabase",
                &[
                    "project",
                    "database",
                    "schema",
                    "apiUrl",
                    "restUrl",
                    "databaseUrlRef",
                    "anonKeyRef",
                ],
            ),
            "cloudflare/worker@1" => (
                "cloudflare",
                &["url", "workerName", "deploymentId", "versionId"],
            ),
            "cloudflare/tunnel@1" => (
                "cloudflare",
                &["tunnelId", "tunnelName", "privateHostname", "tokenRef"],
            ),
            "cloudflare/route@1" => ("cloudflare", &["hostname"]),
            other => return Err(format!("unsupported resource kind {other}")),
        };
        Ok(ResourceContract::new(
            controller_name(controller),
            outputs
                .iter()
                .map(|name| OutputName::new(*name).expect("built-in API output name"))
                .collect(),
        ))
    }
}

struct DemoSupabaseController {
    name: ControllerName,
}

impl DemoSupabaseController {
    fn new() -> Self {
        Self {
            name: controller_name("supabase"),
        }
    }

    fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerReport, ControllerError> {
        let mut outputs = Vec::new();
        for resource in slice.resources() {
            for (name, value) in [
                ("project", serde_json::json!("henosis-local")),
                ("database", serde_json::json!("postgres")),
                ("schema", serde_json::json!("catalog")),
                ("apiUrl", serde_json::json!("http://127.0.0.1:4484")),
                (
                    "restUrl",
                    serde_json::json!("http://127.0.0.1:4484/rest/v1"),
                ),
                (
                    "databaseUrlRef",
                    serde_json::json!("demo-fake://supabase/database"),
                ),
                (
                    "anonKeyRef",
                    serde_json::json!("demo-fake://supabase/anon-key"),
                ),
            ] {
                if resource
                    .outputs()
                    .any(|declaration| declaration.name().as_str() == name)
                {
                    outputs.push(
                        output(resource, name, value)
                            .map_err(|error| ControllerError::new(error.to_string()))?,
                    );
                }
            }
        }
        ready_report(slice, Some(publication_id(b"demo-supabase-fake")), outputs)
            .map_err(|error| ControllerError::new(error.to_string()))
    }
}

impl Controller for DemoSupabaseController {
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
        Box::pin(async move {
            match command {
                ControllerCommand::Reconcile(slice) => self
                    .reconcile(slice)
                    .map(|report| ControllerPass::Converged(Some(report))),
                ControllerCommand::Supersede(_) | ControllerCommand::Retire(_) => {
                    Ok(ControllerPass::Converged(None))
                }
            }
        })
    }
}

#[derive(Default)]
struct RecordedCloudflareTransport {
    resources: Mutex<BTreeMap<(GraphId, ResourceId), CloudflareObservation>>,
}

impl CloudflareTransport for RecordedCloudflareTransport {
    fn observe<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<CloudflareObservation, CloudflareError>> {
        async move {
            Ok(self
                .resources
                .lock()
                .expect("recorded Cloudflare lock is not poisoned")
                .get(&(graph, resource.id()))
                .cloned()
                .unwrap_or(CloudflareObservation::Missing))
        }
        .boxed()
    }

    fn act<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
        action: CloudflareAction,
    ) -> BoxFuture<'a, Result<(), CloudflareError>> {
        async move {
            let mut resources = self
                .resources
                .lock()
                .expect("recorded Cloudflare lock is not poisoned");
            let key = (graph, resource.id());
            match action {
                CloudflareAction::UploadWorker(_) => {
                    resources.insert(
                        key,
                        CloudflareObservation::Worker {
                            digest: resource.digest(),
                            subdomain_enabled: false,
                            observation: WorkerObservation {
                                url: format!(
                                    "https://{}.workers.demo.invalid",
                                    resource.path().address().name()
                                ),
                                worker_name: resource.path().address().name().to_string(),
                                deployment_id: format!("recorded-{}", resource.id()),
                                version_id: "recorded-v1".into(),
                            },
                        },
                    );
                }
                CloudflareAction::EnableWorkerSubdomain => {
                    let Some(CloudflareObservation::Worker {
                        subdomain_enabled, ..
                    }) = resources.get_mut(&key)
                    else {
                        return Err(CloudflareError::Unavailable(
                            "recorded Worker disappeared".into(),
                        ));
                    };
                    *subdomain_enabled = true;
                }
                CloudflareAction::CreateTunnel => {
                    resources.insert(
                        key,
                        CloudflareObservation::Tunnel {
                            configured: false,
                            observation: TunnelObservation {
                                tunnel_id: format!("recorded-{}", resource.id()),
                                tunnel_name: resource.id().to_string(),
                                private_hostname: "supabase.internal.demo.invalid".into(),
                                token_ref: "demo-fake://cloudflare/tunnel-token".into(),
                            },
                        },
                    );
                }
                CloudflareAction::ConfigureTunnel(_) => {
                    let Some(CloudflareObservation::Tunnel { configured, .. }) =
                        resources.get_mut(&key)
                    else {
                        return Err(CloudflareError::Unavailable(
                            "recorded Tunnel disappeared".into(),
                        ));
                    };
                    *configured = true;
                }
                CloudflareAction::WriteRoute(body) => {
                    resources.insert(
                        key,
                        CloudflareObservation::Route {
                            matches: true,
                            observation: RouteObservation {
                                hostname: body.pattern,
                            },
                        },
                    );
                }
                CloudflareAction::Delete => {
                    resources.remove(&key);
                }
            }
            Ok(())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use henosis_controller_runtime::controller_name;
    use henosis_controller_runtime::ready_report;
    use henosis_orchestrator::ControllerEffect;
    use henosis_types::ContentDigest;
    use henosis_types::Generation;
    use tokio::sync::Notify;

    use super::*;

    struct PanicsOnceController {
        calls: AtomicUsize,
        name: ControllerName,
    }

    impl Controller for PanicsOnceController {
        fn name(&self) -> &ControllerName {
            &self.name
        }

        fn execute<'a>(
            &'a self,
            command: &'a ControllerCommand,
        ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("test controller panic");
                }
                let ControllerCommand::Reconcile(slice) = command else {
                    return Ok(ControllerPass::Converged(None));
                };
                Ok(ControllerPass::Converged(Some(
                    ready_report(slice, None, Vec::new()).unwrap(),
                )))
            })
        }
    }

    struct FailsFirstReport {
        calls: AtomicUsize,
        accepted: Notify,
    }

    impl ControllerReportHandler for FailsFirstReport {
        fn report(
            &self,
            _report: ControllerReport,
        ) -> BoxFuture<'_, anyhow::Result<Vec<ControllerEffect>>> {
            Box::pin(async move {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    anyhow::bail!("transient test failure");
                }
                self.accepted.notify_one();
                Ok(Vec::new())
            })
        }
    }

    #[tokio::test]
    async fn panic_and_transient_report_failure_do_not_stick_or_drop_lane() {
        let graph = GraphId::from_bytes([7; 16]);
        let name = controller_name("test");
        let controller = Arc::new(PanicsOnceController {
            calls: AtomicUsize::new(0),
            name: name.clone(),
        });
        let reports = Arc::new(FailsFirstReport {
            calls: AtomicUsize::new(0),
            accepted: Notify::new(),
        });
        let dispatcher = ControllerDispatcher::start(
            BTreeMap::from([(name.clone(), controller.clone() as Arc<dyn Controller>)]),
            reports.clone() as Arc<dyn ControllerReportHandler>,
        );
        let slice = ControllerSlice::new(
            graph,
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            name.clone(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
        dispatcher.dispatch(vec![ControllerEffect::new(
            name,
            ControllerCommand::Reconcile(slice),
        )]);

        tokio::time::timeout(Duration::from_secs(5), reports.accepted.notified())
            .await
            .expect("report should be retried and accepted");
        assert_eq!(controller.calls.load(Ordering::SeqCst), 2);
        assert_eq!(reports.calls.load(Ordering::SeqCst), 2);
    }
}
