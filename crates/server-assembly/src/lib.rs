//! Composition root for the Henosis server process.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_app::BundleStore;
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
use henosis_controller_runtime::DirectoryArtifactStore;
use henosis_controller_runtime::GitRepository;
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
use tracing::info;

pub struct ServerAssembly {
    pub bind: String,
    pub bundle_root: PathBuf,
    pub bundle_store: Arc<dyn BundleStore>,
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
    let durable_bundle_root = PathBuf::from(
        std::env::var("HENOSIS_DURABLE_BUNDLE_ROOT")
            .map_err(|_| anyhow::anyhow!("HENOSIS_DURABLE_BUNDLE_ROOT is required"))?,
    );
    let deploy_remote = PathBuf::from(
        std::env::var("HENOSIS_DEPLOY_REMOTE")
            .map_err(|_| anyhow::anyhow!("HENOSIS_DEPLOY_REMOTE is required"))?,
    );
    let engine_config = EngineConfig::default();
    let bundles = Arc::new(VerifiedBundleDirectory::new(durable_bundle_root));
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
        TargetAssembly::Demo => Arc::new(henosis_controller_cloudflare::CloudflareController::new(
            RecordedCloudflareTransport::default(),
        )),
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
            Arc::new(SupabaseController::new(target, bundles.clone()))
        }
        TargetAssembly::Demo => Arc::new(DemoSupabaseController::new()),
    };
    controllers.insert(supabase.name().clone(), supabase);

    Ok(ServerAssembly {
        bind,
        bundle_root,
        bundle_store: bundles,
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

