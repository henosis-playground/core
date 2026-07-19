//! Composition root for the Henosis server process.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_cloudflare::{
    CloudflareAction, CloudflareError, CloudflareObservation, CloudflareTransport,
    LiveCloudflareConfig, LiveCloudflareTransport, RouteObservation, TunnelObservation,
    WorkerObservation,
};
use henosis_controller_k8s::K8sController;
use henosis_controller_runtime::{
    DirectoryArtifactStore, GitRepository, controller_name, output, publication_id, ready_report,
};
use henosis_evaluation_engine::{
    BundleSource, EngineConfig, EvaluationEngine, ResourceContract, ResourceRegistry,
};
use henosis_types::{
    BundleRef, Controller, ControllerCommand, ControllerError, ControllerName, ControllerPass,
    ControllerReport, ControllerSlice, GraphId, KindVersion, OutputName, Resource, ResourceId,
};
use tracing::info;

pub struct ServerAssembly {
    pub bind: String,
    pub bundle_root: PathBuf,
    pub engine_config: EngineConfig,
    pub evaluator: Arc<dyn henosis_types::Evaluator>,
    pub controllers: BTreeMap<ControllerName, Arc<dyn Controller>>,
}

pub fn from_environment() -> anyhow::Result<ServerAssembly> {
    let bind = std::env::var("HENOSIS_BIND").unwrap_or_else(|_| "127.0.0.1:4481".into());
    let bundle_root = PathBuf::from(
        std::env::var("HENOSIS_BUNDLE_ROOT").unwrap_or_else(|_| ".henosis/bundles".into()),
    );
    let deploy_remote = PathBuf::from(
        std::env::var("HENOSIS_DEPLOY_REMOTE")
            .map_err(|_| anyhow::anyhow!("HENOSIS_DEPLOY_REMOTE is required"))?,
    );
    let engine_config = EngineConfig::default();
    let evaluator: Arc<dyn henosis_types::Evaluator> = Arc::new(EvaluationEngine::new(
        Arc::new(FileBundleSource {
            root: bundle_root.clone(),
        }),
        Arc::new(BuiltinResourceRegistry),
        engine_config.clone(),
    )?);

    let mut controllers: BTreeMap<ControllerName, Arc<dyn Controller>> = BTreeMap::new();
    let k8s: Arc<dyn Controller> = Arc::new(K8sController::new(GitRepository::new(deploy_remote)));
    controllers.insert(k8s.name().clone(), k8s);
    let cloudflare: Arc<dyn Controller> = if std::env::var("HENOSIS_CLOUDFLARE_LIVE").as_deref()
        == Ok("1")
    {
        let artifact_root = std::env::var("HENOSIS_ARTIFACT_ROOT").map_err(|_| {
            anyhow::anyhow!("HENOSIS_ARTIFACT_ROOT is required for live Cloudflare")
        })?;
        let transport = LiveCloudflareTransport::connect(
            &LiveCloudflareConfig::default(),
            Arc::new(DirectoryArtifactStore::new(artifact_root)),
        )?;
        info!("Cloudflare controller uses LIVE transport (metadata/identity safety rail enforced)");
        Arc::new(henosis_controller_cloudflare::CloudflareController::new(
            transport,
        ))
    } else {
        Arc::new(henosis_controller_cloudflare::CloudflareController::new(
            RecordedCloudflareTransport::default(),
        ))
    };
    controllers.insert(cloudflare.name().clone(), cloudflare);
    let supabase: Arc<dyn Controller> = Arc::new(DemoSupabaseController::new());
    controllers.insert(supabase.name().clone(), supabase);

    Ok(ServerAssembly {
        bind,
        bundle_root,
        engine_config,
        evaluator,
        controllers,
    })
}

struct FileBundleSource {
    root: PathBuf,
}

impl BundleSource for FileBundleSource {
    fn load(
        &self,
        bundle: BundleRef,
    ) -> BoxFuture<'_, Result<Arc<[u8]>, henosis_types::EvaluationError>> {
        let path = self
            .root
            .join(bundle.digest().to_string())
            .join("module.js");
        Box::pin(async move {
            tokio::fs::read(&path)
                .await
                .map(Arc::<[u8]>::from)
                .map_err(|error| {
                    henosis_types::EvaluationError::new(format!(
                        "cannot load bundle {}: {error}",
                        path.display()
                    ))
                })
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
