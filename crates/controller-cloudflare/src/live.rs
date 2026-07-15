//! Live Cloudflare API transport.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use base64::Engine as _;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_types::ArtifactDigest;
use henosis_types::ArtifactStore;
use henosis_types::ContentDigest;
use henosis_types::GraphId;
use henosis_types::Resource;
use henosis_types::ResourceId;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use serde::Deserialize;
use serde::Serialize;
use sha2::Digest as _;
use sha2::Sha256;
use uuid::Uuid;

use crate::ArtifactKind;
use crate::CloudflareAction;
use crate::CloudflareError;
use crate::CloudflareObservation;
use crate::CloudflareTransport;
use crate::RouteBody;
use crate::RouteObservation;
use crate::TunnelBody;
use crate::TunnelObservation;
use crate::WorkerBody;
use crate::WorkerObservation;

const DEFAULT_API_BASE: &str = "https://api.cloudflare.com/client/v4";
const LOGIN_HELP: &str = "run `wrangler login` and retry";

#[derive(Clone, Debug)]
pub struct LiveCloudflareConfig {
    pub enabled: bool,
    pub account_id: Option<String>,
    pub api_base: String,
    pub wrangler: PathBuf,
    pub wrangler_config: PathBuf,
}

impl Default for LiveCloudflareConfig {
    fn default() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            enabled: std::env::var("HENOSIS_CLOUDFLARE_LIVE").as_deref() == Ok("1"),
            account_id: std::env::var("CLOUDFLARE_ACCOUNT_ID").ok(),
            api_base: DEFAULT_API_BASE.into(),
            wrangler: PathBuf::from("wrangler"),
            wrangler_config: home.join(".config/.wrangler/config/default.toml"),
        }
    }
}

pub struct LiveCloudflareTransport {
    session: CloudflareSession,
    artifacts: Arc<dyn ArtifactStore>,
}

#[derive(Clone, Debug)]
struct CloudflareSession {
    account_id: String,
    api_base: String,
    client: Client,
    token: String,
}

#[derive(Deserialize)]
struct WranglerConfig {
    oauth_token: Option<String>,
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    success: bool,
    result: Option<T>,
    #[serde(default)]
    errors: Option<Vec<ApiError>>,
}

#[derive(Deserialize)]
struct ApiError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct Membership {
    account: Account,
}

#[derive(Deserialize)]
struct Account {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct Subdomain {
    subdomain: String,
}

#[derive(Deserialize)]
struct ScriptSubdomain {
    enabled: bool,
}

#[derive(Deserialize)]
struct ScriptSettings {
    #[serde(default)]
    tags: Vec<String>,
}

#[derive(Deserialize)]
struct TunnelConfiguration {
    config: TunnelConfigurationBody,
}

#[derive(Deserialize)]
struct TunnelConfigurationBody {
    #[serde(default)]
    ingress: Vec<TunnelIngress>,
}

#[derive(Deserialize)]
struct TunnelIngress {
    service: String,
}

#[derive(Deserialize)]
struct DeploymentList {
    #[serde(default)]
    deployments: Vec<Deployment>,
}

#[derive(Deserialize)]
struct Deployment {
    id: String,
    #[serde(default)]
    versions: Vec<DeploymentVersion>,
}

#[derive(Deserialize)]
struct DeploymentVersion {
    version_id: String,
}

#[derive(Deserialize)]
struct Tunnel {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct Zone {
    id: String,
    name: String,
}

#[derive(Deserialize)]
struct WorkerRoute {
    id: String,
    pattern: String,
    #[serde(default)]
    script: String,
}

/// Static-assets blob written by the shared frontend artifact builder.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssetsArtifact {
    format: String,
    files: BTreeMap<String, Vec<u8>>,
}

#[derive(Deserialize)]
struct AssetsUploadSession {
    jwt: String,
    #[serde(default)]
    buckets: Vec<Vec<String>>,
}

#[derive(Deserialize)]
struct AssetsUploadResult {
    jwt: String,
}

#[derive(Serialize)]
struct AssetManifestEntry {
    hash: String,
    size: usize,
}

#[derive(Serialize)]
struct TunnelCreate<'a> {
    name: &'a str,
    tunnel_secret: String,
    config_src: &'static str,
}

#[derive(Serialize)]
struct RouteWrite<'a> {
    pattern: &'a str,
    script: &'a str,
}

impl LiveCloudflareTransport {
    pub fn connect(
        config: &LiveCloudflareConfig,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Result<Self, CloudflareError> {
        if !config.enabled {
            return Err(CloudflareError::Config(
                "live Cloudflare mutations are disabled; set HENOSIS_CLOUDFLARE_LIVE=1 to opt in"
                    .into(),
            ));
        }
        let session = CloudflareSession::connect(config)?;
        Ok(Self { session, artifacts })
    }

    async fn upload_worker(
        &self,
        graph: GraphId,
        resource: &Resource,
        body: &WorkerBody,
    ) -> Result<(), CloudflareError> {
        let name = worker_name(resource);
        if body.source.entry.kind != ArtifactKind::CloudflareWorker {
            return Err(CloudflareError::Contract(
                "Worker source entry must reference a cloudflare-worker artifact".into(),
            ));
        }
        let bytes = self
            .artifacts
            .fetch(body.source.entry.digest)
            .await
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let assets_jwt = match &body.source.assets {
            Some(reference) if reference.kind == ArtifactKind::StaticAssets => {
                Some(self.upload_assets(&name, reference.digest).await?)
            }
            Some(_) => {
                return Err(CloudflareError::Contract(
                    "Worker assets must reference a static-assets artifact".into(),
                ));
            }
            None => None,
        };
        if assets_jwt.is_some() && body.vars.contains_key("ASSETS") {
            return Err(CloudflareError::Contract(
                "Worker variable ASSETS conflicts with the static-assets binding".into(),
            ));
        }
        let mut bindings = body
            .vars
            .iter()
            .map(|(name, value)| {
                serde_json::json!({
                    "type": "plain_text",
                    "name": name,
                    "text": plain_binding(value),
                })
            })
            .collect::<Vec<_>>();
        if assets_jwt.is_some() {
            bindings.push(serde_json::json!({"type": "assets", "name": "ASSETS"}));
        }
        for (name, service) in &body.services {
            if body.vars.contains_key(name) || (assets_jwt.is_some() && name == "ASSETS") {
                return Err(CloudflareError::Contract(format!(
                    "Worker service binding {name} conflicts with another binding"
                )));
            }
            self.require_owned_worker(graph, service).await?;
            bindings.push(serde_json::json!({
                "type": "service",
                "name": name,
                "service": service,
            }));
        }
        let mut metadata = serde_json::json!({
            "main_module": "worker.mjs",
            "bindings": bindings,
            "tags": ownership_tags(graph, resource),
        });
        if let Some(date) = &body.compatibility_date {
            metadata["compatibility_date"] = serde_json::Value::String(date.clone());
        }
        if !body.compatibility_flags.is_empty() {
            metadata["compatibility_flags"] = serde_json::json!(body.compatibility_flags);
        }
        if let Some(jwt) = assets_jwt {
            metadata["assets"] = serde_json::json!({"jwt": jwt});
        }
        let metadata = serde_json::to_string(&metadata)
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let metadata = Part::text(metadata)
            .mime_str("application/json")
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let module = Part::bytes(bytes.to_vec())
            .file_name("worker.mjs")
            .mime_str("application/javascript+module")
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let form = Form::new()
            .part("metadata", metadata)
            .part("worker.mjs", module);
        let _: serde_json::Value = self
            .session
            .request(
                self.session
                    .client
                    .put(self.session.account_url(&format!("workers/scripts/{name}")))
                    .multipart(form),
                "Worker module upload",
            )
            .await?;
        Ok(())
    }

    async fn upload_assets(
        &self,
        worker_name: &str,
        digest: ArtifactDigest,
    ) -> Result<String, CloudflareError> {
        let bytes = self
            .artifacts
            .fetch(digest)
            .await
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let archive: AssetsArtifact = serde_json::from_slice(&bytes).map_err(|error| {
            CloudflareError::Contract(format!(
                "Worker assets artifact {digest} is not valid JSON: {error}"
            ))
        })?;
        if archive.format != "henosis-static-assets-v1" || archive.files.is_empty() {
            return Err(CloudflareError::Contract(format!(
                "Worker assets artifact {digest} must use henosis-static-assets-v1 and contain at \
                 least one file"
            )));
        }
        let mut manifest = BTreeMap::new();
        let mut files = BTreeMap::new();
        for (relative_path, raw) in archive.files {
            validate_asset_path(&relative_path)?;
            let path = format!("/{relative_path}");
            let encoded = base64::engine::general_purpose::STANDARD.encode(&raw);
            let hash = asset_hash(&path, &encoded);
            manifest.insert(
                path.clone(),
                AssetManifestEntry {
                    hash: hash.clone(),
                    size: raw.len(),
                },
            );
            let candidate = (encoded, asset_content_type(&path).to_owned());
            if let Some(existing) = files.get(&hash) {
                if existing != &candidate {
                    return Err(CloudflareError::Contract(format!(
                        "Worker assets artifact {digest} contains a truncated-hash collision at \
                         {hash}"
                    )));
                }
            } else {
                files.insert(hash, candidate);
            }
        }
        let session: AssetsUploadSession = self
            .session
            .request(
                self.session
                    .client
                    .post(self.session.account_url(&format!(
                        "workers/scripts/{worker_name}/assets-upload-session"
                    )))
                    .json(&serde_json::json!({"manifest": manifest})),
                "Worker assets upload session",
            )
            .await?;
        let mut completion_jwt = session.jwt.clone();
        for bucket in session.buckets {
            let mut form = Form::new();
            for hash in bucket {
                let (encoded, content_type) = files.get(&hash).ok_or_else(|| {
                    CloudflareError::Provider(format!(
                        "Worker assets upload requested unknown content hash {hash}"
                    ))
                })?;
                let part = Part::text(encoded.clone())
                    .mime_str(content_type)
                    .map_err(|error| CloudflareError::Contract(error.to_string()))?;
                form = form.part(hash, part);
            }
            let result: AssetsUploadResult = self
                .session
                .request_with_bearer(
                    self.session
                        .client
                        .post(self.session.account_url("workers/assets/upload"))
                        .query(&[("base64", "true")])
                        .multipart(form),
                    &session.jwt,
                    "Worker assets bucket upload",
                )
                .await?;
            completion_jwt = result.jwt;
        }
        Ok(completion_jwt)
    }

    async fn observe_worker(
        &self,
        graph: GraphId,
        resource: &Resource,
    ) -> Result<CloudflareObservation, CloudflareError> {
        let name = worker_name(resource);
        let Some(settings): Option<ScriptSettings> = self
            .session
            .request_optional(
                self.session
                    .client
                    .get(self.session.account_url(&format!("workers/scripts/{name}/settings"))),
                "Worker settings observation",
            )
            .await?
        else {
            return Ok(CloudflareObservation::Missing);
        };
        if !tags_match_owner(&settings.tags, graph, resource.id()) {
            return Ok(CloudflareObservation::Foreign);
        }
        let digest = tagged_digest(&settings.tags).ok_or_else(|| {
            CloudflareError::Provider(format!(
                "Worker {name:?} has ownership tags but no valid desired-state digest tag"
            ))
        })?;
        let subdomain: ScriptSubdomain = self
            .session
            .request(
                self.session
                    .client
                    .get(self.session.account_url(&format!("workers/scripts/{name}/subdomain"))),
                "Worker subdomain observation",
            )
            .await?;
        let deployments: DeploymentList = self
            .session
            .request(
                self.session.client.get(
                    self.session
                        .account_url(&format!("workers/scripts/{name}/deployments")),
                ),
                "Worker deployment observation",
            )
            .await?;
        let deployment = deployments.deployments.first().ok_or_else(|| {
            CloudflareError::Provider(format!("Worker {name:?} has no active deployment"))
        })?;
        let version = deployment.versions.first().ok_or_else(|| {
            CloudflareError::Provider(format!("Worker {name:?} deployment has no version"))
        })?;
        let subdomain_name = self.session.workers_subdomain().await?;
        Ok(CloudflareObservation::Worker {
            digest,
            subdomain_enabled: subdomain.enabled,
            observation: WorkerObservation {
                url: format!("https://{name}.{subdomain_name}.workers.dev"),
                worker_name: name,
                deployment_id: deployment.id.clone(),
                version_id: version.version_id.clone(),
            },
        })
    }

    async fn require_owned_worker(
        &self,
        graph: GraphId,
        name: &str,
    ) -> Result<(), CloudflareError> {
        let Some(settings): Option<ScriptSettings> = self
            .session
            .request_optional(
                self.session
                    .client
                    .get(self.session.account_url(&format!("workers/scripts/{name}/settings"))),
                "Worker ownership observation",
            )
            .await?
        else {
            return Err(CloudflareError::Provider(format!(
                "referenced Worker {name:?} does not exist"
            )));
        };
        if settings.tags.iter().any(|tag| tag == &graph_tag(graph)) {
            Ok(())
        } else {
            Err(CloudflareError::Provider(format!(
                "refusing to use Worker {name:?}; its graph ownership tag does not match {graph}"
            )))
        }
    }

    async fn observe_tunnel(
        &self,
        graph: GraphId,
        resource: &Resource,
        body: &TunnelBody,
    ) -> Result<CloudflareObservation, CloudflareError> {
        let name = tunnel_identity(graph, resource.id());
        let tunnels: Vec<Tunnel> = self
            .session
            .request(
                self.session
                    .client
                    .get(self.session.account_url("cfd_tunnel"))
                    .query(&[("name", name.as_str()), ("is_deleted", "false")]),
                "Cloudflare Tunnel observation",
            )
            .await?;
        let Some(tunnel) = tunnels.into_iter().find(|item| item.name == name) else {
            return Ok(CloudflareObservation::Missing);
        };
        let configuration: TunnelConfiguration = self
            .session
            .request(
                self.session.client.get(
                    self.session
                        .account_url(&format!("cfd_tunnel/{}/configurations", tunnel.id)),
                ),
                "Cloudflare Tunnel configuration observation",
            )
            .await?;
        let expected = [
            format!("http://{}:{}", body.origin.host, body.origin.port),
            "http_status:404".into(),
        ];
        let actual = configuration
            .config
            .ingress
            .into_iter()
            .map(|entry| entry.service)
            .collect::<Vec<_>>();
        Ok(CloudflareObservation::Tunnel {
            configured: actual == expected,
            observation: TunnelObservation {
                tunnel_id: tunnel.id.clone(),
                tunnel_name: name,
                private_hostname: format!("{}.cfargotunnel.com", tunnel.id),
                token_ref: format!("cloudflare://tunnels/{}/token", tunnel.id),
            },
        })
    }

    async fn create_tunnel(
        &self,
        graph: GraphId,
        resource: &Resource,
    ) -> Result<(), CloudflareError> {
        let name = tunnel_identity(graph, resource.id());
        let _: Tunnel = self
            .session
            .request(
                self.session
                    .client
                    .post(self.session.account_url("cfd_tunnel"))
                    .json(&TunnelCreate {
                        name: &name,
                        tunnel_secret: uuid_pair_base64(),
                        config_src: "cloudflare",
                    }),
                "Cloudflare Tunnel create",
            )
            .await?;
        Ok(())
    }

    async fn configure_tunnel(
        &self,
        graph: GraphId,
        resource: &Resource,
        body: &TunnelBody,
    ) -> Result<(), CloudflareError> {
        let name = tunnel_identity(graph, resource.id());
        let tunnels: Vec<Tunnel> = self
            .session
            .request(
                self.session
                    .client
                    .get(self.session.account_url("cfd_tunnel"))
                    .query(&[("name", name.as_str()), ("is_deleted", "false")]),
                "Cloudflare Tunnel observation",
            )
            .await?;
        let tunnel = tunnels
            .into_iter()
            .find(|item| item.name == name)
            .ok_or_else(|| CloudflareError::Unavailable("Tunnel disappeared before configuration".into()))?;
        let config = serde_json::json!({
            "config": {
                "ingress": [
                    {"service": format!("http://{}:{}", body.origin.host, body.origin.port)},
                    {"service": "http_status:404"}
                ]
            }
        });
        let _: serde_json::Value = self
            .session
            .request(
                self.session
                    .client
                    .put(self.session.account_url(&format!(
                        "cfd_tunnel/{}/configurations",
                        tunnel.id
                    )))
                    .json(&config),
                "Cloudflare Tunnel configuration",
            )
            .await?;
        Ok(())
    }

    async fn observe_route(
        &self,
        graph: GraphId,
        body: &RouteBody,
    ) -> Result<CloudflareObservation, CloudflareError> {
        self.require_owned_worker(graph, &body.worker_name).await?;
        let zone = self.session.resolve_zone(&body.zone).await?;
        let routes: Vec<WorkerRoute> = self
            .session
            .request(
                self.session.client.get(
                    self.session
                        .url(&format!("zones/{}/workers/routes", zone.id)),
                ),
                "Worker route observation",
            )
            .await?;
        let Some(route) = routes.into_iter().find(|route| route.pattern == body.pattern) else {
            return Ok(CloudflareObservation::Missing);
        };
        if route.script != body.worker_name {
            return Ok(CloudflareObservation::Foreign);
        }
        Ok(CloudflareObservation::Route {
            matches: true,
            observation: RouteObservation {
                hostname: hostname_from_pattern(&body.pattern),
            },
        })
    }

    async fn write_route(
        &self,
        graph: GraphId,
        body: &RouteBody,
    ) -> Result<(), CloudflareError> {
        self.require_owned_worker(graph, &body.worker_name).await?;
        let zone = self.session.resolve_zone(&body.zone).await?;
        let routes: Vec<WorkerRoute> = self
            .session
            .request(
                self.session.client.get(
                    self.session
                        .url(&format!("zones/{}/workers/routes", zone.id)),
                ),
                "Worker route observation",
            )
            .await?;
        let existing = routes.into_iter().find(|route| route.pattern == body.pattern);
        if let Some(route) = &existing
            && route.script != body.worker_name
        {
            return Err(CloudflareError::Provider(format!(
                "refusing to replace route {:?} owned by script {:?}",
                body.pattern, route.script
            )));
        }
        let request = RouteWrite {
            pattern: &body.pattern,
            script: &body.worker_name,
        };
        match existing {
            Some(route) => {
                let _: WorkerRoute = self
                    .session
                    .request(
                        self.session
                            .client
                            .put(self.session.url(&format!(
                                "zones/{}/workers/routes/{}",
                                zone.id, route.id
                            )))
                            .json(&request),
                        "Worker route update",
                    )
                    .await?;
            }
            None => {
                let _: WorkerRoute = self
                    .session
                    .request(
                        self.session
                            .client
                            .post(self.session.url(&format!(
                                "zones/{}/workers/routes",
                                zone.id
                            )))
                            .json(&request),
                        "Worker route create",
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn delete_resource(
        &self,
        graph: GraphId,
        resource: &Resource,
    ) -> Result<(), CloudflareError> {
        match resource.kind().name().as_str() {
            "cloudflare/worker" => {
                let name = worker_name(resource);
                let Some(settings): Option<ScriptSettings> = self
                    .session
                    .request_optional(
                        self.session.client.get(
                            self.session
                                .account_url(&format!("workers/scripts/{name}/settings")),
                        ),
                        "Worker retirement ownership observation",
                    )
                    .await?
                else {
                    return Ok(());
                };
                if !tags_match_owner(&settings.tags, graph, resource.id()) {
                    return Err(CloudflareError::Provider(format!(
                        "refusing to delete Worker {name:?}; ownership tags do not match"
                    )));
                }
                self.session
                    .request_empty(
                        self.session.client.delete(
                            self.session
                                .account_url(&format!("workers/scripts/{name}")),
                        ),
                        "Worker retirement",
                    )
                    .await
            }
            "cloudflare/tunnel" => {
                let name = tunnel_identity(graph, resource.id());
                let tunnels: Vec<Tunnel> = self
                    .session
                    .request(
                        self.session
                            .client
                            .get(self.session.account_url("cfd_tunnel"))
                            .query(&[("name", name.as_str()), ("is_deleted", "false")]),
                        "Cloudflare Tunnel retirement observation",
                    )
                    .await?;
                let Some(tunnel) = tunnels.into_iter().find(|item| item.name == name) else {
                    return Ok(());
                };
                self.session
                    .request_empty(
                        self.session
                            .client
                            .delete(self.session.account_url(&format!("cfd_tunnel/{}", tunnel.id)))
                            .query(&[("cascade", "true")]),
                        "Cloudflare Tunnel retirement",
                    )
                    .await
            }
            "cloudflare/route" => {
                let body: RouteBody = serde_json::from_value(resource.body().as_json().clone())
                    .map_err(|error| CloudflareError::Contract(error.to_string()))?;
                self.require_owned_worker(graph, &body.worker_name).await?;
                let zone = self.session.resolve_zone(&body.zone).await?;
                let routes: Vec<WorkerRoute> = self
                    .session
                    .request(
                        self.session.client.get(
                            self.session
                                .url(&format!("zones/{}/workers/routes", zone.id)),
                        ),
                        "Worker route retirement observation",
                    )
                    .await?;
                let Some(route) = routes
                    .into_iter()
                    .find(|route| route.pattern == body.pattern && route.script == body.worker_name)
                else {
                    return Ok(());
                };
                self.session
                    .request_empty(
                        self.session.client.delete(self.session.url(&format!(
                            "zones/{}/workers/routes/{}",
                            zone.id, route.id
                        ))),
                        "Worker route retirement",
                    )
                    .await
            }
            _ => Err(CloudflareError::Contract(format!(
                "unsupported Cloudflare resource kind {}",
                resource.kind()
            ))),
        }
    }
}

impl CloudflareTransport for LiveCloudflareTransport {
    fn observe<'a>(
        &'a self,
        graph: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<CloudflareObservation, CloudflareError>> {
        async move {
            match resource.kind().name().as_str() {
                "cloudflare/worker" => self.observe_worker(graph, resource).await,
                "cloudflare/tunnel" => {
                    let body: TunnelBody = serde_json::from_value(resource.body().as_json().clone())
                        .map_err(|error| CloudflareError::Contract(error.to_string()))?;
                    self.observe_tunnel(graph, resource, &body).await
                }
                "cloudflare/route" => {
                    let body: RouteBody = serde_json::from_value(resource.body().as_json().clone())
                        .map_err(|error| CloudflareError::Contract(error.to_string()))?;
                    self.observe_route(graph, &body).await
                }
                _ => Err(CloudflareError::Contract(format!(
                    "unsupported Cloudflare resource kind {}",
                    resource.kind()
                ))),
            }
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
            match action {
                CloudflareAction::UploadWorker(body) => {
                    self.upload_worker(graph, resource, &body).await
                }
                CloudflareAction::EnableWorkerSubdomain => {
                    self.session.enable_worker_subdomain(&worker_name(resource)).await
                }
                CloudflareAction::CreateTunnel => self.create_tunnel(graph, resource).await,
                CloudflareAction::ConfigureTunnel(body) => {
                    self.configure_tunnel(graph, resource, &body).await
                }
                CloudflareAction::WriteRoute(body) => self.write_route(graph, &body).await,
                CloudflareAction::Delete => self.delete_resource(graph, resource).await,
            }
        }
        .boxed()
    }
}

impl CloudflareSession {
    fn connect(config: &LiveCloudflareConfig) -> Result<Self, CloudflareError> {
        refresh_wrangler_login(config)?;
        let source = fs::read_to_string(&config.wrangler_config).map_err(|error| {
            CloudflareError::Config(format!(
                "cannot read Wrangler OAuth credentials at {}: {error}; {LOGIN_HELP}",
                config.wrangler_config.display()
            ))
        })?;
        let credentials: WranglerConfig = toml::from_str(&source).map_err(|error| {
            CloudflareError::Config(format!(
                "cannot parse Wrangler OAuth credentials: {error}; {LOGIN_HELP}"
            ))
        })?;
        let token = credentials
            .oauth_token
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                CloudflareError::Config(format!(
                    "Wrangler OAuth credentials contain no oauth_token; {LOGIN_HELP}"
                ))
            })?;
        let client = Client::builder()
            .user_agent("henosis-controller-cloudflare/0.1")
            .build()
            .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
        let runtime = tokio::runtime::Handle::try_current().ok();
        let account_id = match &config.account_id {
            Some(value) => value.clone(),
            None => match runtime {
                Some(handle) => tokio::task::block_in_place(|| {
                    handle.block_on(discover_account(&client, &config.api_base, &token))
                })?,
                None => tokio::runtime::Runtime::new()
                    .map_err(|error| CloudflareError::Unavailable(error.to_string()))?
                    .block_on(discover_account(&client, &config.api_base, &token))?,
            },
        };
        Ok(Self {
            account_id,
            api_base: config.api_base.clone(),
            client,
            token,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}/{}", self.api_base.trim_end_matches('/'), path)
    }

    fn account_url(&self, path: &str) -> String {
        self.url(&format!("accounts/{}/{path}", self.account_id))
    }

    async fn request<T>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &str,
    ) -> Result<T, CloudflareError>
    where
        T: for<'de> Deserialize<'de>,
    {
        self.request_with_bearer(request, &self.token, operation)
            .await
    }

    async fn request_optional<T>(
        &self,
        request: reqwest::RequestBuilder,
        operation: &str,
    ) -> Result<Option<T>, CloudflareError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = request
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let status = response.status();
        let envelope: ApiEnvelope<T> = response.json().await.map_err(|error| {
            CloudflareError::Provider(format!("{operation} returned invalid JSON: {error}"))
        })?;
        if !status.is_success() || !envelope.success {
            return Err(CloudflareError::Provider(format!(
                "{operation} returned {status}: {}",
                api_errors(envelope.errors.as_deref().unwrap_or_default())
            )));
        }
        Ok(envelope.result)
    }

    async fn request_with_bearer<T>(
        &self,
        request: reqwest::RequestBuilder,
        bearer: &str,
        operation: &str,
    ) -> Result<T, CloudflareError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let response = request
            .bearer_auth(bearer)
            .send()
            .await
            .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
        let status = response.status();
        let envelope: ApiEnvelope<T> = response.json().await.map_err(|error| {
            CloudflareError::Provider(format!("{operation} returned invalid JSON: {error}"))
        })?;
        if !status.is_success() || !envelope.success {
            return Err(CloudflareError::Provider(format!(
                "{operation} returned {status}: {}",
                api_errors(envelope.errors.as_deref().unwrap_or_default())
            )));
        }
        envelope
            .result
            .ok_or_else(|| CloudflareError::Provider(format!("{operation} returned no result")))
    }

    async fn request_empty(
        &self,
        request: reqwest::RequestBuilder,
        operation: &str,
    ) -> Result<(), CloudflareError> {
        let response = request
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(());
        }
        let status = response.status();
        let envelope: ApiEnvelope<serde_json::Value> = response.json().await.map_err(|error| {
            CloudflareError::Provider(format!("{operation} returned invalid JSON: {error}"))
        })?;
        if status.is_success() && envelope.success {
            Ok(())
        } else {
            Err(CloudflareError::Provider(format!(
                "{operation} returned {status}: {}",
                api_errors(envelope.errors.as_deref().unwrap_or_default())
            )))
        }
    }

    async fn workers_subdomain(&self) -> Result<String, CloudflareError> {
        let result: Subdomain = self
            .request(
                self.client.get(self.account_url("workers/subdomain")),
                "workers.dev subdomain observation",
            )
            .await?;
        if result.subdomain.is_empty() {
            return Err(CloudflareError::Config(
                "Cloudflare returned an empty workers.dev subdomain; configure one in the Workers \
                 dashboard"
                    .into(),
            ));
        }
        Ok(result.subdomain)
    }

    async fn enable_worker_subdomain(&self, worker_name: &str) -> Result<(), CloudflareError> {
        let result: ScriptSubdomain = self
            .request(
                self.client
                    .post(self.account_url(&format!("workers/scripts/{worker_name}/subdomain")))
                    .json(&serde_json::json!({
                        "enabled": true,
                        "previews_enabled": false,
                    })),
                "Worker workers.dev enablement",
            )
            .await?;
        if !result.enabled {
            return Err(CloudflareError::Provider(
                "Cloudflare did not confirm workers.dev enablement for the uploaded Worker".into(),
            ));
        }
        Ok(())
    }

    async fn resolve_zone(&self, name_or_id: &str) -> Result<Zone, CloudflareError> {
        if looks_like_cloudflare_id(name_or_id) {
            let zone: Zone = self
                .request(
                    self.client.get(self.url(&format!("zones/{name_or_id}"))),
                    "zone observation",
                )
                .await?;
            return Ok(zone);
        }
        let zones: Vec<Zone> = self
            .request(
                self.client.get(self.url("zones")).query(&[
                    ("name", name_or_id),
                    ("account.id", self.account_id.as_str()),
                ]),
                "zone discovery",
            )
            .await?;
        zones
            .into_iter()
            .find(|zone| zone.name == name_or_id)
            .ok_or_else(|| {
                CloudflareError::Config(format!(
                    "Cloudflare zone {name_or_id:?} is not present in account {}",
                    self.account_id
                ))
            })
    }
}

async fn discover_account(
    client: &Client,
    api_base: &str,
    token: &str,
) -> Result<String, CloudflareError> {
    let response = client
        .get(format!("{}/memberships", api_base.trim_end_matches('/')))
        .bearer_auth(token)
        .send()
        .await
        .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
    let status = response.status();
    let envelope: ApiEnvelope<Vec<Membership>> = response.json().await.map_err(|error| {
        CloudflareError::Provider(format!("account discovery returned invalid JSON: {error}"))
    })?;
    if !status.is_success() || !envelope.success {
        return Err(CloudflareError::Provider(format!(
            "account discovery returned {status}: {}",
            api_errors(envelope.errors.as_deref().unwrap_or_default())
        )));
    }
    let memberships = envelope.result.unwrap_or_default();
    match memberships.as_slice() {
        [] => Err(CloudflareError::Config(format!(
            "Wrangler login has no Cloudflare account memberships; {LOGIN_HELP}"
        ))),
        [membership] => Ok(membership.account.id.clone()),
        _ => Err(CloudflareError::Config(format!(
            "Wrangler login has multiple Cloudflare accounts ({}); set CLOUDFLARE_ACCOUNT_ID to \
             one of: {}",
            memberships.len(),
            memberships
                .iter()
                .map(|membership| membership.account.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))),
    }
}

fn refresh_wrangler_login(config: &LiveCloudflareConfig) -> Result<(), CloudflareError> {
    let output = Command::new(&config.wrangler)
        .arg("whoami")
        .output()
        .map_err(|error| {
            CloudflareError::Config(format!(
                "cannot run {} to verify Wrangler OAuth credentials: {error}; {LOGIN_HELP}",
                config.wrangler.display()
            ))
        })?;
    if output.status.success() {
        return Ok(());
    }
    Err(CloudflareError::Config(format!(
        "Wrangler OAuth credentials are absent or expired; {LOGIN_HELP}"
    )))
}

fn looks_like_cloudflare_id(value: &str) -> bool {
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn ownership_tags(graph: GraphId, resource: &Resource) -> Vec<String> {
    vec![
        graph_tag(graph),
        resource_tag(resource.id()),
        format!("digest={}", resource.digest()),
    ]
}

fn graph_tag(graph: GraphId) -> String {
    format!("graph={graph}")
}

fn resource_tag(resource: ResourceId) -> String {
    format!("resource={resource}")
}

fn tags_match_owner(tags: &[String], graph: GraphId, resource: ResourceId) -> bool {
    tags.iter().any(|tag| tag == &graph_tag(graph))
        && tags.iter().any(|tag| tag == &resource_tag(resource))
}

fn tagged_digest(tags: &[String]) -> Option<ContentDigest> {
    let value = tags.iter().find_map(|tag| tag.strip_prefix("digest="))?;
    let bytes = hex::decode(value).ok()?;
    Some(ContentDigest::from_bytes(bytes.try_into().ok()?))
}

fn api_errors(errors: &[ApiError]) -> String {
    if errors.is_empty() {
        return "no structured error detail".into();
    }
    errors
        .iter()
        .map(|error| format!("{}: {}", error.code, error.message))
        .collect::<Vec<_>>()
        .join(", ")
}

fn plain_binding(value: &serde_json::Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn uuid_pair_base64() -> String {
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(Uuid::now_v7().as_bytes());
    value.extend_from_slice(Uuid::now_v7().as_bytes());
    base64::engine::general_purpose::STANDARD.encode(value)
}

fn hostname_from_pattern(pattern: &str) -> String {
    pattern
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or(pattern)
        .trim_start_matches("*.")
        .to_owned()
}

fn validate_asset_path(path: &str) -> Result<(), CloudflareError> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains('\\')
        || path
            .split('/')
            .any(|segment| matches!(segment, "" | "." | ".."))
    {
        return Err(CloudflareError::Contract(format!(
            "Worker asset path {path:?} must be a relative file path without dot segments"
        )));
    }
    Ok(())
}

fn asset_content_type(path: &str) -> &'static str {
    match path.rsplit('.').next().unwrap_or_default() {
        "css" => "text/css",
        "gif" => "image/gif",
        "html" | "htm" => "text/html",
        "ico" => "image/x-icon",
        "jpeg" | "jpg" => "image/jpeg",
        "js" | "mjs" => "application/javascript",
        "json" | "map" => "application/json",
        "png" => "image/png",
        "svg" => "image/svg+xml",
        "txt" => "text/plain",
        "webp" => "image/webp",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        _ => "application/octet-stream",
    }
}

fn asset_hash(path: &str, encoded: &str) -> String {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let extension = file_name
        .rfind('.')
        .map(|offset| &file_name[offset..])
        .unwrap_or("");
    let digest = Sha256::digest(format!("{encoded}{extension}").as_bytes());
    digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn worker_name(resource: &Resource) -> String {
    let safe = resource
        .path()
        .address()
        .name()
        .as_str()
        .chars()
        .map(|character| {
            if character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    safe.trim_matches('-').chars().take(63).collect()
}

fn tunnel_identity(graph: GraphId, resource: ResourceId) -> String {
    let digest = Sha256::digest(format!("{graph}/{resource}").as_bytes());
    hex::encode(&digest[..16])
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use henosis_controller_runtime::DirectoryArtifactStore;
    use henosis_types::ComponentName;
    use henosis_types::ContentDigest;
    use henosis_types::Controller;
    use henosis_types::ControllerCommand;
    use henosis_types::ControllerSlice;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NativeValue;
    use henosis_types::NewResource;
    use henosis_types::OutputAvailability;
    use henosis_types::OutputDeclaration;
    use henosis_types::OutputName;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[test]
    fn live_transport_requires_explicit_opt_in() {
        let config = LiveCloudflareConfig {
            enabled: false,
            ..LiveCloudflareConfig::default()
        };
        let error = LiveCloudflareTransport::connect(
            &config,
            Arc::new(DirectoryArtifactStore::new("unused")),
        )
        .err()
        .expect("disabled live transport must fail closed");
        assert!(error.to_string().contains("HENOSIS_CLOUDFLARE_LIVE=1"));
    }

    #[test]
    fn tunnel_identity_is_an_opaque_id_without_a_vanity_prefix() {
        let identity = tunnel_identity(
            GraphId::from_bytes([3; 16]),
            ResourceId::from_bytes([7; 16]),
        );
        assert_eq!(identity.len(), 32);
        assert!(identity.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert!(!identity.starts_with("henosis-"));
    }

    #[test]
    fn route_hostname_discards_scheme_wildcard_and_path() {
        assert_eq!(
            hostname_from_pattern("https://*.example.com/api/*"),
            "example.com"
        );
    }

    #[test]
    fn asset_hash_matches_cloudflare_direct_upload_recipe() {
        assert_eq!(
            asset_hash("/index.html", "SGVub3Npcw=="),
            "9a8b3c02561c79304ac328088cf69a18"
        );
    }

    /// Live benchmark Worker smoke test.
    ///
    /// The `demo-d26-live` recipe compiles the benchmark TypeScript in the
    /// frontend lane, writes a directory-backed content-addressed artifact
    /// store, and sets the three digest variables consumed here. The test is
    /// both ignored and gated by `HENOSIS_CLOUDFLARE_LIVE=1` so ordinary
    /// test runs stay offline.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "mutates the logged-in Cloudflare account; run `just demo-d26-live`"]
    async fn live_benchmark_workers_upload_serve_publish_and_retire() {
        assert_eq!(
            std::env::var("HENOSIS_CLOUDFLARE_LIVE").as_deref(),
            Ok("1"),
            "live Cloudflare mutation requires HENOSIS_CLOUDFLARE_LIVE=1"
        );
        let artifact_root = std::env::var("HENOSIS_CLOUDFLARE_ARTIFACT_ROOT")
            .expect("HENOSIS_CLOUDFLARE_ARTIFACT_ROOT must name the prepared artifact store");
        let artifacts = Arc::new(DirectoryArtifactStore::new(artifact_root));
        let backend_digest = read_artifact_digest("HENOSIS_CLOUDFLARE_BACKEND_DIGEST");
        let frontend_digest = read_artifact_digest("HENOSIS_CLOUDFLARE_FRONTEND_DIGEST");
        let assets_digest = read_artifact_digest("HENOSIS_CLOUDFLARE_FRONTEND_ASSETS_DIGEST");
        let graph = GraphId::from_bytes(*Uuid::now_v7().as_bytes());
        let backend = worker_resource(
            "backend",
            crate::SourceRef {
                entry: crate::ArtifactReference {
                    kind: ArtifactKind::CloudflareWorker,
                    digest: backend_digest,
                },
                assets: None,
            },
            BTreeMap::from([
                (
                    "SUPABASE_REST_URL".into(),
                    serde_json::json!("https://example.com"),
                ),
                (
                    "SUPABASE_TUNNEL_HOST".into(),
                    serde_json::json!("benchmark.invalid"),
                ),
            ]),
        );
        let frontend = worker_resource(
            "frontend",
            crate::SourceRef {
                entry: crate::ArtifactReference {
                    kind: ArtifactKind::CloudflareWorker,
                    digest: frontend_digest,
                },
                assets: Some(crate::ArtifactReference {
                    kind: ArtifactKind::StaticAssets,
                    digest: assets_digest,
                }),
            },
            BTreeMap::from([(
                "BACKEND_URL".into(),
                serde_json::json!("https://example.com"),
            )]),
        );
        let resource_ids = [backend.id(), frontend.id()];
        let slice = ControllerSlice::new(
            graph,
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"cloudflare-live-benchmark"),
            crate::controller_name(crate::CONTROLLER_NAME),
            vec![backend, frontend],
            Vec::new(),
        );
        let controller = crate::CloudflareController::new(
            LiveCloudflareTransport::connect(&LiveCloudflareConfig::default(), artifacts)
                .expect("Wrangler login must be valid; run `wrangler login`"),
        );
        let report = controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap()
            .expect("reconcile publishes a report");
        if report.outputs().len() == 0 {
            let failure = format!("{:?}", report.dispositions().collect::<Vec<_>>());
            controller
                .execute(&ControllerCommand::Retire(Retirement {
                    graph_id: graph,
                    last_generation: slice.generation(),
                    controller: controller.name().clone(),
                    resources: slice.resources().to_vec(),
                }))
                .await
                .expect("partial live reconciliation must clean up");
            panic!("live Cloudflare reconciliation failed: {failure}");
        }
        let frontend_url = report
            .outputs()
            .find(|output| {
                output.key_value().resource_id() == resource_ids[1]
                    && output.key_value().output().as_str() == "url"
            })
            .and_then(|output| output.value().as_json().as_str())
            .expect("frontend URL is published")
            .to_owned();
        for resource_id in resource_ids {
            let worker_name = report
                .outputs()
                .find(|output| {
                    output.key_value().resource_id() == resource_id
                        && output.key_value().output().as_str() == "workerName"
                })
                .and_then(|output| output.value().as_json().as_str())
                .unwrap();
            let deployment_id = report
                .outputs()
                .find(|output| {
                    output.key_value().resource_id() == resource_id
                        && output.key_value().output().as_str() == "deploymentId"
                })
                .and_then(|output| output.value().as_json().as_str())
                .unwrap();
            let version_id = report
                .outputs()
                .find(|output| {
                    output.key_value().resource_id() == resource_id
                        && output.key_value().output().as_str() == "versionId"
                })
                .and_then(|output| output.value().as_json().as_str())
                .unwrap();
            println!(
                "LIVE publish worker={worker_name} deployment={deployment_id} version={version_id}"
            );
        }
        let served = async {
            let mut last = String::new();
            for attempt in 1..=60 {
                match controller
                    .transport
                    .session
                    .client
                    .get(&frontend_url)
                    .send()
                    .await
                {
                    Ok(response) => {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_default();
                        if status.is_success() {
                            return Ok(body);
                        }
                        last = format!("attempt {attempt}: {status} {body:?}");
                    }
                    Err(error) => last = format!("attempt {attempt}: {error}"),
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
            Err(last)
        }
        .await;
        controller
            .execute(&ControllerCommand::Retire(Retirement {
                graph_id: graph,
                last_generation: slice.generation(),
                controller: controller.name().clone(),
                resources: slice.resources().to_vec(),
            }))
            .await
            .unwrap();
        let served = served.expect("deployed benchmark frontend must answer successfully");
        println!("LIVE curl url={frontend_url} response={served:?}");
        assert!(served.contains("Henosis benchmark frontend"));
        for resource_id in resource_ids {
            let managed = worker_name(
                slice
                    .resources()
                    .iter()
                    .find(|resource| resource.id() == resource_id)
                    .unwrap(),
            );
            let status = controller
                .transport
                .session
                .client
                .get(
                    controller
                        .transport
                        .session
                        .account_url(&format!("workers/scripts/{managed}")),
                )
                .bearer_auth(&controller.transport.session.token)
                .send()
                .await
                .unwrap()
                .status();
            println!("LIVE retire worker={managed} verification_status={status}");
            assert_eq!(status, StatusCode::NOT_FOUND);
        }
    }

    fn read_artifact_digest(variable: &str) -> ArtifactDigest {
        std::env::var(variable)
            .unwrap_or_else(|_| panic!("{variable} must name a prepared workload artifact"))
            .parse()
            .unwrap_or_else(|error| {
                panic!("{variable} is not a canonical artifact digest: {error}")
            })
    }

    fn worker_resource(
        name: &str,
        source: crate::SourceRef,
        vars: BTreeMap<String, serde_json::Value>,
    ) -> Resource {
        let outputs = ["url", "workerName", "deploymentId", "versionId"]
            .into_iter()
            .map(|name| {
                OutputDeclaration::new(OutputName::new(name).unwrap(), OutputAvailability::Observed)
            })
            .collect();
        Resource::new(NewResource {
            id: ResourceId::from_bytes(*Uuid::now_v7().as_bytes()),
            path: ResourcePath::new(
                ComponentName::new("cloudflare_live_benchmark").unwrap(),
                ResourceAddress::new(
                    KindVersion::new(
                        KindName::new("cloudflare/worker").unwrap(),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    ResourceName::new(name).unwrap(),
                ),
            ),
            controller: crate::controller_name(crate::CONTROLLER_NAME),
            body: NativeValue::try_from(
                serde_json::to_value(WorkerBody {
                    source,
                    compatibility_date: Some("2026-07-15".into()),
                    compatibility_flags: Vec::new(),
                    vars,
                    services: BTreeMap::new(),
                })
                .unwrap(),
            )
            .unwrap(),
            outputs,
        })
        .unwrap()
    }
}
