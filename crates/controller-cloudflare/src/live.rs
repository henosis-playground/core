//! Live Cloudflare API transport.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::sync::Mutex;

use base64::Engine as _;
use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_types::BundleArtifactReader;
use henosis_types::BundleRef;
use henosis_types::ComponentName;
use henosis_types::GraphId;
use henosis_types::Resource;
use henosis_types::ResourceId;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::multipart::Form;
use reqwest::multipart::Part;
use serde::Deserialize;
use serde::Serialize;
use uuid::Uuid;

use crate::CloudflareError;
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
    pub account_id: Option<String>,
    pub api_base: String,
    pub wrangler: PathBuf,
    pub wrangler_config: PathBuf,
}

impl Default for LiveCloudflareConfig {
    fn default() -> Self {
        let home = std::env::var_os("HOME").map_or_else(|| PathBuf::from("."), PathBuf::from);
        Self {
            account_id: std::env::var("CLOUDFLARE_ACCOUNT_ID").ok(),
            api_base: DEFAULT_API_BASE.into(),
            wrangler: PathBuf::from("wrangler"),
            wrangler_config: home.join(".config/.wrangler/config/default.toml"),
        }
    }
}

/// Resolves the closure that emitted a resource.
///
/// The current plan resource carries component identity but not its closure
/// digest. The server supplies this read-only index from the accepted bundle
/// manifest; bytes still flow exclusively through `BundleArtifactReader`.
pub trait ComponentBundleResolver: Send + Sync {
    fn resolve(&self, component: &ComponentName) -> Result<BundleRef, CloudflareError>;
}

pub struct LiveCloudflareTransport {
    session: CloudflareSession,
    artifacts: Arc<dyn BundleArtifactReader>,
    bundles: Arc<dyn ComponentBundleResolver>,
    managed: Mutex<BTreeMap<ResourceId, ManagedResource>>,
}

#[derive(Clone, Debug)]
struct CloudflareSession {
    account_id: String,
    api_base: String,
    client: Client,
    token: String,
}

#[derive(Clone, Debug)]
enum ManagedResource {
    Worker { name: String },
    Tunnel { id: String, name: String },
    Route { zone_id: String, route_id: String },
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
    errors: Vec<ApiError>,
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
struct WorkerUpload {
    #[serde(default)]
    id: String,
    #[serde(default)]
    etag: String,
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

#[derive(Serialize)]
struct TunnelCreate<'a> {
    name: &'a str,
    tunnel_secret: String,
}

#[derive(Serialize)]
struct RouteWrite<'a> {
    pattern: &'a str,
    script: &'a str,
}

impl LiveCloudflareTransport {
    pub fn connect(
        config: &LiveCloudflareConfig,
        artifacts: Arc<dyn BundleArtifactReader>,
        bundles: Arc<dyn ComponentBundleResolver>,
    ) -> Result<Self, CloudflareError> {
        let session = CloudflareSession::connect(config)?;
        Ok(Self {
            session,
            artifacts,
            bundles,
            managed: Mutex::new(BTreeMap::new()),
        })
    }

    async fn upload_worker(
        &self,
        resource: &Resource,
        body: &WorkerBody,
    ) -> Result<WorkerObservation, CloudflareError> {
        let name = managed_name(resource.path().address().name().as_str(), resource.id());
        require_owned_name(&name)?;
        let bundle = self.bundles.resolve(resource.path().instance())?;
        let bytes = self
            .artifacts
            .read(bundle, &body.source.entry)
            .await
            .map_err(|error| CloudflareError::Contract(error.to_string()))?;
        let bindings = body
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
        let mut metadata = serde_json::json!({
            "main_module": "worker.mjs",
            "bindings": bindings,
        });
        if let Some(date) = &body.compatibility_date {
            metadata["compatibility_date"] = serde_json::Value::String(date.clone());
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
        let form = Form::new().part("metadata", metadata).part("worker.mjs", module);
        let upload: WorkerUpload = self
            .session
            .request(
                self.session
                    .client
                    .put(self.session.account_url(&format!("workers/scripts/{name}")))
                    .multipart(form),
                "Worker module upload",
            )
            .await?;
        let subdomain = self.session.ensure_workers_subdomain().await?;
        let deployments: Vec<Deployment> = self
            .session
            .request(
                self.session.client.get(
                    self.session
                        .account_url(&format!("workers/scripts/{name}/deployments")),
                ),
                "Worker deployment observation",
            )
            .await?;
        let deployment = deployments.first();
        let deployment_id = deployment
            .map(|value| value.id.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| upload.id.clone());
        let version_id = deployment
            .and_then(|value| value.versions.first())
            .map(|value| value.version_id.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or(upload.etag);
        if deployment_id.is_empty() || version_id.is_empty() {
            return Err(CloudflareError::Provider(
                "Worker upload succeeded but Cloudflare did not confirm deployment and version identities"
                    .into(),
            ));
        }
        self.managed
            .lock()
            .expect("live Cloudflare resource lock is not poisoned")
            .insert(resource.id(), ManagedResource::Worker { name: name.clone() });
        Ok(WorkerObservation {
            url: format!("https://{name}.{subdomain}.workers.dev"),
            worker_name: name,
            deployment_id,
            version_id,
        })
    }

    async fn create_tunnel(
        &self,
        resource: &Resource,
        body: &TunnelBody,
    ) -> Result<TunnelObservation, CloudflareError> {
        let name = managed_name(resource.path().address().name().as_str(), resource.id());
        require_owned_name(&name)?;
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
        let tunnel = if let Some(existing) = tunnels.into_iter().find(|item| item.name == name) {
            existing
        } else {
            let secret = uuid_pair_base64();
            self.session
                .request(
                    self.session
                        .client
                        .post(self.session.account_url("cfd_tunnel"))
                        .json(&TunnelCreate {
                            name: &name,
                            tunnel_secret: secret,
                        }),
                    "Cloudflare Tunnel create",
                )
                .await?
        };
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
                self.session.client.put(self.session.account_url(&format!(
                    "cfd_tunnel/{}/configurations",
                    tunnel.id
                )))
                .json(&config),
                "Cloudflare Tunnel configuration",
            )
            .await?;
        self.managed
            .lock()
            .expect("live Cloudflare resource lock is not poisoned")
            .insert(
                resource.id(),
                ManagedResource::Tunnel {
                    id: tunnel.id.clone(),
                    name: name.clone(),
                },
            );
        Ok(TunnelObservation {
            tunnel_id: tunnel.id.clone(),
            tunnel_name: name,
            private_hostname: format!("{}.cfargotunnel.com", tunnel.id),
            token_ref: format!("cloudflare://tunnels/{}/token", tunnel.id),
        })
    }

    async fn write_route(
        &self,
        resource: &Resource,
        body: &RouteBody,
    ) -> Result<RouteObservation, CloudflareError> {
        require_owned_name(&body.worker_name)?;
        let zone = self.session.resolve_zone(&body.zone).await?;
        let routes: Vec<WorkerRoute> = self
            .session
            .request(
                self.session
                    .client
                    .get(self.session.url(&format!("zones/{}/workers/routes", zone.id))),
                "Worker route observation",
            )
            .await?;
        let existing = routes.into_iter().find(|route| route.pattern == body.pattern);
        if let Some(route) = &existing {
            if !route.script.is_empty() && !route.script.starts_with("henosis-") {
                return Err(CloudflareError::Provider(format!(
                    "refusing to replace route {:?} owned by script {:?}",
                    body.pattern, route.script
                )));
            }
        }
        let request = RouteWrite {
            pattern: &body.pattern,
            script: &body.worker_name,
        };
        let route: WorkerRoute = match existing {
            Some(route) => {
                self.session
                    .request(
                        self.session.client.put(self.session.url(&format!(
                            "zones/{}/workers/routes/{}",
                            zone.id, route.id
                        )))
                        .json(&request),
                        "Worker route update",
                    )
                    .await?
            }
            None => {
                self.session
                    .request(
                        self.session.client.post(self.session.url(&format!(
                            "zones/{}/workers/routes",
                            zone.id
                        )))
                        .json(&request),
                        "Worker route create",
                    )
                    .await?
            }
        };
        self.managed
            .lock()
            .expect("live Cloudflare resource lock is not poisoned")
            .insert(
                resource.id(),
                ManagedResource::Route {
                    zone_id: zone.id,
                    route_id: route.id,
                },
            );
        Ok(RouteObservation {
            hostname: hostname_from_pattern(&body.pattern),
        })
    }

    async fn delete_managed(&self, resource: ResourceId) -> Result<(), CloudflareError> {
        let managed = self
            .managed
            .lock()
            .expect("live Cloudflare resource lock is not poisoned")
            .get(&resource)
            .cloned();
        let Some(managed) = managed else {
            return Ok(());
        };
        let request = match &managed {
            ManagedResource::Worker { name } => {
                require_owned_name(name)?;
                self.session
                    .client
                    .delete(self.session.account_url(&format!("workers/scripts/{name}")))
            }
            ManagedResource::Tunnel { id, name } => {
                require_owned_name(name)?;
                self.session
                    .client
                    .delete(self.session.account_url(&format!("cfd_tunnel/{id}")))
                    .query(&[("cascade", "true")])
            }
            ManagedResource::Route { zone_id, route_id } => self.session.client.delete(
                self.session
                    .url(&format!("zones/{zone_id}/workers/routes/{route_id}")),
            ),
        };
        self.session.request_empty(request, "resource retirement").await?;
        self.managed
            .lock()
            .expect("live Cloudflare resource lock is not poisoned")
            .remove(&resource);
        Ok(())
    }
}

impl CloudflareTransport for LiveCloudflareTransport {
    fn apply_worker<'a>(
        &'a self,
        _graph: GraphId,
        resource: &'a Resource,
        body: &'a WorkerBody,
    ) -> BoxFuture<'a, Result<WorkerObservation, CloudflareError>> {
        self.upload_worker(resource, body).boxed()
    }

    fn apply_tunnel<'a>(
        &'a self,
        _graph: GraphId,
        resource: &'a Resource,
        body: &'a TunnelBody,
    ) -> BoxFuture<'a, Result<TunnelObservation, CloudflareError>> {
        self.create_tunnel(resource, body).boxed()
    }

    fn apply_route<'a>(
        &'a self,
        _graph: GraphId,
        resource: &'a Resource,
        body: &'a RouteBody,
    ) -> BoxFuture<'a, Result<RouteObservation, CloudflareError>> {
        self.write_route(resource, body).boxed()
    }

    fn delete(
        &self,
        _graph: GraphId,
        resource: ResourceId,
    ) -> BoxFuture<'_, Result<(), CloudflareError>> {
        self.delete_managed(resource).boxed()
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
        let token = credentials.oauth_token.filter(|value| !value.is_empty()).ok_or_else(|| {
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
        let response = request
            .bearer_auth(&self.token)
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
                api_errors(&envelope.errors)
            )));
        }
        envelope.result.ok_or_else(|| {
            CloudflareError::Provider(format!("{operation} returned no result"))
        })
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
                api_errors(&envelope.errors)
            )))
        }
    }

    async fn ensure_workers_subdomain(&self) -> Result<String, CloudflareError> {
        let request = self
            .client
            .get(self.account_url("workers/subdomain"))
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
        if request.status().is_success() {
            let envelope: ApiEnvelope<Subdomain> = request.json().await.map_err(|error| {
                CloudflareError::Provider(format!("workers.dev observation returned invalid JSON: {error}"))
            })?;
            if envelope.success {
                if let Some(result) = envelope.result {
                    return Ok(result.subdomain);
                }
            }
        }
        let result: Subdomain = self
            .request(
                self.client
                    .put(self.account_url("workers/subdomain"))
                    .json(&serde_json::json!({"enabled": true})),
                "workers.dev subdomain enablement",
            )
            .await?;
        Ok(result.subdomain)
    }

    async fn resolve_zone(&self, name_or_id: &str) -> Result<Zone, CloudflareError> {
        let zones: Vec<Zone> = self
            .request(
                self.client
                    .get(self.url("zones"))
                    .query(&[("name", name_or_id), ("account.id", self.account_id.as_str())]),
                "zone discovery",
            )
            .await?;
        if let Some(zone) = zones.into_iter().find(|zone| zone.name == name_or_id || zone.id == name_or_id) {
            return Ok(zone);
        }
        Err(CloudflareError::Config(format!(
            "Cloudflare zone {name_or_id:?} is not present in account {}",
            self.account_id
        )))
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
            api_errors(&envelope.errors)
        )));
    }
    let memberships = envelope.result.unwrap_or_default();
    match memberships.as_slice() {
        [] => Err(CloudflareError::Config(format!(
            "Wrangler login has no Cloudflare account memberships; {LOGIN_HELP}"
        ))),
        [membership] => Ok(membership.account.id.clone()),
        _ => Err(CloudflareError::Config(format!(
            "Wrangler login has multiple Cloudflare accounts ({}); set CLOUDFLARE_ACCOUNT_ID to one of: {}",
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
    let detail = String::from_utf8_lossy(&output.stderr);
    Err(CloudflareError::Config(format!(
        "Wrangler OAuth credentials are absent or expired: {}; {LOGIN_HELP}",
        redact_command_detail(&detail)
    )))
}

fn require_owned_name(name: &str) -> Result<(), CloudflareError> {
    if name.starts_with("henosis-") {
        Ok(())
    } else {
        Err(CloudflareError::Config(format!(
            "refusing to mutate unmanaged Cloudflare resource {name:?}; managed names start with henosis-"
        )))
    }
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
        .map_or_else(|| value.to_string(), str::to_owned)
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

fn redact_command_detail(detail: &str) -> String {
    detail
        .lines()
        .find(|line| !line.trim().is_empty())
        .unwrap_or("verification failed")
        .trim()
        .chars()
        .take(240)
        .collect()
}

fn managed_name(resource_name: &str, resource_id: ResourceId) -> String {
    let safe = resource_name
        .chars()
        .map(|character| {
            if character.is_ascii_lowercase() || character.is_ascii_digit() || character == '-' {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    let suffix = resource_id.to_string();
    let suffix = &suffix[suffix.len().saturating_sub(8)..];
    let maximum_base = 63_usize.saturating_sub("henosis--".len() + suffix.len());
    let safe = safe.trim_matches('-');
    let safe = &safe[..safe.len().min(maximum_base)];
    format!("henosis-{safe}-{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_names_are_provider_safe_and_owned() {
        let name = managed_name("Front_end/Production", ResourceId::from_bytes([7; 16]));
        assert!(name.starts_with("henosis-"));
        assert!(name.len() <= 63);
        assert!(name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
    }

    #[test]
    fn route_hostname_discards_scheme_wildcard_and_path() {
        assert_eq!(hostname_from_pattern("https://*.example.com/api/*"), "example.com");
    }
}
