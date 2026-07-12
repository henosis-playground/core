//! `ConnectRPC` process for the Henosis graph orchestrator.

mod error;
mod service;
mod watch_response;

use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use connectrpc::Router;
use connectrpc::Server;
use henosis_db_queries::MetadataDb;
use henosis_journal::Journal;
use henosis_orchestrator::ConnectorConfig;
use henosis_orchestrator::Orchestrator;
use henosis_proto::connect::henosis::v1::ConnectorCallbackServiceRegisterMarker;
use henosis_proto::connect::henosis::v1::GraphServiceRegisterMarker;
use henosis_types::ConnectorKey;
use serde::Deserialize;
use tracing_subscriber::EnvFilter;

use crate::service::Api;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Config::from_env()?;
    let journal = Journal::connect(
        config.s2_access_token,
        &config.s2_account_endpoint,
        &config.s2_basin_endpoint,
        &config.s2_basin,
    )?;
    let metadata = MetadataDb::connect(&config.database_url).await?;
    let core = Arc::new(Orchestrator::new(journal, metadata, config.connectors)?);
    core.initialize().await.map_err(anyhow::Error::new)?;
    let api = Arc::new(Api::new(core, config.auth_tokens));
    let router = Router::new()
        .add_service::<_, GraphServiceRegisterMarker>(Arc::clone(&api))
        .add_service::<_, ConnectorCallbackServiceRegisterMarker>(api);
    Server::new(router)
        .serve(config.listen)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    Ok(())
}

#[derive(Debug)]
struct Config {
    listen: SocketAddr,
    database_url: String,
    s2_access_token: String,
    s2_account_endpoint: String,
    s2_basin_endpoint: String,
    s2_basin: String,
    auth_tokens: Vec<String>,
    connectors: Vec<ConnectorConfig>,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
        let auth_tokens =
            serde_json::from_str::<Vec<String>>(&required("HENOSIS_AUTH_TOKENS_JSON")?)
                .context("HENOSIS_AUTH_TOKENS_JSON must be a JSON string array")?;
        if auth_tokens.is_empty() || auth_tokens.iter().any(String::is_empty) {
            return Err(anyhow::anyhow!(
                "at least one non-empty bearer token is required"
            ));
        }
        let configured = serde_json::from_str::<BTreeMap<ConnectorKey, RawConnectorConfig>>(
            &env::var("HENOSIS_CONNECTORS_JSON").unwrap_or_else(|_| "{}".to_owned()),
        )
        .context("HENOSIS_CONNECTORS_JSON must map connector keys to endpoint/token objects")?;
        let connectors = configured
            .into_iter()
            .map(|(key, value)| ConnectorConfig::new(key, value.endpoint, value.token))
            .collect();
        Ok(Self {
            listen: env::var("HENOSIS_LISTEN")
                .unwrap_or_else(|_| "0.0.0.0:8080".to_owned())
                .parse()
                .context("invalid HENOSIS_LISTEN")?,
            database_url: database_url()?,
            s2_access_token: required("S2_ACCESS_TOKEN")?,
            s2_account_endpoint: required("S2_ACCOUNT_ENDPOINT")?,
            s2_basin_endpoint: required("S2_BASIN_ENDPOINT")?,
            s2_basin: required("S2_BASIN")?,
            auth_tokens,
            connectors,
        })
    }
}

#[derive(Debug, Deserialize)]
struct RawConnectorConfig {
    endpoint: String,
    token: String,
}

fn database_url() -> anyhow::Result<String> {
    if let Ok(url) = env::var("DATABASE_URL") {
        return Ok(url);
    }
    let path = env::var_os("CORE_POSTGRES_PASSWORD_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/run/secrets/core_postgres_password"));
    let password = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    Ok(format!(
        "postgres://henosis_core:{}@core-postgres:5432/henosis_core",
        password.trim()
    ))
}

fn required(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("{name} is required"))
}
