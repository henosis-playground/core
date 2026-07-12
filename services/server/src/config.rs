//! Environment-backed server configuration.

use std::collections::BTreeMap;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use henosis_orchestrator::ConnectorConfig;
use henosis_types::ConnectorKey;
use serde::Deserialize;

#[derive(Debug)]
pub(crate) struct Config {
    pub(crate) listen: SocketAddr,
    pub(crate) database_url: String,
    pub(crate) s2_access_token: String,
    pub(crate) s2_account_endpoint: String,
    pub(crate) s2_basin_endpoint: String,
    pub(crate) s2_basin: String,
    pub(crate) auth_tokens: Vec<String>,
    pub(crate) connectors: Vec<ConnectorConfig>,
}

impl Config {
    pub(crate) fn from_env() -> anyhow::Result<Self> {
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
