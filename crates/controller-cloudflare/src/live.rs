//! Live Cloudflare API transport support.

use std::fs;
use std::path::PathBuf;
use std::process::Command;

use reqwest::blocking::Client;
use serde::Deserialize;

use crate::CloudflareError;

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

#[derive(Clone, Debug)]
pub(crate) struct CloudflareSession {
    pub account_id: String,
    pub api_base: String,
    pub client: Client,
    pub token: String,
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

impl CloudflareSession {
    pub fn connect(config: &LiveCloudflareConfig) -> Result<Self, CloudflareError> {
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
        let account_id = match &config.account_id {
            Some(value) => value.clone(),
            None => discover_account(&client, &config.api_base, &token)?,
        };
        Ok(Self {
            account_id,
            api_base: config.api_base.clone(),
            client,
            token,
        })
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

fn discover_account(
    client: &Client,
    api_base: &str,
    token: &str,
) -> Result<String, CloudflareError> {
    let response = client
        .get(format!("{api_base}/memberships"))
        .bearer_auth(token)
        .send()
        .map_err(|error| CloudflareError::Unavailable(error.to_string()))?;
    let status = response.status();
    let envelope: ApiEnvelope<Vec<Membership>> = response
        .json()
        .map_err(|error| CloudflareError::Provider(format!("account discovery returned invalid JSON: {error}")))?;
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

pub(crate) fn api_errors(errors: &[ApiError]) -> String {
    if errors.is_empty() {
        return "no structured error detail".into();
    }
    errors
        .iter()
        .map(|error| format!("{}: {}", error.code, error.message))
        .collect::<Vec<_>>()
        .join(", ")
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

pub(crate) fn managed_name(resource_name: &str, resource_id: henosis_types::ResourceId) -> String {
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
        let name = managed_name("Front_end/Production", henosis_types::ResourceId::from_bytes([7; 16]));
        assert!(name.starts_with("henosis-"));
        assert!(name.len() <= 63);
        assert!(name.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'));
    }
}
