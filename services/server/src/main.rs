//! `ConnectRPC` process for the Henosis graph orchestrator.

mod config;
mod error;
mod service;
mod watch_response;

use std::sync::Arc;

use connectrpc::Router;
use connectrpc::Server;
use henosis_db_queries::DbPool;
use henosis_journal::Journal;
use henosis_orchestrator::Orchestrator;
use henosis_proto::connect::henosis::v1::ConnectorCallbackServiceRegisterMarker;
use henosis_proto::connect::henosis::v1::GraphServiceRegisterMarker;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
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
    let metadata = DbPool::connect(&config.database_url).await?;
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
