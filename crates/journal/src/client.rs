use anyhow::Context;
use faultline::Error as Fault;
use faultline::Never;
use s2_sdk::S2;
use s2_sdk::S2Stream;
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::BasinEndpoint;
use s2_sdk::types::BasinName;
use s2_sdk::types::S2Config;
use s2_sdk::types::S2Endpoints;
use s2_sdk::types::StreamName;
use types::domain::ComponentUuid;
use types::domain::GraphUuid;

use crate::Journal;
use crate::JournalError;
use crate::stream::journal_invariant;
use crate::stream::never_invariant;

impl Journal {
    /// Build a journal client for explicit S2 endpoints.
    pub fn connect(
        access_token: impl Into<String>,
        account_endpoint: &str,
        basin_endpoint: &str,
        basin: &str,
    ) -> anyhow::Result<Self> {
        let endpoints = S2Endpoints::new(
            AccountEndpoint::new(account_endpoint).context("invalid S2 account endpoint")?,
            BasinEndpoint::new(basin_endpoint).context("invalid S2 basin endpoint")?,
        )
        .context("invalid S2 endpoint configuration")?;
        let client = S2::new(S2Config::new(access_token).with_endpoints(endpoints))
            .context("failed to construct S2 client")?;
        let basin = basin
            .parse::<BasinName>()
            .context("invalid S2 basin name")?;
        Ok(Self {
            basin: client.basin(basin),
        })
    }

    pub(super) fn graph_stream(
        &self,
        graph_id: GraphUuid,
    ) -> Result<S2Stream, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        graph_id
            .to_string()
            .parse::<StreamName>()
            .map(|name| self.basin.stream(name))
            .map_err(journal_invariant)
    }

    pub(super) fn component_stream(
        &self,
        component_id: ComponentUuid,
    ) -> Result<S2Stream, Fault<Never, anyhow::Error, anyhow::Error>> {
        format!("component-{component_id}")
            .parse::<StreamName>()
            .map(|name| self.basin.stream(name))
            .map_err(never_invariant)
    }

    pub(super) fn named_stream(
        &self,
        name: &str,
    ) -> Result<S2Stream, Fault<Never, anyhow::Error, anyhow::Error>> {
        name.parse::<StreamName>()
            .map(|name| self.basin.stream(name))
            .map_err(never_invariant)
    }
}
