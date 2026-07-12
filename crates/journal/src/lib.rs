//! S2-backed durable stores for graph, registry, and component-spec streams.
//!
//! Wire records are parsed by `henosis-proto` into validated domain events.
//! Domain histories own all folding rules and never observe protobuf values.

use std::convert::Infallible;

use anyhow::Context;
use faultline::Error as Fault;
use faultline::Never;
use futures::StreamExt;
use henosis_proto::journal::DecodeStreamError;
use henosis_proto::journal::WireRecord;
use henosis_proto::journal::decode_graph_stream;
use henosis_proto::journal::decode_registry_stream;
use henosis_proto::journal::decode_spec_stream;
use henosis_proto::journal::encode_graph_event;
use henosis_proto::journal::encode_registry_event;
use henosis_proto::journal::encode_spec_record;
use henosis_types::GraphEvent;
use henosis_types::GraphHistory;
use henosis_types::GraphId;
use henosis_types::RegisteredComponentSpec;
use henosis_types::RegistryEvent;
use henosis_types::RegistryHistory;
use henosis_types::RequestId;
use henosis_types::SpecCatalog;
use henosis_types::SpecHistory;
use s2_sdk::S2;
use s2_sdk::S2Basin;
use s2_sdk::S2Stream;
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord;
use s2_sdk::types::AppendRecordBatch;
use s2_sdk::types::BasinEndpoint;
use s2_sdk::types::BasinName;
use s2_sdk::types::ReadFrom;
use s2_sdk::types::ReadInput;
use s2_sdk::types::ReadLimits;
use s2_sdk::types::ReadStart;
use s2_sdk::types::ReadStop;
use s2_sdk::types::S2Config;
use s2_sdk::types::S2Endpoints;
use s2_sdk::types::S2Error;
use s2_sdk::types::StreamName;
use thiserror::Error;

const REGISTRY_STREAM: &str = "registry";
const SPEC_STREAM: &str = "component-specs";

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum JournalError {
    #[error("graph does not exist")]
    NotFound,
    #[error("journal compare-and-append failed; current tail is {current_tail}")]
    CasConflict { current_tail: u64 },
}

#[derive(Clone, Debug)]
pub struct Journal {
    basin: S2Basin,
}

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

    /// Load and fold one graph stream through its current head.
    pub async fn graph_load(
        &self,
        graph_id: GraphId,
    ) -> Result<GraphHistory, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        let stream = self.graph_stream(graph_id)?;
        let tail = stream
            .check_tail()
            .await
            .map_err(classify_graph_read)?
            .seq_num;
        if tail == 0 {
            return Err(Fault::Domain(JournalError::NotFound));
        }
        let records = read_records(&stream, tail)
            .await
            .map_err(upcast_read_fault)?;
        let mut parsed = Box::pin(decode_graph_stream(futures::stream::iter(
            records.into_iter().map(Ok::<_, Infallible>),
        )));
        let mut history = GraphHistory::new(graph_id);
        while let Some(item) = parsed.next().await {
            let event = item.map_err(decode_invariant)?;
            history.apply(event).map_err(journal_invariant)?;
        }
        Ok(history)
    }

    /// Compare-and-append one already-validated domain event.
    pub async fn graph_append(
        &self,
        graph_id: GraphId,
        expected_tail: u64,
        event: &GraphEvent,
    ) -> Result<u64, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        append_record(
            &self.graph_stream(graph_id)?,
            expected_tail,
            encode_graph_event(event),
        )
        .await
    }

    /// List graph identities recorded in the discovery registry.
    pub async fn graph_list(
        &self,
    ) -> Result<Vec<GraphId>, Fault<Never, anyhow::Error, anyhow::Error>> {
        Ok(self.registry_load().await?.graph_ids().collect::<Vec<_>>())
    }

    /// Convergently record one graph lifecycle level in the registry.
    pub async fn graph_registry_ensure(
        &self,
        graph_id: GraphId,
        request_id: RequestId,
        retired: bool,
    ) -> Result<(), Fault<Never, anyhow::Error, anyhow::Error>> {
        for _ in 0..8 {
            let stream = self.named_stream(REGISTRY_STREAM)?;
            let tail = stream.check_tail().await.map_err(never_transient)?.seq_num;
            let registry = self.registry_load_until(&stream, tail).await?;
            let current = registry.retirement(graph_id);
            if current == Some(retired) || (!retired && current.is_some()) {
                return Ok(());
            }
            let event = if retired {
                RegistryEvent::Retired {
                    graph_id,
                    request_id,
                }
            } else {
                RegistryEvent::Created {
                    graph_id,
                    request_id,
                }
            };
            match append_record(&stream, tail, encode_registry_event(event)).await {
                Ok(_) => return Ok(()),
                Err(Fault::Domain(JournalError::CasConflict { .. })) => {}
                Err(Fault::Domain(JournalError::NotFound)) => {
                    return Err(Fault::Invariant(anyhow::anyhow!(
                        "registry append unexpectedly reported graph not found"
                    )));
                }
                Err(Fault::Transient(error)) => return Err(Fault::Transient(error)),
                Err(Fault::Invariant(error)) => return Err(Fault::Invariant(error)),
            }
        }
        Err(Fault::Transient(anyhow::anyhow!(
            "registry compare-and-append did not converge"
        )))
    }

    /// Append a content-addressed spec before its first graph reference.
    ///
    /// Resending identical content returns the stored value without appending.
    pub async fn component_spec_register(
        &self,
        component: RegisteredComponentSpec,
    ) -> Result<RegisteredComponentSpec, Fault<Never, anyhow::Error, anyhow::Error>> {
        for _ in 0..8 {
            let stream = self.named_stream(SPEC_STREAM)?;
            let tail = stream.check_tail().await.map_err(never_transient)?.seq_num;
            let mut history = self.spec_history_load_until(&stream, tail).await?;
            if let Some(stored) = history.catalog().get(component.hash()) {
                if stored == &component {
                    return Ok(stored.clone());
                }
                return Err(Fault::Invariant(anyhow::anyhow!(
                    "component spec hash collision"
                )));
            }
            history
                .apply(henosis_types::SequencedSpecEvent::new(
                    tail,
                    component.clone(),
                ))
                .map_err(never_invariant)?;
            match append_record(&stream, tail, encode_spec_record(&component)).await {
                Ok(_) => return Ok(component),
                Err(Fault::Domain(JournalError::CasConflict { .. })) => {}
                Err(Fault::Domain(JournalError::NotFound)) => {
                    return Err(Fault::Invariant(anyhow::anyhow!(
                        "spec append unexpectedly reported graph not found"
                    )));
                }
                Err(Fault::Transient(error)) => return Err(Fault::Transient(error)),
                Err(Fault::Invariant(error)) => return Err(Fault::Invariant(error)),
            }
        }
        Err(Fault::Transient(anyhow::anyhow!(
            "spec compare-and-append did not converge"
        )))
    }

    /// Load the current immutable component-spec catalog.
    pub async fn component_spec_catalog(
        &self,
    ) -> Result<SpecCatalog, Fault<Never, anyhow::Error, anyhow::Error>> {
        let stream = self.named_stream(SPEC_STREAM)?;
        let tail = stream.check_tail().await.map_err(never_transient)?.seq_num;
        Ok(self
            .spec_history_load_until(&stream, tail)
            .await?
            .catalog()
            .clone())
    }

    async fn registry_load(
        &self,
    ) -> Result<RegistryHistory, Fault<Never, anyhow::Error, anyhow::Error>> {
        let stream = self.named_stream(REGISTRY_STREAM)?;
        let tail = stream.check_tail().await.map_err(never_transient)?.seq_num;
        self.registry_load_until(&stream, tail).await
    }

    async fn registry_load_until(
        &self,
        stream: &S2Stream,
        tail: u64,
    ) -> Result<RegistryHistory, Fault<Never, anyhow::Error, anyhow::Error>> {
        let records = read_records(stream, tail).await?;
        let mut parsed = Box::pin(decode_registry_stream(futures::stream::iter(
            records.into_iter().map(Ok::<_, Infallible>),
        )));
        let mut history = RegistryHistory::default();
        while let Some(item) = parsed.next().await {
            let event = item.map_err(decode_invariant)?;
            history.apply(event).map_err(never_invariant)?;
        }
        Ok(history)
    }

    async fn spec_history_load_until(
        &self,
        stream: &S2Stream,
        tail: u64,
    ) -> Result<SpecHistory, Fault<Never, anyhow::Error, anyhow::Error>> {
        let records = read_records(stream, tail).await?;
        let mut parsed = Box::pin(decode_spec_stream(futures::stream::iter(
            records.into_iter().map(Ok::<_, Infallible>),
        )));
        let mut history = SpecHistory::default();
        while let Some(item) = parsed.next().await {
            let event = item.map_err(decode_invariant)?;
            history.apply(event).map_err(never_invariant)?;
        }
        Ok(history)
    }

    fn graph_stream(
        &self,
        graph_id: GraphId,
    ) -> Result<S2Stream, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        graph_id
            .to_string()
            .parse::<StreamName>()
            .map(|name| self.basin.stream(name))
            .map_err(journal_invariant)
    }

    fn named_stream(
        &self,
        name: &str,
    ) -> Result<S2Stream, Fault<Never, anyhow::Error, anyhow::Error>> {
        name.parse::<StreamName>()
            .map(|name| self.basin.stream(name))
            .map_err(never_invariant)
    }
}

async fn read_records(
    stream: &S2Stream,
    tail: u64,
) -> Result<Vec<WireRecord>, Fault<Never, anyhow::Error, anyhow::Error>> {
    let mut records = Vec::new();
    let mut next = 0;
    while next < tail {
        let batch = stream
            .read(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(next)))
                    .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(1_000)))
                    .with_ignore_command_records(true),
            )
            .await
            .map_err(never_transient)?;
        if batch.records.is_empty() {
            return Err(Fault::Invariant(anyhow::anyhow!(
                "empty S2 batch before advertised tail {tail} at sequence {next}"
            )));
        }
        for record in batch.records {
            next = record.seq_num.saturating_add(1);
            records.push(WireRecord::new(record.seq_num, record.body.to_vec()));
        }
    }
    Ok(records)
}

async fn append_record(
    stream: &S2Stream,
    expected_tail: u64,
    body: Vec<u8>,
) -> Result<u64, Fault<JournalError, anyhow::Error, anyhow::Error>> {
    let record = AppendRecord::new(body).map_err(journal_invariant)?;
    let batch = AppendRecordBatch::try_from_iter([record]).map_err(journal_invariant)?;
    match stream
        .append(AppendInput::new(batch).with_match_seq_num(expected_tail))
        .await
    {
        Ok(ack) => Ok(ack.start.seq_num),
        Err(S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(
            current_tail,
        ))) => Err(Fault::Domain(JournalError::CasConflict { current_tail })),
        Err(error) => Err(Fault::Transient(anyhow::Error::new(error))),
    }
}

fn classify_graph_read(error: S2Error) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    match &error {
        S2Error::Server(response)
            if response.code.to_ascii_lowercase().contains("not_found")
                || response.message.to_ascii_lowercase().contains("not found") =>
        {
            Fault::Domain(JournalError::NotFound)
        }
        _ => Fault::Transient(anyhow::Error::new(error)),
    }
}

fn upcast_read_fault(
    error: Fault<Never, anyhow::Error, anyhow::Error>,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    error.squash()
}

fn decode_invariant<T>(
    error: DecodeStreamError<Infallible>,
) -> Fault<T, anyhow::Error, anyhow::Error> {
    match error {
        DecodeStreamError::Source(source) => match source {},
        DecodeStreamError::Invalid { sequence, error } => Fault::Invariant(anyhow::anyhow!(
            "invalid durable record at sequence {sequence}: {error}"
        )),
    }
}

fn never_transient(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Transient(anyhow::Error::new(error))
}

fn never_invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}

fn journal_invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}
