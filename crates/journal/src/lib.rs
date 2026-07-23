//! Durable core-event journal plus the S2 implementation of the storage
//! boundary.
//!
//! Every S2 write uses an append session with `match_seq_num` optimistic
//! concurrency control. An OCC violation poisons that session. The journal
//! drops it before authoritative reload, fold, re-derivation, and reappend. An
//! unknown commit result also drops the session before the journal reads back
//! and compares the attempted records. The next append always opens a fresh
//! session. No token or epoch participates in S2 write correctness.
//!
//! One graph stream carries facts whose value is their history: accepted graph
//! generations and plans, component/static-output replacements, observed-output
//! publications, stalls, and retirement. The root registry stream carries graph
//! registration and retirement so graph streams can be discovered after a cold
//! start. Controller dispositions are level reports, not log-shaped facts: a
//! controller re-observes and re-reports them on every pass, so they
//! deliberately remain memory-only. When a report includes an output
//! publication, only the generation-scoped `OutputsPublished` fact is durable.

use std::collections::BTreeMap;
use std::str::FromStr as _;
use std::sync::Arc;

use async_trait::async_trait;
use faultline::Error;
use futures::StreamExt;
use futures::stream::BoxStream;
use henosis_storage::AppendAck;
use henosis_storage::AppendOutcome;
use henosis_storage::AppendRecord;
use henosis_storage::AppendSession as StorageAppendSession;
use henosis_storage::StorageDomainError;
use henosis_storage::StorageEngine;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use henosis_types::CoreEvent;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphIntent;
use s2_sdk::S2;
use s2_sdk::S2Basin;
use s2_sdk::append_session::AppendSession as S2AppendSession;
use s2_sdk::append_session::AppendSessionConfig;
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord as S2AppendRecord;
use s2_sdk::types::AppendRecordBatch;
use s2_sdk::types::AppendRetryPolicy;
use s2_sdk::types::BasinEndpoint;
use s2_sdk::types::BasinName;
use s2_sdk::types::EnsureStreamInput;
use s2_sdk::types::ReadFrom;
use s2_sdk::types::ReadInput;
use s2_sdk::types::ReadLimits;
use s2_sdk::types::ReadStart;
use s2_sdk::types::ReadStop;
use s2_sdk::types::RetentionPolicy;
use s2_sdk::types::RetryConfig;
use s2_sdk::types::S2Config;
use s2_sdk::types::S2Endpoints;
use s2_sdk::types::S2Error;
use s2_sdk::types::StreamConfig;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::Mutex;

const REGISTRY_STREAM: &str = "registry";

type AppendSessionSlot = Arc<Mutex<Option<Box<dyn StorageAppendSession>>>>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RegistryEvent {
    GraphRegistered(GraphIntent),
    GraphRegistrationSuperseded(GraphIntent),
    GraphRetired {
        graph_id: GraphId,
        last_generation: Generation,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalEvent {
    Registry(RegistryEvent),
    Graph(GraphId, CoreEvent),
}

#[derive(Clone)]
pub struct JournalFollower {
    additions: tokio::sync::mpsc::UnboundedSender<(StreamName, StreamPosition)>,
}

impl JournalFollower {
    pub fn add_graph(&self, graph_id: GraphId, from: StreamPosition) {
        let _ = self.additions.send((graph_stream(graph_id), from));
    }
}

#[derive(Clone)]
pub struct Journal {
    storage: Arc<dyn StorageEngine>,
    append_sessions: Arc<Mutex<BTreeMap<StreamName, AppendSessionSlot>>>,
}

impl Journal {
    #[must_use]
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self {
            storage,
            append_sessions: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn append(
        &self,
        graph_id: GraphId,
        expected: StreamPosition,
        event: &CoreEvent,
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.append_all(graph_id, expected, std::slice::from_ref(event))
            .await
    }

    pub async fn append_all(
        &self,
        graph_id: GraphId,
        expected: StreamPosition,
        events: &[CoreEvent],
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.append_records(
            &graph_stream(graph_id),
            expected,
            events.iter().map(EventRecord::Core),
        )
        .await
    }

    pub async fn append_registry(
        &self,
        expected: StreamPosition,
        event: &RegistryEvent,
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.append_records(
            &registry_stream(),
            expected,
            std::iter::once(EventRecord::Registry(event)),
        )
        .await
    }

    pub async fn tail(
        &self,
        graph_id: GraphId,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.storage.tail(&graph_stream(graph_id)).await
    }

    pub async fn load(
        &self,
        graph_id: GraphId,
    ) -> Result<Vec<CoreEvent>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.load_with_tail(graph_id)
            .await
            .map(|(events, _)| events)
    }

    pub async fn load_with_tail(
        &self,
        graph_id: GraphId,
    ) -> Result<
        (Vec<CoreEvent>, StreamPosition),
        Error<StorageDomainError, anyhow::Error, anyhow::Error>,
    > {
        let stream = graph_stream(graph_id);
        let (records, tail) = self.load_records(&stream).await?;
        let events = records
            .into_iter()
            .map(decode_graph_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((events, tail))
    }

    pub async fn load_registry(
        &self,
    ) -> Result<
        (Vec<RegistryEvent>, StreamPosition),
        Error<StorageDomainError, anyhow::Error, anyhow::Error>,
    > {
        let (records, tail) = self.load_records(&registry_stream()).await?;
        let events = records
            .into_iter()
            .map(decode_registry_record)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((events, tail))
    }

    pub fn follow(
        &self,
        graph_id: GraphId,
        from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<CoreEvent, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    > {
        Box::pin(
            self.storage
                .follow(graph_stream(graph_id), from)
                .map(|record| record.and_then(decode_graph_record)),
        )
    }

    pub fn follow_all(
        &self,
        registry_from: StreamPosition,
        graphs: impl IntoIterator<Item = (GraphId, StreamPosition)>,
    ) -> (
        JournalFollower,
        BoxStream<
            'static,
            Result<JournalEvent, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
        >,
    ) {
        let streams = std::iter::once((registry_stream(), registry_from)).chain(
            graphs
                .into_iter()
                .map(|(graph_id, from)| (graph_stream(graph_id), from)),
        );
        let (additions, merged) =
            henosis_multi_stream_merge::subscribe_dynamic(Arc::clone(&self.storage), streams);
        let events = merged.map(|item| {
            item.and_then(|item| {
                let record = item.record().clone();
                if record.stream() == &registry_stream() {
                    return decode_registry_record(record).map(JournalEvent::Registry);
                }
                let graph_id = record
                    .stream()
                    .as_str()
                    .strip_prefix("graph-")
                    .and_then(|value| GraphId::from_str(value).ok())
                    .ok_or_else(|| {
                        Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                            anyhow::anyhow!("invalid graph journal stream {}", record.stream()),
                        )
                    })?;
                decode_graph_record(record).map(|event| JournalEvent::Graph(graph_id, event))
            })
        });
        (JournalFollower { additions }, Box::pin(events))
    }

    async fn append_records<'a>(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: impl Iterator<Item = EventRecord<'a>>,
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let records = records.map(encode_record).collect::<Result<Vec<_>, _>>()?;
        if records.is_empty() {
            return Ok(AppendAck::new(expected, expected));
        }
        let slot = self.append_session_slot(stream).await;
        let mut session = slot.lock().await;
        if session.is_none() {
            *session = Some(self.storage.open_append_session(stream).await?);
        }
        let outcome = session
            .as_mut()
            .expect("append session was opened")
            .append(expected, records.clone())
            .await;
        match outcome {
            Ok(AppendOutcome::Acknowledged(ack)) => Ok(ack),
            Ok(AppendOutcome::CommitUnknown) => {
                *session = None;
                drop(session);
                self.resolve_unknown_append(stream, expected, &records)
                    .await
            }
            Err(error) => {
                *session = None;
                Err(error)
            }
        }
    }

    async fn append_session_slot(&self, stream: &StreamName) -> AppendSessionSlot {
        self.append_sessions
            .lock()
            .await
            .entry(stream.clone())
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone()
    }

    async fn resolve_unknown_append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: &[AppendRecord],
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let attempted_end = expected.sequence().saturating_add(records.len() as u64);
        let captured_tail = self.storage.tail(stream).await?;
        if captured_tail.sequence() < attempted_end {
            return Err(Error::Domain(StorageDomainError::CasConflict {
                expected: expected.sequence(),
                actual: captured_tail.sequence(),
            }));
        }

        let mut stored = Vec::with_capacity(records.len());
        let mut cursor = expected;
        while stored.len() < records.len() {
            let page = self
                .storage
                .read(stream, cursor, (records.len() - stored.len()).min(1_000))
                .await?;
            if page.is_empty() {
                break;
            }
            cursor = StreamPosition::new(
                page.last()
                    .expect("non-empty page has a last record")
                    .sequence()
                    .saturating_add(1),
            );
            stored.extend(page);
        }
        let matches = stored.len() == records.len()
            && stored
                .iter()
                .zip(records)
                .all(|(stored, expected)| stored.body() == expected.body());
        let current_tail = self.storage.tail(stream).await?;
        if matches {
            Ok(AppendAck::new(expected, current_tail))
        } else {
            Err(Error::Domain(StorageDomainError::CasConflict {
                expected: expected.sequence(),
                actual: current_tail.sequence(),
            }))
        }
    }

    async fn load_records(
        &self,
        stream: &StreamName,
    ) -> Result<
        (Vec<StoredRecord>, StreamPosition),
        Error<StorageDomainError, anyhow::Error, anyhow::Error>,
    > {
        let tail = self.storage.tail(stream).await?;
        if tail.sequence() == 0 {
            return Ok((Vec::new(), tail));
        }
        let mut records = Vec::new();
        let mut cursor = StreamPosition::default();
        while cursor.sequence() < tail.sequence() {
            let remaining = tail.sequence().saturating_sub(cursor.sequence()) as usize;
            let page = self
                .storage
                .read(stream, cursor, remaining.min(1_000))
                .await?;
            if page.is_empty() {
                return Err(
                    Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                        anyhow::anyhow!(
                            "journal read made no progress on {stream}: cursor {}, captured tail \
                             {}",
                            cursor.sequence(),
                            tail.sequence()
                        ),
                    ),
                );
            }
            for record in &page {
                if record.sequence() != cursor.sequence() {
                    return Err(
                        Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                            anyhow::anyhow!(
                                "journal read gap on {stream}: expected sequence {}, got {}",
                                cursor.sequence(),
                                record.sequence()
                            ),
                        ),
                    );
                }
                cursor = StreamPosition::new(cursor.sequence().saturating_add(1));
                if cursor.sequence() > tail.sequence() {
                    return Err(
                        Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                            anyhow::anyhow!("journal read passed captured tail on {stream}"),
                        ),
                    );
                }
            }
            records.extend(page);
        }
        Ok((records, cursor))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JournalRecord<T> {
    schema: u32,
    event: T,
}

enum EventRecord<'a> {
    Core(&'a CoreEvent),
    Registry(&'a RegistryEvent),
}

fn encode_record(
    record: EventRecord<'_>,
) -> Result<AppendRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    let body = match record {
        EventRecord::Core(event) => serde_json::to_vec(&JournalRecord { schema: 1, event }),
        EventRecord::Registry(event) => serde_json::to_vec(&JournalRecord { schema: 1, event }),
    }
    .map_err(|error| {
        Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(anyhow::Error::new(
            error,
        ))
    })?;
    Ok(AppendRecord::new(body))
}

fn decode_graph_record(
    record: StoredRecord,
) -> Result<CoreEvent, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    decode_record(record)
}

fn decode_registry_record(
    record: StoredRecord,
) -> Result<RegistryEvent, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    decode_record(record)
}

fn decode_record<T: for<'de> Deserialize<'de>>(
    record: StoredRecord,
) -> Result<T, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    let decoded: JournalRecord<T> = serde_json::from_slice(record.body()).map_err(|error| {
        Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(anyhow::anyhow!(
            "invalid durable record on {} at sequence {}: {error}",
            record.stream(),
            record.sequence()
        ))
    })?;
    if decoded.schema != 1 {
        return Err(
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(anyhow::anyhow!(
                "unsupported journal schema {} on {} at sequence {}",
                decoded.schema,
                record.stream(),
                record.sequence()
            )),
        );
    }
    Ok(decoded.event)
}

#[must_use]
pub const fn is_durable(event: &CoreEvent) -> bool {
    !matches!(event, CoreEvent::ControllerReported(_))
}

fn graph_stream(graph_id: GraphId) -> StreamName {
    StreamName::new(format!("graph-{graph_id}")).expect("TypeID forms a valid stream name")
}

fn registry_stream() -> StreamName {
    StreamName::new(REGISTRY_STREAM).expect("registry is a valid stream name")
}

#[derive(Clone)]
pub struct S2Storage {
    basin: S2Basin,
}

impl S2Storage {
    pub fn connect(
        access_token: impl Into<String>,
        account_endpoint: &str,
        basin_endpoint: &str,
        basin: &str,
    ) -> anyhow::Result<Self> {
        let endpoints = S2Endpoints::new(
            AccountEndpoint::new(account_endpoint)?,
            BasinEndpoint::new(basin_endpoint)?,
        )?;
        let retry = RetryConfig::new().with_append_retry_policy(AppendRetryPolicy::NoSideEffects);
        let client = S2::new(
            S2Config::new(access_token)
                .with_endpoints(endpoints)
                .with_retry(retry),
        )?;
        let basin = basin.parse::<BasinName>()?;
        Ok(Self {
            basin: client.basin(basin),
        })
    }

    async fn ensure_stream(
        &self,
        stream: &StreamName,
    ) -> Result<s2_sdk::S2Stream, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let name = stream
            .as_str()
            .parse::<s2_sdk::types::StreamName>()
            .map_err(|error| {
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                    anyhow::Error::new(error),
                )
            })?;
        self.basin
            .ensure_stream(
                EnsureStreamInput::new(name.clone()).with_config(
                    StreamConfig::new().with_retention_policy(RetentionPolicy::Infinite),
                ),
            )
            .await
            .map_err(classify_definite_failure)?;
        Ok(self.basin.stream(name))
    }
}

struct S2StorageAppendSession {
    session: S2AppendSession,
    poisoned: bool,
}

#[async_trait]
impl StorageAppendSession for S2StorageAppendSession {
    async fn append(
        &mut self,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendOutcome, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        if self.poisoned {
            return Err(Error::Transient(anyhow::anyhow!(
                "append session is poisoned"
            )));
        }
        let records = records
            .into_iter()
            .map(|record| S2AppendRecord::new(record.body().to_vec()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                    anyhow::Error::new(error),
                )
            })?;
        let batch = AppendRecordBatch::try_from_iter(records).map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                anyhow::Error::new(error),
            )
        })?;
        let ticket = match self
            .session
            .submit(AppendInput::new(batch).with_match_seq_num(expected.sequence()))
            .await
        {
            Ok(ticket) => ticket,
            Err(error) => {
                self.poisoned = true;
                return Err(classify_definite_failure(error));
            }
        };
        match ticket.await {
            Ok(ack) => Ok(AppendOutcome::Acknowledged(AppendAck::new(
                StreamPosition::new(ack.start.seq_num),
                StreamPosition::new(ack.tail.seq_num),
            ))),
            Err(S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(actual))) => {
                self.poisoned = true;
                Err(
                    Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Domain(
                        StorageDomainError::CasConflict {
                            expected: expected.sequence(),
                            actual,
                        },
                    ),
                )
            }
            Err(_error) => {
                self.poisoned = true;
                Ok(AppendOutcome::CommitUnknown)
            }
        }
    }
}

fn classify_definite_failure(
    error: S2Error,
) -> Error<StorageDomainError, anyhow::Error, anyhow::Error> {
    match &error {
        S2Error::Validation(_)
        | S2Error::MalformedAccessToken(_)
        | S2Error::AppendConditionFailed(_)
        | S2Error::ReadUnwritten(_) => Error::Invariant(anyhow::Error::new(error)),
        S2Error::Server(response)
            if matches!(
                response.code.as_str(),
                "bad_header"
                    | "bad_path"
                    | "bad_query"
                    | "bad_json"
                    | "bad_proto"
                    | "bad_frame"
                    | "decryption_failed"
                    | "authn"
                    | "permission_denied"
                    | "quota_exhausted"
                    | "basin_not_found"
                    | "stream_not_found"
                    | "access_token_not_found"
                    | "resource_already_exists"
                    | "basin_deletion_pending"
                    | "stream_deletion_pending"
                    | "invalid"
                    | "not_implemented"
            ) =>
        {
            Error::Invariant(anyhow::Error::new(error))
        }
        S2Error::Server(_) | S2Error::Client(_) => Error::Transient(anyhow::Error::new(error)),
    }
}

#[async_trait]
impl StorageEngine for S2Storage {
    async fn open_append_session(
        &self,
        stream: &StreamName,
    ) -> Result<
        Box<dyn StorageAppendSession>,
        Error<StorageDomainError, anyhow::Error, anyhow::Error>,
    > {
        Ok(Box::new(S2StorageAppendSession {
            session: self
                .ensure_stream(stream)
                .await?
                .append_session(AppendSessionConfig::new()),
            poisoned: false,
        }))
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let s2_stream = self.ensure_stream(stream).await?;
        let batch = match s2_stream
            .read(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(from.sequence())))
                    .with_stop(
                        ReadStop::new().with_limits(
                            ReadLimits::new()
                                .with_count(limit.min(1_000))
                                .with_bytes(1024 * 1024),
                        ),
                    )
                    .with_ignore_command_records(true),
            )
            .await
        {
            Ok(batch) => batch,
            Err(S2Error::ReadUnwritten(tail)) if from.sequence() >= tail.seq_num => {
                return Ok(Vec::new());
            }
            Err(error) => return Err(classify_definite_failure(error)),
        };
        Ok(batch
            .records
            .into_iter()
            .map(|record| {
                StoredRecord::new(
                    stream.clone(),
                    record.seq_num,
                    record.timestamp,
                    record.body.to_vec(),
                )
            })
            .collect())
    }

    async fn tail(
        &self,
        stream: &StreamName,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        self.ensure_stream(stream)
            .await?
            .check_tail()
            .await
            .map(|tail| StreamPosition::new(tail.seq_num))
            .map_err(classify_definite_failure)
    }

    fn follow(
        &self,
        stream: StreamName,
        from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    > {
        let storage = self.clone();
        Box::pin(async_stream::stream! {
            let mut next = from;
            loop {
                let s2_stream = match storage.ensure_stream(&stream).await {
                    Ok(value) => value,
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                };
                let batch = s2_stream
                    .read(
                        ReadInput::new()
                            .with_start(
                                ReadStart::new()
                                    .with_from(ReadFrom::SeqNum(next.sequence()))
                                    .with_clamp_to_tail(true),
                            )
                            .with_stop(
                                ReadStop::new()
                                    .with_limits(ReadLimits::new().with_count(1_000))
                                    .with_wait(30),
                            )
                            .with_ignore_command_records(true),
                    )
                    .await;
                match batch {
                    Ok(batch) => {
                        for record in batch.records {
                            next = StreamPosition::new(record.seq_num.saturating_add(1));
                            yield Ok(StoredRecord::new(
                                stream.clone(),
                                record.seq_num,
                                record.timestamp,
                                record.body.to_vec(),
                            ));
                        }
                    }
                    Err(error) => {
                        yield Err(classify_definite_failure(error));
                        return;
                    }
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use henosis_storage::AppendOutcome;
    use henosis_storage::MemoryStorage;

    #[derive(Clone, Default)]
    struct BoundedReadStorage {
        inner: MemoryStorage,
    }

    #[async_trait]
    impl StorageEngine for BoundedReadStorage {
        async fn open_append_session(
            &self,
            stream: &StreamName,
        ) -> Result<
            Box<dyn StorageAppendSession>,
            Error<StorageDomainError, anyhow::Error, anyhow::Error>,
        > {
            self.inner.open_append_session(stream).await
        }

        async fn append(
            &self,
            stream: &StreamName,
            expected: StreamPosition,
            records: Vec<AppendRecord>,
        ) -> Result<AppendOutcome, Error<StorageDomainError, anyhow::Error, anyhow::Error>>
        {
            self.inner.append(stream, expected, records).await
        }

        async fn read(
            &self,
            stream: &StreamName,
            from: StreamPosition,
            limit: usize,
        ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>>
        {
            let records = self.inner.read(stream, from, limit.min(1_000)).await?;
            let mut bytes = 0_usize;
            Ok(records
                .into_iter()
                .take_while(|record| {
                    bytes = bytes.saturating_add(record.body().len());
                    bytes <= 64 * 1024
                })
                .collect())
        }

        async fn tail(
            &self,
            stream: &StreamName,
        ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>>
        {
            self.inner.tail(stream).await
        }

        fn follow(
            &self,
            stream: StreamName,
            from: StreamPosition,
        ) -> BoxStream<
            'static,
            Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
        > {
            self.inner.follow(stream, from)
        }
    }

    #[tokio::test]
    async fn replay_reads_every_page_past_count_and_byte_limits() {
        let storage = Arc::new(BoundedReadStorage::default());
        let stream = StreamName::new("long-journal").expect("stream name is valid");
        let records = (0..1_201)
            .map(|index| {
                let mut body = vec![b'x'; 1_024];
                body[..8].copy_from_slice(&(index as u64).to_le_bytes());
                AppendRecord::new(body)
            })
            .collect::<Vec<_>>();
        let outcome = storage
            .append(&stream, StreamPosition::default(), records)
            .await
            .expect("fixture append succeeds");
        assert!(matches!(outcome, AppendOutcome::Acknowledged(_)));

        let journal = Journal::new(storage);
        let (loaded, tail) = journal
            .load_records(&stream)
            .await
            .expect("all bounded pages replay");
        assert_eq!(loaded.len(), 1_201);
        assert_eq!(tail.sequence(), 1_201);
        for (index, record) in loaded.iter().enumerate() {
            assert_eq!(record.sequence(), index as u64);
        }
    }
}
