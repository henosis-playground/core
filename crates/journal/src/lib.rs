//! Durable core-event journal plus the S2 implementation of the storage
//! boundary.
//!
//! One graph stream carries facts whose value is their history: accepted graph
//! generations and plans, component/static-output replacements, observed-output
//! publications, stalls, and retirement. The root registry stream carries graph
//! registration and retirement so graph streams can be discovered after a cold
//! start. Controller dispositions are level reports, not log-shaped facts: a
//! controller re-observes and re-reports them on every pass, so they
//! deliberately remain memory-only. When a report includes an output
//! publication, only the generation-fenced `OutputsPublished` fact is durable.

use std::sync::Arc;

use async_trait::async_trait;
use faultline::Error;
use futures::StreamExt;
use futures::stream::BoxStream;
use henosis_storage::AppendAck;
use henosis_storage::AppendOutcome;
use henosis_storage::AppendRecord;
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
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord as S2AppendRecord;
use s2_sdk::types::AppendRecordBatch;
use s2_sdk::types::AppendRetryPolicy;
use s2_sdk::types::BasinEndpoint;
use s2_sdk::types::BasinName;
use s2_sdk::types::ReadFrom;
use s2_sdk::types::ReadInput;
use s2_sdk::types::ReadLimits;
use s2_sdk::types::ReadStart;
use s2_sdk::types::ReadStop;
use s2_sdk::types::RetryConfig;
use s2_sdk::types::S2Config;
use s2_sdk::types::S2Endpoints;
use s2_sdk::types::S2Error;
use serde::Deserialize;
use serde::Serialize;

const REGISTRY_STREAM: &str = "registry";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RegistryEvent {
    GraphRegistered(GraphIntent),
    GraphRegistrationSuperseded(GraphIntent),
    GraphRetired {
        graph_id: GraphId,
        last_generation: Generation,
    },
}

#[derive(Clone)]
pub struct Journal {
    storage: Arc<dyn StorageEngine>,
}

impl Journal {
    #[must_use]
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
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
        match self
            .storage
            .append(stream, expected, records.clone())
            .await?
        {
            AppendOutcome::Acknowledged(ack) => Ok(ack),
            AppendOutcome::CommitUnknown => {
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
                        .zip(&records)
                        .all(|(stored, expected)| stored.body() == expected.body());
                if matches {
                    let tail = StreamPosition::new(
                        expected.sequence().saturating_add(records.len() as u64),
                    );
                    Ok(AppendAck::new(expected, tail))
                } else {
                    Err(Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Domain(
                        StorageDomainError::CasConflict {
                            expected: expected.sequence(),
                            actual: cursor.sequence(),
                        },
                    ))
                }
            }
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

    fn stream(&self, name: &StreamName) -> Result<s2_sdk::S2Stream, anyhow::Error> {
        let name = name.as_str().parse::<s2_sdk::types::StreamName>()?;
        Ok(self.basin.stream(name))
    }
}

#[async_trait]
impl StorageEngine for S2Storage {
    async fn append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendOutcome, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let stream = self.stream(stream).map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(error)
        })?;
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
        match stream
            .append(AppendInput::new(batch).with_match_seq_num(expected.sequence()))
            .await
        {
            Ok(ack) => Ok(AppendOutcome::Acknowledged(AppendAck::new(
                StreamPosition::new(ack.start.seq_num),
                StreamPosition::new(ack.tail.seq_num),
            ))),
            Err(S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(actual))) => {
                Err(
                    Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Domain(
                        StorageDomainError::CasConflict {
                            expected: expected.sequence(),
                            actual,
                        },
                    ),
                )
            }
            Err(_error) => Ok(AppendOutcome::CommitUnknown),
        }
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let s2_stream = self.stream(stream).map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(error)
        })?;
        let batch =
            s2_stream
                .read(
                    ReadInput::new()
                        .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(from.sequence())))
                        .with_stop(ReadStop::new().with_limits(
                            ReadLimits::new().with_count(limit).with_bytes(1024 * 1024),
                        ))
                        .with_ignore_command_records(true),
                )
                .await
                .map_err(|error| {
                    Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(
                        anyhow::Error::new(error),
                    )
                })?;
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
        self.stream(stream)
            .map_err(|error| {
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(error)
            })?
            .check_tail()
            .await
            .map(|tail| StreamPosition::new(tail.seq_num))
            .map_err(|error| {
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(
                    anyhow::Error::new(error),
                )
            })
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
                let s2_stream = match storage.stream(&stream) {
                    Ok(value) => value,
                    Err(error) => {
                        yield Err(Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(error));
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
                        yield Err(Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(anyhow::Error::new(error)));
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
