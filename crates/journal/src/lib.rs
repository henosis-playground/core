//! Durable core-event journal plus the S2 implementation of the storage
//! boundary.

use std::sync::Arc;

use async_trait::async_trait;
use faultline::Error;
use futures::StreamExt;
use futures::stream::BoxStream;
use henosis_storage::AppendAck;
use henosis_storage::AppendRecord;
use henosis_storage::StorageDomainError;
use henosis_storage::StorageEngine;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use henosis_types::CoreEvent;
use henosis_types::GraphId;
use s2_sdk::S2;
use s2_sdk::S2Basin;
use s2_sdk::types::AccountEndpoint;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord as S2AppendRecord;
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
use serde::Deserialize;
use serde::Serialize;

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
        let body = serde_json::to_vec(&JournalRecord {
            schema: 1,
            event: event.clone(),
        })
        .map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Invariant(
                anyhow::Error::new(error),
            )
        })?;
        self.storage
            .append(
                &graph_stream(graph_id),
                expected,
                vec![AppendRecord::new(body)],
            )
            .await
    }

    pub async fn load(
        &self,
        graph_id: GraphId,
    ) -> Result<Vec<CoreEvent>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let stream = graph_stream(graph_id);
        let tail = self.storage.tail(&stream).await?;
        let records = self
            .storage
            .read(&stream, StreamPosition::default(), tail.sequence() as usize)
            .await?;
        records.into_iter().map(decode_record).collect()
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
                .map(|record| record.and_then(decode_record)),
        )
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JournalRecord {
    schema: u32,
    event: CoreEvent,
}

fn decode_record(
    record: StoredRecord,
) -> Result<CoreEvent, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    let decoded: JournalRecord = serde_json::from_slice(record.body()).map_err(|error| {
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

fn graph_stream(graph_id: GraphId) -> StreamName {
    StreamName::new(format!("graph-{graph_id}")).expect("TypeID forms a valid stream name")
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
        let client = S2::new(S2Config::new(access_token).with_endpoints(endpoints))?;
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
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
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
            Ok(ack) => Ok(AppendAck::new(
                StreamPosition::new(ack.start.seq_num),
                StreamPosition::new(ack.tail.seq_num),
            )),
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
            Err(error) => Err(
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(
                    anyhow::Error::new(error),
                ),
            ),
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
        let batch = s2_stream
            .read(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(from.sequence())))
                    .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(limit)))
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
