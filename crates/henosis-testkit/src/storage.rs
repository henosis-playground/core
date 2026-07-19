use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;

use async_trait::async_trait;
use faultline::Error;
use futures::stream::BoxStream;
use henosis_storage::AppendAck;
use henosis_storage::AppendOutcome;
use henosis_storage::AppendRecord;
use henosis_storage::StorageDomainError;
use henosis_storage::StorageEngine;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use thiserror::Error as ThisError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AppendFault {
    Acknowledge,
    RejectBeforeCommit,
    CommitThenTimeout,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemS2Append {
    pub acknowledgement: Option<AppendAck>,
    pub committed: bool,
}

#[derive(Clone, Debug, ThisError, Eq, PartialEq)]
pub enum MemS2Error {
    #[error("append compare-and-set failed: expected {expected}, current tail is {actual}")]
    CasConflict { expected: u64, actual: u64 },
    #[error("append failed before commit")]
    Rejected,
    #[error("append timed out after commit; durability is ambiguous to the caller")]
    TimeoutAfterCommit,
}

#[derive(Clone, Debug, Default)]
pub struct MemS2 {
    state: Arc<Mutex<MemS2State>>,
}

#[derive(Debug, Default)]
struct MemS2State {
    streams: BTreeMap<StreamName, Vec<StoredRecord>>,
    faults: VecDeque<AppendFault>,
    clock: u64,
}

impl MemS2 {
    pub fn script(&self, faults: impl IntoIterator<Item = AppendFault>) {
        self.state
            .lock()
            .expect("MemS2 lock is not poisoned")
            .faults
            .extend(faults);
    }

    pub fn append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<MemS2Append, MemS2Error> {
        let mut state = self.state.lock().expect("MemS2 lock is not poisoned");
        let actual = state.streams.get(stream).map(Vec::len).unwrap_or_default() as u64;
        if expected.sequence() != actual {
            return Err(MemS2Error::CasConflict {
                expected: expected.sequence(),
                actual,
            });
        }
        let fault = state.faults.pop_front().unwrap_or(AppendFault::Acknowledge);
        if fault == AppendFault::RejectBeforeCommit {
            return Err(MemS2Error::Rejected);
        }
        let start = actual;
        for record in records {
            let sequence = state.streams.get(stream).map(Vec::len).unwrap_or_default() as u64;
            let timestamp = state.clock;
            state.clock = state.clock.saturating_add(1);
            state
                .streams
                .entry(stream.clone())
                .or_default()
                .push(StoredRecord::new(
                    stream.clone(),
                    sequence,
                    timestamp,
                    record.body().to_vec(),
                ));
        }
        let tail =
            StreamPosition::new(state.streams.get(stream).map(Vec::len).unwrap_or_default() as u64);
        let acknowledgement = AppendAck::new(StreamPosition::new(start), tail);
        if fault == AppendFault::CommitThenTimeout {
            return Err(MemS2Error::TimeoutAfterCommit);
        }
        Ok(MemS2Append {
            acknowledgement: Some(acknowledgement),
            committed: true,
        })
    }

    #[must_use]
    pub fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Vec<StoredRecord> {
        self.state
            .lock()
            .expect("MemS2 lock is not poisoned")
            .streams
            .get(stream)
            .into_iter()
            .flat_map(|records| records.iter())
            .skip(from.sequence() as usize)
            .take(limit)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn tail(&self, stream: &StreamName) -> StreamPosition {
        let state = self.state.lock().expect("MemS2 lock is not poisoned");
        StreamPosition::new(state.streams.get(stream).map(Vec::len).unwrap_or_default() as u64)
    }
}

#[async_trait]
impl StorageEngine for MemS2 {
    async fn append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendOutcome, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        match MemS2::append(self, stream, expected, records) {
            Ok(result) => {
                Ok(AppendOutcome::Acknowledged(result.acknowledgement.expect(
                    "acknowledged MemS2 append has an acknowledgement",
                )))
            }
            Err(MemS2Error::CasConflict { expected, actual }) => {
                Err(Error::Domain(StorageDomainError::CasConflict {
                    expected,
                    actual,
                }))
            }
            Err(MemS2Error::Rejected) => Err(Error::Transient(anyhow::anyhow!(
                "append failed before commit"
            ))),
            Err(MemS2Error::TimeoutAfterCommit) => Ok(AppendOutcome::CommitUnknown),
        }
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        Ok(MemS2::read(self, stream, from, limit))
    }

    async fn tail(
        &self,
        stream: &StreamName,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        Ok(MemS2::tail(self, stream))
    }

    fn follow(
        &self,
        _stream: StreamName,
        _from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    > {
        Box::pin(futures::stream::pending())
    }
}
