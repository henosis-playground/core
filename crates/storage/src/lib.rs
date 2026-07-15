//! Swappable append-only stream storage with an in-memory deterministic fake.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use faultline::Error;
use futures::stream::BoxStream;
use thiserror::Error as ThisError;
use tokio::sync::Mutex;
use tokio::sync::Notify;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct StreamName(String);

impl StreamName {
    pub fn new(value: impl Into<String>) -> Result<Self, StreamNameError> {
        let value = value.into();
        if value.is_empty() || value.len() > 512 {
            return Err(StreamNameError);
        }
        if !value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/')
        }) {
            return Err(StreamNameError);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl FromStr for StreamName {
    type Err = StreamNameError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

#[derive(Clone, Copy, Debug, ThisError, Eq, PartialEq)]
#[error("stream name is empty, too long, or contains an unsupported character")]
pub struct StreamNameError;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StreamPosition(u64);

impl StreamPosition {
    #[must_use]
    pub const fn new(sequence: u64) -> Self {
        Self(sequence)
    }

    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AppendRecord {
    body: Vec<u8>,
}

impl AppendRecord {
    #[must_use]
    pub const fn new(body: Vec<u8>) -> Self {
        Self { body }
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredRecord {
    stream: StreamName,
    sequence: u64,
    timestamp: u64,
    body: Vec<u8>,
}

impl StoredRecord {
    #[must_use]
    pub const fn new(
        stream: StreamName,
        sequence: u64,
        timestamp: u64,
        body: Vec<u8>,
    ) -> Self {
        Self {
            stream,
            sequence,
            timestamp,
            body,
        }
    }

    #[must_use]
    pub const fn stream(&self) -> &StreamName {
        &self.stream
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn timestamp(&self) -> u64 {
        self.timestamp
    }

    #[must_use]
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AppendAck {
    start: StreamPosition,
    tail: StreamPosition,
}

impl AppendAck {
    #[must_use]
    pub const fn new(start: StreamPosition, tail: StreamPosition) -> Self {
        Self { start, tail }
    }

    #[must_use]
    pub const fn start(self) -> StreamPosition {
        self.start
    }

    #[must_use]
    pub const fn tail(self) -> StreamPosition {
        self.tail
    }
}

#[derive(Clone, Debug, ThisError, Eq, PartialEq)]
pub enum StorageDomainError {
    #[error("append compare-and-set failed: expected {expected}, current tail is {actual}")]
    CasConflict { expected: u64, actual: u64 },
}

#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>>;

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>>;

    async fn tail(
        &self,
        stream: &StreamName,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>>;

    fn follow(
        &self,
        stream: StreamName,
        from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    >;
}

#[derive(Clone, Debug, Default)]
pub struct MemoryStorage {
    inner: Arc<MemoryStorageInner>,
}

#[derive(Debug, Default)]
struct MemoryStorageInner {
    state: Mutex<MemoryState>,
    changed: Notify,
}

#[derive(Debug, Default)]
struct MemoryState {
    streams: BTreeMap<StreamName, Vec<StoredRecord>>,
    clock: u64,
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn append(
        &self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendAck, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let mut state = self.inner.state.lock().await;
        let actual = state.streams.get(stream).map_or(0, Vec::len) as u64;
        if expected.sequence() != actual {
            return Err(Error::Domain(StorageDomainError::CasConflict {
                expected: expected.sequence(),
                actual,
            }));
        }
        let start = actual;
        for record in records {
            let timestamp = state.clock;
            state.clock = state.clock.saturating_add(1);
            let sequence = state.streams.get(stream).map_or(0, Vec::len) as u64;
            state
                .streams
                .entry(stream.clone())
                .or_default()
                .push(StoredRecord::new(
                    stream.clone(),
                    sequence,
                    timestamp,
                    record.body,
                ));
        }
        let tail = state.streams.get(stream).map_or(0, Vec::len) as u64;
        drop(state);
        self.inner.changed.notify_waiters();
        Ok(AppendAck::new(
            StreamPosition::new(start),
            StreamPosition::new(tail),
        ))
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let state = self.inner.state.lock().await;
        Ok(state
            .streams
            .get(stream)
            .into_iter()
            .flat_map(|records| records.iter())
            .skip(from.sequence() as usize)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn tail(
        &self,
        stream: &StreamName,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let state = self.inner.state.lock().await;
        Ok(StreamPosition::new(
            state.streams.get(stream).map_or(0, Vec::len) as u64,
        ))
    }

    fn follow(
        &self,
        stream: StreamName,
        from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    > {
        let inner = Arc::clone(&self.inner);
        Box::pin(async_stream::stream! {
            let mut next = from.sequence();
            loop {
                let notified = inner.changed.notified();
                let available = {
                    let state = inner.state.lock().await;
                    state
                        .streams
                        .get(&stream)
                        .and_then(|records| records.get(next as usize))
                        .cloned()
                };
                if let Some(record) = available {
                    next = next.saturating_add(1);
                    yield Ok(record);
                } else {
                    notified.await;
                }
            }
        })
    }
}
