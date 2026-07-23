//! Merge many independently ordered streams into one observation-ordered
//! virtual stream.
//!
//! The virtual offset is assigned by this subscriber as records become
//! observable. It does not claim a source-wide timestamp order that the
//! underlying independent streams cannot provide.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use faultline::Error;
use faultline::Never;
use futures::StreamExt;
use futures::stream::BoxStream;
use futures::stream::select_all;
use henosis_storage::StorageDomainError;
use henosis_storage::StorageEngine;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;

#[async_trait]
pub trait StreamCatalog: Send + Sync {
    async fn streams(&self) -> Result<Vec<StreamName>, Error<Never, anyhow::Error, anyhow::Error>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergedRecord {
    virtual_offset: u64,
    record: StoredRecord,
}

impl MergedRecord {
    #[must_use]
    pub const fn new(virtual_offset: u64, record: StoredRecord) -> Self {
        Self {
            virtual_offset,
            record,
        }
    }

    #[must_use]
    pub const fn virtual_offset(&self) -> u64 {
        self.virtual_offset
    }

    #[must_use]
    pub const fn record(&self) -> &StoredRecord {
        &self.record
    }
}

pub struct DynamicSubscription {
    pub additions: tokio::sync::mpsc::UnboundedSender<(StreamName, StreamPosition)>,
    pub records: BoxStream<
        'static,
        Result<MergedRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    >,
}

pub fn subscribe_dynamic(
    storage: Arc<dyn StorageEngine>,
    streams: impl IntoIterator<Item = (StreamName, StreamPosition)>,
) -> DynamicSubscription {
    let (add, mut additions) =
        tokio::sync::mpsc::unbounded_channel::<(StreamName, StreamPosition)>();
    let mut known = BTreeSet::new();
    let mut followers = futures::stream::SelectAll::new();
    for (stream, from) in streams {
        if known.insert(stream.clone()) {
            followers.push(resilient_follow(Arc::clone(&storage), stream, from));
        }
    }
    let merged = async_stream::stream! {
        let mut offset = 0_u64;
        loop {
            if followers.is_empty() {
                let Some((stream, from)) = additions.recv().await else {
                    break;
                };
                if known.insert(stream.clone()) {
                    followers.push(resilient_follow(Arc::clone(&storage), stream, from));
                }
                continue;
            }
            tokio::select! {
                addition = additions.recv() => {
                    let Some((stream, from)) = addition else {
                        while let Some(item) = followers.next().await {
                            yield item.map(|record| MergedRecord::new(offset, record));
                            offset = offset.saturating_add(1);
                        }
                        break;
                    };
                    if known.insert(stream.clone()) {
                        followers.push(resilient_follow(Arc::clone(&storage), stream, from));
                    }
                }
                Some(item) = followers.next() => {
                    yield item.map(|record| MergedRecord::new(offset, record));
                    offset = offset.saturating_add(1);
                }
            }
        }
    };
    DynamicSubscription {
        additions: add,
        records: Box::pin(merged),
    }
}

fn resilient_follow(
    storage: Arc<dyn StorageEngine>,
    stream: StreamName,
    from: StreamPosition,
) -> BoxStream<'static, Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>>
{
    Box::pin(async_stream::stream! {
        let mut next = from;
        loop {
            let mut follower = storage.follow(stream.clone(), next);
            while let Some(item) = follower.next().await {
                match item {
                    Ok(record) => {
                        next = StreamPosition::new(record.sequence().saturating_add(1));
                        yield Ok(record);
                    }
                    Err(Error::Transient(error)) => {
                        yield Err(Error::Transient(error));
                        break;
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
}

pub async fn subscribe(
    storage: Arc<dyn StorageEngine>,
    catalog: &dyn StreamCatalog,
    cursors: &BTreeMap<StreamName, StreamPosition>,
) -> Result<
    BoxStream<
        'static,
        Result<MergedRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    >,
    Error<Never, anyhow::Error, anyhow::Error>,
> {
    let mut streams = catalog.streams().await?;
    streams.sort();
    streams.dedup();
    let followers = streams
        .into_iter()
        .map(|stream| {
            let from = cursors.get(&stream).copied().unwrap_or_default();
            resilient_follow(Arc::clone(&storage), stream, from)
        })
        .collect::<Vec<_>>();
    let merged = select_all(followers).scan(0_u64, |offset, item| {
        let current = *offset;
        *offset = offset.saturating_add(1);
        std::future::ready(Some(item.map(|record| MergedRecord::new(current, record))))
    });
    Ok(Box::pin(merged))
}
