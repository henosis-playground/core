//! Merge many independently ordered streams into one observation-ordered virtual stream.
//!
//! The virtual offset is assigned by this subscriber as records become observable. It does not
//! claim a source-wide timestamp order that the underlying independent streams cannot provide.

use std::collections::BTreeMap;
use std::sync::Arc;

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
    async fn streams(
        &self,
    ) -> Result<Vec<StreamName>, Error<Never, anyhow::Error, anyhow::Error>>;
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
            storage.follow(stream, from)
        })
        .collect::<Vec<_>>();
    let merged = select_all(followers).scan(0_u64, |offset, item| {
        let current = *offset;
        *offset = offset.saturating_add(1);
        std::future::ready(Some(item.map(|record| MergedRecord::new(current, record))))
    });
    Ok(Box::pin(merged))
}
