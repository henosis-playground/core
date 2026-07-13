use std::convert::Infallible;

use faultline::Error as Fault;
use faultline::Never;
use futures::StreamExt;
use henosis_proto::journal::decode_registry_stream;
use henosis_proto::journal::encode_registry_event;
use henosis_types::GraphUuid;
use henosis_types::RegistryEvent;
use henosis_types::RegistryHistory;
use henosis_types::RequestUuid;
use s2_sdk::S2Stream;

use crate::Journal;
use crate::JournalError;
use crate::stream::append_record;
use crate::stream::decode_invariant;
use crate::stream::named_tail;
use crate::stream::never_invariant;
use crate::stream::read_records;

const REGISTRY_STREAM: &str = "registry";

impl Journal {
    /// List graph identities recorded in the discovery registry.
    pub async fn graph_list(
        &self,
    ) -> Result<Vec<GraphUuid>, Fault<Never, anyhow::Error, anyhow::Error>> {
        Ok(self.registry_load().await?.graph_ids().collect::<Vec<_>>())
    }

    /// Convergently record one graph lifecycle level in the registry.
    pub async fn graph_registry_ensure(
        &self,
        graph_id: GraphUuid,
        request_id: RequestUuid,
        retired: bool,
    ) -> Result<(), Fault<Never, anyhow::Error, anyhow::Error>> {
        for _ in 0..8 {
            let stream = self.named_stream(REGISTRY_STREAM)?;
            let tail = named_tail(&stream).await?;
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

    async fn registry_load(
        &self,
    ) -> Result<RegistryHistory, Fault<Never, anyhow::Error, anyhow::Error>> {
        let stream = self.named_stream(REGISTRY_STREAM)?;
        let tail = named_tail(&stream).await?;
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
}
