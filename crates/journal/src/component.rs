use std::convert::Infallible;
use std::time::Duration;
use std::time::UNIX_EPOCH;

use faultline::Error as Fault;
use faultline::Never;
use futures::StreamExt;
use futures::TryStreamExt;
use henosis_proto::journal::decode_spec_stream;
use henosis_proto::journal::encode_spec_record;
use s2_sdk::S2Stream;
use s2_sdk::types::ListAllStreamsInput;
use types::domain::Component;
use types::domain::ComponentCatalog;
use types::domain::ComponentCatalogError;
use types::domain::ComponentHistory;
use types::domain::ComponentUuid;
use types::domain::NewComponent;

use crate::Journal;
use crate::JournalError;
use crate::stream::append_record;
use crate::stream::decode_invariant;
use crate::stream::named_tail;
use crate::stream::never_invariant;
use crate::stream::read_records;

const COMPONENT_STREAM_PREFIX: &str = "component-";
type NeverFault = Fault<Never, anyhow::Error, anyhow::Error>;

impl Journal {
    /// Create a component and durably append its initial specification.
    pub async fn component_register(
        &self,
        component: NewComponent,
    ) -> Result<Component, Fault<Never, anyhow::Error, anyhow::Error>> {
        for _ in 0..8 {
            let stream = self.component_stream(component.id())?;
            let tail = named_tail(&stream).await?;
            if tail != 0 {
                let history = self
                    .component_history_load_until(component.id(), &stream, tail)
                    .await?;
                let stored = history.latest().ok_or_else(|| {
                    NeverFault::Invariant(anyhow::anyhow!(
                        "non-empty component stream has no state"
                    ))
                })?;
                if component.spec() == stored.spec() {
                    return Ok(stored.clone());
                }
                return Err(NeverFault::Invariant(anyhow::anyhow!(
                    "component identity is already registered with another specification"
                )));
            }
            match append_record(&stream, 0, encode_spec_record(&component)).await {
                Ok(position) => {
                    let time_recorded = UNIX_EPOCH
                        .checked_add(Duration::from_millis(position.timestamp))
                        .ok_or_else(|| {
                            NeverFault::Invariant(anyhow::anyhow!(
                                "S2 component timestamp is outside the system-time range"
                            ))
                        })?;
                    return Ok(Component::new(
                        component.id(),
                        position.seq_num,
                        time_recorded,
                        time_recorded,
                        component.into_spec(),
                    ));
                }
                Err(Fault::Domain(JournalError::CasConflict { .. })) => {}
                Err(Fault::Domain(JournalError::NotFound)) => {
                    return Err(NeverFault::Invariant(anyhow::anyhow!(
                        "component append unexpectedly reported graph not found"
                    )));
                }
                Err(Fault::Transient(error)) => return Err(Fault::Transient(error)),
                Err(Fault::Invariant(error)) => return Err(NeverFault::Invariant(error)),
            }
        }
        Err(Fault::Transient(anyhow::anyhow!(
            "component compare-and-append did not converge"
        )))
    }

    /// Load the latest generation of a component.
    pub async fn component_latest(
        &self,
        component_id: ComponentUuid,
    ) -> Result<Option<Component>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let stream = self.component_stream(component_id)?;
        let tail = named_tail(&stream).await?;
        if tail == 0 {
            return Ok(None);
        }
        Ok(self
            .component_history_load_until(component_id, &stream, tail)
            .await?
            .latest()
            .cloned())
    }

    /// Load one component generation.
    pub async fn component_at_generation(
        &self,
        component_id: ComponentUuid,
        generation: u64,
    ) -> Result<Option<Component>, Fault<Never, anyhow::Error, anyhow::Error>> {
        let stream = self.component_stream(component_id)?;
        let tail = named_tail(&stream).await?;
        if generation >= tail {
            return Ok(None);
        }
        Ok(self
            .component_history_load_until(component_id, &stream, generation.saturating_add(1))
            .await?
            .at_generation(generation)
            .cloned())
    }

    /// Load the latest generation of every component stream.
    pub async fn component_catalog(
        &self,
    ) -> Result<ComponentCatalog, Fault<Never, anyhow::Error, anyhow::Error>> {
        let prefix = COMPONENT_STREAM_PREFIX.parse().map_err(never_invariant)?;
        let mut streams = self
            .basin
            .list_all_streams(ListAllStreamsInput::new().with_prefix(prefix));
        let mut components = Vec::new();
        while let Some(info) = streams
            .try_next()
            .await
            .map_err(|error| NeverFault::Transient(anyhow::Error::new(error)))?
        {
            let name = info.name.to_string();
            let component_id = name
                .strip_prefix(COMPONENT_STREAM_PREFIX)
                .ok_or_else(|| {
                    NeverFault::Invariant(anyhow::anyhow!("invalid component stream name"))
                })?
                .parse::<ComponentUuid>()
                .map_err(|error| NeverFault::Invariant(anyhow::Error::new(error)))?;
            components.push(self.component_latest(component_id).await?.ok_or_else(|| {
                NeverFault::Invariant(anyhow::anyhow!("listed component stream is empty"))
            })?);
        }
        let mut catalog = ComponentCatalog::default();
        while !components.is_empty() {
            let before = components.len();
            let mut deferred = Vec::new();
            for component in components {
                match catalog.apply(component.clone()) {
                    Ok(()) => {}
                    Err(ComponentCatalogError::MissingDependency) => deferred.push(component),
                    Err(error) => {
                        return Err(NeverFault::Invariant(anyhow::Error::new(error)));
                    }
                }
            }
            if deferred.len() == before {
                return Err(NeverFault::Invariant(anyhow::anyhow!(
                    "component dependencies contain missing identities or a cycle"
                )));
            }
            components = deferred;
        }
        Ok(catalog)
    }

    async fn component_history_load_until(
        &self,
        component_id: ComponentUuid,
        stream: &S2Stream,
        tail: u64,
    ) -> Result<ComponentHistory, Fault<Never, anyhow::Error, anyhow::Error>> {
        let records = read_records(stream, tail).await?;
        let mut parsed = Box::pin(decode_spec_stream(futures::stream::iter(
            records.into_iter().map(Ok::<_, Infallible>),
        )));
        let mut history = ComponentHistory::new(component_id);
        while let Some(item) = parsed.next().await {
            let event = item.map_err(decode_invariant)?;
            history.apply(event).map_err(never_invariant)?;
        }
        Ok(history)
    }
}
