use std::convert::Infallible;

use faultline::Error as Fault;
use faultline::Never;
use futures::StreamExt;
use henosis_proto::journal::decode_spec_stream;
use henosis_proto::journal::encode_spec_record;
use henosis_types::RegisteredComponentSpec;
use henosis_types::SpecCatalog;
use henosis_types::SpecHistory;
use s2_sdk::S2Stream;

use crate::Journal;
use crate::JournalError;
use crate::stream::append_record;
use crate::stream::decode_invariant;
use crate::stream::named_tail;
use crate::stream::never_invariant;
use crate::stream::read_records;

const SPEC_STREAM: &str = "component-specs";

impl Journal {
    /// Append a content-addressed spec before its first graph reference.
    ///
    /// Resending identical content returns the stored value without appending.
    pub async fn component_spec_register(
        &self,
        component: RegisteredComponentSpec,
    ) -> Result<RegisteredComponentSpec, Fault<Never, anyhow::Error, anyhow::Error>> {
        for _ in 0..8 {
            let stream = self.named_stream(SPEC_STREAM)?;
            let tail = named_tail(&stream).await?;
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
        let tail = named_tail(&stream).await?;
        Ok(self
            .spec_history_load_until(&stream, tail)
            .await?
            .catalog()
            .clone())
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
}
