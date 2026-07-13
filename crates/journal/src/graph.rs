use std::convert::Infallible;

use faultline::Error as Fault;
use futures::StreamExt;
use henosis_proto::journal::decode_graph_stream;
use henosis_proto::journal::encode_graph_event;
use types::domain::GraphEvent;
use types::domain::GraphHistory;
use types::domain::GraphUuid;

use crate::Journal;
use crate::JournalError;
use crate::stream::append_record;
use crate::stream::classify_graph_read;
use crate::stream::decode_invariant;
use crate::stream::journal_invariant;
use crate::stream::read_records;
use crate::stream::upcast_read_fault;

impl Journal {
    /// Load and fold one graph stream through its current head.
    pub async fn graph_load(
        &self,
        graph_id: GraphUuid,
    ) -> Result<GraphHistory, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        let stream = self.graph_stream(graph_id)?;
        let tail = stream
            .check_tail()
            .await
            .map_err(classify_graph_read)?
            .seq_num;
        if tail == 0 {
            return Err(Fault::Domain(JournalError::NotFound));
        }
        let records = read_records(&stream, tail)
            .await
            .map_err(upcast_read_fault)?;
        let mut parsed = Box::pin(decode_graph_stream(futures::stream::iter(
            records.into_iter().map(Ok::<_, Infallible>),
        )));
        let mut history = GraphHistory::new(graph_id);
        while let Some(item) = parsed.next().await {
            let event = item.map_err(decode_invariant)?;
            history.apply(event).map_err(journal_invariant)?;
        }
        Ok(history)
    }

    /// Compare-and-append one already-validated domain event.
    pub async fn graph_append(
        &self,
        graph_id: GraphUuid,
        expected_tail: u64,
        event: &GraphEvent,
    ) -> Result<u64, Fault<JournalError, anyhow::Error, anyhow::Error>> {
        append_record(
            &self.graph_stream(graph_id)?,
            expected_tail,
            encode_graph_event(event),
        )
        .await
        .map(|position| position.seq_num)
    }
}
