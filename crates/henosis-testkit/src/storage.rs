use std::collections::BTreeMap;
use std::collections::VecDeque;

use henosis_storage::AppendAck;
use henosis_storage::AppendRecord;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use thiserror::Error;

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

#[derive(Clone, Debug, Error, Eq, PartialEq)]
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
    streams: BTreeMap<StreamName, Vec<StoredRecord>>,
    faults: VecDeque<AppendFault>,
    clock: u64,
}

impl MemS2 {
    pub fn script(&mut self, faults: impl IntoIterator<Item = AppendFault>) {
        self.faults.extend(faults);
    }

    pub fn append(
        &mut self,
        stream: &StreamName,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<MemS2Append, MemS2Error> {
        let actual = self.tail(stream).sequence();
        if expected.sequence() != actual {
            return Err(MemS2Error::CasConflict {
                expected: expected.sequence(),
                actual,
            });
        }
        let fault = self.faults.pop_front().unwrap_or(AppendFault::Acknowledge);
        if fault == AppendFault::RejectBeforeCommit {
            return Err(MemS2Error::Rejected);
        }
        let start = actual;
        for record in records {
            let sequence = self.tail(stream).sequence();
            self.streams
                .entry(stream.clone())
                .or_default()
                .push(StoredRecord::new(
                    stream.clone(),
                    sequence,
                    self.clock,
                    record.body().to_vec(),
                ));
            self.clock = self.clock.saturating_add(1);
        }
        let acknowledgement = AppendAck::new(StreamPosition::new(start), self.tail(stream));
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
        self.streams
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
        StreamPosition::new(self.streams.get(stream).map(Vec::len).unwrap_or_default() as u64)
    }
}
