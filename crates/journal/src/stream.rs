use std::convert::Infallible;

use faultline::Error as Fault;
use faultline::Never;
use henosis_proto::journal::DecodeStreamError;
use henosis_proto::journal::WireRecord;
use s2_sdk::S2Stream;
use s2_sdk::types::AppendConditionFailed;
use s2_sdk::types::AppendInput;
use s2_sdk::types::AppendRecord;
use s2_sdk::types::AppendRecordBatch;
use s2_sdk::types::ReadFrom;
use s2_sdk::types::ReadInput;
use s2_sdk::types::ReadLimits;
use s2_sdk::types::ReadStart;
use s2_sdk::types::ReadStop;
use s2_sdk::types::S2Error;

use crate::JournalError;

pub(super) async fn read_records(
    stream: &S2Stream,
    tail: u64,
) -> Result<Vec<WireRecord>, Fault<Never, anyhow::Error, anyhow::Error>> {
    let mut records = Vec::new();
    let mut next = 0;
    while next < tail {
        let batch = stream
            .read(
                ReadInput::new()
                    .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(next)))
                    .with_stop(ReadStop::new().with_limits(ReadLimits::new().with_count(1_000)))
                    .with_ignore_command_records(true),
            )
            .await
            .map_err(never_transient)?;
        if batch.records.is_empty() {
            return Err(Fault::Invariant(anyhow::anyhow!(
                "empty S2 batch before advertised tail {tail} at sequence {next}"
            )));
        }
        for record in batch.records {
            next = record.seq_num.saturating_add(1);
            records.push(WireRecord::new(record.seq_num, record.body.to_vec()));
        }
    }
    Ok(records)
}

pub(super) async fn append_record(
    stream: &S2Stream,
    expected_tail: u64,
    body: Vec<u8>,
) -> Result<u64, Fault<JournalError, anyhow::Error, anyhow::Error>> {
    let record = AppendRecord::new(body).map_err(journal_invariant)?;
    let batch = AppendRecordBatch::try_from_iter([record]).map_err(journal_invariant)?;
    match stream
        .append(AppendInput::new(batch).with_match_seq_num(expected_tail))
        .await
    {
        Ok(ack) => Ok(ack.start.seq_num),
        Err(S2Error::AppendConditionFailed(AppendConditionFailed::SeqNumMismatch(
            current_tail,
        ))) => Err(Fault::Domain(JournalError::CasConflict { current_tail })),
        Err(error) => Err(Fault::Transient(anyhow::Error::new(error))),
    }
}

pub(super) async fn named_tail(
    stream: &S2Stream,
) -> Result<u64, Fault<Never, anyhow::Error, anyhow::Error>> {
    match stream.check_tail().await {
        Ok(tail) => Ok(tail.seq_num),
        Err(S2Error::Server(response))
            if response.code.to_ascii_lowercase().contains("not_found")
                || response.message.to_ascii_lowercase().contains("not found") =>
        {
            Ok(0)
        }
        Err(error) => Err(never_transient(error)),
    }
}

pub(super) fn classify_graph_read(
    error: S2Error,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    match &error {
        S2Error::Server(response)
            if response.code.to_ascii_lowercase().contains("not_found")
                || response.message.to_ascii_lowercase().contains("not found") =>
        {
            Fault::Domain(JournalError::NotFound)
        }
        _ => Fault::Transient(anyhow::Error::new(error)),
    }
}

pub(super) fn upcast_read_fault(
    error: Fault<Never, anyhow::Error, anyhow::Error>,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    error.squash()
}

pub(super) fn decode_invariant<T>(
    error: DecodeStreamError<Infallible>,
) -> Fault<T, anyhow::Error, anyhow::Error> {
    match error {
        DecodeStreamError::Source(source) => match source {},
        DecodeStreamError::Invalid { sequence, error } => Fault::Invariant(anyhow::anyhow!(
            "invalid durable record at sequence {sequence}: {error}"
        )),
    }
}

fn never_transient(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Transient(anyhow::Error::new(error))
}

pub(super) fn never_invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<Never, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}

pub(super) fn journal_invariant(
    error: impl std::error::Error + Send + Sync + 'static,
) -> Fault<JournalError, anyhow::Error, anyhow::Error> {
    Fault::Invariant(anyhow::Error::new(error))
}
