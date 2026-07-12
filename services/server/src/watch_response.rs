use buffa::MessageField;
use henosis_proto::proto::henosis::v1 as pb;
use henosis_types::DurableGraphState;
use henosis_types::GraphLifecycle;
use henosis_types::SliceReport;

pub(crate) fn snapshot(sequence: u64, state: &DurableGraphState) -> pb::WatchGraphResponse {
    pb::WatchGraphResponse {
        item: pb::WatchGraphSnapshot {
            sequence: Some(sequence),
            state: MessageField::some(state.into()),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn change(sequence: u64, state: &DurableGraphState) -> pb::WatchGraphResponse {
    pb::WatchGraphResponse {
        item: pb::WatchGraphChange {
            sequence: Some(sequence),
            state: MessageField::some(state.into()),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn volatile_status(sequence: u64, reports: &[SliceReport]) -> pb::WatchGraphResponse {
    pb::WatchGraphResponse {
        item: pb::WatchGraphVolatileStatus {
            delivered_sequence: Some(sequence),
            reports: reports.iter().map(Into::into).collect(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn progress(sequence: u64) -> pb::WatchGraphResponse {
    pb::WatchGraphResponse {
        item: pb::WatchGraphProgress {
            delivered_sequence: Some(sequence),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn is_retired(state: &DurableGraphState) -> bool {
    state.lifecycle() == GraphLifecycle::Retired
}
