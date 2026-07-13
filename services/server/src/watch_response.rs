use buffa::MessageField;
use henosis_proto::protobuf;
use types::domain::DurableGraphState;
use types::domain::GraphLifecycle;
use types::domain::SliceReport;

pub(crate) fn snapshot(
    sequence: u64,
    state: &DurableGraphState,
) -> protobuf::v1::WatchGraphResponse {
    protobuf::v1::WatchGraphResponse {
        item: protobuf::v1::WatchGraphSnapshot {
            sequence: Some(sequence),
            state: MessageField::some(state.into()),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn change(sequence: u64, state: &DurableGraphState) -> protobuf::v1::WatchGraphResponse {
    protobuf::v1::WatchGraphResponse {
        item: protobuf::v1::WatchGraphChange {
            sequence: Some(sequence),
            state: MessageField::some(state.into()),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn volatile_status(
    sequence: u64,
    reports: &[SliceReport],
) -> protobuf::v1::WatchGraphResponse {
    protobuf::v1::WatchGraphResponse {
        item: protobuf::v1::WatchGraphVolatileStatus {
            delivered_sequence: Some(sequence),
            reports: reports.iter().map(Into::into).collect(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

pub(crate) fn progress(sequence: u64) -> protobuf::v1::WatchGraphResponse {
    protobuf::v1::WatchGraphResponse {
        item: protobuf::v1::WatchGraphProgress {
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
