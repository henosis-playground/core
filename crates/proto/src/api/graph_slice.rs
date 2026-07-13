use buffa::MessageField;
use types::domain;

use crate::protobuf;

impl From<&domain::GraphSlice> for protobuf::v1::GraphSlice {
    fn from(value: &domain::GraphSlice) -> Self {
        Self {
            graph_id: Some(value.graph_id().into_bytes().to_vec()),
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            components: value.components().map(Into::into).collect(),
            upstream_outputs: value.upstream_outputs().map(Into::into).collect(),
            sequence: Some(value.sequence()),
            ..Self::default()
        }
    }
}

#[must_use]
pub fn reconcile_slice_request(
    slice: &domain::GraphSlice,
    superseded: &[domain::Component],
) -> protobuf::v1::ReconcileSliceRequest {
    protobuf::v1::ReconcileSliceRequest {
        slice: MessageField::some(slice.into()),
        superseded_components: superseded.iter().map(Into::into).collect(),
        ..Default::default()
    }
}

#[must_use]
pub fn retire_slice_request(slice: &domain::GraphSlice) -> protobuf::v1::RetireSliceRequest {
    protobuf::v1::RetireSliceRequest {
        slice: MessageField::some(slice.into()),
        ..Default::default()
    }
}
