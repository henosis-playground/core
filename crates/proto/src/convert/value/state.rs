use buffa::MessageField;
use henosis_types as domain;

use crate::proto::henosis::v1 as pb;

impl From<&domain::PublishedSliceOutputs> for pb::PublishedSliceOutputs {
    fn from(value: &domain::PublishedSliceOutputs) -> Self {
        Self {
            generation: Some(value.generation()),
            connector: Some(value.connector().to_string()),
            outputs: value.outputs().iter().map(Into::into).collect(),
            publication_sequence: Some(value.publication_sequence()),
            publication_id: Some(value.publication_id().into_bytes().to_vec()),
            input_sequence: Some(value.input_sequence()),
            ..Self::default()
        }
    }
}

impl From<&domain::DurableGraphState> for pb::DurableGraphState {
    fn from(value: &domain::DurableGraphState) -> Self {
        let lifecycle = match value.lifecycle() {
            domain::GraphLifecycle::Active => pb::GraphLifecycle::Active,
            domain::GraphLifecycle::Retired => pb::GraphLifecycle::Retired,
        };
        Self {
            graph: MessageField::some(value.graph().into()),
            published_outputs: value.published_outputs().map(Into::into).collect(),
            lifecycle: Some(lifecycle.into()),
            ..Self::default()
        }
    }
}

impl From<&domain::GraphState> for pb::GraphState {
    fn from(value: &domain::GraphState) -> Self {
        Self {
            durable: MessageField::some(value.durable().into()),
            reports: value.reports().map(Into::into).collect(),
            ..Self::default()
        }
    }
}
