use iddqd::IdOrdItem;
use iddqd::id_upcast;

use crate::DurableGraphState;
use crate::Fingerprint;
use crate::Graph;
use crate::GraphUuid;
use crate::MutationKind;
use crate::OutputPublication;
use crate::RecordedSliceReport;
use crate::RequestUuid;

/// Domain event stored in one graph stream.
#[derive(Clone, Debug)]
pub enum GraphEvent {
    Created {
        graph: Graph,
        request_id: RequestUuid,
        request_fingerprint: Fingerprint,
    },
    GenerationAccepted {
        graph: Graph,
        request_id: RequestUuid,
        mutation_kind: MutationKind,
        request_fingerprint: Fingerprint,
    },
    OutputsPublished(OutputPublication),
    SliceReported(RecordedSliceReport),
    Retired {
        graph_id: GraphUuid,
        last_generation: u64,
        request_id: RequestUuid,
        request_fingerprint: Fingerprint,
    },
}

/// Graph discovery event stored in the registry stream.
#[derive(Clone, Copy, Debug)]
pub enum RegistryEvent {
    Created {
        graph_id: GraphUuid,
        request_id: RequestUuid,
    },
    Retired {
        graph_id: GraphUuid,
        request_id: RequestUuid,
    },
}

/// Graph event paired with its S2 sequence.
#[derive(Clone, Debug)]
pub struct SequencedGraphEvent {
    pub(super) sequence: u64,
    pub(super) event: GraphEvent,
}

impl SequencedGraphEvent {
    #[must_use]
    pub const fn new(sequence: u64, event: GraphEvent) -> Self {
        Self { sequence, event }
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn event(&self) -> &GraphEvent {
        &self.event
    }
}

/// Registry event paired with its S2 sequence.
#[derive(Clone, Copy, Debug)]
pub struct SequencedRegistryEvent {
    sequence: u64,
    event: RegistryEvent,
}

impl SequencedRegistryEvent {
    #[must_use]
    pub const fn new(sequence: u64, event: RegistryEvent) -> Self {
        Self { sequence, event }
    }

    #[must_use]
    pub const fn sequence(self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn event(self) -> RegistryEvent {
        self.event
    }
}

/// Durable graph state paired with the event sequence that produced it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SequencedGraphState {
    pub(super) sequence: u64,
    pub(super) state: DurableGraphState,
}

impl SequencedGraphState {
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn state(&self) -> &DurableGraphState {
        &self.state
    }
}

impl IdOrdItem for SequencedGraphState {
    type Key<'a> = u64;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.sequence
    }
}
