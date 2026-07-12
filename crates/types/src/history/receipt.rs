use iddqd::IdHashItem;
use iddqd::id_upcast;

use crate::ConnectorKey;
use crate::Fingerprint;
use crate::Graph;
use crate::GraphId;
use crate::PublicationId;
use crate::RequestId;
use crate::SliceReport;

/// Graph mutation represented by a request receipt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MutationKind {
    Create,
    AddComponents,
    UpdateComponents,
    RemoveComponents,
    Retire,
}

/// Durable response replayed for an idempotent graph mutation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MutationResponse {
    Graph(Graph),
    Retired {
        graph_id: GraphId,
        last_generation: u64,
    },
}

/// Durable graph-mutation request identity and response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationReceipt {
    pub(super) request_id: RequestId,
    pub(super) kind: MutationKind,
    pub(super) fingerprint: Fingerprint,
    pub(super) response: MutationResponse,
}

impl MutationReceipt {
    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    #[must_use]
    pub const fn kind(&self) -> MutationKind {
        self.kind
    }

    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn response(&self) -> &MutationResponse {
        &self.response
    }
}

impl IdHashItem for MutationReceipt {
    type Key<'a> = RequestId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.request_id
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct OutputRequestKey<'a> {
    connector: &'a ConnectorKey,
    request_id: RequestId,
}

impl<'a> OutputRequestKey<'a> {
    pub const fn new(connector: &'a ConnectorKey, request_id: RequestId) -> Self {
        Self {
            connector,
            request_id,
        }
    }
}

/// Durable idempotency receipt for one connector report request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputRequestReceipt {
    pub(super) connector: ConnectorKey,
    pub(super) request_id: RequestId,
    pub(super) fingerprint: Fingerprint,
    pub(super) publication_sequence: Option<u64>,
}

impl OutputRequestReceipt {
    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn publication_sequence(&self) -> Option<u64> {
        self.publication_sequence
    }
}

impl IdHashItem for OutputRequestReceipt {
    type Key<'a> = OutputRequestKey<'a>;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        OutputRequestKey::new(&self.connector, self.request_id)
    }
}

/// Slice report plus the identities needed for durable replay.
#[derive(Clone, Debug)]
pub struct RecordedSliceReport {
    pub report: SliceReport,
    pub request_id: RequestId,
    pub request_fingerprint: Fingerprint,
    pub publication_id: Option<PublicationId>,
    pub publication_fingerprint: Option<Fingerprint>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PublicationKey<'a> {
    connector: &'a ConnectorKey,
    publication_id: PublicationId,
}

impl<'a> PublicationKey<'a> {
    pub const fn new(connector: &'a ConnectorKey, publication_id: PublicationId) -> Self {
        Self {
            connector,
            publication_id,
        }
    }
}

/// Durable idempotency receipt for one connector publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationReceipt {
    pub(super) connector: ConnectorKey,
    pub(super) publication_id: PublicationId,
    pub(super) fingerprint: Fingerprint,
    pub(super) publication_sequence: u64,
}

impl PublicationReceipt {
    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub const fn fingerprint(&self) -> Fingerprint {
        self.fingerprint
    }

    #[must_use]
    pub const fn publication_sequence(&self) -> u64 {
        self.publication_sequence
    }
}

impl IdHashItem for PublicationReceipt {
    type Key<'a> = PublicationKey<'a>;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        PublicationKey::new(&self.connector, self.publication_id)
    }
}
