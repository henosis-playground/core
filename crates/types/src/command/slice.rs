use crate::ConnectorKey;
use crate::GraphId;
use crate::PublicationId;
use crate::RequestId;
use crate::SliceReport;

/// Records one connector's report for a graph slice.
#[derive(Clone, Debug)]
pub struct ReportSlice {
    request_id: RequestId,
    report: SliceReport,
    publication_id: Option<PublicationId>,
}

impl ReportSlice {
    #[must_use]
    pub const fn new(
        request_id: RequestId,
        report: SliceReport,
        publication_id: Option<PublicationId>,
    ) -> Self {
        Self {
            request_id,
            report,
            publication_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }

    #[must_use]
    pub const fn report(&self) -> &SliceReport {
        &self.report
    }

    #[must_use]
    pub const fn publication_id(&self) -> Option<PublicationId> {
        self.publication_id
    }
}

/// Fetches an immutable connector slice by durable sequence.
#[derive(Clone, Debug)]
pub struct FetchSlice {
    graph_id: GraphId,
    connector: ConnectorKey,
    sequence: u64,
}

impl FetchSlice {
    #[must_use]
    pub const fn new(graph_id: GraphId, connector: ConnectorKey, sequence: u64) -> Self {
        Self {
            graph_id,
            connector,
            sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}
