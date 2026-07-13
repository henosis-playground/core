use crate::ConnectorKey;
use crate::GraphUuid;
use crate::PublicationUuid;
use crate::RequestUuid;
use crate::SliceReport;

/// Records one connector's report for a graph slice.
#[derive(Clone, Debug)]
pub struct ReportSlice {
    request_id: RequestUuid,
    report: SliceReport,
    publication_id: Option<PublicationUuid>,
}

impl ReportSlice {
    #[must_use]
    pub const fn new(
        request_id: RequestUuid,
        report: SliceReport,
        publication_id: Option<PublicationUuid>,
    ) -> Self {
        Self {
            request_id,
            report,
            publication_id,
        }
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestUuid {
        self.request_id
    }

    #[must_use]
    pub const fn report(&self) -> &SliceReport {
        &self.report
    }

    #[must_use]
    pub const fn publication_id(&self) -> Option<PublicationUuid> {
        self.publication_id
    }
}

/// Fetches an immutable connector slice by durable sequence.
#[derive(Clone, Debug)]
pub struct FetchSlice {
    graph_id: GraphUuid,
    connector: ConnectorKey,
    sequence: u64,
}

impl FetchSlice {
    #[must_use]
    pub const fn new(graph_id: GraphUuid, connector: ConnectorKey, sequence: u64) -> Self {
        Self {
            graph_id,
            connector,
            sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
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
