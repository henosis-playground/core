use crate::ComponentReplacement;
use crate::ComponentSpecHash;
use crate::ConnectorKey;
use crate::GraphId;
use crate::PublicationId;
use crate::RegisteredComponentSpec;
use crate::RequestId;
use crate::SliceReport;

#[derive(Clone, Debug)]
pub struct RegisterComponentSpec {
    component: RegisteredComponentSpec,
}

impl RegisterComponentSpec {
    #[must_use]
    pub const fn new(component: RegisteredComponentSpec) -> Self {
        Self { component }
    }

    #[must_use]
    pub const fn component(&self) -> &RegisteredComponentSpec {
        &self.component
    }

    #[must_use]
    pub fn into_component(self) -> RegisteredComponentSpec {
        self.component
    }
}

#[derive(Clone, Debug)]
pub struct CreateGraph {
    graph_id: GraphId,
    component_spec_hashes: Vec<ComponentSpecHash>,
    request_id: RequestId,
}

impl CreateGraph {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        component_spec_hashes: Vec<ComponentSpecHash>,
        request_id: RequestId,
    ) -> Self {
        Self {
            graph_id,
            component_spec_hashes,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub fn component_spec_hashes(&self) -> &[ComponentSpecHash] {
        &self.component_spec_hashes
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[derive(Clone, Debug)]
pub struct AddComponents {
    graph_id: GraphId,
    expected_generation: u64,
    component_spec_hashes: Vec<ComponentSpecHash>,
    request_id: RequestId,
}

impl AddComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        expected_generation: u64,
        component_spec_hashes: Vec<ComponentSpecHash>,
        request_id: RequestId,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            component_spec_hashes,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub fn component_spec_hashes(&self) -> &[ComponentSpecHash] {
        &self.component_spec_hashes
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[derive(Clone, Debug)]
pub struct UpdateComponents {
    graph_id: GraphId,
    expected_generation: u64,
    replacements: Vec<ComponentReplacement>,
    request_id: RequestId,
}

impl UpdateComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        expected_generation: u64,
        replacements: Vec<ComponentReplacement>,
        request_id: RequestId,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            replacements,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub fn replacements(&self) -> &[ComponentReplacement] {
        &self.replacements
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[derive(Clone, Debug)]
pub struct RemoveComponents {
    graph_id: GraphId,
    expected_generation: u64,
    component_spec_hashes: Vec<ComponentSpecHash>,
    request_id: RequestId,
}

impl RemoveComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        expected_generation: u64,
        component_spec_hashes: Vec<ComponentSpecHash>,
        request_id: RequestId,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            component_spec_hashes,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub fn component_spec_hashes(&self) -> &[ComponentSpecHash] {
        &self.component_spec_hashes
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestId {
        self.request_id
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RetireGraph {
    graph_id: GraphId,
    expected_generation: u64,
    request_id: RequestId,
}

impl RetireGraph {
    #[must_use]
    pub const fn new(graph_id: GraphId, expected_generation: u64, request_id: RequestId) -> Self {
        Self {
            graph_id,
            expected_generation,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub const fn request_id(self) -> RequestId {
        self.request_id
    }
}

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

#[derive(Clone, Copy, Debug)]
pub struct GetGraph {
    graph_id: GraphId,
}

impl GetGraph {
    #[must_use]
    pub const fn new(graph_id: GraphId) -> Self {
        Self { graph_id }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphId {
        self.graph_id
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WatchGraph {
    graph_id: GraphId,
    after_sequence: Option<u64>,
}

impl WatchGraph {
    #[must_use]
    pub const fn new(graph_id: GraphId, after_sequence: Option<u64>) -> Self {
        Self {
            graph_id,
            after_sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn after_sequence(self) -> Option<u64> {
        self.after_sequence
    }
}

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
