use crate::ComponentReplacement;
use crate::ComponentSpecHash;
use crate::GraphId;
use crate::RequestId;

/// Creates generation one of a graph.
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

/// Adds component specs to the next graph generation.
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

/// Replaces component specs in the next graph generation.
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

/// Removes component specs from the next graph generation.
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

/// Retires an active graph at an expected generation.
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

/// Reads the current state of one graph.
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

/// Reads one accepted generation of a graph.
#[derive(Clone, Copy, Debug)]
pub struct GetGraphGeneration {
    graph_id: GraphId,
    generation: u64,
}

impl GetGraphGeneration {
    #[must_use]
    pub const fn new(graph_id: GraphId, generation: u64) -> Self {
        Self {
            graph_id,
            generation,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn generation(self) -> u64 {
        self.generation
    }
}

/// Watches durable and volatile state changes for one graph.
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
