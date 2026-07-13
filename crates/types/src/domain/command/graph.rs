use crate::domain::ComponentReplacement;
use crate::domain::ComponentUuid;
use crate::domain::GraphUuid;
use crate::domain::RequestUuid;

/// Creates generation one of a graph.
#[derive(Clone, Debug)]
pub struct CreateGraph {
    graph_id: GraphUuid,
    component_ids: Vec<ComponentUuid>,
    request_id: RequestUuid,
}

impl CreateGraph {
    #[must_use]
    pub const fn new(
        graph_id: GraphUuid,
        component_ids: Vec<ComponentUuid>,
        request_id: RequestUuid,
    ) -> Self {
        Self {
            graph_id,
            component_ids,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub fn component_ids(&self) -> &[ComponentUuid] {
        &self.component_ids
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestUuid {
        self.request_id
    }
}

/// Adds component specs to the next graph generation.
#[derive(Clone, Debug)]
pub struct AddComponents {
    graph_id: GraphUuid,
    expected_generation: u64,
    component_ids: Vec<ComponentUuid>,
    request_id: RequestUuid,
}

impl AddComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphUuid,
        expected_generation: u64,
        component_ids: Vec<ComponentUuid>,
        request_id: RequestUuid,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            component_ids,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub fn component_ids(&self) -> &[ComponentUuid] {
        &self.component_ids
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestUuid {
        self.request_id
    }
}

/// Replaces component specs in the next graph generation.
#[derive(Clone, Debug)]
pub struct UpdateComponents {
    graph_id: GraphUuid,
    expected_generation: u64,
    replacements: Vec<ComponentReplacement>,
    request_id: RequestUuid,
}

impl UpdateComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphUuid,
        expected_generation: u64,
        replacements: Vec<ComponentReplacement>,
        request_id: RequestUuid,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            replacements,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
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
    pub const fn request_id(&self) -> RequestUuid {
        self.request_id
    }
}

/// Removes component specs from the next graph generation.
#[derive(Clone, Debug)]
pub struct RemoveComponents {
    graph_id: GraphUuid,
    expected_generation: u64,
    component_ids: Vec<ComponentUuid>,
    request_id: RequestUuid,
}

impl RemoveComponents {
    #[must_use]
    pub const fn new(
        graph_id: GraphUuid,
        expected_generation: u64,
        component_ids: Vec<ComponentUuid>,
        request_id: RequestUuid,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            component_ids,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(&self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub fn component_ids(&self) -> &[ComponentUuid] {
        &self.component_ids
    }

    #[must_use]
    pub const fn request_id(&self) -> RequestUuid {
        self.request_id
    }
}

/// Retires an active graph at an expected generation.
#[derive(Clone, Copy, Debug)]
pub struct RetireGraph {
    graph_id: GraphUuid,
    expected_generation: u64,
    request_id: RequestUuid,
}

impl RetireGraph {
    #[must_use]
    pub const fn new(
        graph_id: GraphUuid,
        expected_generation: u64,
        request_id: RequestUuid,
    ) -> Self {
        Self {
            graph_id,
            expected_generation,
            request_id,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn expected_generation(self) -> u64 {
        self.expected_generation
    }

    #[must_use]
    pub const fn request_id(self) -> RequestUuid {
        self.request_id
    }
}

/// Reads the current state of one graph.
#[derive(Clone, Copy, Debug)]
pub struct GetGraph {
    graph_id: GraphUuid,
}

impl GetGraph {
    #[must_use]
    pub const fn new(graph_id: GraphUuid) -> Self {
        Self { graph_id }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphUuid {
        self.graph_id
    }
}

/// Reads one accepted generation of a graph.
#[derive(Clone, Copy, Debug)]
pub struct GetGraphGeneration {
    graph_id: GraphUuid,
    generation: u64,
}

impl GetGraphGeneration {
    #[must_use]
    pub const fn new(graph_id: GraphUuid, generation: u64) -> Self {
        Self {
            graph_id,
            generation,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphUuid {
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
    graph_id: GraphUuid,
    after_sequence: Option<u64>,
}

impl WatchGraph {
    #[must_use]
    pub const fn new(graph_id: GraphUuid, after_sequence: Option<u64>) -> Self {
        Self {
            graph_id,
            after_sequence,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> GraphUuid {
        self.graph_id
    }

    #[must_use]
    pub const fn after_sequence(self) -> Option<u64> {
        self.after_sequence
    }
}
