use crate::GraphId;

/// User-facing graph label loaded from the datastore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GraphLabel {
    graph_id: GraphId,
    display_label: String,
}

impl GraphLabel {
    /// Construct a value loaded and validated by the datastore boundary.
    #[doc(hidden)]
    #[must_use]
    pub const fn new(graph_id: GraphId, display_label: String) -> Self {
        Self {
            graph_id,
            display_label,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub fn display_label(&self) -> &str {
        &self.display_label
    }
}

/// Input used to create or replace a graph label.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewGraphLabel {
    pub graph_id: GraphId,
    pub display_label: String,
}
