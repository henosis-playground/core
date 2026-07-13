use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use crate::domain::ComponentUuid;
use crate::domain::GraphUuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// One component-spec identity included in a graph generation.
pub struct GraphComponent {
    component_id: ComponentUuid,
}

impl GraphComponent {
    #[must_use]
    pub const fn new(component_id: ComponentUuid) -> Self {
        Self { component_id }
    }

    #[must_use]
    pub const fn component_id(self) -> ComponentUuid {
        self.component_id
    }
}

impl IdOrdItem for GraphComponent {
    type Key<'a> = ComponentUuid;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
/// Unvalidated input used to construct a graph generation.
pub struct NewGraph {
    pub id: GraphUuid,
    pub generation: u64,
    pub component_ids: Vec<ComponentUuid>,
}

/// A complete accepted world at one graph generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Graph {
    id: GraphUuid,
    generation: u64,
    components: IdOrdMap<GraphComponent>,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
/// Invalid graph-generation input.
pub enum GraphError {
    #[error("graph generation must be greater than zero")]
    InvalidGeneration,
    #[error("a graph may reference a component spec only once")]
    DuplicateComponent,
}

impl Graph {
    pub fn new(new: NewGraph) -> Result<Self, GraphError> {
        if new.generation == 0 {
            return Err(GraphError::InvalidGeneration);
        }
        let mut components = IdOrdMap::with_capacity(new.component_ids.len());
        for hash in new.component_ids {
            components
                .insert_unique(GraphComponent::new(hash))
                .map_err(|_| GraphError::DuplicateComponent)?;
        }
        Ok(Self {
            id: new.id,
            generation: new.generation,
            components,
        })
    }

    #[must_use]
    pub const fn id(&self) -> GraphUuid {
        self.id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub fn components(&self) -> impl ExactSizeIterator<Item = &GraphComponent> {
        self.components.iter()
    }

    #[must_use]
    pub fn component_ids(&self) -> Vec<ComponentUuid> {
        self.components()
            .map(|component| component.component_id())
            .collect()
    }

    #[must_use]
    pub fn contains(&self, hash: ComponentUuid) -> bool {
        self.components.contains_key(&hash)
    }

    pub fn add(&mut self, hashes: &[ComponentUuid]) -> Result<(), GraphEditError> {
        if hashes.is_empty() {
            return Err(GraphEditError::EmptyEdit);
        }
        if hashes.iter().any(|hash| self.contains(*hash)) {
            return Err(GraphEditError::AlreadyPresent);
        }
        let mut candidate = self.components.clone();
        for hash in hashes {
            candidate
                .insert_unique(GraphComponent::new(*hash))
                .map_err(|_| GraphEditError::DuplicateInput)?;
        }
        self.components = candidate;
        Ok(())
    }

    pub fn replace(&mut self, replacements: &[ComponentReplacement]) -> Result<(), GraphEditError> {
        if replacements.is_empty() {
            return Err(GraphEditError::EmptyEdit);
        }
        let mut candidate = self.components.clone();
        for replacement in replacements {
            if candidate.remove(&replacement.current()).is_none() {
                return Err(GraphEditError::NotFound);
            }
        }
        for replacement in replacements {
            candidate
                .insert_unique(GraphComponent::new(replacement.replacement()))
                .map_err(|_| GraphEditError::AlreadyPresent)?;
        }
        self.components = candidate;
        Ok(())
    }

    pub fn remove(&mut self, hashes: &[ComponentUuid]) -> Result<(), GraphEditError> {
        if hashes.is_empty() {
            return Err(GraphEditError::EmptyEdit);
        }
        let mut candidate = self.components.clone();
        for hash in hashes {
            if candidate.remove(hash).is_none() {
                return Err(GraphEditError::NotFound);
            }
        }
        self.components = candidate;
        Ok(())
    }

    pub fn advance_generation(&mut self) {
        self.generation = self.generation.saturating_add(1);
    }
}

impl IdOrdItem for Graph {
    type Key<'a> = u64;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Replacement of one component-spec identity with another.
pub struct ComponentReplacement {
    current: ComponentUuid,
    replacement: ComponentUuid,
}

impl ComponentReplacement {
    #[must_use]
    pub const fn new(current: ComponentUuid, replacement: ComponentUuid) -> Self {
        Self {
            current,
            replacement,
        }
    }

    #[must_use]
    pub const fn current(self) -> ComponentUuid {
        self.current
    }

    #[must_use]
    pub const fn replacement(self) -> ComponentUuid {
        self.replacement
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
/// Invalid edit to an accepted graph generation.
pub enum GraphEditError {
    #[error("an edit must contain at least one component")]
    EmptyEdit,
    #[error("an edit repeats a component identity")]
    DuplicateInput,
    #[error("a component is already present")]
    AlreadyPresent,
    #[error("a component is not present")]
    NotFound,
}
