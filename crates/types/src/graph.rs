use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ComponentName;
use crate::ContentDigest;
use crate::Generation;
use crate::GraphId;
use crate::GraphName;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BundleRef(ContentDigest);

impl BundleRef {
    #[must_use]
    pub const fn new(digest: ContentDigest) -> Self {
        Self(digest)
    }

    #[must_use]
    pub const fn digest(self) -> ContentDigest {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentIntent {
    name: ComponentName,
    bundle: BundleRef,
}

impl ComponentIntent {
    #[must_use]
    pub const fn new(name: ComponentName, bundle: BundleRef) -> Self {
        Self { name, bundle }
    }

    #[must_use]
    pub const fn name(&self) -> &ComponentName {
        &self.name
    }

    #[must_use]
    pub const fn bundle(&self) -> BundleRef {
        self.bundle
    }
}

impl IdOrdItem for ComponentIntent {
    type Key<'a> = &'a ComponentName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewGraphIntent {
    pub id: GraphId,
    pub name: GraphName,
    pub components: Vec<ComponentIntent>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GraphIntent {
    id: GraphId,
    name: GraphName,
    generation: Generation,
    components: IdOrdMap<ComponentIntent>,
}

impl GraphIntent {
    pub fn new(new: NewGraphIntent) -> Result<Self, GraphIntentError> {
        Self::from_parts(
            new.id,
            new.name,
            Generation::new(1).expect("one is a valid generation"),
            new.components,
        )
    }

    pub fn replace_components(
        &self,
        components: Vec<ComponentIntent>,
    ) -> Result<Self, GraphIntentError> {
        Self::from_parts(
            self.id,
            self.name.clone(),
            self.generation.next(),
            components,
        )
    }

    fn from_parts(
        id: GraphId,
        name: GraphName,
        generation: Generation,
        components: Vec<ComponentIntent>,
    ) -> Result<Self, GraphIntentError> {
        if components.is_empty() {
            return Err(GraphIntentError::Empty);
        }
        let mut keyed = IdOrdMap::with_capacity(components.len());
        for component in components {
            keyed
                .insert_unique(component)
                .map_err(|_| GraphIntentError::DuplicateComponent)?;
        }
        Ok(Self {
            id,
            name,
            generation,
            components: keyed,
        })
    }

    #[must_use]
    pub const fn id(&self) -> GraphId {
        self.id
    }

    #[must_use]
    pub const fn name(&self) -> &GraphName {
        &self.name
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    pub fn components(&self) -> impl ExactSizeIterator<Item = &ComponentIntent> {
        self.components.iter()
    }

    #[must_use]
    pub fn component(&self, name: &ComponentName) -> Option<&ComponentIntent> {
        self.components.get(name)
    }
}

impl IdOrdItem for GraphIntent {
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.id
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GraphIntentError {
    #[error("graph intent must contain at least one component")]
    Empty,
    #[error("graph intent contains a component name more than once")]
    DuplicateComponent,
}
