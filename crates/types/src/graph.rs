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
use crate::InputName;
use crate::OutputAvailability;
use crate::OutputName;
use crate::OutputRef;

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
pub struct ComponentInput {
    name: InputName,
    source: OutputRef,
    optional: bool,
}

impl ComponentInput {
    #[must_use]
    pub const fn new(name: InputName, source: OutputRef, optional: bool) -> Self {
        Self {
            name,
            source,
            optional,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &InputName {
        &self.name
    }

    #[must_use]
    pub const fn source(&self) -> &OutputRef {
        &self.source
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        self.optional
    }
}

impl IdOrdItem for ComponentInput {
    type Key<'a> = &'a InputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentOutput {
    name: OutputName,
    availability: OutputAvailability,
    optional: bool,
}

impl ComponentOutput {
    #[must_use]
    pub const fn new(name: OutputName, availability: OutputAvailability, optional: bool) -> Self {
        Self {
            name,
            availability,
            optional,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &OutputName {
        &self.name
    }

    #[must_use]
    pub const fn availability(&self) -> OutputAvailability {
        self.availability
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        self.optional
    }
}

impl IdOrdItem for ComponentOutput {
    type Key<'a> = &'a OutputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewComponentIntent {
    pub name: ComponentName,
    pub bundle: BundleRef,
    pub inputs: Vec<ComponentInput>,
    pub outputs: Vec<ComponentOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentIntent {
    name: ComponentName,
    bundle: BundleRef,
    inputs: IdOrdMap<ComponentInput>,
    outputs: IdOrdMap<ComponentOutput>,
}

impl ComponentIntent {
    pub fn new(new: NewComponentIntent) -> Result<Self, ComponentIntentError> {
        let mut inputs = IdOrdMap::with_capacity(new.inputs.len());
        for input in new.inputs {
            inputs
                .insert_unique(input)
                .map_err(|_| ComponentIntentError::DuplicateInput)?;
        }
        let mut outputs = IdOrdMap::with_capacity(new.outputs.len());
        for output in new.outputs {
            outputs
                .insert_unique(output)
                .map_err(|_| ComponentIntentError::DuplicateOutput)?;
        }
        Ok(Self {
            name: new.name,
            bundle: new.bundle,
            inputs,
            outputs,
        })
    }

    #[must_use]
    pub const fn name(&self) -> &ComponentName {
        &self.name
    }

    #[must_use]
    pub const fn bundle(&self) -> BundleRef {
        self.bundle
    }

    pub fn inputs(&self) -> impl ExactSizeIterator<Item = &ComponentInput> {
        self.inputs.iter()
    }

    #[must_use]
    pub fn input(&self, name: &InputName) -> Option<&ComponentInput> {
        self.inputs.get(name)
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &ComponentOutput> {
        self.outputs.iter()
    }

    #[must_use]
    pub fn output(&self, name: &OutputName) -> Option<&ComponentOutput> {
        self.outputs.get(name)
    }
}

impl IdOrdItem for ComponentIntent {
    type Key<'a> = &'a ComponentName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentIntentError {
    #[error("component declares an input name more than once")]
    DuplicateInput,
    #[error("component declares an output name more than once")]
    DuplicateOutput,
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
        for component in &keyed {
            for input in component.inputs() {
                let producer = keyed
                    .get(input.source().component())
                    .ok_or(GraphIntentError::UnknownInputComponent)?;
                if producer.output(input.source().output()).is_none() {
                    return Err(GraphIntentError::UnknownInputOutput);
                }
            }
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
    #[error("component input refers to an unknown producer component")]
    UnknownInputComponent,
    #[error("component input refers to an unknown producer output")]
    UnknownInputOutput,
}
