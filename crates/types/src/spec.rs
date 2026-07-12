use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use crate::ComponentSpecHash;
use crate::ConnectorKey;

/// Unvalidated input used to construct a component spec.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewComponentSpec {
    pub name: String,
    pub connector: ConnectorKey,
    pub outputs_schema: Vec<u8>,
    pub depends_on: Vec<ComponentSpecHash>,
    pub connector_context: Vec<u8>,
}

/// Immutable connector input whose identity is assigned at the proto boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentSpec {
    name: String,
    connector: ConnectorKey,
    outputs_schema: Vec<u8>,
    depends_on: Vec<ComponentSpecHash>,
    connector_context: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentSpecError {
    #[error("component name must not be empty")]
    EmptyName,
    #[error("component dependencies must be unique")]
    DuplicateDependency,
}

impl ComponentSpec {
    pub fn new(mut new: NewComponentSpec) -> Result<Self, ComponentSpecError> {
        if new.name.is_empty() {
            return Err(ComponentSpecError::EmptyName);
        }
        let original_len = new.depends_on.len();
        new.depends_on.sort_unstable();
        new.depends_on.dedup();
        if new.depends_on.len() != original_len {
            return Err(ComponentSpecError::DuplicateDependency);
        }
        Ok(Self {
            name: new.name,
            connector: new.connector,
            outputs_schema: new.outputs_schema,
            depends_on: new.depends_on,
            connector_context: new.connector_context,
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub fn outputs_schema(&self) -> &[u8] {
        &self.outputs_schema
    }

    #[must_use]
    pub fn depends_on(&self) -> &[ComponentSpecHash] {
        &self.depends_on
    }

    #[must_use]
    pub fn connector_context(&self) -> &[u8] {
        &self.connector_context
    }
}

/// A registered spec and its stable content identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RegisteredComponentSpec {
    hash: ComponentSpecHash,
    spec: ComponentSpec,
}

impl RegisteredComponentSpec {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(hash: ComponentSpecHash, spec: ComponentSpec) -> Self {
        Self { hash, spec }
    }

    #[must_use]
    pub const fn hash(&self) -> ComponentSpecHash {
        self.hash
    }

    #[must_use]
    pub const fn spec(&self) -> &ComponentSpec {
        &self.spec
    }
}

impl IdOrdItem for RegisteredComponentSpec {
    type Key<'a> = ComponentSpecHash;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.hash
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpecCatalog {
    specs: IdOrdMap<RegisteredComponentSpec>,
}

#[derive(Clone, Debug)]
pub struct SequencedSpecEvent {
    sequence: u64,
    component: RegisteredComponentSpec,
}

impl SequencedSpecEvent {
    #[must_use]
    pub const fn new(sequence: u64, component: RegisteredComponentSpec) -> Self {
        Self {
            sequence,
            component,
        }
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn component(&self) -> &RegisteredComponentSpec {
        &self.component
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpecHistory {
    next_sequence: u64,
    catalog: SpecCatalog,
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SpecHistoryError {
    #[error("spec stream sequence is not contiguous")]
    NonContiguous,
    #[error(transparent)]
    Catalog(#[from] SpecCatalogError),
}

impl SpecHistory {
    /// Fold one already-parsed domain event into this history.
    pub fn apply(&mut self, event: SequencedSpecEvent) -> Result<(), SpecHistoryError> {
        if event.sequence != self.next_sequence {
            return Err(SpecHistoryError::NonContiguous);
        }
        self.catalog.apply(event.component)?;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn catalog(&self) -> &SpecCatalog {
        &self.catalog
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SpecCatalogError {
    #[error("component spec hash is already registered with different content")]
    HashCollision,
    #[error("component spec references an unregistered dependency")]
    MissingDependency,
}

impl SpecCatalog {
    pub fn apply(&mut self, registered: RegisteredComponentSpec) -> Result<(), SpecCatalogError> {
        if registered
            .spec()
            .depends_on()
            .iter()
            .any(|dependency| !self.specs.contains_key(dependency))
        {
            return Err(SpecCatalogError::MissingDependency);
        }
        match self.specs.get(&registered.hash()) {
            Some(stored) if stored.spec() != registered.spec() => {
                Err(SpecCatalogError::HashCollision)
            }
            Some(_) => Ok(()),
            None => {
                self.specs
                    .insert_unique(registered)
                    .expect("absence was checked before insertion");
                Ok(())
            }
        }
    }

    #[must_use]
    pub fn get(&self, hash: ComponentSpecHash) -> Option<&RegisteredComponentSpec> {
        self.specs.get(&hash)
    }

    #[must_use]
    pub fn contains(&self, hash: ComponentSpecHash) -> bool {
        self.specs.contains_key(&hash)
    }

    #[must_use]
    pub fn missing<'a>(
        &'a self,
        hashes: impl IntoIterator<Item = &'a ComponentSpecHash>,
    ) -> Vec<ComponentSpecHash> {
        hashes
            .into_iter()
            .copied()
            .filter(|hash| !self.contains(*hash))
            .collect()
    }

    #[must_use]
    pub fn iter(&self) -> impl Iterator<Item = &RegisteredComponentSpec> {
        self.specs.iter()
    }
}
