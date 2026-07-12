use iddqd::IdOrdItem;
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

/// Invalid component-spec input.
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
    /// Construct a value after the boundary has verified its content hash.
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
