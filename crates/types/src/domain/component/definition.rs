use std::time::SystemTime;

use iddqd::IdOrdItem;
use iddqd::id_upcast;
use thiserror::Error;

use crate::domain::ComponentUuid;
use crate::domain::ConnectorKey;

/// Unaccepted component specification input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewComponentSpec {
    name: String,
    connector: ConnectorKey,
    outputs_schema: Vec<u8>,
    depends_on: Vec<ComponentUuid>,
    connector_context: Vec<u8>,
}

/// Invalid component-specification input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentSpecError {
    #[error("component name must not be empty")]
    EmptyName,
    #[error("component dependencies must be unique")]
    DuplicateDependency,
}

impl NewComponentSpec {
    pub fn new(
        name: &str,
        connector: ConnectorKey,
        outputs_schema: &[u8],
        mut depends_on: Vec<ComponentUuid>,
        connector_context: &[u8],
    ) -> Result<Self, ComponentSpecError> {
        if name.is_empty() {
            return Err(ComponentSpecError::EmptyName);
        }
        let original_len = depends_on.len();
        depends_on.sort_unstable();
        depends_on.dedup();
        if depends_on.len() != original_len {
            return Err(ComponentSpecError::DuplicateDependency);
        }
        Ok(Self {
            name: name.to_owned(),
            connector,
            outputs_schema: outputs_schema.to_owned(),
            depends_on,
            connector_context: connector_context.to_owned(),
        })
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub fn outputs_schema(&self) -> &[u8] {
        &self.outputs_schema
    }

    #[must_use]
    pub fn depends_on(&self) -> &[ComponentUuid] {
        &self.depends_on
    }

    #[must_use]
    pub fn connector_context(&self) -> &[u8] {
        &self.connector_context
    }
}

/// An accepted component specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentSpec {
    name: String,
    connector: ConnectorKey,
    outputs_schema: Vec<u8>,
    depends_on: Vec<ComponentUuid>,
    connector_context: Vec<u8>,
}

impl ComponentSpec {
    fn from_new(spec: NewComponentSpec) -> Self {
        Self {
            name: spec.name,
            connector: spec.connector,
            outputs_schema: spec.outputs_schema,
            depends_on: spec.depends_on,
            connector_context: spec.connector_context,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    #[must_use]
    pub fn outputs_schema(&self) -> &[u8] {
        &self.outputs_schema
    }

    #[must_use]
    pub fn depends_on(&self) -> &[ComponentUuid] {
        &self.depends_on
    }

    #[must_use]
    pub fn connector_context(&self) -> &[u8] {
        &self.connector_context
    }
}

impl PartialEq<ComponentSpec> for NewComponentSpec {
    fn eq(&self, other: &ComponentSpec) -> bool {
        self.name == other.name
            && self.connector == other.connector
            && self.outputs_schema == other.outputs_schema
            && self.depends_on == other.depends_on
            && self.connector_context == other.connector_context
    }
}

/// A component accepted at one generation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Component {
    id: ComponentUuid,
    generation: u64,
    time_created: SystemTime,
    time_modified: SystemTime,
    spec: ComponentSpec,
}

impl Component {
    // See the domain construction policy in domain/mod.rs.
    #[doc(hidden)]
    #[must_use]
    pub fn new(
        id: ComponentUuid,
        generation: u64,
        time_created: SystemTime,
        time_modified: SystemTime,
        spec: NewComponentSpec,
    ) -> Self {
        Self {
            id,
            generation,
            time_created,
            time_modified,
            spec: ComponentSpec::from_new(spec),
        }
    }

    #[must_use]
    pub const fn id(&self) -> ComponentUuid {
        self.id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn time_created(&self) -> SystemTime {
        self.time_created
    }

    #[must_use]
    pub const fn time_modified(&self) -> SystemTime {
        self.time_modified
    }

    #[must_use]
    pub const fn spec(&self) -> &ComponentSpec {
        &self.spec
    }
}

impl IdOrdItem for Component {
    type Key<'a> = ComponentUuid;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.id
    }
}
