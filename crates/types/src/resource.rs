use std::fmt;
use std::num::NonZeroU32;

use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ComponentName;
use crate::ContentDigest;
use crate::ControllerName;
use crate::KindName;
use crate::NativeValue;
use crate::OutputName;
use crate::ResourceId;
use crate::ResourceName;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KindVersion {
    name: KindName,
    version: NonZeroU32,
}

impl KindVersion {
    #[must_use]
    pub const fn new(name: KindName, version: NonZeroU32) -> Self {
        Self { name, version }
    }

    #[must_use]
    pub const fn name(&self) -> &KindName {
        &self.name
    }

    #[must_use]
    pub const fn version(&self) -> NonZeroU32 {
        self.version
    }
}

impl fmt::Display for KindVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}@{}", self.name, self.version)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourcePath {
    instance: ComponentName,
    kind: KindName,
    name: ResourceName,
}

impl ResourcePath {
    #[must_use]
    pub const fn new(instance: ComponentName, kind: KindName, name: ResourceName) -> Self {
        Self {
            instance,
            kind,
            name,
        }
    }

    #[must_use]
    pub const fn instance(&self) -> &ComponentName {
        &self.instance
    }

    #[must_use]
    pub const fn kind(&self) -> &KindName {
        &self.kind
    }

    #[must_use]
    pub const fn name(&self) -> &ResourceName {
        &self.name
    }
}

impl fmt::Display for ResourcePath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}/{}", self.instance, self.kind, self.name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutputMode {
    Static(NativeValue),
    Observed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutputDeclaration {
    name: OutputName,
    mode: OutputMode,
}

impl OutputDeclaration {
    #[must_use]
    pub const fn new(name: OutputName, mode: OutputMode) -> Self {
        Self { name, mode }
    }

    #[must_use]
    pub const fn name(&self) -> &OutputName {
        &self.name
    }

    #[must_use]
    pub const fn mode(&self) -> &OutputMode {
        &self.mode
    }
}

impl IdOrdItem for OutputDeclaration {
    type Key<'a> = &'a OutputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewResource {
    pub id: ResourceId,
    pub path: ResourcePath,
    pub controller: ControllerName,
    pub kind: KindVersion,
    pub body: NativeValue,
    pub outputs: Vec<OutputDeclaration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    id: ResourceId,
    path: ResourcePath,
    controller: ControllerName,
    kind: KindVersion,
    body: NativeValue,
    outputs: IdOrdMap<OutputDeclaration>,
}

impl Resource {
    pub fn new(new: NewResource) -> Result<Self, ResourceError> {
        if new.path.kind() != new.kind.name() {
            return Err(ResourceError::PathKindMismatch);
        }
        let mut outputs = IdOrdMap::with_capacity(new.outputs.len());
        for output in new.outputs {
            outputs
                .insert_unique(output)
                .map_err(|_| ResourceError::DuplicateOutput)?;
        }
        Ok(Self {
            id: new.id,
            path: new.path,
            controller: new.controller,
            kind: new.kind,
            body: new.body,
            outputs,
        })
    }

    #[must_use]
    pub const fn id(&self) -> ResourceId {
        self.id
    }

    #[must_use]
    pub const fn path(&self) -> &ResourcePath {
        &self.path
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub const fn kind(&self) -> &KindVersion {
        &self.kind
    }

    #[must_use]
    pub const fn body(&self) -> &NativeValue {
        &self.body
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &OutputDeclaration> {
        self.outputs.iter()
    }

    #[must_use]
    pub fn output(&self, name: &OutputName) -> Option<&OutputDeclaration> {
        self.outputs.get(name)
    }

    #[must_use]
    pub fn digest(&self) -> ContentDigest {
        let bytes = serde_json::to_vec(self).expect("validated resource serializes");
        ContentDigest::digest(&bytes)
    }
}

impl IdOrdItem for Resource {
    type Key<'a> = ResourceId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.id
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ResourceError {
    #[error("resource path kind must match kind@version")]
    PathKindMismatch,
    #[error("resource declares an output more than once")]
    DuplicateOutput,
}
