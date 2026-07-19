use futures::future::BoxFuture;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ContentDigest;
use crate::ControllerName;
use crate::Generation;
use crate::GraphId;
use crate::NativeValue;
use crate::OutputName;
use crate::PublicationId;
use crate::Resource;
use crate::ResourceId;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControllerSlice {
    graph_id: GraphId,
    generation: Generation,
    plan_digest: ContentDigest,
    controller: ControllerName,
    resources: Vec<Resource>,
    superseded: Vec<Resource>,
}

impl ControllerSlice {
    #[must_use]
    pub fn new(
        graph_id: GraphId,
        generation: Generation,
        plan_digest: ContentDigest,
        controller: ControllerName,
        mut resources: Vec<Resource>,
        mut superseded: Vec<Resource>,
    ) -> Self {
        resources.sort_by_key(Resource::id);
        superseded.sort_by_key(Resource::id);
        superseded.dedup_by_key(|resource| resource.id());
        Self {
            graph_id,
            generation,
            plan_digest,
            controller,
            resources,
            superseded,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn plan_digest(&self) -> ContentDigest {
        self.plan_digest
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }

    #[must_use]
    pub fn superseded(&self) -> &[Resource] {
        &self.superseded
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Supersession {
    pub graph_id: GraphId,
    pub generation: Generation,
    pub controller: ControllerName,
    pub resources: Vec<Resource>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Retirement {
    pub graph_id: GraphId,
    pub last_generation: Generation,
    pub controller: ControllerName,
    pub resources: Vec<Resource>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ControllerCommand {
    Reconcile(ControllerSlice),
    Supersede(Supersession),
    Retire(Retirement),
}

impl ControllerCommand {
    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        match self {
            Self::Reconcile(slice) => slice.graph_id(),
            Self::Supersede(supersession) => supersession.graph_id,
            Self::Retire(retirement) => retirement.graph_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ResourceDispositionKind {
    Ready,
    Reconciling { message: String },
    Failed { message: String },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ResourceDisposition {
    resource_id: ResourceId,
    kind: ResourceDispositionKind,
}

impl ResourceDisposition {
    #[must_use]
    pub const fn new(resource_id: ResourceId, kind: ResourceDispositionKind) -> Self {
        Self { resource_id, kind }
    }

    #[must_use]
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    #[must_use]
    pub const fn kind(&self) -> &ResourceDispositionKind {
        &self.kind
    }
}

impl IdOrdItem for ResourceDisposition {
    type Key<'a> = ResourceId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.resource_id
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ObservedOutputKey {
    resource_id: ResourceId,
    output: OutputName,
}

impl ObservedOutputKey {
    #[must_use]
    pub const fn new(resource_id: ResourceId, output: OutputName) -> Self {
        Self {
            resource_id,
            output,
        }
    }

    #[must_use]
    pub const fn resource_id(&self) -> ResourceId {
        self.resource_id
    }

    #[must_use]
    pub const fn output(&self) -> &OutputName {
        &self.output
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedOutput {
    key: ObservedOutputKey,
    value: NativeValue,
}

impl ObservedOutput {
    #[must_use]
    pub const fn new(key: ObservedOutputKey, value: NativeValue) -> Self {
        Self { key, value }
    }

    #[must_use]
    pub const fn key_value(&self) -> &ObservedOutputKey {
        &self.key
    }

    #[must_use]
    pub const fn value(&self) -> &NativeValue {
        &self.value
    }
}

impl IdOrdItem for ObservedOutput {
    type Key<'a> = &'a ObservedOutputKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.key
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewControllerReport {
    pub graph_id: GraphId,
    pub generation: Generation,
    pub plan_digest: ContentDigest,
    pub controller: ControllerName,
    pub publication_id: Option<PublicationId>,
    pub dispositions: Vec<ResourceDisposition>,
    pub outputs: Vec<ObservedOutput>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControllerReport {
    graph_id: GraphId,
    generation: Generation,
    plan_digest: ContentDigest,
    controller: ControllerName,
    publication_id: Option<PublicationId>,
    dispositions: IdOrdMap<ResourceDisposition>,
    outputs: IdOrdMap<ObservedOutput>,
}

impl ControllerReport {
    pub fn new(new: NewControllerReport) -> Result<Self, ControllerReportError> {
        if new.publication_id.is_none() && !new.outputs.is_empty() {
            return Err(ControllerReportError::PublicationIdRequired);
        }
        let mut dispositions = IdOrdMap::with_capacity(new.dispositions.len());
        for disposition in new.dispositions {
            dispositions
                .insert_unique(disposition)
                .map_err(|_| ControllerReportError::DuplicateDisposition)?;
        }
        let mut outputs = IdOrdMap::with_capacity(new.outputs.len());
        for output in new.outputs {
            if !dispositions.contains_key(&output.key_value().resource_id()) {
                return Err(ControllerReportError::OutputWithoutDisposition);
            }
            outputs
                .insert_unique(output)
                .map_err(|_| ControllerReportError::DuplicateOutput)?;
        }
        Ok(Self {
            graph_id: new.graph_id,
            generation: new.generation,
            plan_digest: new.plan_digest,
            controller: new.controller,
            publication_id: new.publication_id,
            dispositions,
            outputs,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn plan_digest(&self) -> ContentDigest {
        self.plan_digest
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub const fn publication_id(&self) -> Option<PublicationId> {
        self.publication_id
    }

    pub fn dispositions(&self) -> impl ExactSizeIterator<Item = &ResourceDisposition> {
        self.dispositions.iter()
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &ObservedOutput> {
        self.outputs.iter()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ControllerReportError {
    #[error("a publication identity is required when outputs are present")]
    PublicationIdRequired,
    #[error("a slice report contains more than one disposition for a resource")]
    DuplicateDisposition,
    #[error("a slice report contains more than one value for an observed output")]
    DuplicateOutput,
    #[error("an observed output must belong to a resource disposition in the same atomic report")]
    OutputWithoutDisposition,
}

#[derive(Debug, Error)]
#[error("controller operation failed: {message}")]
pub struct ControllerError {
    message: String,
}

impl ControllerError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerPass {
    Acted,
    Converged(Option<ControllerReport>),
    Failed(ControllerReport),
}

pub trait Controller: Send + Sync {
    fn name(&self) -> &ControllerName;

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>>;
}
