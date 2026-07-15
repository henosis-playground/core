use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ComponentName;
use crate::ContentDigest;
use crate::Generation;
use crate::NativeValue;
use crate::OutputName;
use crate::Resource;
use crate::ResourceId;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OutputRef {
    component: ComponentName,
    output: OutputName,
}

impl OutputRef {
    #[must_use]
    pub const fn new(component: ComponentName, output: OutputName) -> Self {
        Self { component, output }
    }

    #[must_use]
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub const fn output(&self) -> &OutputName {
        &self.output
    }
}

impl std::fmt::Display for OutputRef {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}.outputs.{}", self.component, self.output)
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct OutputKey {
    generation: Generation,
    reference: OutputRef,
}

impl OutputKey {
    #[must_use]
    pub const fn new(generation: Generation, reference: OutputRef) -> Self {
        Self {
            generation,
            reference,
        }
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn reference(&self) -> &OutputRef {
        &self.reference
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum OutputSource {
    Static,
    Observed {
        resource_id: ResourceId,
        resource_output: OutputName,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutputRecord {
    key: OutputKey,
    value: NativeValue,
    source: OutputSource,
}

impl OutputRecord {
    #[must_use]
    pub const fn new(key: OutputKey, value: NativeValue, source: OutputSource) -> Self {
        Self { key, value, source }
    }

    #[must_use]
    pub const fn key_value(&self) -> &OutputKey {
        &self.key
    }

    #[must_use]
    pub const fn value(&self) -> &NativeValue {
        &self.value
    }

    #[must_use]
    pub const fn source(&self) -> &OutputSource {
        &self.source
    }
}

impl IdOrdItem for OutputRecord {
    type Key<'a> = &'a OutputKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.key
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockedMarker {
    component: ComponentName,
    blocked_on: Vec<OutputRef>,
}

impl BlockedMarker {
    pub fn new(
        component: ComponentName,
        mut blocked_on: Vec<OutputRef>,
    ) -> Result<Self, BlockedMarkerError> {
        blocked_on.sort();
        blocked_on.dedup();
        if blocked_on.is_empty() {
            return Err(BlockedMarkerError);
        }
        Ok(Self {
            component,
            blocked_on,
        })
    }

    #[must_use]
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub fn blocked_on(&self) -> &[OutputRef] {
        &self.blocked_on
    }
}

impl IdOrdItem for BlockedMarker {
    type Key<'a> = &'a ComponentName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.component
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("a blocked component must name at least one unavailable input")]
pub struct BlockedMarkerError;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewPlan {
    pub generation: Generation,
    pub resources: Vec<Resource>,
    pub blocked: Vec<BlockedMarker>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    generation: Generation,
    digest: ContentDigest,
    resources: IdOrdMap<Resource>,
    blocked: IdOrdMap<BlockedMarker>,
}

impl Plan {
    pub fn new(new: NewPlan) -> Result<Self, PlanError> {
        let mut resources = IdOrdMap::with_capacity(new.resources.len());
        for resource in new.resources {
            resources
                .insert_unique(resource)
                .map_err(|_| PlanError::DuplicateResource)?;
        }
        let mut blocked = IdOrdMap::with_capacity(new.blocked.len());
        for marker in new.blocked {
            blocked
                .insert_unique(marker)
                .map_err(|_| PlanError::DuplicateBlockedComponent)?;
        }
        let encoded = serde_json::to_vec(&(new.generation, &resources, &blocked))
            .expect("validated plan contents serialize");
        Ok(Self {
            generation: new.generation,
            digest: ContentDigest::digest(&encoded),
            resources,
            blocked,
        })
    }

    #[must_use]
    pub const fn generation(&self) -> Generation {
        self.generation
    }

    #[must_use]
    pub const fn digest(&self) -> ContentDigest {
        self.digest
    }

    pub fn resources(&self) -> impl ExactSizeIterator<Item = &Resource> {
        self.resources.iter()
    }

    #[must_use]
    pub fn resource(&self, id: ResourceId) -> Option<&Resource> {
        self.resources.get(&id)
    }

    pub fn blocked(&self) -> impl ExactSizeIterator<Item = &BlockedMarker> {
        self.blocked.iter()
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.blocked.is_empty()
    }

    #[must_use]
    pub fn diff(&self, previous: Option<&Self>) -> PlanDiff {
        let mut added_or_changed = Vec::new();
        let mut removed = Vec::new();
        for resource in self.resources() {
            if previous.and_then(|plan| plan.resource(resource.id())) != Some(resource) {
                added_or_changed.push(resource.clone());
            }
        }
        if let Some(previous) = previous {
            for resource in previous.resources() {
                if self.resource(resource.id()).is_none() {
                    removed.push(resource.id());
                }
            }
        }
        PlanDiff {
            added_or_changed,
            removed,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanDiff {
    added_or_changed: Vec<Resource>,
    removed: Vec<ResourceId>,
}

impl PlanDiff {
    #[must_use]
    pub fn added_or_changed(&self) -> &[Resource] {
        &self.added_or_changed
    }

    #[must_use]
    pub fn removed(&self) -> &[ResourceId] {
        &self.removed
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.added_or_changed.is_empty() && self.removed.is_empty()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum PlanError {
    #[error("plan contains a resource identity more than once")]
    DuplicateResource,
    #[error("plan contains more than one blocked marker for a component")]
    DuplicateBlockedComponent,
}
