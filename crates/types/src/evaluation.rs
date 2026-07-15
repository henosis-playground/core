use std::collections::BTreeSet;

use futures::future::BoxFuture;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::BundleRef;
use crate::ComponentName;
use crate::ControllerName;
use crate::Generation;
use crate::GraphId;
use crate::InputName;
use crate::KindVersion;
use crate::NativeValue;
use crate::NativeValueError;
use crate::NewResource;
use crate::OutputDeclaration;
use crate::OutputName;
use crate::OutputRef;
use crate::Resource;
use crate::ResourceAddress;
use crate::ResourceError;
use crate::ResourceId;
use crate::ResourceName;
use crate::ResourcePath;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InputCellState {
    Available(NativeValue),
    Blocked,
    Absent,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum InputCellSource {
    Output(OutputRef),
    Config,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InputCell {
    name: InputName,
    source: InputCellSource,
    optional: bool,
    state: InputCellState,
}

impl InputCell {
    pub fn new(
        name: InputName,
        source: OutputRef,
        optional: bool,
        state: InputCellState,
    ) -> Result<Self, InputCellError> {
        if !optional && matches!(state, InputCellState::Absent) {
            return Err(InputCellError);
        }
        Ok(Self {
            name,
            source: InputCellSource::Output(source),
            optional,
            state,
        })
    }

    #[must_use]
    pub const fn config(name: InputName, value: NativeValue) -> Self {
        Self {
            name,
            source: InputCellSource::Config,
            optional: false,
            state: InputCellState::Available(value),
        }
    }

    #[must_use]
    pub const fn name(&self) -> &InputName {
        &self.name
    }

    #[must_use]
    pub const fn source(&self) -> &InputCellSource {
        &self.source
    }

    #[must_use]
    pub const fn output_source(&self) -> Option<&OutputRef> {
        match &self.source {
            InputCellSource::Output(source) => Some(source),
            InputCellSource::Config => None,
        }
    }

    #[must_use]
    pub const fn is_optional(&self) -> bool {
        self.optional
    }

    #[must_use]
    pub const fn state(&self) -> &InputCellState {
        &self.state
    }
}

impl IdOrdItem for InputCell {
    type Key<'a> = &'a InputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("absent is valid only for optional inputs")]
pub struct InputCellError;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvaluationSnapshot {
    cells: IdOrdMap<InputCell>,
}

impl EvaluationSnapshot {
    pub fn new(cells: Vec<InputCell>) -> Result<Self, EvaluationSnapshotError> {
        let mut keyed = IdOrdMap::with_capacity(cells.len());
        for cell in cells {
            keyed
                .insert_unique(cell)
                .map_err(|_| EvaluationSnapshotError::DuplicateInput)?;
        }
        Ok(Self { cells: keyed })
    }

    #[must_use]
    pub fn get(&self, name: &InputName) -> Option<&InputCell> {
        self.cells.get(name)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &InputCell> {
        self.cells.iter()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum EvaluationSnapshotError {
    #[error("snapshot contains an input more than once")]
    DuplicateInput,
}

#[derive(Clone, Debug)]
pub struct EvaluationRequest {
    graph_id: GraphId,
    generation: Generation,
    component: ComponentName,
    bundle: BundleRef,
    snapshot: EvaluationSnapshot,
}

impl EvaluationRequest {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        generation: Generation,
        component: ComponentName,
        bundle: BundleRef,
        snapshot: EvaluationSnapshot,
    ) -> Self {
        Self {
            graph_id,
            generation,
            component,
            bundle,
            snapshot,
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
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub const fn bundle(&self) -> BundleRef {
        self.bundle
    }

    #[must_use]
    pub const fn snapshot(&self) -> &EvaluationSnapshot {
        &self.snapshot
    }
}

#[derive(Clone, Debug)]
pub struct NewEvaluationResource {
    pub id: ResourceId,
    pub component: ComponentName,
    pub kind: KindVersion,
    pub name: ResourceName,
    pub controller: ControllerName,
    pub body: serde_json::Value,
    pub canonical: String,
    pub outputs: Vec<OutputDeclaration>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EvaluationResource {
    address: ResourceAddress,
    resource: Resource,
}

impl EvaluationResource {
    pub fn new(new: NewEvaluationResource) -> Result<Self, EvaluationProtocolError> {
        let body = NativeValue::from_canonical(new.body, &new.canonical)?;
        let address = ResourceAddress::new(new.kind, new.name);
        let resource = Resource::new(NewResource {
            id: new.id,
            path: ResourcePath::new(new.component, address.clone()),
            controller: new.controller,
            body,
            outputs: new.outputs,
        })?;
        Ok(Self { address, resource })
    }

    #[must_use]
    pub const fn address(&self) -> &ResourceAddress {
        &self.address
    }

    #[must_use]
    pub const fn resource(&self) -> &Resource {
        &self.resource
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StaticOutput {
    name: OutputName,
    value: NativeValue,
}

impl StaticOutput {
    #[must_use]
    pub const fn new(name: OutputName, value: NativeValue) -> Self {
        Self { name, value }
    }

    #[must_use]
    pub const fn name(&self) -> &OutputName {
        &self.name
    }

    #[must_use]
    pub const fn value(&self) -> &NativeValue {
        &self.value
    }
}

impl IdOrdItem for StaticOutput {
    type Key<'a> = &'a OutputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObservedOutputBinding {
    name: OutputName,
    resource: ResourceAddress,
    output: OutputName,
}

impl ObservedOutputBinding {
    #[must_use]
    pub const fn new(name: OutputName, resource: ResourceAddress, output: OutputName) -> Self {
        Self {
            name,
            resource,
            output,
        }
    }

    #[must_use]
    pub const fn name(&self) -> &OutputName {
        &self.name
    }

    #[must_use]
    pub const fn resource(&self) -> &ResourceAddress {
        &self.resource
    }

    #[must_use]
    pub const fn output(&self) -> &OutputName {
        &self.output
    }
}

impl IdOrdItem for ObservedOutputBinding {
    type Key<'a> = &'a OutputName;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.name
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockedDetail {
    input: InputName,
    source: OutputRef,
    operation: String,
    message: String,
}

impl BlockedDetail {
    #[must_use]
    pub fn new(
        input: InputName,
        source: OutputRef,
        operation: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            input,
            source,
            operation: operation.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub const fn input(&self) -> &InputName {
        &self.input
    }

    #[must_use]
    pub const fn source(&self) -> &OutputRef {
        &self.source
    }

    #[must_use]
    pub fn operation(&self) -> &str {
        &self.operation
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewCompleteEvaluation {
    pub resources: Vec<EvaluationResource>,
    pub outputs: Vec<StaticOutput>,
    pub observed_outputs: Vec<ObservedOutputBinding>,
    pub reads: Vec<InputName>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewBlockedEvaluation {
    pub resources: Vec<EvaluationResource>,
    pub blocked: BlockedDetail,
    pub reads: Vec<InputName>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompleteEvaluation {
    resources: Vec<EvaluationResource>,
    outputs: IdOrdMap<StaticOutput>,
    observed_outputs: IdOrdMap<ObservedOutputBinding>,
    reads: Vec<InputName>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BlockedEvaluation {
    resources: Vec<EvaluationResource>,
    blocked: BlockedDetail,
    reads: Vec<InputName>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EvaluationAttempt {
    Complete(CompleteEvaluation),
    Blocked(BlockedEvaluation),
}

impl EvaluationAttempt {
    pub fn complete(
        snapshot: &EvaluationSnapshot,
        new: NewCompleteEvaluation,
    ) -> Result<Self, EvaluationProtocolError> {
        let (resources, addresses) = validate_resources(new.resources)?;
        let mut outputs = IdOrdMap::with_capacity(new.outputs.len());
        for output in new.outputs {
            outputs
                .insert_unique(output)
                .map_err(|_| EvaluationProtocolError::DuplicateStaticOutput)?;
        }
        let mut observed_outputs = IdOrdMap::with_capacity(new.observed_outputs.len());
        for binding in new.observed_outputs {
            if !addresses.contains(binding.resource()) {
                return Err(EvaluationProtocolError::UnknownObservedResource);
            }
            observed_outputs
                .insert_unique(binding)
                .map_err(|_| EvaluationProtocolError::DuplicateObservedOutput)?;
        }
        let reads = validate_reads(snapshot, new.reads)?;
        Ok(Self::Complete(CompleteEvaluation {
            resources,
            outputs,
            observed_outputs,
            reads,
        }))
    }

    pub fn blocked(
        snapshot: &EvaluationSnapshot,
        new: NewBlockedEvaluation,
    ) -> Result<Self, EvaluationProtocolError> {
        let (resources, _) = validate_resources(new.resources)?;
        let cell = snapshot
            .get(new.blocked.input())
            .ok_or(EvaluationProtocolError::UnknownRead)?;
        if cell.output_source() != Some(new.blocked.source())
            || !matches!(cell.state(), InputCellState::Blocked)
        {
            return Err(EvaluationProtocolError::BlockedSourceMismatch);
        }
        let reads = validate_reads(snapshot, new.reads)?;
        if !reads.contains(new.blocked.input()) {
            return Err(EvaluationProtocolError::BlockedInputNotRead);
        }
        Ok(Self::Blocked(BlockedEvaluation {
            resources,
            blocked: new.blocked,
            reads,
        }))
    }

    #[must_use]
    pub fn resources(&self) -> &[EvaluationResource] {
        match self {
            Self::Complete(result) => &result.resources,
            Self::Blocked(result) => &result.resources,
        }
    }

    #[must_use]
    pub fn reads(&self) -> &[InputName] {
        match self {
            Self::Complete(result) => &result.reads,
            Self::Blocked(result) => &result.reads,
        }
    }

    #[must_use]
    pub const fn complete_result(&self) -> Option<&CompleteEvaluation> {
        match self {
            Self::Complete(result) => Some(result),
            Self::Blocked(_) => None,
        }
    }

    #[must_use]
    pub const fn blocked_result(&self) -> Option<&BlockedEvaluation> {
        match self {
            Self::Complete(_) => None,
            Self::Blocked(result) => Some(result),
        }
    }
}

impl CompleteEvaluation {
    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &StaticOutput> {
        self.outputs.iter()
    }

    pub fn observed_outputs(&self) -> impl ExactSizeIterator<Item = &ObservedOutputBinding> {
        self.observed_outputs.iter()
    }
}

impl BlockedEvaluation {
    #[must_use]
    pub const fn blocked(&self) -> &BlockedDetail {
        &self.blocked
    }
}

fn validate_resources(
    resources: Vec<EvaluationResource>,
) -> Result<(Vec<EvaluationResource>, BTreeSet<ResourceAddress>), EvaluationProtocolError> {
    let mut addresses = BTreeSet::new();
    let mut ids = BTreeSet::new();
    for resource in &resources {
        if !addresses.insert(resource.address().clone()) {
            return Err(EvaluationProtocolError::DuplicateResourceAddress);
        }
        if !ids.insert(resource.resource().id()) {
            return Err(EvaluationProtocolError::DuplicateResourceId);
        }
    }
    Ok((resources, addresses))
}

fn validate_reads(
    snapshot: &EvaluationSnapshot,
    mut reads: Vec<InputName>,
) -> Result<Vec<InputName>, EvaluationProtocolError> {
    let original = reads.clone();
    reads.sort();
    reads.dedup();
    if reads != original {
        return Err(EvaluationProtocolError::ReadsNotSortedSet);
    }
    if reads.iter().any(|read| snapshot.get(read).is_none()) {
        return Err(EvaluationProtocolError::UnknownRead);
    }
    Ok(reads)
}

#[derive(Debug, Error)]
pub enum EvaluationProtocolError {
    #[error(transparent)]
    Canonical(#[from] NativeValueError),
    #[error(transparent)]
    Resource(#[from] ResourceError),
    #[error("evaluation emitted a resource address more than once")]
    DuplicateResourceAddress,
    #[error("evaluation emitted a resource TypeID more than once")]
    DuplicateResourceId,
    #[error("evaluation emitted a static output more than once")]
    DuplicateStaticOutput,
    #[error("evaluation bound an observed component output more than once")]
    DuplicateObservedOutput,
    #[error("observed output binding points to a resource not emitted by this evaluation")]
    UnknownObservedResource,
    #[error("reads must be a sorted unique set")]
    ReadsNotSortedSet,
    #[error("evaluation read an undeclared input")]
    UnknownRead,
    #[error("blocked detail does not agree with the snapshot input declaration")]
    BlockedSourceMismatch,
    #[error("blocked input must be present in reads")]
    BlockedInputNotRead,
}

#[derive(Debug, Error)]
#[error("component evaluation failed: {message}")]
pub struct EvaluationError {
    message: String,
}

impl EvaluationError {
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// Hermetic TypeScript-host boundary. A real `deno_core` implementation is
/// deliberately external.
pub trait Evaluator: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        request: EvaluationRequest,
    ) -> BoxFuture<'a, Result<EvaluationAttempt, EvaluationError>>;
}
