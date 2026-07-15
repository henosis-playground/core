use futures::future::BoxFuture;
use thiserror::Error;

use crate::BlockedMarker;
use crate::BundleRef;
use crate::ComponentName;
use crate::Generation;
use crate::GraphId;
use crate::OutputRef;
use crate::OutputSnapshot;
use crate::Resource;

#[derive(Clone, Debug)]
pub struct EvaluationRequest {
    graph_id: GraphId,
    generation: Generation,
    component: ComponentName,
    bundle: BundleRef,
    inputs: OutputSnapshot,
}

impl EvaluationRequest {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        generation: Generation,
        component: ComponentName,
        bundle: BundleRef,
        inputs: OutputSnapshot,
    ) -> Self {
        Self {
            graph_id,
            generation,
            component,
            bundle,
            inputs,
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
    pub const fn inputs(&self) -> &OutputSnapshot {
        &self.inputs
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewComponentEvaluation {
    pub component: ComponentName,
    pub resources: Vec<Resource>,
    pub blocked_on: Vec<OutputRef>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentEvaluation {
    component: ComponentName,
    resources: Vec<Resource>,
    blocked_on: Vec<OutputRef>,
}

impl ComponentEvaluation {
    pub fn new(mut new: NewComponentEvaluation) -> Result<Self, ComponentEvaluationError> {
        if new
            .resources
            .iter()
            .any(|resource| resource.path().instance() != &new.component)
        {
            return Err(ComponentEvaluationError::ForeignResource);
        }
        new.blocked_on.sort();
        new.blocked_on.dedup();
        Ok(Self {
            component: new.component,
            resources: new.resources,
            blocked_on: new.blocked_on,
        })
    }

    #[must_use]
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub fn resources(&self) -> &[Resource] {
        &self.resources
    }

    #[must_use]
    pub fn blocked_on(&self) -> &[OutputRef] {
        &self.blocked_on
    }

    pub fn blocked_marker(&self) -> Option<BlockedMarker> {
        if self.blocked_on.is_empty() {
            None
        } else {
            Some(
                BlockedMarker::new(self.component.clone(), self.blocked_on.clone())
                    .expect("non-empty blocked inputs were checked"),
            )
        }
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentEvaluationError {
    #[error("an evaluator returned a resource owned by another component")]
    ForeignResource,
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

/// Boundary implemented later by the hermetic isolate engine.
pub trait Evaluator: Send + Sync {
    fn evaluate<'a>(
        &'a self,
        request: EvaluationRequest,
    ) -> BoxFuture<'a, Result<ComponentEvaluation, EvaluationError>>;
}
