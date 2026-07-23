use std::collections::BTreeSet;

use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

use crate::ComponentName;
use crate::ContentDigest;
use crate::ControllerName;
use crate::ControllerReport;
use crate::Generation;
use crate::GraphId;
use crate::GraphIntent;
use crate::OutputRecord;
use crate::Plan;
use crate::PublicationId;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct OutputPublication {
    graph_id: GraphId,
    generation: Generation,
    controller: ControllerName,
    publication_id: PublicationId,
    outputs: Vec<OutputRecord>,
}

impl OutputPublication {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        generation: Generation,
        controller: ControllerName,
        publication_id: PublicationId,
        outputs: Vec<OutputRecord>,
    ) -> Self {
        Self {
            graph_id,
            generation,
            controller,
            publication_id,
            outputs,
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
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub const fn publication_id(&self) -> PublicationId {
        self.publication_id
    }

    #[must_use]
    pub fn outputs(&self) -> &[OutputRecord] {
        &self.outputs
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewComponentOutputs {
    pub graph_id: GraphId,
    pub generation: Generation,
    pub component: ComponentName,
    pub outputs: Vec<OutputRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ComponentOutputs {
    graph_id: GraphId,
    generation: Generation,
    component: ComponentName,
    outputs: Vec<OutputRecord>,
}

impl ComponentOutputs {
    pub fn new(new: NewComponentOutputs) -> Result<Self, ComponentOutputsError> {
        let mut keys = BTreeSet::new();
        for output in &new.outputs {
            if output.key_value().generation() != new.generation
                || output.key_value().reference().component() != &new.component
            {
                return Err(ComponentOutputsError::ForeignOutput);
            }
            if !keys.insert(output.key_value().clone()) {
                return Err(ComponentOutputsError::DuplicateOutput);
            }
        }
        Ok(Self {
            graph_id: new.graph_id,
            generation: new.generation,
            component: new.component,
            outputs: new.outputs,
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
    pub const fn component(&self) -> &ComponentName {
        &self.component
    }

    #[must_use]
    pub fn outputs(&self) -> &[OutputRecord] {
        &self.outputs
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ComponentOutputsError {
    #[error("component output replacement contains an output for another component or generation")]
    ForeignOutput,
    #[error("component output replacement contains the same output more than once")]
    DuplicateOutput,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Stall {
    graph_id: GraphId,
    generation: Generation,
    cycle: Vec<ComponentName>,
}

impl Stall {
    #[must_use]
    pub const fn new(graph_id: GraphId, generation: Generation, cycle: Vec<ComponentName>) -> Self {
        Self {
            graph_id,
            generation,
            cycle,
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
    pub fn cycle(&self) -> &[ComponentName] {
        &self.cycle
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ControllerProgress {
    graph_id: GraphId,
    generation: Generation,
    plan_digest: ContentDigest,
    controller: ControllerName,
}

impl ControllerProgress {
    #[must_use]
    pub const fn new(
        graph_id: GraphId,
        generation: Generation,
        plan_digest: ContentDigest,
        controller: ControllerName,
    ) -> Self {
        Self {
            graph_id,
            generation,
            plan_digest,
            controller,
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
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CoreEvent {
    GraphCreated(GraphIntent),
    GraphUpdated(GraphIntent),
    PlanAccepted {
        graph_id: GraphId,
        plan: Plan,
    },
    ControllerReported(ControllerReport),
    ControllerProgressed(ControllerProgress),
    ComponentOutputsReplaced(ComponentOutputs),
    OutputsPublished(OutputPublication),
    StallDetected(Stall),
    GraphRetired {
        graph_id: GraphId,
        last_generation: Generation,
    },
}
