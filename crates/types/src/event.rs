use serde::Deserialize;
use serde::Serialize;

use crate::ComponentName;
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
pub enum CoreEvent {
    GraphCreated(GraphIntent),
    GraphUpdated(GraphIntent),
    PlanAccepted {
        graph_id: GraphId,
        plan: Plan,
    },
    ControllerReported(ControllerReport),
    OutputsPublished(OutputPublication),
    StallDetected(Stall),
    GraphRetired {
        graph_id: GraphId,
        last_generation: Generation,
    },
}
