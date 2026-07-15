use std::collections::BTreeMap;
use std::collections::VecDeque;

use henosis_types::ControllerName;
use henosis_types::Generation;
use henosis_types::NativeValue;
use henosis_types::OutputName;
use henosis_types::ResourceId;
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TargetOperation {
    pub idempotency_key: String,
    pub generation: Generation,
    pub controller: ControllerName,
    pub resources: Vec<ResourceId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutputDelivery {
    pub generation: Generation,
    pub resource: ResourceId,
    pub output: OutputName,
    pub value: NativeValue,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TargetFault {
    Apply,
    FailBeforeApply,
    ApplyThenTimeout,
    PartialApply(usize),
    DelayOutput,
    DuplicateOutput,
    StaleOutput(Generation),
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
#[error("idempotency key {key} was reused for a different target operation")]
pub struct IdempotencyViolation {
    pub key: String,
}

#[derive(Clone, Debug, Default)]
pub struct FakeTarget {
    accepted: BTreeMap<String, TargetOperation>,
    applied: BTreeMap<ResourceId, Generation>,
    faults: VecDeque<TargetFault>,
    delayed: Vec<OutputDelivery>,
}

impl FakeTarget {
    pub fn script(&mut self, faults: impl IntoIterator<Item = TargetFault>) {
        self.faults.extend(faults);
    }

    pub fn apply(&mut self, operation: TargetOperation) -> Result<bool, IdempotencyViolation> {
        if let Some(previous) = self.accepted.get(&operation.idempotency_key) {
            if previous != &operation {
                return Err(IdempotencyViolation {
                    key: operation.idempotency_key,
                });
            }
            return Ok(false);
        }
        self.accepted
            .insert(operation.idempotency_key.clone(), operation.clone());
        let fault = self.faults.pop_front().unwrap_or(TargetFault::Apply);
        let apply_count = match fault {
            TargetFault::FailBeforeApply => 0,
            TargetFault::PartialApply(count) => count.min(operation.resources.len()),
            _ => operation.resources.len(),
        };
        for resource in operation.resources.into_iter().take(apply_count) {
            self.applied.insert(resource, operation.generation);
        }
        Ok(!matches!(
            fault,
            TargetFault::FailBeforeApply | TargetFault::ApplyThenTimeout
        ))
    }

    pub fn delay(&mut self, output: OutputDelivery) {
        self.delayed.push(output);
    }

    pub fn drain_delayed(&mut self) -> Vec<OutputDelivery> {
        std::mem::take(&mut self.delayed)
    }

    #[must_use]
    pub fn generation_of(&self, resource: ResourceId) -> Option<Generation> {
        self.applied.get(&resource).copied()
    }
}
