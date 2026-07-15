use serde::{Deserialize, Serialize};

use crate::Seed;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceEvent {
    pub step: u64,
    pub action: String,
    pub outcome: String,
    pub state_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NormalizedTrace {
    pub root_seed: Seed,
    pub scheduler_seed: Seed,
    pub events: Vec<TraceEvent>,
    pub final_state_hash: String,
}

#[derive(Clone, Debug)]
pub struct TraceRecorder {
    root_seed: Seed,
    scheduler_seed: Seed,
    events: Vec<TraceEvent>,
}

impl TraceRecorder {
    #[must_use]
    pub fn new(root_seed: Seed) -> Self {
        Self {
            root_seed,
            scheduler_seed: root_seed.child("scheduler"),
            events: Vec::new(),
        }
    }

    pub fn record(
        &mut self,
        action: impl Into<String>,
        outcome: impl Into<String>,
        canonical_state: &[u8],
    ) {
        self.events.push(TraceEvent {
            step: self.events.len() as u64,
            action: action.into(),
            outcome: outcome.into(),
            state_hash: blake3::hash(canonical_state).to_hex().to_string(),
        });
    }

    #[must_use]
    pub fn finish(self, canonical_state: &[u8]) -> NormalizedTrace {
        NormalizedTrace {
            root_seed: self.root_seed,
            scheduler_seed: self.scheduler_seed,
            events: self.events,
            final_state_hash: blake3::hash(canonical_state).to_hex().to_string(),
        }
    }
}
