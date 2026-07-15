use std::cmp::Reverse;
use std::collections::BinaryHeap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SimInstant(u64);

impl SimInstant {
    #[must_use]
    pub const fn from_ticks(ticks: u64) -> Self {
        Self(ticks)
    }

    #[must_use]
    pub const fn ticks(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct TimerId(u64);

impl TimerId {
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimClock {
    now: SimInstant,
    next_id: u64,
    timers: BinaryHeap<Reverse<(SimInstant, TimerId)>>,
}

impl SimClock {
    #[must_use]
    pub const fn now(&self) -> SimInstant {
        self.now
    }

    pub fn schedule_after(&mut self, ticks: u64) -> TimerId {
        let id = TimerId(self.next_id);
        self.next_id = self.next_id.saturating_add(1);
        let deadline = SimInstant(self.now.0.saturating_add(ticks));
        self.timers.push(Reverse((deadline, id)));
        id
    }

    #[must_use]
    pub fn has_timers(&self) -> bool {
        !self.timers.is_empty()
    }

    pub fn advance_to_next(&mut self) -> Vec<TimerId> {
        let Some(Reverse((deadline, _))) = self.timers.peek().copied() else {
            return Vec::new();
        };
        self.now = deadline;
        let mut ready = Vec::new();
        while self
            .timers
            .peek()
            .is_some_and(|Reverse((candidate, _))| *candidate == deadline)
        {
            let Reverse((_, id)) = self.timers.pop().expect("peeked timer exists");
            ready.push(id);
        }
        ready
    }
}
