//! Deterministic fakes used by the Henosis domain simulator.

mod clock;
mod diagnostics;
mod evaluator;
mod rng;
mod storage;
mod target;
mod trace;

pub use clock::{SimClock, SimInstant, TimerId};
pub use diagnostics::{Quiescence, StallReport, WaitEdge, WaitGraph, WaitNode};
pub use evaluator::{ComponentProgram, ProgramEvaluator, ResourceProgram};
pub use rng::{NamedRng, Seed};
pub use storage::{AppendFault, MemS2, MemS2Append, MemS2Error};
pub use target::{FakeTarget, IdempotencyViolation, OutputDelivery, TargetFault, TargetOperation};
pub use trace::{NormalizedTrace, TraceEvent, TraceRecorder};
