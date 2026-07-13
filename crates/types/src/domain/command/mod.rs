//! Validated service commands accepted by the orchestrator.

mod component;
mod graph;
mod slice;

pub use component::NewComponent;
pub use graph::AddComponents;
pub use graph::CreateGraph;
pub use graph::GetGraph;
pub use graph::GetGraphGeneration;
pub use graph::RemoveComponents;
pub use graph::RetireGraph;
pub use graph::UpdateComponents;
pub use graph::WatchGraph;
pub use slice::FetchSlice;
pub use slice::ReportSlice;
