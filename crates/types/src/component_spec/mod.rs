//! Immutable connector input and its content-addressed catalog.

mod catalog;
mod definition;
mod history;

pub use catalog::SpecCatalog;
pub use catalog::SpecCatalogError;
pub use definition::ComponentSpec;
pub use definition::ComponentSpecError;
pub use definition::NewComponentSpec;
pub use definition::RegisteredComponentSpec;
pub use history::SequencedSpecEvent;
pub use history::SpecHistory;
pub use history::SpecHistoryError;
