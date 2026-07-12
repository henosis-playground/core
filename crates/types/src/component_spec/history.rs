use thiserror::Error;

use crate::RegisteredComponentSpec;
use crate::SpecCatalog;
use crate::SpecCatalogError;

/// One component-spec record paired with its S2 sequence.
#[derive(Clone, Debug)]
pub struct SequencedSpecEvent {
    sequence: u64,
    component: RegisteredComponentSpec,
}

impl SequencedSpecEvent {
    #[must_use]
    pub const fn new(sequence: u64, component: RegisteredComponentSpec) -> Self {
        Self {
            sequence,
            component,
        }
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn component(&self) -> &RegisteredComponentSpec {
        &self.component
    }
}

/// Folded history of the global component-spec stream.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SpecHistory {
    next_sequence: u64,
    catalog: SpecCatalog,
}

/// Invalid component-spec stream history.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SpecHistoryError {
    #[error("spec stream sequence is not contiguous")]
    NonContiguous,
    #[error(transparent)]
    Catalog(#[from] SpecCatalogError),
}

impl SpecHistory {
    /// Fold one already-parsed domain event into this history.
    pub fn apply(&mut self, event: SequencedSpecEvent) -> Result<(), SpecHistoryError> {
        if event.sequence != self.next_sequence {
            return Err(SpecHistoryError::NonContiguous);
        }
        self.catalog.apply(event.component)?;
        self.next_sequence = self.next_sequence.saturating_add(1);
        Ok(())
    }

    #[must_use]
    pub const fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    #[must_use]
    pub const fn catalog(&self) -> &SpecCatalog {
        &self.catalog
    }
}
