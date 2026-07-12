use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

use crate::ComponentOutputs;
use crate::ComponentSpecHash;
use crate::ConnectorKey;
use crate::Diagnostic;
use crate::GraphId;

/// Revision and URI published by a connector.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublicationEvidence {
    pub revision: String,
    pub uri: String,
}

impl PublicationEvidence {
    #[must_use]
    pub fn revision(&self) -> &str {
        &self.revision
    }

    #[must_use]
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// Reconciliation level reported for one component.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentDispositionKind {
    Pending,
    Reconciling,
    Ready,
    Failed,
}

/// Reconciliation level for one component-spec identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ComponentDisposition {
    component_spec_hash: ComponentSpecHash,
    kind: ComponentDispositionKind,
}

impl ComponentDisposition {
    #[must_use]
    pub const fn new(
        component_spec_hash: ComponentSpecHash,
        kind: ComponentDispositionKind,
    ) -> Self {
        Self {
            component_spec_hash,
            kind,
        }
    }

    #[must_use]
    pub const fn component_spec_hash(self) -> ComponentSpecHash {
        self.component_spec_hash
    }

    #[must_use]
    pub const fn kind(self) -> ComponentDispositionKind {
        self.kind
    }
}

impl IdOrdItem for ComponentDisposition {
    type Key<'a> = ComponentSpecHash;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.component_spec_hash
    }
}

/// Validated connector report for one graph generation and input sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SliceReport {
    graph_id: GraphId,
    generation: u64,
    connector: ConnectorKey,
    dispositions: IdOrdMap<ComponentDisposition>,
    outputs: IdOrdMap<ComponentOutputs>,
    diagnostics: Vec<Diagnostic>,
    sequence: u64,
    publication: Option<PublicationEvidence>,
}

/// Unvalidated input used to construct a slice report.
#[derive(Clone, Debug)]
pub struct NewSliceReport {
    pub graph_id: GraphId,
    pub generation: u64,
    pub connector: ConnectorKey,
    pub dispositions: Vec<ComponentDisposition>,
    pub outputs: Vec<ComponentOutputs>,
    pub diagnostics: Vec<Diagnostic>,
    pub sequence: u64,
    pub publication: Option<PublicationEvidence>,
}

/// Invalid slice-report input.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum SliceReportError {
    #[error("slice report generation must be greater than zero")]
    InvalidGeneration,
    #[error("slice report dispositions must identify unique component specs")]
    DuplicateDisposition,
    #[error("slice report outputs must identify unique component specs")]
    DuplicateOutput,
}

impl SliceReport {
    pub fn new(new: NewSliceReport) -> Result<Self, SliceReportError> {
        if new.generation == 0 {
            return Err(SliceReportError::InvalidGeneration);
        }
        let dispositions = IdOrdMap::from_iter_unique(new.dispositions)
            .map_err(|_| SliceReportError::DuplicateDisposition)?;
        let outputs = IdOrdMap::from_iter_unique(new.outputs)
            .map_err(|_| SliceReportError::DuplicateOutput)?;
        Ok(Self {
            graph_id: new.graph_id,
            generation: new.generation,
            connector: new.connector,
            dispositions,
            outputs,
            diagnostics: new.diagnostics,
            sequence: new.sequence,
            publication: new.publication,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub const fn connector(&self) -> &ConnectorKey {
        &self.connector
    }

    pub fn dispositions(&self) -> impl ExactSizeIterator<Item = &ComponentDisposition> {
        self.dispositions.iter()
    }

    pub fn outputs(&self) -> impl ExactSizeIterator<Item = &ComponentOutputs> {
        self.outputs.iter()
    }

    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    #[must_use]
    pub const fn publication(&self) -> Option<&PublicationEvidence> {
        self.publication.as_ref()
    }
}

impl IdOrdItem for SliceReport {
    type Key<'a> = (u64, &'a ConnectorKey);

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        (self.generation, &self.connector)
    }
}
