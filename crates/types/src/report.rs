use crate::ComponentOutputs;
use crate::ComponentSpecHash;
use crate::ConnectorKey;
use crate::Graph;
use crate::GraphId;
use crate::PublishedSliceOutputs;
use crate::RegisteredComponentSpec;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostic {
    code: String,
    message: String,
    component_spec_hash: Option<ComponentSpecHash>,
    pointer: String,
    help: String,
    severity: DiagnosticSeverity,
}

impl Diagnostic {
    #[must_use]
    pub fn error(code: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: String::new(),
            component_spec_hash: None,
            pointer: String::new(),
            help: String::new(),
            severity: DiagnosticSeverity::Error,
        }
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new(
        code: String,
        message: String,
        component_spec_hash: Option<ComponentSpecHash>,
        pointer: String,
        help: String,
        severity: DiagnosticSeverity,
    ) -> Self {
        Self {
            code,
            message,
            component_spec_hash,
            pointer,
            help,
            severity,
        }
    }

    #[must_use]
    pub fn for_component(mut self, hash: ComponentSpecHash) -> Self {
        self.component_spec_hash = Some(hash);
        self
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    #[must_use]
    pub const fn component_spec_hash(&self) -> Option<ComponentSpecHash> {
        self.component_spec_hash
    }

    #[must_use]
    pub fn pointer(&self) -> &str {
        &self.pointer
    }

    #[must_use]
    pub fn help(&self) -> &str {
        &self.help
    }

    #[must_use]
    pub const fn severity(&self) -> DiagnosticSeverity {
        self.severity
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComponentDispositionKind {
    Pending,
    Reconciling,
    Ready,
    Failed,
}

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SliceReport {
    graph_id: GraphId,
    generation: u64,
    connector: ConnectorKey,
    dispositions: IdOrdMap<ComponentDisposition>,
    outputs: IdOrdMap<ComponentOutputs>,
    diagnostics: Vec<Diagnostic>,
    sequence: u64,
}

#[derive(Clone, Debug)]
pub struct NewSliceReport {
    pub graph_id: GraphId,
    pub generation: u64,
    pub connector: ConnectorKey,
    pub dispositions: Vec<ComponentDisposition>,
    pub outputs: Vec<ComponentOutputs>,
    pub diagnostics: Vec<Diagnostic>,
    pub sequence: u64,
}

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

    #[must_use]
    pub fn dispositions(&self) -> impl ExactSizeIterator<Item = &ComponentDisposition> {
        self.dispositions.iter()
    }

    #[must_use]
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
}

impl IdOrdItem for SliceReport {
    type Key<'a> = &'a ConnectorKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.connector
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GraphLifecycle {
    Active,
    Retired,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableGraphState {
    graph: Graph,
    published_outputs: IdOrdMap<PublishedSliceOutputs>,
    lifecycle: GraphLifecycle,
}

impl DurableGraphState {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(
        graph: Graph,
        published_outputs: IdOrdMap<PublishedSliceOutputs>,
        lifecycle: GraphLifecycle,
    ) -> Self {
        Self {
            graph,
            published_outputs,
            lifecycle,
        }
    }

    #[must_use]
    pub const fn graph(&self) -> &Graph {
        &self.graph
    }

    #[must_use]
    pub fn published_outputs(&self) -> impl ExactSizeIterator<Item = &PublishedSliceOutputs> {
        self.published_outputs.iter()
    }

    #[must_use]
    pub const fn lifecycle(&self) -> GraphLifecycle {
        self.lifecycle
    }

    pub(crate) fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    pub(crate) fn published_outputs_mut(&mut self) -> &mut IdOrdMap<PublishedSliceOutputs> {
        &mut self.published_outputs
    }

    pub(crate) fn retire(&mut self) {
        self.lifecycle = GraphLifecycle::Retired;
    }
}

#[derive(Clone, Debug)]
pub struct GraphState {
    durable: DurableGraphState,
    reports: IdOrdMap<SliceReport>,
}

impl GraphState {
    pub fn new(
        durable: DurableGraphState,
        reports: Vec<SliceReport>,
    ) -> Result<Self, DuplicateConnectorReport> {
        Ok(Self {
            durable,
            reports: IdOrdMap::from_iter_unique(reports).map_err(|_| DuplicateConnectorReport)?,
        })
    }

    #[must_use]
    pub const fn durable(&self) -> &DurableGraphState {
        &self.durable
    }

    #[must_use]
    pub fn reports(&self) -> impl ExactSizeIterator<Item = &SliceReport> {
        self.reports.iter()
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
#[error("graph state contains more than one report for a connector")]
pub struct DuplicateConnectorReport;

#[derive(Clone, Debug)]
pub struct GraphSlice {
    graph_id: GraphId,
    generation: u64,
    connector: ConnectorKey,
    components: IdOrdMap<RegisteredComponentSpec>,
    upstream_outputs: IdOrdMap<ComponentOutputs>,
    sequence: u64,
}

impl GraphSlice {
    pub fn new(
        graph_id: GraphId,
        generation: u64,
        connector: ConnectorKey,
        components: Vec<RegisteredComponentSpec>,
        upstream_outputs: Vec<ComponentOutputs>,
        sequence: u64,
    ) -> Result<Self, GraphSliceError> {
        if generation == 0 {
            return Err(GraphSliceError::InvalidGeneration);
        }
        Ok(Self {
            graph_id,
            generation,
            connector,
            components: IdOrdMap::from_iter_unique(components)
                .map_err(|_| GraphSliceError::DuplicateComponent)?,
            upstream_outputs: IdOrdMap::from_iter_unique(upstream_outputs)
                .map_err(|_| GraphSliceError::DuplicateUpstreamOutput)?,
            sequence,
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

    #[must_use]
    pub fn components(&self) -> impl ExactSizeIterator<Item = &RegisteredComponentSpec> {
        self.components.iter()
    }

    #[must_use]
    pub fn upstream_outputs(&self) -> impl ExactSizeIterator<Item = &ComponentOutputs> {
        self.upstream_outputs.iter()
    }

    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum GraphSliceError {
    #[error("graph slice generation must be greater than zero")]
    InvalidGeneration,
    #[error("graph slice components must have unique identities")]
    DuplicateComponent,
    #[error("graph slice upstream outputs must have unique component identities")]
    DuplicateUpstreamOutput,
}
