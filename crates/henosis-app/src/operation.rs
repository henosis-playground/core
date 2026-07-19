use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{ArtifactRequirement, BundleError, BundleRequest, Bundler};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlePin {
    pub component: String,
    pub bundle_id: String,
    #[serde(default)]
    pub input_bindings: BTreeMap<String, serde_json::Value>,
    pub source: Option<SourceProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SourceProvenance {
    Local {
        repository: Option<String>,
        base_revision: Option<String>,
        dirty: bool,
    },
    Vcs {
        repository: String,
        revision: String,
        reference: Option<String>,
    },
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphSourcePolicy {
    #[default]
    AcceptLocal,
    RequireVcs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphIntent {
    Create {
        graph: String,
        bundles: Vec<BundlePin>,
        source_policy: GraphSourcePolicy,
    },
    Update {
        graph: String,
        expected_generation: u64,
        bundles: Vec<BundlePin>,
    },
    Retire {
        graph: String,
    },
}

impl GraphIntent {
    #[must_use]
    pub fn graph(&self) -> &str {
        match self {
            Self::Create { graph, .. } | Self::Update { graph, .. } | Self::Retire { graph } => graph,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphPhase {
    Planning,
    Blocked,
    Reconciling,
    Ready,
    Failed,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphSummary {
    pub graph: String,
    pub generation: u64,
    pub phase: GraphPhase,
    pub created: bool,
    pub retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedOn {
    pub component: String,
    pub input: String,
    pub producer: Option<String>,
    pub output: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDisposition {
    pub resource: String,
    pub state: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphOutput {
    pub reference: String,
    pub value: serde_json::Value,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStatus {
    pub graph: String,
    pub generation: u64,
    pub phase: GraphPhase,
    pub blocked_on: Vec<BlockedOn>,
    pub outputs: Vec<GraphOutput>,
    pub observed_ready: usize,
    pub planned_resources: usize,
    pub diagnostic: Option<String>,
    pub bundles: Vec<BundlePin>,
    pub source_policy: GraphSourcePolicy,
    pub dispositions: Vec<ResourceDisposition>,
}

impl GraphStatus {
    #[must_use]
    pub fn planning(graph: impl Into<String>, generation: u64) -> Self {
        Self {
            graph: graph.into(),
            generation,
            phase: GraphPhase::Planning,
            blocked_on: Vec::new(),
            outputs: Vec::new(),
            observed_ready: 0,
            planned_resources: 0,
            diagnostic: None,
            bundles: Vec::new(),
            source_policy: GraphSourcePolicy::AcceptLocal,
            dispositions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceRequest {
    pub repository: String,
    pub revision: Option<String>,
    pub reference: Option<String>,
    pub component: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedSource {
    pub repository: PathBuf,
    pub provenance: SourceProvenance,
    pub component: Option<String>,
}

pub trait CheckoutService: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn checkout(
        &self,
        request: &SourceRequest,
    ) -> impl Future<Output = Result<PreparedSource, Self::Error>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactBinding {
    pub component: String,
    pub input: String,
    pub digest: String,
    pub source: PathBuf,
}

pub trait ArtifactService: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn build(
        &self,
        repository: &Path,
        requirements: &[ArtifactRequirement],
    ) -> Result<Vec<ArtifactBinding>, Self::Error>;
}

pub trait CoreClient: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    fn status(
        &self,
        graph: &str,
    ) -> impl Future<Output = Result<Option<GraphStatus>, Self::Error>> + Send;

    fn apply(
        &self,
        intent: GraphIntent,
    ) -> impl Future<Output = Result<GraphStatus, Self::Error>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyGraph {
    pub graph: String,
    pub sources: Vec<SourceRequest>,
    pub create: bool,
    pub source_policy: GraphSourcePolicy,
    pub preserve_unmentioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    Unchanged(GraphStatus),
    Applied(GraphStatus),
}

impl ApplyOutcome {
    #[must_use]
    pub const fn status(&self) -> &GraphStatus {
        match self {
            Self::Unchanged(status) | Self::Applied(status) => status,
        }
    }

    #[must_use]
    pub const fn changed(&self) -> bool {
        matches!(self, Self::Applied(_))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OperationError {
    #[error(transparent)]
    Bundle(#[from] BundleError),
    #[error("cannot prepare source: {0}")]
    Checkout(String),
    #[error("cannot build workload artifact: {0}")]
    Artifact(String),
    #[error("cannot call Henosis core: {0}")]
    Core(String),
    #[error("graph `{0}` does not exist; pass create intent explicitly")]
    GraphMissing(String),
    #[error("component `{0}` was produced by more than one source")]
    DuplicateComponent(String),
    #[error("source did not produce requested component `{0}`")]
    MissingComponent(String),
    #[error("artifact binding `{component}.{input}` has no matching bundle requirement")]
    UnexpectedArtifact { component: String, input: String },
    #[error("bundle requirement `{component}.{input}` has no artifact binding")]
    MissingArtifact { component: String, input: String },
}

pub struct GraphOperation<C, B, A, K> {
    core: C,
    bundler: B,
    artifacts: A,
    checkouts: K,
    bundle_root: PathBuf,
}

impl<C, B, A, K> GraphOperation<C, B, A, K>
where
    C: CoreClient,
    B: Bundler,
    A: ArtifactService,
    K: CheckoutService,
{
    #[must_use]
    pub fn new(
        core: C,
        bundler: B,
        artifacts: A,
        checkouts: K,
        bundle_root: impl Into<PathBuf>,
    ) -> Self {
        Self {
            core,
            bundler,
            artifacts,
            checkouts,
            bundle_root: bundle_root.into(),
        }
    }

    pub async fn apply(&self, request: ApplyGraph) -> Result<ApplyOutcome, OperationError> {
        let mut pins = Vec::new();
        let mut names = BTreeSet::new();
        for source_request in &request.sources {
            let source = self
                .checkouts
                .checkout(source_request)
                .await
                .map_err(|error| OperationError::Checkout(error.to_string()))?;
            let bundles = self.bundler.bundle(&BundleRequest {
                repository: source.repository.clone(),
                output: self.bundle_root.clone(),
            })?;
            let requirements = bundles
                .bundles
                .iter()
                .flat_map(|bundle| bundle.artifact_requirements.iter().cloned())
                .collect::<Vec<_>>();
            let bindings = self
                .artifacts
                .build(&source.repository, &requirements)
                .map_err(|error| OperationError::Artifact(error.to_string()))?;
            validate_bindings(&requirements, &bindings)?;
            let mut selected = bundles.bundles.into_iter().filter(|bundle| {
                source
                    .component
                    .as_ref()
                    .is_none_or(|component| component == &bundle.component)
            });
            let mut found = false;
            for bundle in &mut selected {
                found = true;
                if !names.insert(bundle.component.clone()) {
                    return Err(OperationError::DuplicateComponent(bundle.component));
                }
                pins.push(BundlePin {
                    component: bundle.component.clone(),
                    bundle_id: bundle.bundle_id,
                    input_bindings: bindings
                        .iter()
                        .filter(|binding| binding.component == bundle.component)
                        .map(|binding| {
                            (
                                binding.input.clone(),
                                serde_json::Value::String(binding.digest.clone()),
                            )
                        })
                        .collect(),
                    source: Some(source.provenance.clone()),
                });
            }
            if !found
                && let Some(component) = source.component
            {
                return Err(OperationError::MissingComponent(component));
            }
        }
        pins.sort_by(|left, right| left.component.cmp(&right.component));

        let current = self
            .core
            .status(&request.graph)
            .await
            .map_err(|error| OperationError::Core(error.to_string()))?;
        let intent = match current {
            Some(current) => {
                if request.preserve_unmentioned {
                    let replacing = pins
                        .iter()
                        .map(|pin| pin.component.clone())
                        .collect::<BTreeSet<_>>();
                    pins.extend(
                        current
                            .bundles
                            .iter()
                            .filter(|pin| !replacing.contains(&pin.component))
                            .cloned(),
                    );
                    pins.sort_by(|left, right| left.component.cmp(&right.component));
                }
                if current.bundles == pins {
                    return Ok(ApplyOutcome::Unchanged(current));
                }
                GraphIntent::Update {
                    graph: request.graph,
                    expected_generation: current.generation,
                    bundles: pins,
                }
            }
            None if request.create => GraphIntent::Create {
                graph: request.graph,
                bundles: pins,
                source_policy: request.source_policy,
            },
            None => return Err(OperationError::GraphMissing(request.graph)),
        };
        self.core
            .apply(intent)
            .await
            .map(ApplyOutcome::Applied)
            .map_err(|error| OperationError::Core(error.to_string()))
    }

    pub async fn retire(&self, graph: impl Into<String>) -> Result<GraphStatus, OperationError> {
        self.core
            .apply(GraphIntent::Retire { graph: graph.into() })
            .await
            .map_err(|error| OperationError::Core(error.to_string()))
    }
}

fn validate_bindings(
    requirements: &[ArtifactRequirement],
    bindings: &[ArtifactBinding],
) -> Result<(), OperationError> {
    let expected = requirements
        .iter()
        .map(|item| (item.component.as_str(), item.input.as_str()))
        .collect::<BTreeSet<_>>();
    let actual = bindings
        .iter()
        .map(|item| (item.component.as_str(), item.input.as_str()))
        .collect::<BTreeSet<_>>();
    if let Some((component, input)) = actual.difference(&expected).next() {
        return Err(OperationError::UnexpectedArtifact {
            component: (*component).to_string(),
            input: (*input).to_string(),
        });
    }
    if let Some((component, input)) = expected.difference(&actual).next() {
        return Err(OperationError::MissingArtifact {
            component: (*component).to_string(),
            input: (*input).to_string(),
        });
    }
    Ok(())
}
