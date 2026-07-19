use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
            Self::Create { graph, .. } | Self::Update { graph, .. } | Self::Retire { graph } => {
                graph
            }
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

#[derive(Clone)]
pub struct PreparedSource {
    pub repository: PathBuf,
    pub provenance: SourceProvenance,
    pub component: Option<String>,
    pub lease: Option<Arc<dyn std::any::Any + Send + Sync>>,
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
    pub kind: crate::WorkloadArtifactKind,
    pub digest: String,
    pub source: PathBuf,
    pub stored: PathBuf,
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
pub struct ApplyOutcome {
    pub status: GraphStatus,
    pub changed: bool,
    pub changed_components: Vec<String>,
    pub pins: Vec<BundlePin>,
    pub dependencies: Vec<PathBuf>,
    pub artifacts: Vec<ArtifactBinding>,
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
    #[error("artifact binding `{component}.{input}` was produced more than once")]
    DuplicateArtifact { component: String, input: String },
    #[error(
        "artifact binding `{component}.{input}` has kind {actual:?}, but the bundle requires {expected:?}"
    )]
    IncompatibleArtifact {
        component: String,
        input: String,
        expected: crate::WorkloadArtifactKind,
        actual: crate::WorkloadArtifactKind,
    },
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
        let mut dependencies = Vec::new();
        let mut artifacts = Vec::new();
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
            let selected = bundles
                .bundles
                .into_iter()
                .filter(|bundle| {
                    source
                        .component
                        .as_ref()
                        .is_none_or(|component| component == &bundle.component)
                })
                .collect::<Vec<_>>();
            if selected.is_empty()
                && let Some(component) = source.component
            {
                return Err(OperationError::MissingComponent(component));
            }
            let requirements = selected
                .iter()
                .flat_map(|bundle| bundle.artifact_requirements.iter().cloned())
                .collect::<Vec<_>>();
            let bindings = self
                .artifacts
                .build(&source.repository, &requirements)
                .map_err(|error| OperationError::Artifact(error.to_string()))?;
            validate_bindings(&requirements, &bindings)?;
            for bundle in selected {
                if !names.insert(bundle.component.clone()) {
                    return Err(OperationError::DuplicateComponent(bundle.component));
                }
                dependencies.extend(bundle.dependencies);
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
            artifacts.extend(bindings);
        }
        pins.sort_by(|left, right| left.component.cmp(&right.component));
        dependencies.sort();
        dependencies.dedup();
        artifacts.sort_by(|left, right| {
            (&left.component, &left.input).cmp(&(&right.component, &right.input))
        });

        let current = self
            .core
            .status(&request.graph)
            .await
            .map_err(|error| OperationError::Core(error.to_string()))?;
        let changed_components;
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
                if same_deployable_pins(&current.bundles, &pins) {
                    return Ok(ApplyOutcome {
                        status: current,
                        changed: false,
                        changed_components: Vec::new(),
                        pins,
                        dependencies,
                        artifacts,
                    });
                }
                changed_components = changed_component_names(Some(&current), &pins);
                GraphIntent::Update {
                    graph: request.graph,
                    expected_generation: current.generation,
                    bundles: pins.clone(),
                }
            }
            None if request.create => {
                changed_components = changed_component_names(None, &pins);
                GraphIntent::Create {
                    graph: request.graph,
                    bundles: pins.clone(),
                    source_policy: request.source_policy,
                }
            }
            None => return Err(OperationError::GraphMissing(request.graph)),
        };
        let status = self
            .core
            .apply(intent)
            .await
            .map_err(|error| OperationError::Core(error.to_string()))?;
        Ok(ApplyOutcome {
            status,
            changed: true,
            changed_components,
            pins,
            dependencies,
            artifacts,
        })
    }

    pub async fn retire(&self, graph: impl Into<String>) -> Result<GraphStatus, OperationError> {
        self.core
            .apply(GraphIntent::Retire {
                graph: graph.into(),
            })
            .await
            .map_err(|error| OperationError::Core(error.to_string()))
    }
}

fn changed_component_names(current: Option<&GraphStatus>, pins: &[BundlePin]) -> Vec<String> {
    let current = current
        .into_iter()
        .flat_map(|status| &status.bundles)
        .map(|pin| (pin.component.as_str(), pin))
        .collect::<BTreeMap<_, _>>();
    let desired = pins
        .iter()
        .map(|pin| (pin.component.as_str(), pin))
        .collect::<BTreeMap<_, _>>();
    current
        .keys()
        .chain(desired.keys())
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(
            |component| match (current.get(component), desired.get(component)) {
                (Some(old), Some(new)) => !same_deployable_pin(old, new),
                (None, None) => false,
                _ => true,
            },
        )
        .map(str::to_owned)
        .collect()
}

fn same_deployable_pins(current: &[BundlePin], desired: &[BundlePin]) -> bool {
    current.len() == desired.len()
        && current
            .iter()
            .zip(desired)
            .all(|(old, new)| same_deployable_pin(old, new))
}

fn same_deployable_pin(old: &BundlePin, new: &BundlePin) -> bool {
    old.component == new.component
        && old.bundle_id == new.bundle_id
        && old.input_bindings == new.input_bindings
}

fn validate_bindings(
    requirements: &[ArtifactRequirement],
    bindings: &[ArtifactBinding],
) -> Result<(), OperationError> {
    let expected = requirements
        .iter()
        .map(|item| ((item.component.as_str(), item.input.as_str()), item.kind))
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();
    for binding in bindings {
        let key = (binding.component.as_str(), binding.input.as_str());
        if actual.insert(key, binding.kind).is_some() {
            return Err(OperationError::DuplicateArtifact {
                component: binding.component.clone(),
                input: binding.input.clone(),
            });
        }
    }
    if let Some(((component, input), _)) =
        actual.iter().find(|(key, _)| !expected.contains_key(*key))
    {
        return Err(OperationError::UnexpectedArtifact {
            component: (*component).to_owned(),
            input: (*input).to_owned(),
        });
    }
    if let Some(((component, input), _)) =
        expected.iter().find(|(key, _)| !actual.contains_key(*key))
    {
        return Err(OperationError::MissingArtifact {
            component: (*component).to_owned(),
            input: (*input).to_owned(),
        });
    }
    if let Some(((component, input), expected_kind)) = expected
        .iter()
        .find(|(key, kind)| actual.get(*key) != Some(kind))
    {
        return Err(OperationError::IncompatibleArtifact {
            component: (*component).to_owned(),
            input: (*input).to_owned(),
            expected: *expected_kind,
            actual: actual[&(*component, *input)],
        });
    }
    Ok(())
}
