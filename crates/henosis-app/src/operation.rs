use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use henosis_types::ArtifactDigest;
use henosis_types::BundleRef;
use henosis_types::ComponentName;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphSourcePolicy;
use henosis_types::InputName;
use henosis_types::NativeValue;
use henosis_types::OutputName;
use henosis_types::OutputRef;
use henosis_types::OutputSource;
use henosis_types::ResourceDispositionKind;
use henosis_types::ResourceId;
use henosis_types::SourceProvenance;
use serde::Deserialize;
use serde::Serialize;

use crate::ArtifactRequirement;
use crate::BundleError;
use crate::BundleRequest;
use crate::Bundler;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundlePin {
    pub component: ComponentName,
    pub bundle: BundleRef,
    #[serde(default)]
    pub input_bindings: BTreeMap<InputName, NativeValue>,
    pub source: Option<SourceProvenance>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GraphIntent {
    Create {
        graph: GraphId,
        bundles: Vec<BundlePin>,
        source_policy: GraphSourcePolicy,
    },
    Update {
        graph: GraphId,
        expected_generation: Generation,
        bundles: Vec<BundlePin>,
    },
    Retire {
        graph: GraphId,
    },
}

impl GraphIntent {
    #[must_use]
    pub const fn graph(&self) -> GraphId {
        match self {
            Self::Create { graph, .. } | Self::Update { graph, .. } | Self::Retire { graph } => {
                *graph
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
    pub graph: GraphId,
    pub generation: Generation,
    pub phase: GraphPhase,
    pub created: bool,
    pub retired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockedOn {
    pub component: ComponentName,
    pub input: InputName,
    pub producer: Option<ComponentName>,
    pub output: Option<OutputName>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceDisposition {
    pub resource: ResourceId,
    pub kind: ResourceDispositionKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphOutput {
    pub reference: OutputRef,
    pub value: NativeValue,
    pub source: OutputSource,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStatus {
    pub graph: GraphId,
    pub generation: Generation,
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
    pub fn planning(graph: GraphId, generation: Generation) -> Self {
        Self {
            graph,
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
    pub component: Option<ComponentName>,
}

#[derive(Clone)]
pub struct PreparedSource {
    pub repository: PathBuf,
    pub provenance: SourceProvenance,
    pub component: Option<ComponentName>,
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
    pub component: ComponentName,
    pub input: InputName,
    pub kind: crate::WorkloadArtifactKind,
    pub digest: ArtifactDigest,
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
        graph: GraphId,
    ) -> impl Future<Output = Result<Option<GraphStatus>, Self::Error>> + Send;

    fn apply(
        &self,
        intent: GraphIntent,
    ) -> impl Future<Output = Result<GraphStatus, Self::Error>> + Send;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyGraph {
    pub graph: GraphId,
    pub sources: Vec<SourceRequest>,
    pub create: bool,
    pub source_policy: GraphSourcePolicy,
    pub preserve_unmentioned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyOutcome {
    pub status: GraphStatus,
    pub changed: bool,
    pub changed_components: Vec<ComponentName>,
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
    #[error("bundle produced an invalid domain value: {0}")]
    InvalidBundleDomain(String),
    #[error("graph `{0}` does not exist; pass create intent explicitly")]
    GraphMissing(GraphId),
    #[error("component `{0}` was produced by more than one source")]
    DuplicateComponent(ComponentName),
    #[error("source did not produce requested component `{0}`")]
    MissingComponent(ComponentName),
    #[error("artifact binding `{component}.{input}` has no matching bundle requirement")]
    UnexpectedArtifact {
        component: ComponentName,
        input: InputName,
    },
    #[error("bundle requirement `{component}.{input}` has no artifact binding")]
    MissingArtifact {
        component: ComponentName,
        input: InputName,
    },
    #[error("artifact binding `{component}.{input}` was produced more than once")]
    DuplicateArtifact {
        component: ComponentName,
        input: InputName,
    },
    #[error(
        "artifact binding `{component}.{input}` has kind {actual:?}, but the bundle requires \
         {expected:?}"
    )]
    IncompatibleArtifact {
        component: ComponentName,
        input: InputName,
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
                        .is_none_or(|component| component.as_str() == bundle.component)
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
                let component = ComponentName::new(bundle.component)
                    .map_err(|error| OperationError::InvalidBundleDomain(error.to_string()))?;
                if !names.insert(component.clone()) {
                    return Err(OperationError::DuplicateComponent(component));
                }
                let digest = parse_content_digest(&bundle.bundle_id)?;
                dependencies.extend(bundle.dependencies);
                pins.push(BundlePin {
                    component: component.clone(),
                    bundle: BundleRef::new(digest),
                    input_bindings: bindings
                        .iter()
                        .filter(|binding| binding.component == component)
                        .map(|binding| {
                            (
                                binding.input.clone(),
                                NativeValue::new(serde_json::json!(binding.digest.to_string()))
                                    .expect("artifact digest is finite JSON"),
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
            .status(request.graph)
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

    pub async fn retire(&self, graph: GraphId) -> Result<GraphStatus, OperationError> {
        self.core
            .apply(GraphIntent::Retire { graph })
            .await
            .map_err(|error| OperationError::Core(error.to_string()))
    }
}

fn parse_content_digest(value: &str) -> Result<henosis_types::ContentDigest, OperationError> {
    let bytes = hex::decode(value)
        .map_err(|error| OperationError::InvalidBundleDomain(error.to_string()))?;
    let bytes: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        OperationError::InvalidBundleDomain(format!(
            "bundle digest must contain 32 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(henosis_types::ContentDigest::from_bytes(bytes))
}

fn changed_component_names(
    current: Option<&GraphStatus>,
    pins: &[BundlePin],
) -> Vec<ComponentName> {
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
        .map(|name| {
            desired
                .get(name)
                .or_else(|| current.get(name))
                .expect("name came from current or desired pins")
                .component
                .clone()
        })
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
        && old.bundle == new.bundle
        && old.input_bindings == new.input_bindings
}

fn validate_bindings(
    requirements: &[ArtifactRequirement],
    bindings: &[ArtifactBinding],
) -> Result<(), OperationError> {
    let expected = requirements
        .iter()
        .map(|item| {
            Ok((
                (
                    ComponentName::new(item.component.clone()).map_err(|error| {
                        OperationError::InvalidBundleDomain(error.to_string())
                    })?,
                    InputName::new(item.input.clone()).map_err(|error| {
                        OperationError::InvalidBundleDomain(error.to_string())
                    })?,
                ),
                item.kind,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, OperationError>>()?;
    let mut actual = BTreeMap::new();
    for binding in bindings {
        let key = (binding.component.clone(), binding.input.clone());
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
            component: component.clone(),
            input: input.clone(),
        });
    }
    if let Some(((component, input), _)) =
        expected.iter().find(|(key, _)| !actual.contains_key(*key))
    {
        return Err(OperationError::MissingArtifact {
            component: component.clone(),
            input: input.clone(),
        });
    }
    if let Some(((component, input), expected_kind)) = expected
        .iter()
        .find(|(key, kind)| actual.get(*key) != Some(kind))
    {
        return Err(OperationError::IncompatibleArtifact {
            component: component.clone(),
            input: input.clone(),
            expected: *expected_kind,
            actual: actual[&(component.clone(), input.clone())],
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pin(digest: &str, source: Option<SourceProvenance>) -> BundlePin {
        BundlePin {
            component: ComponentName::new("web").unwrap(),
            bundle: BundleRef::new(henosis_types::ContentDigest::from_bytes([0xaa; 32])),
            input_bindings: BTreeMap::from([(
                InputName::new("workerArtifact").unwrap(),
                NativeValue::new(serde_json::json!(digest)).unwrap(),
            )]),
            source,
        }
    }

    #[test]
    fn provenance_does_not_create_a_deployable_change() {
        let old = pin("sha256:11", None);
        let new = pin(
            "sha256:11",
            Some(SourceProvenance::Local {
                repository: None,
                base_revision: None,
                dirty: true,
            }),
        );
        assert!(same_deployable_pins(&[old], &[new]));
    }

    #[test]
    fn artifact_binding_change_is_deployable() {
        let mut current = GraphStatus::planning(
            GraphId::from_bytes([1; 16]),
            Generation::new(1).unwrap(),
        );
        current.bundles = vec![pin("sha256:11", None)];
        let desired = vec![pin("sha256:22", None)];
        assert_eq!(
            changed_component_names(Some(&current), &desired),
            [ComponentName::new("web").unwrap()]
        );
    }

    #[test]
    fn duplicate_artifact_bindings_are_rejected() {
        let requirement = ArtifactRequirement {
            component: "web".to_owned(),
            input: "workerArtifact".to_owned(),
            kind: crate::WorkloadArtifactKind::CloudflareWorker,
            path: "worker.ts".to_owned(),
            source_path: PathBuf::from("component.ts"),
            line: 1,
            column: 1,
        };
        let binding = ArtifactBinding {
            component: ComponentName::new(requirement.component.clone()).unwrap(),
            input: InputName::new(requirement.input.clone()).unwrap(),
            kind: requirement.kind,
            digest: "sha256:1111111111111111111111111111111111111111111111111111111111111111"
                .parse()
                .unwrap(),
            source: PathBuf::from("worker.ts"),
            stored: PathBuf::from("artifacts/11"),
        };
        let error = validate_bindings(&[requirement], &[binding.clone(), binding]).unwrap_err();
        assert!(matches!(error, OperationError::DuplicateArtifact { .. }));
    }
}
