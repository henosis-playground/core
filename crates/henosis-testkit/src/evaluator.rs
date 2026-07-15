use std::collections::BTreeMap;
use std::num::NonZeroU32;
use std::sync::RwLock;

use futures::future::BoxFuture;
use henosis_types::BlockedDetail;
use henosis_types::BundleRef;
use henosis_types::ComponentName;
use henosis_types::ContentDigest;
use henosis_types::ControllerName;
use henosis_types::EvaluationAttempt;
use henosis_types::EvaluationError;
use henosis_types::EvaluationRequest;
use henosis_types::EvaluationResource;
use henosis_types::Evaluator;
use henosis_types::InputCellState;
use henosis_types::InputName;
use henosis_types::KindName;
use henosis_types::KindVersion;
use henosis_types::NativeValue;
use henosis_types::NewBlockedEvaluation;
use henosis_types::NewCompleteEvaluation;
use henosis_types::NewEvaluationResource;
use henosis_types::ObservedOutputBinding;
use henosis_types::OutputAvailability;
use henosis_types::OutputDeclaration;
use henosis_types::OutputName;
use henosis_types::ResourceAddress;
use henosis_types::ResourceId;
use henosis_types::ResourceName;
use henosis_types::StaticOutput;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResourceProgram {
    pub id: ResourceId,
    pub name: ResourceName,
    pub controller: ControllerName,
    pub required_values: Vec<InputName>,
    pub observed_component_output: Option<OutputName>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ComponentProgram {
    pub resources: Vec<ResourceProgram>,
    pub static_outputs: BTreeMap<OutputName, NativeValue>,
}

#[derive(Debug, Default)]
pub struct ProgramEvaluator {
    programs: RwLock<BTreeMap<ContentDigest, ComponentProgram>>,
}

impl ProgramEvaluator {
    pub fn register(&self, program: ComponentProgram) -> BundleRef {
        let encoded = format!("{program:?}");
        let digest = ContentDigest::digest(encoded.as_bytes());
        self.programs
            .write()
            .expect("program registry lock is not poisoned")
            .insert(digest, program);
        BundleRef::new(digest)
    }

    fn evaluate_program(
        request: &EvaluationRequest,
        program: &ComponentProgram,
    ) -> Result<EvaluationAttempt, EvaluationError> {
        let blocked = program.resources.iter().find_map(|resource| {
            resource.required_values.iter().find_map(|input| {
                let cell = request.snapshot().get(input)?;
                matches!(cell.state(), InputCellState::Blocked).then(|| cell.clone())
            })
        });
        if let Some(cell) = blocked {
            return EvaluationAttempt::blocked(
                request.snapshot(),
                NewBlockedEvaluation {
                    resources: Vec::new(),
                    blocked: BlockedDetail::new(
                        cell.name().clone(),
                        cell.output_source()
                            .expect("only output-sourced inputs can block")
                            .clone(),
                        "read value",
                        "waiting for deterministic input",
                    ),
                    reads: vec![cell.name().clone()],
                },
            )
            .map_err(|error| EvaluationError::new(error.to_string()));
        }
        let mut resources = Vec::new();
        let mut bindings = Vec::new();
        for resource in &program.resources {
            let body = request
                .snapshot()
                .iter()
                .filter(|cell| resource.required_values.contains(cell.name()))
                .filter_map(|cell| match cell.state() {
                    InputCellState::Available(value) => {
                        Some((cell.name().as_str().to_owned(), value.as_json().clone()))
                    }
                    InputCellState::Blocked | InputCellState::Absent => None,
                })
                .collect::<serde_json::Map<_, _>>();
            let observed = resource
                .observed_component_output
                .as_ref()
                .map(|name| OutputDeclaration::new(name.clone(), OutputAvailability::Observed));
            let address = ResourceAddress::new(kind(), resource.name.clone());
            let value = serde_json::Value::Object(body);
            let native = NativeValue::new(value.clone())
                .map_err(|error| EvaluationError::new(error.to_string()))?;
            resources.push(
                EvaluationResource::new(NewEvaluationResource {
                    id: resource.id,
                    component: request.component().clone(),
                    kind: kind(),
                    name: resource.name.clone(),
                    controller: resource.controller.clone(),
                    body: value,
                    canonical: native.canonical().to_owned(),
                    outputs: observed.iter().cloned().collect(),
                })
                .map_err(|error| EvaluationError::new(error.to_string()))?,
            );
            if let Some(name) = &resource.observed_component_output {
                bindings.push(ObservedOutputBinding::new(
                    name.clone(),
                    address,
                    name.clone(),
                ));
            }
        }
        let outputs = program
            .static_outputs
            .iter()
            .map(|(name, value)| StaticOutput::new(name.clone(), value.clone()))
            .collect();
        let mut reads = program
            .resources
            .iter()
            .flat_map(|resource| resource.required_values.clone())
            .collect::<Vec<_>>();
        reads.sort();
        reads.dedup();
        EvaluationAttempt::complete(
            request.snapshot(),
            NewCompleteEvaluation {
                resources,
                outputs,
                observed_outputs: bindings,
                reads,
            },
        )
        .map_err(|error| EvaluationError::new(error.to_string()))
    }
}

impl Evaluator for ProgramEvaluator {
    fn evaluate<'a>(
        &'a self,
        request: EvaluationRequest,
    ) -> BoxFuture<'a, Result<EvaluationAttempt, EvaluationError>> {
        Box::pin(async move {
            let program = self
                .programs
                .read()
                .expect("program registry lock is not poisoned")
                .get(&request.bundle().digest())
                .cloned()
                .ok_or_else(|| EvaluationError::new("unregistered deterministic program"))?;
            Self::evaluate_program(&request, &program)
        })
    }
}

fn kind() -> KindVersion {
    KindVersion::new(
        KindName::new("test/resource").expect("fixture kind is valid"),
        NonZeroU32::new(1).expect("one is non-zero"),
    )
}

#[allow(dead_code)]
fn _component_name_is_part_of_protocol(_: &ComponentName) {}
