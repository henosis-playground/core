//! Kubernetes publication controller.
//!
//! Each `k8s/object@1` resource owns one directory on the graph branch. The
//! directory and the Kubernetes object both carry the graph and resource
//! `TypeID`s, so a fresh controller instance can observe and retire the
//! resource without process memory.

use std::collections::BTreeMap;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::PerResourceReconciler;
use henosis_controller_runtime::PublicationMode;
use henosis_controller_runtime::ReconcileDecision;
use henosis_controller_runtime::ResourceConvergence;
use henosis_controller_runtime::ResourceGoal;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_controller_runtime::reconcile_absent;
use henosis_controller_runtime::reconcile_slice;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::Resource;
use serde_json::Map;
use serde_json::Value;

const CONTROLLER_NAME: &str = "k8s";
const KIND: &str = "k8s/object";
const GRAPH_LABEL: &str = "henosis.dev/graph-id";
const RESOURCE_LABEL: &str = "henosis.dev/resource-id";

pub struct K8sController {
    name: ControllerName,
    repository: GitRepository,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum K8sObservation {
    Missing,
    Owned(BTreeMap<String, Vec<u8>>),
    Foreign,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct K8sPublishAction {
    files: BTreeMap<String, Vec<u8>>,
}

impl K8sController {
    #[must_use]
    pub fn new(repository: GitRepository) -> Self {
        Self {
            name: controller_name(CONTROLLER_NAME),
            repository,
        }
    }

    async fn reconcile(
        &self,
        slice: &ControllerSlice,
    ) -> Result<ControllerReport, ControllerError> {
        let convergence = match reconcile_slice(self, slice).await {
            Ok(convergence) => convergence,
            Err(error) => {
                return failed_report(slice, error.to_string())
                    .map_err(|report_error| ControllerError::new(report_error.to_string()));
            }
        };
        ready_report(
            slice,
            Some(publication_id(&convergence.evidence)),
            convergence.outputs,
        )
        .map_err(|error| ControllerError::new(error.to_string()))
    }

    async fn remove(
        &self,
        graph_id: GraphId,
        resources: &[Resource],
    ) -> Result<(), ControllerError> {
        reconcile_absent(self, graph_id, resources)
            .await
            .map_err(|error| ControllerError::new(error.to_string()))?;
        if self
            .repository
            .read_directory(&branch(graph_id), "resources")
            .map_err(|error| ControllerError::new(error.to_string()))?
            .is_empty()
        {
            self.repository
                .delete_branch(&branch(graph_id))
                .map_err(|error| ControllerError::new(error.to_string()))?;
        }
        Ok(())
    }
}

impl PerResourceReconciler for K8sController {
    type Action = K8sPublishAction;
    type Error = String;
    type Observation = K8sObservation;

    fn observe<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<Self::Observation, Self::Error>> {
        async move {
            let files = self
                .repository
                .read_directory(&branch(graph_id), &resource_directory(resource))
                .map_err(|error| error.to_string())?;
            if files.is_empty() {
                return Ok(K8sObservation::Missing);
            }
            if ownership_matches(&files, graph_id, resource.id()) {
                Ok(K8sObservation::Owned(files))
            } else {
                Ok(K8sObservation::Foreign)
            }
        }
        .boxed()
    }

    fn diff(
        &self,
        graph_id: GraphId,
        _desired: &[Resource],
        resource: &Resource,
        goal: ResourceGoal,
        observed: &Self::Observation,
    ) -> Result<ReconcileDecision<Self::Action>, Self::Error> {
        if observed == &K8sObservation::Foreign {
            return Err(format!(
                "refusing to mutate Kubernetes publication for {} because its ownership labels do \
                 not match graph {} and resource {}",
                resource.path(),
                graph_id,
                resource.id()
            ));
        }
        match goal {
            ResourceGoal::Absent if observed == &K8sObservation::Missing => {
                Ok(ReconcileDecision::Converged(ResourceConvergence::default()))
            }
            ResourceGoal::Absent => Ok(ReconcileDecision::Act(K8sPublishAction {
                files: BTreeMap::new(),
            })),
            ResourceGoal::Present => {
                let desired = render_resource(graph_id, resource)?;
                if observed == &K8sObservation::Owned(desired.clone()) {
                    Ok(ReconcileDecision::Converged(ResourceConvergence {
                        outputs: Vec::new(),
                        evidence: desired
                            .values()
                            .flat_map(|bytes| bytes.iter().copied())
                            .collect(),
                    }))
                } else {
                    Ok(ReconcileDecision::Act(K8sPublishAction { files: desired }))
                }
            }
        }
    }

    fn act<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
        action: Self::Action,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        async move {
            let directory = resource_directory(resource);
            let files = action
                .files
                .into_iter()
                .map(|(path, bytes)| (format!("{directory}/{path}"), bytes))
                .collect();
            self.repository
                .publish(
                    &branch(graph_id),
                    PublicationMode::ReplaceDirectory(&directory),
                    &files,
                    &format!("Reconcile Kubernetes resource {}", resource.id()),
                )
                .map_err(|error| error.to_string())?;
            Ok(())
        }
        .boxed()
    }
}

impl Controller for K8sController {
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<Option<ControllerReport>, ControllerError>> {
        async move {
            match command {
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).await.map(Some),
                ControllerCommand::Supersede(supersession) => {
                    self.remove(supersession.graph_id, &supersession.resources)
                        .await?;
                    Ok(None)
                }
                ControllerCommand::Retire(retirement) => {
                    self.remove(retirement.graph_id, &retirement.resources)
                        .await?;
                    Ok(None)
                }
            }
        }
        .boxed()
    }
}

fn render_resource(
    graph_id: GraphId,
    resource: &Resource,
) -> Result<BTreeMap<String, Vec<u8>>, String> {
    validate_resource(resource)?;
    let mut body = resource.body().as_json().clone();
    let object = body
        .as_object_mut()
        .expect("resource validation proved the body is an object");
    let metadata = object
        .entry("metadata")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| format!("{} metadata must be an object", resource.path()))?;
    let labels = metadata
        .entry("labels")
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| format!("{} metadata.labels must be an object", resource.path()))?;
    labels.insert(GRAPH_LABEL.into(), Value::String(graph_id.to_string()));
    labels.insert(
        RESOURCE_LABEL.into(),
        Value::String(resource.id().to_string()),
    );
    let yaml = serde_yaml::to_string(&body)
        .map_err(|error| format!("cannot serialize {} as YAML: {error}", resource.path()))?;
    Ok(BTreeMap::from([(
        "resource.yaml".into(),
        format!("---\n{yaml}").into_bytes(),
    )]))
}

fn ownership_matches(
    files: &BTreeMap<String, Vec<u8>>,
    graph_id: GraphId,
    resource_id: henosis_types::ResourceId,
) -> bool {
    let Some(bytes) = files.get("resource.yaml") else {
        return false;
    };
    let Ok(value) = serde_yaml::from_slice::<Value>(bytes) else {
        return false;
    };
    value
        .pointer("/metadata/labels")
        .and_then(Value::as_object)
        .is_some_and(|labels| {
            labels.get(GRAPH_LABEL).and_then(Value::as_str) == Some(graph_id.to_string().as_str())
                && labels.get(RESOURCE_LABEL).and_then(Value::as_str)
                    == Some(resource_id.to_string().as_str())
        })
}

fn validate_resource(resource: &Resource) -> Result<(), String> {
    if resource.kind().name().as_str() != KIND || resource.kind().version().get() != 1 {
        return Err(format!(
            "error[k8s.kind.unsupported]: {} owns {}, expected k8s/object@1\n  = help: emit \
             native Kubernetes objects through @henosis/platform-k8s",
            resource.path(),
            resource.kind()
        ));
    }
    let object = resource.body().as_json().as_object().ok_or_else(|| {
        format!(
            "error[k8s.object.invalid]: {} body is not an object",
            resource.path()
        )
    })?;
    for field in ["apiVersion", "kind"] {
        if !object.get(field).is_some_and(Value::is_string) {
            return Err(format!(
                "error[k8s.object.invalid]: {} is missing string field {field:?}",
                resource.path()
            ));
        }
    }
    Ok(())
}

fn resource_directory(resource: &Resource) -> String {
    format!("resources/{}", resource.id())
}

fn branch(graph_id: GraphId) -> String {
    format!("env/{graph_id}")
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;
    use std::process::Command;

    use henosis_types::ComponentName;
    use henosis_types::ContentDigest;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NewResource;
    use henosis_types::OutputDeclaration;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceId;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[tokio::test]
    async fn converges_one_action_at_a_time_without_flapping() {
        let remote = bare_repository();
        let controller = K8sController::new(GitRepository::new(remote.path()));
        let slice = slice();
        let first = reconcile_slice(&controller, &slice).await.unwrap();
        assert_eq!((first.actions, first.passes), (1, 2));
        let second = reconcile_slice(&controller, &slice).await.unwrap();
        assert_eq!((second.actions, second.passes), (0, 1));
    }

    #[tokio::test]
    async fn fresh_controller_retires_from_target_observation() {
        let remote = bare_repository();
        let slice = slice();
        K8sController::new(GitRepository::new(remote.path()))
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap();
        let restarted = K8sController::new(GitRepository::new(remote.path()));
        restarted
            .execute(&ControllerCommand::Retire(Retirement {
                graph_id: slice.graph_id(),
                last_generation: slice.generation(),
                controller: restarted.name().clone(),
                resources: slice.resources().to_vec(),
            }))
            .await
            .unwrap();
        assert!(!branch_exists(remote.path(), &branch(slice.graph_id())));
    }

    #[tokio::test]
    async fn refuses_matching_path_with_wrong_ownership_labels() {
        let remote = bare_repository();
        let slice = slice();
        let resource = &slice.resources()[0];
        let repository = GitRepository::new(remote.path());
        repository
            .publish(
                &branch(slice.graph_id()),
                PublicationMode::ReplaceDirectory(&resource_directory(resource)),
                &BTreeMap::from([(
                    format!("{}/resource.yaml", resource_directory(resource)),
                    b"---\napiVersion: apps/v1\nkind: Deployment\nmetadata:\n  labels:\n    henosis.dev/graph-id: graph_wrong\n    henosis.dev/resource-id: resource_wrong\n".to_vec(),
                )]),
                "Seed foreign resource",
            )
            .unwrap();
        let before = revision(remote.path(), &branch(slice.graph_id()));
        let error = reconcile_slice(&K8sController::new(repository), &slice)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("ownership labels do not match"));
        assert_eq!(revision(remote.path(), &branch(slice.graph_id())), before);
    }

    fn bare_repository() -> tempfile::TempDir {
        let remote = tempfile::tempdir().unwrap();
        git(remote.path(), ["init", "--bare", "--quiet"]);
        remote
    }

    fn slice() -> ControllerSlice {
        let resource = Resource::new(NewResource {
            id: ResourceId::from_bytes([4; 16]),
            path: ResourcePath::new(
                ComponentName::new("api").unwrap(),
                ResourceAddress::new(
                    KindVersion::new(
                        KindName::new(KIND).unwrap(),
                        NonZeroU32::new(1).unwrap(),
                    ),
                    ResourceName::new("deployment").unwrap(),
                ),
            ),
            controller: controller_name(CONTROLLER_NAME),
            body: serde_json::json!({"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api"}})
                .try_into()
                .unwrap(),
            outputs: Vec::<OutputDeclaration>::new(),
        })
        .unwrap();
        ControllerSlice::new(
            GraphId::from_bytes([3; 16]),
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller_name(CONTROLLER_NAME),
            vec![resource],
            Vec::new(),
        )
    }

    fn revision(remote: &std::path::Path, branch: &str) -> String {
        String::from_utf8(
            Command::new("git")
                .args([
                    "--git-dir",
                    remote.to_str().unwrap(),
                    "rev-parse",
                    &format!("refs/heads/{branch}"),
                ])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .into()
    }

    fn branch_exists(remote: &std::path::Path, branch: &str) -> bool {
        Command::new("git")
            .args([
                "--git-dir",
                remote.to_str().unwrap(),
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ])
            .status()
            .unwrap()
            .success()
    }

    fn git<'a>(current: &std::path::Path, args: impl IntoIterator<Item = &'a str>) {
        assert!(
            Command::new("git")
                .current_dir(current)
                .args(args)
                .status()
                .unwrap()
                .success()
        );
    }
}
