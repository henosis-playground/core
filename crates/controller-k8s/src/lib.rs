//! Kubernetes publication controller.
//!
//! `k8s/object@1` bodies are already concrete Kubernetes objects. This
//! controller preserves that vocabulary, writes one stable YAML file per
//! resource under its component instance, and force updates the graph's
//! `env/<graph-typeid>` branch in one Git commit. Publication is the finish
//! line: without a cluster this controller deliberately claims no observed
//! readiness outputs.

use std::collections::BTreeMap;
use std::sync::Mutex;

use futures::FutureExt as _;
use futures::future::BoxFuture;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::PublicationMode;
use henosis_controller_runtime::controller_name;
use henosis_controller_runtime::failed_report;
use henosis_controller_runtime::publication_id;
use henosis_controller_runtime::ready_report;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerReport;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::Resource;
use henosis_types::ResourceId;

const CONTROLLER_NAME: &str = "k8s";
const KIND: &str = "k8s/object";

pub struct K8sController {
    name: ControllerName,
    repository: GitRepository,
    state: Mutex<BTreeMap<GraphId, PublishedGraph>>,
}

#[derive(Clone, Debug)]
struct PublishedGraph {
    files: BTreeMap<String, Vec<u8>>,
    resources: BTreeMap<ResourceId, String>,
    revision: Option<String>,
}

impl K8sController {
    #[must_use]
    pub fn new(repository: GitRepository) -> Self {
        Self {
            name: controller_name(CONTROLLER_NAME),
            repository,
            state: Mutex::new(BTreeMap::new()),
        }
    }

    fn reconcile(&self, slice: &ControllerSlice) -> Result<ControllerReport, ControllerError> {
        let graph = render(slice).map_err(|message| ControllerError::new(message.clone()));
        let graph = match graph {
            Ok(graph) => graph,
            Err(error) => {
                return failed_report(slice, error.to_string())
                    .map_err(|report_error| ControllerError::new(report_error.to_string()));
            }
        };
        if let Some(revision) = self
            .state
            .lock()
            .expect("k8s controller state lock is not poisoned")
            .get(&slice.graph_id())
            .filter(|published| published.files == graph.files)
            .and_then(|published| published.revision.clone())
        {
            return ready_report(slice, Some(publication_id(revision.as_bytes())), Vec::new())
                .map_err(|error| ControllerError::new(error.to_string()));
        }
        let branch = branch(slice.graph_id());
        let publication = self
            .repository
            .publish(
                &branch,
                PublicationMode::ReplaceBranch,
                &graph.files,
                &format!(
                    "Publish Kubernetes graph {} generation {}",
                    slice.graph_id(),
                    slice.generation()
                ),
            )
            .map_err(|error| ControllerError::new(error.to_string()))?;
        let mut graph = graph;
        graph.revision = Some(publication.revision.clone());
        self.state
            .lock()
            .expect("k8s controller state lock is not poisoned")
            .insert(slice.graph_id(), graph);
        ready_report(
            slice,
            Some(publication_id(publication.revision.as_bytes())),
            Vec::new(),
        )
        .map_err(|error| ControllerError::new(error.to_string()))
    }

    fn supersede(
        &self,
        graph_id: GraphId,
        resources: &[Resource],
    ) -> Result<(), ControllerError> {
        let mut state = self
            .state
            .lock()
            .expect("k8s controller state lock is not poisoned");
        let Some(graph) = state.get_mut(&graph_id) else {
            return Ok(());
        };
        for resource in resources {
            if let Some(path) = graph.resources.remove(&resource.id()) {
                graph.files.remove(&path);
            }
        }
        let publication = self
            .repository
            .publish(
                &branch(graph_id),
                PublicationMode::ReplaceBranch,
                &graph.files,
                &format!("Remove superseded Kubernetes resources for {graph_id}"),
            )
            .map_err(|error| ControllerError::new(error.to_string()))?;
        graph.revision = Some(publication.revision);
        Ok(())
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
                ControllerCommand::Reconcile(slice) => self.reconcile(slice).map(Some),
                ControllerCommand::Supersede(supersession) => {
                    self.supersede(supersession.graph_id, &supersession.resources)?;
                    Ok(None)
                }
                ControllerCommand::Retire(retirement) => {
                    self.repository
                        .delete_branch(&branch(retirement.graph_id))
                        .map_err(|error| ControllerError::new(error.to_string()))?;
                    self.state
                        .lock()
                        .expect("k8s controller state lock is not poisoned")
                        .remove(&retirement.graph_id);
                    Ok(None)
                }
            }
        }
        .boxed()
    }
}

fn render(slice: &ControllerSlice) -> Result<PublishedGraph, String> {
    let mut files = BTreeMap::new();
    let mut resources = BTreeMap::new();
    files.insert(
        ".henosis-publication.json".into(),
        format!(
            "{{\"graph\":\"{}\",\"generation\":{},\"planDigest\":\"{}\"}}\n",
            slice.graph_id(),
            slice.generation().ordinal(),
            slice.plan_digest()
        )
        .into_bytes(),
    );
    for resource in slice.resources() {
        validate_resource(resource)?;
        let path = format!(
            "components/{}/{}--{}.yaml",
            resource.path().instance(),
            resource.path().address().name(),
            resource.id()
        );
        let yaml = serde_yaml::to_string(resource.body().as_json())
            .map_err(|error| format!("cannot serialize {} as YAML: {error}", resource.path()))?;
        files.insert(path.clone(), format!("---\n{yaml}").into_bytes());
        resources.insert(resource.id(), path);
    }
    Ok(PublishedGraph {
        files,
        resources,
        revision: None,
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
        if !object.get(field).is_some_and(serde_json::Value::is_string) {
            return Err(format!(
                "error[k8s.object.invalid]: {} is missing string field {field:?}",
                resource.path()
            ));
        }
    }
    Ok(())
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
    use henosis_types::ControllerCommand;
    use henosis_types::ControllerSlice;
    use henosis_types::Generation;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NewResource;
    use henosis_types::OutputDeclaration;
    use henosis_types::ResourceAddress;
    use henosis_types::ResourceName;
    use henosis_types::ResourcePath;
    use henosis_types::Retirement;

    use super::*;

    #[tokio::test]
    async fn publishes_idempotently_and_retires_the_branch() {
        let remote = tempfile::tempdir().unwrap();
        git(remote.path(), ["init", "--bare", "--quiet"]);
        let controller = K8sController::new(GitRepository::new(remote.path()));
        let slice = slice();
        let report = controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.dispositions().len(), 1);
        assert!(report.outputs().next().is_none());
        let branch = branch(slice.graph_id());
        let first = revision(remote.path(), &branch);
        controller
            .execute(&ControllerCommand::Reconcile(slice.clone()))
            .await
            .unwrap();
        assert_eq!(revision(remote.path(), &branch), first);
        let checkout = tempfile::tempdir().unwrap();
        git(
            checkout.path(),
            [
                "clone",
                "--quiet",
                "--branch",
                &branch,
                remote.path().to_str().unwrap(),
                ".",
            ],
        );
        let component_directory = checkout.path().join("components/api");
        let yaml_files = std::fs::read_dir(component_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .is_some_and(|extension| extension == "yaml")
            })
            .count();
        assert_eq!(yaml_files, 1);
        controller
            .execute(&ControllerCommand::Retire(Retirement {
                graph_id: slice.graph_id(),
                last_generation: slice.generation(),
                controller: controller.name().clone(),
                resources: vec![slice.resources()[0].clone()],
            }))
            .await
            .unwrap();
        let status = Command::new("git")
            .args([
                "--git-dir",
                remote.path().to_str().unwrap(),
                "rev-parse",
                "--verify",
                "--quiet",
                &format!("refs/heads/{branch}"),
            ])
            .output()
            .unwrap()
            .status;
        assert!(!status.success());
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
