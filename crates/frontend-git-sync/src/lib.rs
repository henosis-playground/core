//! Git workflow frontend for long-lived graph intent.
//!
//! This crate is a peer of the bot and CLI. It translates files on a deploy
//! repository's `main` branch into calls to the public `ConnectRPC`
//! `henosis.v1.GraphService`; it is not a core controller and has no private
//! core-side seam.
//!
//! Files live at `henosis/graphs/<graph-typeid>.toml`:
//!
//! ```toml
//! schema = 1
//! graph = "graph_..."
//! name = "production"
//! generation = 7
//!
//! [[components]]
//! name = "api"
//! bundleDigest = "AQID"
//! ```
//!
//! Generation zero creates a graph. Later edits update it with the generation
//! returned by the preceding public RPC. Removing a file retires the graph.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::error::Error as StdError;

use async_trait::async_trait;
use futures::Stream;
use henosis_controller_runtime::GitError;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::PublicationMode;
use henosis_proto::proto::henosis::v1::ComponentIntent;
use henosis_proto::proto::henosis::v1::CreateGraphRequest;
use henosis_proto::proto::henosis::v1::CreateGraphResponse;
use henosis_proto::proto::henosis::v1::GetGraphRequest;
use henosis_proto::proto::henosis::v1::GetGraphResponse;
use henosis_proto::proto::henosis::v1::GraphStatus;
use henosis_proto::proto::henosis::v1::RetireGraphRequest;
use henosis_proto::proto::henosis::v1::RetireGraphResponse;
use henosis_proto::proto::henosis::v1::UpdateGraphRequest;
use henosis_proto::proto::henosis::v1::UpdateGraphResponse;
use henosis_proto::proto::henosis::v1::WatchGraphRequest;
use henosis_proto::proto::henosis::v1::WatchGraphResponse;
use henosis_types::GraphId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

const DIRECTORY: &str = "henosis/graphs";
const BRANCH: &str = "main";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphIntentFile {
    pub schema: u32,
    pub graph: GraphId,
    pub name: String,
    pub generation: u64,
    pub components: Vec<ComponentIntent>,
}

/// Client boundary matching the public `henosis.v1.GraphService` RPCs.
///
/// A transport adapter can implement this for the generated `ConnectRPC`
/// client. Keeping the transport outside this crate lets the local Git fixture
/// exercise the workflow without introducing a private in-process core API.
#[async_trait]
pub trait GraphService: Send + Sync {
    type Error: StdError + Send + Sync + 'static;
    type WatchStream: Stream<Item = Result<WatchGraphResponse, Self::Error>>
        + Send
        + Unpin
        + 'static;

    async fn create_graph(
        &self,
        request: CreateGraphRequest,
    ) -> Result<CreateGraphResponse, Self::Error>;

    async fn update_graph(
        &self,
        request: UpdateGraphRequest,
    ) -> Result<UpdateGraphResponse, Self::Error>;

    async fn retire_graph(
        &self,
        request: RetireGraphRequest,
    ) -> Result<RetireGraphResponse, Self::Error>;

    async fn get_graph(&self, request: GetGraphRequest) -> Result<GetGraphResponse, Self::Error>;

    async fn watch_graph(
        &self,
        request: WatchGraphRequest,
    ) -> Result<Self::WatchStream, Self::Error>;
}

pub struct GitSyncFrontend<S> {
    repository: GitRepository,
    service: S,
    seen: BTreeMap<GraphId, GraphIntentFile>,
}

impl<S> GitSyncFrontend<S>
where
    S: GraphService,
{
    #[must_use]
    pub fn new(repository: GitRepository, service: S) -> Self {
        Self {
            repository,
            service,
            seen: BTreeMap::new(),
        }
    }

    /// Apply changed Git intent through the public graph service.
    ///
    /// Successful create/update responses are written back to Git so the file's
    /// generation remains the compare-and-swap token for its next edit.
    pub async fn poll_git_intent(&mut self) -> Result<usize, GitSyncError> {
        let files = self.repository.read_directory(BRANCH, DIRECTORY)?;
        let mut incoming = BTreeMap::new();
        for (path, bytes) in files {
            if !path.ends_with(".toml") {
                continue;
            }
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                GitSyncError::Decode(format!("{path}: file is not UTF-8: {error}"))
            })?;
            let intent: GraphIntentFile = toml::from_str(text)
                .map_err(|error| GitSyncError::Decode(format!("{path}: {error}")))?;
            validate(&intent)?;
            let expected = format!("{}.toml", intent.graph);
            if path != expected {
                return Err(GitSyncError::Decode(format!(
                    "{path}: graph identity requires filename {expected}"
                )));
            }
            if incoming.insert(intent.graph, intent).is_some() {
                return Err(GitSyncError::Decode(format!(
                    "duplicate graph identity in {path}"
                )));
            }
        }

        let mut changes = 0;
        let mut acknowledged = false;
        for (graph, intent) in &mut incoming {
            if self.seen.get(graph) == Some(intent) {
                continue;
            }
            let status = if intent.generation == 0 {
                let response = self
                    .service
                    .create_graph(CreateGraphRequest {
                        graph_id: Some(graph.to_string()),
                        name: Some(intent.name.clone()),
                        components: intent.components.clone(),
                        ..CreateGraphRequest::default()
                    })
                    .await
                    .map_err(service_error)?;
                response.status.into_option().ok_or_else(|| {
                    GitSyncError::Service("CreateGraph response omitted status".into())
                })?
            } else {
                let response = self
                    .service
                    .update_graph(UpdateGraphRequest {
                        graph_id: Some(graph.to_string()),
                        expected_generation: Some(intent.generation),
                        components: intent.components.clone(),
                        ..UpdateGraphRequest::default()
                    })
                    .await
                    .map_err(service_error)?;
                response.status.into_option().ok_or_else(|| {
                    GitSyncError::Service("UpdateGraph response omitted status".into())
                })?
            };
            acknowledge(intent, &status)?;
            changes += 1;
            acknowledged = true;
        }

        let incoming_graphs = incoming.keys().copied().collect::<BTreeSet<_>>();
        let retired = self
            .seen
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            .difference(&incoming_graphs)
            .copied()
            .collect::<Vec<_>>();
        for graph in retired {
            let expected_generation = self.seen[&graph].generation;
            let response = self
                .service
                .retire_graph(RetireGraphRequest {
                    graph_id: Some(graph.to_string()),
                    expected_generation: Some(expected_generation),
                    ..RetireGraphRequest::default()
                })
                .await
                .map_err(service_error)?;
            let status = response.status.into_option().ok_or_else(|| {
                GitSyncError::Service("RetireGraph response omitted status".into())
            })?;
            validate_status(graph, &status)?;
            changes += 1;
        }

        if acknowledged {
            let rendered = render_files(&incoming)?;
            self.repository.publish(
                BRANCH,
                PublicationMode::ReplaceDirectory(DIRECTORY),
                &rendered,
                "Acknowledge Henosis graph intent",
            )?;
        }
        self.seen = incoming;
        Ok(changes)
    }

    #[must_use]
    pub fn into_service(self) -> S {
        self.service
    }
}

fn service_error(error: impl StdError) -> GitSyncError {
    GitSyncError::Service(error.to_string())
}

fn acknowledge(intent: &mut GraphIntentFile, status: &GraphStatus) -> Result<(), GitSyncError> {
    validate_status(intent.graph, status)?;
    let generation = status.generation.ok_or_else(|| {
        GitSyncError::Service(format!(
            "GraphService status for {} omitted generation",
            intent.graph
        ))
    })?;
    if generation == 0 {
        return Err(GitSyncError::Service(format!(
            "GraphService returned generation zero for {}",
            intent.graph
        )));
    }
    intent.generation = generation;
    Ok(())
}

fn validate_status(graph: GraphId, status: &GraphStatus) -> Result<(), GitSyncError> {
    let actual = status
        .graph_id
        .as_deref()
        .ok_or_else(|| GitSyncError::Service("GraphService status omitted graph_id".into()))?;
    if actual != graph.to_string() {
        return Err(GitSyncError::Service(format!(
            "GraphService returned status for {actual}, expected {graph}"
        )));
    }
    Ok(())
}

fn render_files(
    intents: &BTreeMap<GraphId, GraphIntentFile>,
) -> Result<BTreeMap<String, Vec<u8>>, GitSyncError> {
    intents
        .iter()
        .map(|(graph, intent)| {
            toml::to_string_pretty(intent)
                .map(|text| (format!("{DIRECTORY}/{graph}.toml"), text.into_bytes()))
                .map_err(GitSyncError::Encode)
        })
        .collect()
}

fn validate(intent: &GraphIntentFile) -> Result<(), GitSyncError> {
    if intent.schema != 1 {
        return Err(GitSyncError::Decode(format!(
            "graph {} uses unsupported intent-file schema {}; expected 1",
            intent.graph, intent.schema
        )));
    }
    if intent.name.is_empty() {
        return Err(GitSyncError::Decode(format!(
            "graph {} requires a name",
            intent.graph
        )));
    }
    for component in &intent.components {
        let name = component.name.as_deref().unwrap_or_default();
        let digest = component.bundle_digest.as_deref().unwrap_or_default();
        if name.is_empty() || digest.is_empty() {
            return Err(GitSyncError::Decode(format!(
                "graph {} components require public ComponentIntent name and bundle_digest fields",
                intent.graph
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum GitSyncError {
    #[error("public GraphService call failed: {0}")]
    Service(String),
    #[error("invalid graph intent file: {0}")]
    Decode(String),
    #[error("cannot encode graph intent file: {0}")]
    Encode(#[source] toml::ser::Error),
    #[error(transparent)]
    Git(#[from] GitError),
}

#[cfg(test)]
mod tests {
    use std::process::Command;
    use std::sync::Mutex;

    use futures::stream;
    use henosis_proto::proto::henosis::v1::GraphStatus;

    use super::*;

    #[derive(Debug, Error)]
    #[error("fake graph service failure")]
    struct FakeError;

    #[derive(Default)]
    struct FakeGraphService {
        created: Mutex<Vec<CreateGraphRequest>>,
        updated: Mutex<Vec<UpdateGraphRequest>>,
        retired: Mutex<Vec<RetireGraphRequest>>,
    }

    #[async_trait]
    impl GraphService for FakeGraphService {
        type Error = FakeError;
        type WatchStream = stream::Empty<Result<WatchGraphResponse, FakeError>>;

        async fn create_graph(
            &self,
            request: CreateGraphRequest,
        ) -> Result<CreateGraphResponse, Self::Error> {
            let graph = request.graph_id.clone().unwrap();
            self.created.lock().unwrap().push(request);
            Ok(CreateGraphResponse {
                status: GraphStatus::default()
                    .with_graph_id(graph)
                    .with_generation(1)
                    .into(),
                ..CreateGraphResponse::default()
            })
        }

        async fn update_graph(
            &self,
            request: UpdateGraphRequest,
        ) -> Result<UpdateGraphResponse, Self::Error> {
            let graph = request.graph_id.clone().unwrap();
            let generation = request.expected_generation.unwrap() + 1;
            self.updated.lock().unwrap().push(request);
            Ok(UpdateGraphResponse {
                status: GraphStatus::default()
                    .with_graph_id(graph)
                    .with_generation(generation)
                    .into(),
                ..UpdateGraphResponse::default()
            })
        }

        async fn retire_graph(
            &self,
            request: RetireGraphRequest,
        ) -> Result<RetireGraphResponse, Self::Error> {
            let graph = request.graph_id.clone().unwrap();
            let generation = request.expected_generation.unwrap() + 1;
            self.retired.lock().unwrap().push(request);
            Ok(RetireGraphResponse {
                status: GraphStatus::default()
                    .with_graph_id(graph)
                    .with_generation(generation)
                    .with_retired(true)
                    .into(),
                ..RetireGraphResponse::default()
            })
        }

        async fn get_graph(
            &self,
            _request: GetGraphRequest,
        ) -> Result<GetGraphResponse, Self::Error> {
            Err(FakeError)
        }

        async fn watch_graph(
            &self,
            _request: WatchGraphRequest,
        ) -> Result<Self::WatchStream, Self::Error> {
            Ok(stream::empty())
        }
    }

    #[tokio::test]
    async fn real_git_fixture_round_trips_edits_and_retirement() {
        let remote = tempfile::tempdir().unwrap();
        initialize_main(remote.path());
        let graph = GraphId::from_bytes([9; 16]);
        edit_remote(remote.path(), graph, Some(intent(graph, 0, 1)));

        let service = FakeGraphService::default();
        let mut frontend = GitSyncFrontend::new(GitRepository::new(remote.path()), service);
        assert_eq!(frontend.poll_git_intent().await.unwrap(), 1);
        assert_eq!(frontend.service.created.lock().unwrap().len(), 1);

        let acknowledged = read_remote(remote.path(), graph);
        assert_eq!(acknowledged.generation, 1);
        edit_remote(remote.path(), graph, Some(intent(graph, 1, 2)));
        assert_eq!(frontend.poll_git_intent().await.unwrap(), 1);
        {
            let updates = frontend.service.updated.lock().unwrap();
            assert_eq!(updates[0].expected_generation, Some(1));
            assert_eq!(
                updates[0].components[0].bundle_digest.as_deref(),
                Some(&[2][..])
            );
        }

        edit_remote(remote.path(), graph, None);
        assert_eq!(frontend.poll_git_intent().await.unwrap(), 1);
        assert_eq!(
            frontend.service.retired.lock().unwrap()[0].expected_generation,
            Some(2)
        );
    }

    fn intent(graph: GraphId, generation: u64, digest: u8) -> GraphIntentFile {
        GraphIntentFile {
            schema: 1,
            graph,
            name: "production".into(),
            generation,
            components: vec![
                ComponentIntent::default()
                    .with_name("api")
                    .with_bundle_digest(vec![digest]),
            ],
        }
    }

    fn initialize_main(remote: &std::path::Path) {
        git(remote, ["init", "--bare", "--quiet"]);
        let checkout = tempfile::tempdir().unwrap();
        git(checkout.path(), ["init", "--quiet", "-b", "main"]);
        git(checkout.path(), ["config", "user.name", "Test"]);
        git(
            checkout.path(),
            ["config", "user.email", "test@example.com"],
        );
        std::fs::write(checkout.path().join("README"), "deploy\n").unwrap();
        git(checkout.path(), ["add", "README"]);
        git(checkout.path(), ["commit", "--quiet", "-m", "initial"]);
        git(
            checkout.path(),
            ["remote", "add", "origin", remote.to_str().unwrap()],
        );
        git(checkout.path(), ["push", "--quiet", "origin", "main"]);
    }

    fn edit_remote(remote: &std::path::Path, graph: GraphId, intent: Option<GraphIntentFile>) {
        let checkout = clone_remote(remote);
        let path = checkout
            .path()
            .join(DIRECTORY)
            .join(format!("{graph}.toml"));
        if let Some(intent) = intent {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, toml::to_string_pretty(&intent).unwrap()).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
        git(checkout.path(), ["add", "-A"]);
        git(checkout.path(), ["commit", "--quiet", "-m", "promotion"]);
        git(checkout.path(), ["push", "--quiet", "origin", "main"]);
    }

    fn read_remote(remote: &std::path::Path, graph: GraphId) -> GraphIntentFile {
        let checkout = clone_remote(remote);
        let text = std::fs::read_to_string(
            checkout
                .path()
                .join(DIRECTORY)
                .join(format!("{graph}.toml")),
        )
        .unwrap();
        toml::from_str(&text).unwrap()
    }

    fn clone_remote(remote: &std::path::Path) -> tempfile::TempDir {
        let checkout = tempfile::tempdir().unwrap();
        git(
            checkout.path(),
            [
                "clone",
                "--quiet",
                "--branch",
                "main",
                remote.to_str().unwrap(),
                ".",
            ],
        );
        git(checkout.path(), ["config", "user.name", "Kargo"]);
        git(
            checkout.path(),
            ["config", "user.email", "kargo@example.com"],
        );
        checkout
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
