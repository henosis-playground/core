//! Bidirectional long-lived graph pin synchronization.
//!
//! This controller intentionally does **not** implement
//! `henosis_types::Controller`: resource slices cannot express graph membership
//! or bundle pins, and translating Git edits back into graph intent
//! is not target reconciliation. Its narrow [`GraphIntentApi`] seam is the
//! stress-test result.
//!
//! Files live at `henosis/graphs/<graph-typeid>.toml` on `main`:
//!
//! ```toml
//! schema = 1
//! graph = "graph_..."
//! generation = 7
//!
//! [components.api]
//! bundle_digest = "blake3:..."
//! source_rev = "0123456789abcdef"
//! ```
//!
//! A file is the complete pin set for one long-lived graph. Removing it
//! requests graph retirement.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use henosis_controller_runtime::GitError;
use henosis_controller_runtime::GitRepository;
use henosis_controller_runtime::PublicationMode;
use henosis_types::GraphId;
use serde::Deserialize;
use serde::Serialize;
use thiserror::Error;

const DIRECTORY: &str = "henosis/graphs";
const BRANCH: &str = "main";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraphPins {
    pub schema: u32,
    pub graph: GraphId,
    pub generation: u64,
    pub components: BTreeMap<String, ComponentPin>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComponentPin {
    pub bundle_digest: String,
    pub source_rev: String,
}

pub trait GraphIntentApi {
    fn long_lived_graphs(&self) -> Result<Vec<GraphPins>, String>;
    fn apply_git_intent(&self, pins: GraphPins) -> Result<(), String>;
    fn retire_git_graph(&self, graph: GraphId) -> Result<(), String>;
}

pub struct GitSyncController<A> {
    repository: GitRepository,
    api: A,
    seen: BTreeMap<GraphId, GraphPins>,
}

impl<A> GitSyncController<A>
where
    A: GraphIntentApi,
{
    #[must_use]
    pub fn new(repository: GitRepository, api: A) -> Self {
        Self {
            repository,
            api,
            seen: BTreeMap::new(),
        }
    }

    pub fn publish_core_state(&mut self) -> Result<bool, GitSyncError> {
        let graphs = self.api.long_lived_graphs().map_err(GitSyncError::Core)?;
        let mut files = BTreeMap::new();
        let mut next = BTreeMap::new();
        for graph in graphs {
            validate(&graph)?;
            let text = toml::to_string_pretty(&graph).map_err(GitSyncError::Encode)?;
            files.insert(
                format!("{DIRECTORY}/{}.toml", graph.graph),
                text.into_bytes(),
            );
            next.insert(graph.graph, graph);
        }
        let publication = self.repository.publish(
            BRANCH,
            PublicationMode::ReplaceDirectory(DIRECTORY),
            &files,
            "Synchronize Henosis graph pins",
        )?;
        self.seen = next;
        Ok(publication.changed)
    }

    pub fn poll_git_intent(&mut self) -> Result<usize, GitSyncError> {
        let files = self.repository.read_directory(BRANCH, DIRECTORY)?;
        let mut incoming = BTreeMap::new();
        for (path, bytes) in files {
            if !path.ends_with(".toml") {
                continue;
            }
            let text = std::str::from_utf8(&bytes).map_err(|error| {
                GitSyncError::Decode(format!("{path}: file is not UTF-8: {error}"))
            })?;
            let pins: GraphPins = toml::from_str(text)
                .map_err(|error| GitSyncError::Decode(format!("{path}: {error}")))?;
            validate(&pins)?;
            let expected = format!("{}.toml", pins.graph);
            if path != expected {
                return Err(GitSyncError::Decode(format!(
                    "{path}: graph identity requires filename {expected}"
                )));
            }
            if incoming.insert(pins.graph, pins).is_some() {
                return Err(GitSyncError::Decode(format!(
                    "duplicate graph identity in {path}"
                )));
            }
        }
        let mut changes = 0;
        for (graph, pins) in &incoming {
            if self.seen.get(graph) != Some(pins) {
                self.api
                    .apply_git_intent(pins.clone())
                    .map_err(GitSyncError::Core)?;
                changes += 1;
            }
        }
        let retired = self
            .seen
            .keys()
            .copied()
            .collect::<BTreeSet<_>>()
            .difference(&incoming.keys().copied().collect())
            .copied()
            .collect::<Vec<_>>();
        for graph in retired {
            self.api
                .retire_git_graph(graph)
                .map_err(GitSyncError::Core)?;
            changes += 1;
        }
        self.seen = incoming;
        Ok(changes)
    }

    #[must_use]
    pub fn into_api(self) -> A {
        self.api
    }
}

fn validate(pins: &GraphPins) -> Result<(), GitSyncError> {
    if pins.schema != 1 {
        return Err(GitSyncError::Decode(format!(
            "graph {} uses unsupported pin-file schema {}; expected 1",
            pins.graph, pins.schema
        )));
    }
    if pins.generation == 0 {
        return Err(GitSyncError::Decode(format!(
            "graph {} generation must be greater than zero",
            pins.graph
        )));
    }
    for (component, pin) in &pins.components {
        if component.is_empty() || pin.bundle_digest.is_empty() || pin.source_rev.is_empty() {
            return Err(GitSyncError::Decode(format!(
                "graph {} component {component:?} requires bundle_digest and source_rev",
                pins.graph
            )));
        }
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum GitSyncError {
    #[error("core graph-intent seam failed: {0}")]
    Core(String),
    #[error("invalid graph pin file: {0}")]
    Decode(String),
    #[error("cannot encode graph pin file: {0}")]
    Encode(#[source] toml::ser::Error),
    #[error(transparent)]
    Git(#[from] GitError),
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::process::Command;

    use super::*;

    #[derive(Default)]
    struct FakeApi {
        desired: RefCell<Vec<GraphPins>>,
        applied: RefCell<Vec<GraphPins>>,
        retired: RefCell<Vec<GraphId>>,
    }

    impl GraphIntentApi for FakeApi {
        fn long_lived_graphs(&self) -> Result<Vec<GraphPins>, String> {
            Ok(self.desired.borrow().clone())
        }

        fn apply_git_intent(&self, pins: GraphPins) -> Result<(), String> {
            self.applied.borrow_mut().push(pins);
            Ok(())
        }

        fn retire_git_graph(&self, graph: GraphId) -> Result<(), String> {
            self.retired.borrow_mut().push(graph);
            Ok(())
        }
    }

    #[test]
    fn real_git_fixture_round_trips_edits_and_retirement() {
        let remote = tempfile::tempdir().unwrap();
        initialize_main(remote.path());
        let graph = GraphId::from_bytes([9; 16]);
        let api = FakeApi::default();
        api.desired.borrow_mut().push(pins(graph, "rev-a"));
        let mut controller = GitSyncController::new(GitRepository::new(remote.path()), api);
        assert!(controller.publish_core_state().unwrap());
        assert!(!controller.publish_core_state().unwrap());

        edit_remote(remote.path(), graph, Some(pins(graph, "rev-b")));
        assert_eq!(controller.poll_git_intent().unwrap(), 1);
        assert_eq!(
            controller.api.applied.borrow()[0].components["api"].source_rev,
            "rev-b"
        );

        edit_remote(remote.path(), graph, None);
        assert_eq!(controller.poll_git_intent().unwrap(), 1);
        assert_eq!(controller.api.retired.borrow().as_slice(), &[graph]);
    }

    fn pins(graph: GraphId, source_rev: &str) -> GraphPins {
        GraphPins {
            schema: 1,
            graph,
            generation: 1,
            components: BTreeMap::from([(
                "api".into(),
                ComponentPin {
                    bundle_digest: "blake3:abc".into(),
                    source_rev: source_rev.into(),
                },
            )]),
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

    fn edit_remote(remote: &std::path::Path, graph: GraphId, pins: Option<GraphPins>) {
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
        let path = checkout
            .path()
            .join(DIRECTORY)
            .join(format!("{graph}.toml"));
        if let Some(pins) = pins {
            std::fs::write(path, toml::to_string_pretty(&pins).unwrap()).unwrap();
        } else {
            std::fs::remove_file(path).unwrap();
        }
        git(checkout.path(), ["add", "-A"]);
        git(checkout.path(), ["commit", "--quiet", "-m", "promotion"]);
        git(checkout.path(), ["push", "--quiet", "origin", "main"]);
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
