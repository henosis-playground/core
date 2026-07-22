//! Small controller-side primitives shared by target adapters.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use base64::Engine as _;
use futures::future::BoxFuture;
use henosis_types::ArtifactDigest;
use henosis_types::ArtifactStore;
use henosis_types::ArtifactStoreError;
use henosis_types::ControllerCommand;
use henosis_types::ControllerName;
use henosis_types::ControllerPass;
use henosis_types::ControllerReport;
use henosis_types::ControllerReportError;
use henosis_types::ControllerSlice;
use henosis_types::GraphId;
use henosis_types::NativeValue;
use henosis_types::NewControllerReport;
use henosis_types::ObservedOutput;
use henosis_types::ObservedOutputKey;
use henosis_types::OutputName;
use henosis_types::PublicationId;
use henosis_types::Resource;
use henosis_types::ResourceDisposition;
use henosis_types::ResourceDispositionKind;
use sha2::Digest as _;
use sha2::Sha256;
use thiserror::Error;

// === PER-RESOURCE RECONCILIATION ===

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceGoal {
    Present,
    Absent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileDecision<Action> {
    Act(Action),
    Converged(ResourceConvergence),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ResourceConvergence {
    pub outputs: Vec<ObservedOutput>,
    pub evidence: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SlicePass {
    Acted,
    Converged(SliceConvergence),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SliceConvergence {
    pub outputs: Vec<ObservedOutput>,
    pub evidence: Vec<u8>,
}

pub trait PerResourceReconciler: Send + Sync {
    type Observation: Send;
    type Action: Send;
    type Error;

    fn rank(&self, _resource: &Resource, _goal: ResourceGoal) -> u8 {
        0
    }

    fn observe<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
    ) -> BoxFuture<'a, Result<Self::Observation, Self::Error>>;

    fn diff(
        &self,
        graph_id: GraphId,
        resource: &Resource,
        goal: ResourceGoal,
        observed: &Self::Observation,
    ) -> Result<ReconcileDecision<Self::Action>, Self::Error>;

    fn act<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
        action: Self::Action,
    ) -> BoxFuture<'a, Result<(), Self::Error>>;

    fn act_observed<'a>(
        &'a self,
        graph_id: GraphId,
        resource: &'a Resource,
        _observed: &'a Self::Observation,
        action: Self::Action,
    ) -> BoxFuture<'a, Result<(), Self::Error>> {
        self.act(graph_id, resource, action)
    }
}

pub async fn reconcile_slice<R>(
    reconciler: &R,
    slice: &ControllerSlice,
) -> Result<SlicePass, R::Error>
where
    R: PerResourceReconciler,
{
    let mut convergence = SliceConvergence::default();
    let mut desired = slice.resources().iter().collect::<Vec<_>>();
    desired.sort_by_key(|resource| {
        (
            reconciler.rank(resource, ResourceGoal::Present),
            resource.id(),
        )
    });
    for resource in desired {
        match reconcile_resource(
            reconciler,
            slice.graph_id(),
            resource,
            ResourceGoal::Present,
        )
        .await?
        {
            ResourcePass::Acted => return Ok(SlicePass::Acted),
            ResourcePass::Converged(resource_convergence) => {
                convergence.outputs.extend(resource_convergence.outputs);
                convergence
                    .evidence
                    .extend_from_slice(resource.id().to_string().as_bytes());
                convergence.evidence.push(b'=');
                convergence
                    .evidence
                    .extend_from_slice(&resource_convergence.evidence);
                convergence.evidence.push(b';');
            }
        }
    }
    let mut superseded = slice.superseded().iter().collect::<Vec<_>>();
    superseded.sort_by_key(|resource| {
        (
            reconciler.rank(resource, ResourceGoal::Absent),
            resource.id(),
        )
    });
    for resource in superseded {
        if reconcile_resource(reconciler, slice.graph_id(), resource, ResourceGoal::Absent).await?
            == ResourcePass::Acted
        {
            return Ok(SlicePass::Acted);
        }
    }
    Ok(SlicePass::Converged(convergence))
}

pub async fn reconcile_absent<R>(
    reconciler: &R,
    graph_id: GraphId,
    resources: &[Resource],
) -> Result<SlicePass, R::Error>
where
    R: PerResourceReconciler,
{
    let mut resources = resources.iter().collect::<Vec<_>>();
    resources.sort_by_key(|resource| {
        (
            reconciler.rank(resource, ResourceGoal::Absent),
            resource.id(),
        )
    });
    for resource in resources {
        if reconcile_resource(reconciler, graph_id, resource, ResourceGoal::Absent).await?
            == ResourcePass::Acted
        {
            return Ok(SlicePass::Acted);
        }
    }
    Ok(SlicePass::Converged(SliceConvergence::default()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ResourcePass {
    Acted,
    Converged(ResourceConvergence),
}

async fn reconcile_resource<R>(
    reconciler: &R,
    graph_id: GraphId,
    resource: &Resource,
    goal: ResourceGoal,
) -> Result<ResourcePass, R::Error>
where
    R: PerResourceReconciler,
{
    let observed = reconciler.observe(graph_id, resource).await?;
    match reconciler.diff(graph_id, resource, goal, &observed)? {
        ReconcileDecision::Converged(convergence) => Ok(ResourcePass::Converged(convergence)),
        ReconcileDecision::Act(action) => {
            reconciler
                .act_observed(graph_id, resource, &observed, action)
                .await?;
            Ok(ResourcePass::Acted)
        }
    }
}

// === CONTROLLER SCHEDULING ===

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ControllerWorkKey {
    graph_id: GraphId,
    controller: ControllerName,
}

impl ControllerWorkKey {
    #[must_use]
    pub const fn new(graph_id: GraphId, controller: ControllerName) -> Self {
        Self {
            graph_id,
            controller,
        }
    }

    #[must_use]
    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }
}

#[derive(Clone, Debug, Default)]
pub struct ControllerDesiredState {
    commands: BTreeMap<ControllerWorkKey, ControllerCommand>,
}

impl ControllerDesiredState {
    pub fn update(
        &mut self,
        controller: ControllerName,
        command: ControllerCommand,
    ) -> ControllerWorkKey {
        let key = ControllerWorkKey::new(command.graph_id(), controller);
        let command = merge_current_command(self.commands.get(&key), command);
        self.commands.insert(key.clone(), command);
        key
    }

    #[must_use]
    pub fn current(&self, key: &ControllerWorkKey) -> Option<&ControllerCommand> {
        self.commands.get(key)
    }
}

fn merge_current_command(
    previous: Option<&ControllerCommand>,
    mut current: ControllerCommand,
) -> ControllerCommand {
    let mut cleanup = BTreeMap::new();
    for command in previous.into_iter().chain(std::iter::once(&current)) {
        let resources = match command {
            ControllerCommand::Reconcile(slice) => slice.superseded(),
            ControllerCommand::Supersede(command) => &command.resources,
            ControllerCommand::Retire(command) => &command.resources,
        };
        cleanup.extend(
            resources
                .iter()
                .cloned()
                .map(|resource| (resource.id(), resource)),
        );
    }
    match &mut current {
        ControllerCommand::Reconcile(slice) => {
            for resource in slice.resources() {
                cleanup.remove(&resource.id());
            }
            *slice = slice
                .clone()
                .with_cleanup_obligations(cleanup.into_values().collect());
        }
        ControllerCommand::Supersede(command) => {
            command.resources = cleanup.into_values().collect();
        }
        ControllerCommand::Retire(command) => {
            command.resources = cleanup.into_values().collect();
        }
    }
    current
}

#[derive(Clone, Debug)]
pub struct ScheduledControllerPass {
    key: ControllerWorkKey,
    command: ControllerCommand,
}

impl ScheduledControllerPass {
    #[must_use]
    pub const fn key(&self) -> &ControllerWorkKey {
        &self.key
    }

    #[must_use]
    pub const fn command(&self) -> &ControllerCommand {
        &self.command
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScheduledControllerReport {
    key: ControllerWorkKey,
    report: ControllerReport,
}

impl ScheduledControllerReport {
    #[must_use]
    pub const fn key(&self) -> &ControllerWorkKey {
        &self.key
    }

    #[must_use]
    pub const fn report(&self) -> &ControllerReport {
        &self.report
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerScheduleCompletion {
    Continue,
    Report(Box<ScheduledControllerReport>),
    Retry { attempt: u32, message: String },
    Complete,
}

#[derive(Clone, Debug)]
struct ScheduledControllerWork {
    dirty: bool,
    in_flight: bool,
    pending_report: Option<Box<ControllerReport>>,
    retry_attempt: u32,
}

#[derive(Clone, Debug, Default)]
pub struct ControllerSchedule {
    work: BTreeMap<ControllerWorkKey, ScheduledControllerWork>,
}

impl ControllerSchedule {
    #[must_use]
    pub fn submit(&mut self, key: ControllerWorkKey) -> Option<ControllerWorkKey> {
        if let Some(work) = self.work.get_mut(&key) {
            work.dirty = true;
            work.retry_attempt = 0;
            (!work.in_flight).then_some(key)
        } else {
            self.work.insert(
                key.clone(),
                ScheduledControllerWork {
                    dirty: true,
                    in_flight: false,
                    pending_report: None,
                    retry_attempt: 0,
                },
            );
            Some(key)
        }
    }

    #[must_use]
    pub fn pass(
        &mut self,
        key: &ControllerWorkKey,
        desired: &ControllerDesiredState,
    ) -> Option<ScheduledControllerPass> {
        let command = desired.current(key)?.clone();
        let work = self.work.get_mut(key)?;
        if work.in_flight || !work.dirty {
            return None;
        }
        work.dirty = false;
        work.in_flight = true;
        Some(ScheduledControllerPass {
            key: key.clone(),
            command,
        })
    }

    pub fn complete(
        &mut self,
        pass: &ScheduledControllerPass,
        outcome: ControllerPass,
    ) -> ControllerScheduleCompletion {
        let Some(current) = self.work.get_mut(&pass.key) else {
            return ControllerScheduleCompletion::Complete;
        };
        current.in_flight = false;
        if current.dirty {
            return ControllerScheduleCompletion::Continue;
        }
        match outcome {
            ControllerPass::Acted => {
                current.retry_attempt = 0;
                current.dirty = true;
                ControllerScheduleCompletion::Continue
            }
            ControllerPass::Converged(Some(report)) | ControllerPass::Failed(report) => {
                current.retry_attempt = 0;
                current.pending_report = Some(Box::new(report.clone()));
                ControllerScheduleCompletion::Report(Box::new(ScheduledControllerReport {
                    key: pass.key.clone(),
                    report,
                }))
            }
            ControllerPass::Converged(None) => {
                if current.pending_report.is_some() {
                    ControllerScheduleCompletion::Complete
                } else {
                    self.work.remove(&pass.key);
                    ControllerScheduleCompletion::Complete
                }
            }
            ControllerPass::Retryable(message) => {
                current.retry_attempt = current.retry_attempt.saturating_add(1);
                current.dirty = true;
                ControllerScheduleCompletion::Retry {
                    attempt: current.retry_attempt,
                    message,
                }
            }
        }
    }

    pub fn acknowledge_report(
        &mut self,
        delivered: &ScheduledControllerReport,
    ) -> ControllerScheduleCompletion {
        let Some(current) = self.work.get_mut(&delivered.key) else {
            return ControllerScheduleCompletion::Complete;
        };
        if current.pending_report.as_deref() != Some(&delivered.report) {
            return ControllerScheduleCompletion::Complete;
        }
        current.pending_report = None;
        if current.dirty && !current.in_flight {
            ControllerScheduleCompletion::Continue
        } else if current.dirty || current.in_flight {
            ControllerScheduleCompletion::Complete
        } else {
            self.work.remove(&delivered.key);
            ControllerScheduleCompletion::Complete
        }
    }

    pub fn passes<'a>(
        &'a self,
        desired: &'a ControllerDesiredState,
    ) -> impl Iterator<Item = ScheduledControllerPass> + 'a {
        self.work
            .iter()
            .filter(|(_, work)| !work.in_flight && work.dirty)
            .map(|(key, _)| ScheduledControllerPass {
                key: key.clone(),
                command: desired
                    .current(key)
                    .expect("scheduled controller key has current desired state")
                    .clone(),
            })
    }
}

// === ATOMIC REPORTS ===

pub fn ready_report(
    slice: &ControllerSlice,
    publication: Option<PublicationId>,
    outputs: Vec<ObservedOutput>,
) -> Result<ControllerReport, ControllerReportError> {
    ControllerReport::new(NewControllerReport {
        graph_id: slice.graph_id(),
        generation: slice.generation(),
        plan_digest: slice.plan_digest(),
        controller: slice.controller().clone(),
        publication_id: publication,
        dispositions: slice
            .resources()
            .iter()
            .map(|resource| ResourceDisposition::new(resource.id(), ResourceDispositionKind::Ready))
            .collect(),
        outputs,
    })
}

pub fn failed_report(
    slice: &ControllerSlice,
    message: impl Into<String>,
) -> Result<ControllerReport, ControllerReportError> {
    let message = message.into();
    ControllerReport::new(NewControllerReport {
        graph_id: slice.graph_id(),
        generation: slice.generation(),
        plan_digest: slice.plan_digest(),
        controller: slice.controller().clone(),
        publication_id: None,
        dispositions: slice
            .resources()
            .iter()
            .map(|resource| {
                ResourceDisposition::new(
                    resource.id(),
                    ResourceDispositionKind::Failed {
                        message: message.clone(),
                    },
                )
            })
            .collect(),
        outputs: Vec::new(),
    })
}

pub fn output(
    resource: &Resource,
    name: &str,
    value: serde_json::Value,
) -> Result<ObservedOutput, ReportBuildError> {
    let name =
        OutputName::new(name).map_err(|error| ReportBuildError::Output(error.to_string()))?;
    let declaration = resource.output(&name).ok_or_else(|| {
        ReportBuildError::Output(format!(
            "{} does not declare output {name}",
            resource.path()
        ))
    })?;
    if declaration.availability() != henosis_types::OutputAvailability::Observed {
        return Err(ReportBuildError::Output(format!(
            "{}.{} is not an observed output",
            resource.path(),
            name
        )));
    }
    let value =
        NativeValue::new(value).map_err(|error| ReportBuildError::Output(error.to_string()))?;
    Ok(ObservedOutput::new(
        ObservedOutputKey::new(resource.id(), name),
        value,
    ))
}

#[derive(Debug, Error)]
pub enum ReportBuildError {
    #[error("cannot build controller output: {0}")]
    Output(String),
}

pub fn controller_name(value: &str) -> ControllerName {
    ControllerName::new(value).expect("built-in controller names are valid")
}

pub fn publication_id(evidence: &[u8]) -> PublicationId {
    let digest = blake3::hash(evidence);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    PublicationId::from_bytes(bytes)
}

// === CONTENT-ADDRESSED FILE STORES ===

#[derive(Clone, Debug)]
pub struct DirectoryArtifactStore {
    root: PathBuf,
}

impl DirectoryArtifactStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    #[must_use]
    pub fn path(&self, digest: ArtifactDigest) -> PathBuf {
        self.root
            .join("sha256")
            .join(hex::encode(digest.as_bytes()))
    }
}

impl ArtifactStore for DirectoryArtifactStore {
    fn fetch(
        &self,
        digest: ArtifactDigest,
    ) -> BoxFuture<'_, Result<Arc<[u8]>, ArtifactStoreError>> {
        let path = self.path(digest);
        Box::pin(async move {
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(ArtifactStoreError::Missing { digest });
                }
                Err(error) => {
                    return Err(ArtifactStoreError::Unavailable {
                        digest,
                        message: error.to_string(),
                    });
                }
            };
            let actual = ArtifactDigest::from_bytes(Sha256::digest(&bytes).into());
            if actual != digest {
                return Err(ArtifactStoreError::DigestMismatch { digest, actual });
            }
            Ok(Arc::from(bytes))
        })
    }
}

// === GIT TARGET ===

#[derive(Clone, Debug)]
pub struct GitRepository {
    remote: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationMode<'a> {
    ReplaceBranch,
    ReplaceDirectory(&'a str),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GitPublication {
    pub revision: String,
    pub changed: bool,
}

impl GitRepository {
    #[must_use]
    pub fn new(remote: impl Into<PathBuf>) -> Self {
        Self {
            remote: remote.into(),
        }
    }

    pub fn publish(
        &self,
        branch: &str,
        mode: PublicationMode<'_>,
        files: &BTreeMap<String, Vec<u8>>,
        message: &str,
    ) -> Result<GitPublication, GitError> {
        self.publish_inner(branch, mode, None, files, message, || {})
    }

    pub fn publish_directory_if_unchanged(
        &self,
        branch: &str,
        prefix: &str,
        expected: &BTreeMap<String, Vec<u8>>,
        files: &BTreeMap<String, Vec<u8>>,
        message: &str,
    ) -> Result<GitPublication, GitError> {
        self.publish_inner(
            branch,
            PublicationMode::ReplaceDirectory(prefix),
            Some((prefix, expected)),
            files,
            message,
            || {},
        )
    }

    #[cfg(test)]
    fn publish_with_before_push(
        &self,
        branch: &str,
        mode: PublicationMode<'_>,
        files: &BTreeMap<String, Vec<u8>>,
        message: &str,
        before_push: impl FnOnce(),
    ) -> Result<GitPublication, GitError> {
        self.publish_inner(branch, mode, None, files, message, before_push)
    }

    fn publish_inner(
        &self,
        branch: &str,
        mode: PublicationMode<'_>,
        expected_directory: Option<(&str, &BTreeMap<String, Vec<u8>>)>,
        files: &BTreeMap<String, Vec<u8>>,
        message: &str,
        before_push: impl FnOnce(),
    ) -> Result<GitPublication, GitError> {
        let directory = tempfile::tempdir().map_err(GitError::Io)?;
        run_git(
            None,
            [
                "clone",
                "--quiet",
                remote_text(&self.remote)?,
                directory.path().to_str().ok_or(GitError::NonUtf8Path)?,
            ],
        )?;
        configure(directory.path())?;
        let exists = run_git_status(
            Some(directory.path()),
            [
                "show-ref",
                "--verify",
                "--quiet",
                &format!("refs/remotes/origin/{branch}"),
            ],
        )?;
        let expected_revision = if exists {
            run_git(
                Some(directory.path()),
                [
                    "checkout",
                    "--quiet",
                    "-B",
                    branch,
                    &format!("origin/{branch}"),
                ],
            )?;
            git_output(
                Some(directory.path()),
                ["rev-parse", &format!("origin/{branch}")],
            )?
        } else {
            run_git(
                Some(directory.path()),
                ["checkout", "--quiet", "--orphan", branch],
            )?;
            clear_worktree(directory.path(), None)?;
            String::new()
        };
        if let Some((prefix, expected)) = expected_directory {
            let root = directory.path().join(prefix);
            let mut actual = BTreeMap::new();
            if root.exists() {
                collect_files(&root, &root, &mut actual)?;
            }
            if &actual != expected {
                return Err(GitError::PreconditionFailed);
            }
        }
        match mode {
            PublicationMode::ReplaceBranch => clear_worktree(directory.path(), None)?,
            PublicationMode::ReplaceDirectory(prefix) => {
                clear_worktree(directory.path(), Some(prefix))?;
            }
        }
        for (relative, bytes) in files {
            let path = directory.path().join(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).map_err(GitError::Io)?;
            }
            fs::write(path, bytes).map_err(GitError::Io)?;
        }
        run_git(Some(directory.path()), ["add", "-A"])?;
        let unchanged = run_git_status(Some(directory.path()), ["diff", "--cached", "--quiet"])?;
        if unchanged {
            let revision = git_output(Some(directory.path()), ["rev-parse", "HEAD"])?;
            return Ok(GitPublication {
                revision,
                changed: false,
            });
        }
        run_git(Some(directory.path()), ["commit", "--quiet", "-m", message])?;
        before_push();
        let lease = format!("--force-with-lease=refs/heads/{branch}:{expected_revision}");
        run_git(
            Some(directory.path()),
            [
                "push",
                "--quiet",
                &lease,
                "origin",
                &format!("HEAD:refs/heads/{branch}"),
            ],
        )?;
        Ok(GitPublication {
            revision: git_output(Some(directory.path()), ["rev-parse", "HEAD"])?,
            changed: true,
        })
    }

    pub fn delete_branch(&self, branch: &str) -> Result<bool, GitError> {
        let exists = run_git_status(
            None,
            [
                "ls-remote",
                "--exit-code",
                "--heads",
                remote_text(&self.remote)?,
                &format!("refs/heads/{branch}"),
            ],
        )?;
        if !exists {
            return Ok(false);
        }
        let directory = tempfile::tempdir().map_err(GitError::Io)?;
        run_git(
            None,
            [
                "init",
                "--quiet",
                directory.path().to_str().ok_or(GitError::NonUtf8Path)?,
            ],
        )?;
        let result = git_command()?
            .current_dir(directory.path())
            .args([
                "push",
                remote_text(&self.remote)?,
                &format!(":refs/heads/{branch}"),
            ])
            .output()
            .map_err(GitError::Io)?;
        if result.status.success() {
            return Ok(true);
        }
        let detail = String::from_utf8_lossy(&result.stderr);
        if detail.contains("remote ref does not exist") {
            return Ok(false);
        }
        Err(GitError::Command(detail.into_owned()))
    }

    pub fn read_directory(
        &self,
        branch: &str,
        prefix: &str,
    ) -> Result<BTreeMap<String, Vec<u8>>, GitError> {
        let exists = run_git_status(
            None,
            [
                "ls-remote",
                "--exit-code",
                "--heads",
                remote_text(&self.remote)?,
                &format!("refs/heads/{branch}"),
            ],
        )?;
        if !exists {
            return Ok(BTreeMap::new());
        }
        let directory = tempfile::tempdir().map_err(GitError::Io)?;
        run_git(
            None,
            [
                "clone",
                "--quiet",
                "--branch",
                branch,
                "--single-branch",
                remote_text(&self.remote)?,
                directory.path().to_str().ok_or(GitError::NonUtf8Path)?,
            ],
        )?;
        let root = directory.path().join(prefix);
        let mut files = BTreeMap::new();
        if root.exists() {
            collect_files(&root, &root, &mut files)?;
        }
        Ok(files)
    }
}

fn collect_files(
    root: &Path,
    current: &Path,
    files: &mut BTreeMap<String, Vec<u8>>,
) -> Result<(), GitError> {
    for entry in fs::read_dir(current).map_err(GitError::Io)? {
        let entry = entry.map_err(GitError::Io)?;
        if entry.file_type().map_err(GitError::Io)?.is_dir() {
            collect_files(root, &entry.path(), files)?;
        } else {
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|error| GitError::Command(error.to_string()))?
                .to_string_lossy()
                .replace('\\', "/");
            files.insert(relative, fs::read(entry.path()).map_err(GitError::Io)?);
        }
    }
    Ok(())
}

fn clear_worktree(root: &Path, prefix: Option<&str>) -> Result<(), GitError> {
    let target = prefix
        .map(|prefix| root.join(prefix))
        .unwrap_or_else(|| root.to_path_buf());
    if !target.exists() {
        return Ok(());
    }
    if prefix.is_some() {
        fs::remove_dir_all(target).map_err(GitError::Io)?;
        return Ok(());
    }
    for entry in fs::read_dir(root).map_err(GitError::Io)? {
        let entry = entry.map_err(GitError::Io)?;
        if entry.file_name() == ".git" {
            continue;
        }
        if entry.file_type().map_err(GitError::Io)?.is_dir() {
            fs::remove_dir_all(entry.path()).map_err(GitError::Io)?;
        } else {
            fs::remove_file(entry.path()).map_err(GitError::Io)?;
        }
    }
    Ok(())
}

fn configure(root: &Path) -> Result<(), GitError> {
    run_git(Some(root), ["config", "user.name", "Henosis Controller"])?;
    run_git(
        Some(root),
        ["config", "user.email", "controller@henosis.dev"],
    )?;
    run_git(Some(root), ["config", "commit.gpgsign", "false"])
}

fn remote_text(path: &Path) -> Result<&str, GitError> {
    path.to_str().ok_or(GitError::NonUtf8Path)
}

fn run_git<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<(), GitError> {
    let mut command = git_command()?;
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let output = command.output().map_err(GitError::Io)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(GitError::Command(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ))
    }
}

fn run_git_status<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<bool, GitError> {
    let mut command = git_command()?;
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let output = command.output().map_err(GitError::Io)?;
    Ok(output.status.success())
}

fn git_output<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String, GitError> {
    let mut command = git_command()?;
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let output = command.output().map_err(GitError::Io)?;
    if !output.status.success() {
        return Err(GitError::Command(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn git_command() -> Result<Command, GitError> {
    let mut command = Command::new("git");
    let Ok(path) = std::env::var("HENOSIS_GITHUB_TOKEN_FILE") else {
        return Ok(command);
    };
    let token = fs::read_to_string(&path).map_err(GitError::Io)?;
    let credentials = base64::engine::general_purpose::STANDARD
        .encode(format!("x-access-token:{}", token.trim()));
    command
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "http.extraHeader")
        .env(
            "GIT_CONFIG_VALUE_0",
            format!("Authorization: Basic {credentials}"),
        );
    Ok(command)
}

#[derive(Debug, Error)]
pub enum GitError {
    #[error("git publication changed since it was observed")]
    PreconditionFailed,
    #[error("git target path is not UTF-8")]
    NonUtf8Path,
    #[error("git command failed: {0}")]
    Command(String),
    #[error("git target I/O failed: {0}")]
    Io(#[source] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Barrier;

    use futures::executor::block_on;
    use henosis_types::ContentDigest;
    use henosis_types::Generation;

    use super::*;

    fn submit(
        schedule: &mut ControllerSchedule,
        desired: &mut ControllerDesiredState,
        controller: ControllerName,
        command: ControllerCommand,
    ) -> Option<ControllerWorkKey> {
        let key = desired.update(controller, command);
        schedule.submit(key)
    }

    #[test]
    fn directory_artifact_store_verifies_content_address() {
        let root = tempfile::tempdir().unwrap();
        let bytes = b"export default { fetch() { return new Response('ok') } };";
        let digest = ArtifactDigest::from_bytes(Sha256::digest(bytes).into());
        let store = DirectoryArtifactStore::new(root.path());
        assert!(matches!(
            block_on(store.fetch(digest)),
            Err(ArtifactStoreError::Missing { .. })
        ));
        let path = store.path(digest);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, bytes).unwrap();
        assert_eq!(block_on(store.fetch(digest)).unwrap().as_ref(), bytes);

        fs::write(&path, b"corrupt").unwrap();
        assert!(matches!(
            block_on(store.fetch(digest)),
            Err(ArtifactStoreError::DigestMismatch { .. })
        ));
    }

    #[tokio::test]
    async fn scheduler_supersedes_in_flight_generation_and_keeps_the_lane_running() {
        let graph = GraphId::from_bytes([3; 16]);
        let controller = controller_name("test");
        let command = |generation| {
            ControllerCommand::Reconcile(ControllerSlice::new(
                graph,
                Generation::new(generation).unwrap(),
                ContentDigest::digest(&[generation as u8]),
                controller.clone(),
                BTreeMap::new(),
                Vec::new(),
                Vec::new(),
            ))
        };
        let mut desired = ControllerDesiredState::default();
        let mut schedule = ControllerSchedule::default();
        let key = submit(&mut schedule, &mut desired, controller.clone(), command(1))
            .expect("new lane starts a driver");
        let stale = schedule.pass(&key, &desired).unwrap();
        assert_eq!(
            submit(&mut schedule, &mut desired, controller.clone(), command(2)),
            None
        );
        assert_eq!(
            schedule.complete(&stale, ControllerPass::Converged(None)),
            ControllerScheduleCompletion::Continue
        );
        let current = schedule.pass(&key, &desired).unwrap();
        assert!(matches!(
            current.command(),
            ControllerCommand::Reconcile(slice) if slice.generation() == Generation::new(2).unwrap()
        ));
        assert_eq!(
            schedule.complete(&current, ControllerPass::Acted),
            ControllerScheduleCompletion::Continue
        );
        let final_pass = schedule.pass(&key, &desired).unwrap();
        assert_eq!(
            schedule.complete(&final_pass, ControllerPass::Converged(None)),
            ControllerScheduleCompletion::Complete
        );
        assert!(schedule.pass(&key, &desired).is_none());
    }

    #[test]
    fn dirty_lane_coalesces_updates_to_the_latest_command() {
        let graph = GraphId::from_bytes([9; 16]);
        let controller = controller_name("test");
        let command = |generation| {
            ControllerCommand::Reconcile(ControllerSlice::new(
                graph,
                Generation::new(generation).unwrap(),
                ContentDigest::digest(&[generation as u8]),
                controller.clone(),
                BTreeMap::new(),
                Vec::new(),
                Vec::new(),
            ))
        };
        let mut desired = ControllerDesiredState::default();
        let mut schedule = ControllerSchedule::default();
        let key = submit(&mut schedule, &mut desired, controller.clone(), command(1)).unwrap();
        let first = schedule.pass(&key, &desired).unwrap();
        assert_eq!(
            submit(&mut schedule, &mut desired, controller.clone(), command(2)),
            None
        );
        assert_eq!(
            submit(&mut schedule, &mut desired, controller.clone(), command(3)),
            None
        );
        assert_eq!(
            schedule.complete(&first, ControllerPass::Converged(None)),
            ControllerScheduleCompletion::Continue
        );
        assert!(matches!(
            schedule.pass(&key, &desired).unwrap().command(),
            ControllerCommand::Reconcile(slice)
                if slice.generation() == Generation::new(3).unwrap()
        ));

        // There are no submit-versus-admission orders to test: submit only
        // marks the key dirty, and the next dequeue reads the one
        // current command.
    }

    #[tokio::test]
    async fn scheduler_retains_report_until_acknowledged() {
        let graph = GraphId::from_bytes([4; 16]);
        let controller = controller_name("test");
        let slice = ControllerSlice::new(
            graph,
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller.clone(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
        let report = ready_report(&slice, None, Vec::new()).unwrap();
        let mut desired = ControllerDesiredState::default();
        let mut schedule = ControllerSchedule::default();
        let key = submit(
            &mut schedule,
            &mut desired,
            controller.clone(),
            ControllerCommand::Reconcile(slice.clone()),
        )
        .unwrap();
        let pass = schedule.pass(&key, &desired).unwrap();
        let ControllerScheduleCompletion::Report(pending) =
            schedule.complete(&pass, ControllerPass::Converged(Some(report.clone())))
        else {
            panic!("convergence should enter report delivery");
        };
        assert!(schedule.pass(&key, &desired).is_none());

        let newer = ControllerSlice::new(
            graph,
            Generation::new(2).unwrap(),
            ContentDigest::digest(b"new-plan"),
            controller.clone(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
        assert_eq!(
            submit(
                &mut schedule,
                &mut desired,
                controller,
                ControllerCommand::Reconcile(newer)
            ),
            Some(key.clone())
        );
        assert_eq!(
            schedule.acknowledge_report(&pending),
            ControllerScheduleCompletion::Continue
        );
        assert!(matches!(
            schedule.pass(&key, &desired).unwrap().command(),
            ControllerCommand::Reconcile(slice)
                if slice.generation() == Generation::new(2).unwrap()
        ));
    }

    #[tokio::test]
    async fn retryable_and_terminal_failures_have_distinct_schedule_outcomes() {
        let graph = GraphId::from_bytes([5; 16]);
        let controller = controller_name("test");
        let slice = ControllerSlice::new(
            graph,
            Generation::new(1).unwrap(),
            ContentDigest::digest(b"plan"),
            controller.clone(),
            BTreeMap::new(),
            Vec::new(),
            Vec::new(),
        );
        let mut desired = ControllerDesiredState::default();
        let mut schedule = ControllerSchedule::default();
        let key = submit(
            &mut schedule,
            &mut desired,
            controller,
            ControllerCommand::Reconcile(slice.clone()),
        )
        .unwrap();
        let pass = schedule.pass(&key, &desired).unwrap();
        assert_eq!(
            schedule.complete(&pass, ControllerPass::Retryable("offline".into())),
            ControllerScheduleCompletion::Retry {
                attempt: 1,
                message: "offline".into(),
            }
        );
        let pass = schedule.pass(&key, &desired).unwrap();
        let failed = failed_report(&slice, "invalid desired state").unwrap();
        assert!(matches!(
            schedule.complete(&pass, ControllerPass::Failed(failed)),
            ControllerScheduleCompletion::Report(_)
        ));
    }

    #[test]
    fn stale_git_publication_cannot_overwrite_newer_branch_head() {
        let remote = tempfile::tempdir().unwrap();
        run_git(
            None,
            ["init", "--bare", "--quiet", remote.path().to_str().unwrap()],
        )
        .unwrap();
        let repository = GitRepository::new(remote.path());
        repository
            .publish(
                "env/test",
                PublicationMode::ReplaceBranch,
                &BTreeMap::from([("resource/state".into(), b"base".to_vec())]),
                "base",
            )
            .unwrap();

        let ready = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let stale_repository = repository.clone();
        let stale_ready = Arc::clone(&ready);
        let stale_release = Arc::clone(&release);
        let stale = std::thread::spawn(move || {
            stale_repository.publish_with_before_push(
                "env/test",
                PublicationMode::ReplaceBranch,
                &BTreeMap::from([("resource/state".into(), b"stale".to_vec())]),
                "stale",
                || {
                    stale_ready.wait();
                    stale_release.wait();
                },
            )
        });
        ready.wait();
        repository
            .publish(
                "env/test",
                PublicationMode::ReplaceBranch,
                &BTreeMap::from([("resource/state".into(), b"new".to_vec())]),
                "new",
            )
            .unwrap();
        release.wait();

        assert!(matches!(stale.join().unwrap(), Err(GitError::Command(_))));
        assert_eq!(
            repository.read_directory("env/test", "resource").unwrap(),
            BTreeMap::from([("state".into(), b"new".to_vec())])
        );
    }
}
