//! Small controller-side primitives shared by target adapters.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use henosis_types::{
    ControllerName, ControllerReport, ControllerReportError, ControllerSlice, NativeValue,
    NewControllerReport, ObservedOutput, ObservedOutputKey, OutputName, PublicationId, Resource,
    ResourceDisposition, ResourceDispositionKind,
};
use thiserror::Error;

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
            .map(|resource| {
                ResourceDisposition::new(resource.id(), ResourceDispositionKind::Ready)
            })
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
    let name = OutputName::new(name).map_err(|error| ReportBuildError::Output(error.to_string()))?;
    let declaration = resource
        .output(&name)
        .ok_or_else(|| ReportBuildError::Output(format!("{} does not declare output {name}", resource.path())))?;
    if declaration.availability() != henosis_types::OutputAvailability::Observed {
        return Err(ReportBuildError::Output(format!(
            "{}.{} is not an observed output",
            resource.path(),
            name
        )));
    }
    let value = NativeValue::new(value).map_err(|error| ReportBuildError::Output(error.to_string()))?;
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
        let directory = tempfile::tempdir().map_err(GitError::Io)?;
        run_git(None, ["clone", "--quiet", remote_text(&self.remote)?, directory.path().to_str().ok_or(GitError::NonUtf8Path)?])?;
        configure(directory.path())?;
        let exists = run_git_status(
            Some(directory.path()),
            ["show-ref", "--verify", "--quiet", &format!("refs/remotes/origin/{branch}")],
        )?;
        if exists {
            run_git(Some(directory.path()), ["checkout", "--quiet", "-B", branch, &format!("origin/{branch}")])?;
        } else {
            run_git(Some(directory.path()), ["checkout", "--quiet", "--orphan", branch])?;
            clear_worktree(directory.path(), None)?;
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
        run_git(
            Some(directory.path()),
            ["push", "--quiet", "--force", "origin", &format!("HEAD:refs/heads/{branch}")],
        )?;
        Ok(GitPublication {
            revision: git_output(Some(directory.path()), ["rev-parse", "HEAD"])?,
            changed: true,
        })
    }

    pub fn delete_branch(&self, branch: &str) -> Result<(), GitError> {
        let result = Command::new("git")
            .args(["push", remote_text(&self.remote)?, &format!(":refs/heads/{branch}")])
            .output()
            .map_err(GitError::Io)?;
        if result.status.success() {
            return Ok(());
        }
        let detail = String::from_utf8_lossy(&result.stderr);
        if detail.contains("remote ref does not exist") {
            return Ok(());
        }
        Err(GitError::Command(detail.into_owned()))
    }

    pub fn read_directory(
        &self,
        branch: &str,
        prefix: &str,
    ) -> Result<BTreeMap<String, Vec<u8>>, GitError> {
        let directory = tempfile::tempdir().map_err(GitError::Io)?;
        run_git(None, ["clone", "--quiet", "--branch", branch, "--single-branch", remote_text(&self.remote)?, directory.path().to_str().ok_or(GitError::NonUtf8Path)?])?;
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
    let target = prefix.map_or_else(|| root.to_path_buf(), |prefix| root.join(prefix));
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
    run_git(Some(root), ["config", "user.email", "controller@henosis.dev"])?;
    run_git(Some(root), ["config", "commit.gpgsign", "false"])
}

fn remote_text(path: &Path) -> Result<&str, GitError> {
    path.to_str().ok_or(GitError::NonUtf8Path)
}

fn run_git<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<(), GitError> {
    let mut command = Command::new("git");
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let output = command.output().map_err(GitError::Io)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(GitError::Command(String::from_utf8_lossy(&output.stderr).into_owned()))
    }
}

fn run_git_status<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<bool, GitError> {
    let mut command = Command::new("git");
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let status = command.status().map_err(GitError::Io)?;
    Ok(status.success())
}

fn git_output<'a>(
    current_dir: Option<&Path>,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String, GitError> {
    let mut command = Command::new("git");
    command.args(args);
    if let Some(current_dir) = current_dir {
        command.current_dir(current_dir);
    }
    let output = command.output().map_err(GitError::Io)?;
    if !output.status.success() {
        return Err(GitError::Command(String::from_utf8_lossy(&output.stderr).into_owned()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[derive(Debug, Error)]
pub enum GitError {
    #[error("git target path is not UTF-8")]
    NonUtf8Path,
    #[error("git command failed: {0}")]
    Command(String),
    #[error("git target I/O failed: {0}")]
    Io(#[source] std::io::Error),
}
