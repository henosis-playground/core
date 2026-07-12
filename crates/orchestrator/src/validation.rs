use std::collections::BTreeSet;

use henosis_types::ComponentDispositionKind;
use henosis_types::ComponentSpecHash;
use henosis_types::Diagnostic;
use henosis_types::DiagnosticSeverity;
use henosis_types::Graph;
use henosis_types::GraphSlice;
use henosis_types::SliceReport;
use henosis_types::SpecCatalog;

use crate::Orchestrator;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GraphValidationError {
    Invalid(Vec<Diagnostic>),
    FailedPrecondition(Vec<Diagnostic>),
}

pub(crate) fn validate_graph(
    graph: &Graph,
    specs: &SpecCatalog,
    orchestrator: &Orchestrator,
) -> Result<(), GraphValidationError> {
    let mut invalid = Vec::new();
    let mut preconditions = Vec::new();
    for component in graph.components() {
        let hash = component.spec_hash();
        let Some(registered) = specs.get(hash) else {
            preconditions
                .push(Diagnostic::error("component.spec.unregistered").for_component(hash));
            continue;
        };
        if !orchestrator
            .connectors
            .contains_key(registered.spec().connector())
        {
            preconditions
                .push(Diagnostic::error("component.connector.unregistered").for_component(hash));
        }
        for dependency in registered.spec().depends_on() {
            if *dependency == hash {
                invalid.push(Diagnostic::error("component.depends_on.self").for_component(hash));
            } else if !graph.contains(*dependency) {
                preconditions
                    .push(Diagnostic::error("component.depends_on.missing").for_component(hash));
            }
        }
    }
    if has_cycle(graph, specs) {
        preconditions.push(Diagnostic::error("graph.dependencies.cycle"));
    }
    if !invalid.is_empty() {
        return Err(GraphValidationError::Invalid(invalid));
    }
    if !preconditions.is_empty() {
        return Err(GraphValidationError::FailedPrecondition(preconditions));
    }
    Ok(())
}

fn has_cycle(graph: &Graph, specs: &SpecCatalog) -> bool {
    let mut visited = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    graph.components().any(|component| {
        visit(
            component.spec_hash(),
            graph,
            specs,
            &mut visiting,
            &mut visited,
        )
    })
}

fn visit(
    hash: ComponentSpecHash,
    graph: &Graph,
    specs: &SpecCatalog,
    visiting: &mut BTreeSet<ComponentSpecHash>,
    visited: &mut BTreeSet<ComponentSpecHash>,
) -> bool {
    if visited.contains(&hash) {
        return false;
    }
    if !visiting.insert(hash) {
        return true;
    }
    let cyclic = specs.get(hash).is_some_and(|registered| {
        registered
            .spec()
            .depends_on()
            .iter()
            .copied()
            .filter(|dependency| graph.contains(*dependency))
            .any(|dependency| visit(dependency, graph, specs, visiting, visited))
    });
    visiting.remove(&hash);
    visited.insert(hash);
    cyclic
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReportValidationError {
    Invalid(Vec<Diagnostic>),
    FailedPrecondition(Vec<Diagnostic>),
}

pub(crate) fn validate_report(
    report: &SliceReport,
    slice: &GraphSlice,
) -> Result<bool, ReportValidationError> {
    if report.graph_id() != slice.graph_id()
        || report.generation() != slice.generation()
        || report.connector() != slice.connector()
        || report.sequence() != slice.sequence()
    {
        return Err(ReportValidationError::Invalid(vec![Diagnostic::error(
            "report.slice_identity_mismatch",
        )]));
    }
    let owned = slice
        .components()
        .map(|component| component.hash())
        .collect::<BTreeSet<_>>();
    let dispositions = report
        .dispositions()
        .map(|disposition| disposition.component_spec_hash())
        .collect::<BTreeSet<_>>();
    if dispositions != owned {
        return Err(ReportValidationError::Invalid(vec![Diagnostic::error(
            "report.dispositions.incomplete",
        )]));
    }
    let all_ready = report
        .dispositions()
        .all(|item| item.kind() == ComponentDispositionKind::Ready);
    let has_errors = report
        .diagnostics()
        .iter()
        .any(|item| item.severity() == DiagnosticSeverity::Error);
    let publishable = all_ready && !has_errors;
    if !publishable {
        if report.outputs().len() == 0 {
            return Ok(false);
        }
        return Err(ReportValidationError::FailedPrecondition(vec![
            Diagnostic::error("report.outputs.require_ready_slice"),
        ]));
    }
    let outputs = report
        .outputs()
        .map(|output| output.component_spec_hash())
        .collect::<BTreeSet<_>>();
    if outputs != owned {
        return Err(ReportValidationError::FailedPrecondition(vec![
            Diagnostic::error("report.outputs.incomplete"),
        ]));
    }
    Ok(true)
}
