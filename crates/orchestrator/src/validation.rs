use std::collections::BTreeSet;

use types::domain::ComponentCatalog;
use types::domain::ComponentDispositionKind;
use types::domain::ComponentUuid;
use types::domain::Diagnostic;
use types::domain::DiagnosticSeverity;
use types::domain::Graph;
use types::domain::GraphSlice;
use types::domain::SliceReport;

use crate::Orchestrator;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum GraphValidationError {
    Invalid(Vec<Diagnostic>),
    FailedPrecondition(Vec<Diagnostic>),
}

pub(crate) fn validate_graph(
    graph: &Graph,
    specs: &ComponentCatalog,
    orchestrator: &Orchestrator,
) -> Result<(), GraphValidationError> {
    let mut invalid = Vec::new();
    let mut preconditions = Vec::new();
    for component in graph.components() {
        let hash = component.component_id();
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

fn has_cycle(graph: &Graph, specs: &ComponentCatalog) -> bool {
    let mut visited = BTreeSet::new();
    let mut visiting = BTreeSet::new();
    graph.components().any(|component| {
        visit(
            component.component_id(),
            graph,
            specs,
            &mut visiting,
            &mut visited,
        )
    })
}

fn visit(
    hash: ComponentUuid,
    graph: &Graph,
    specs: &ComponentCatalog,
    visiting: &mut BTreeSet<ComponentUuid>,
    visited: &mut BTreeSet<ComponentUuid>,
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
        .map(|component| component.id())
        .collect::<BTreeSet<_>>();
    let dispositions = report
        .dispositions()
        .map(|disposition| disposition.component_id())
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
    let publishable = !owned.is_empty() && all_ready && !has_errors;
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
        .map(|output| output.component_id())
        .collect::<BTreeSet<_>>();
    if outputs != owned {
        return Err(ReportValidationError::FailedPrecondition(vec![
            Diagnostic::error("report.outputs.incomplete"),
        ]));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use types::domain::ConnectorKey;
    use types::domain::GraphSlice;
    use types::domain::GraphUuid;
    use types::domain::NewSliceReport;
    use types::domain::SliceReport;

    use super::validate_report;

    #[test]
    fn empty_ready_slice_is_non_publishing() {
        let graph_id = GraphUuid::from_bytes([1; 16]);
        let connector = "test".parse::<ConnectorKey>().unwrap();
        let slice =
            GraphSlice::new(graph_id, 1, connector.clone(), Vec::new(), Vec::new(), 0).unwrap();
        let report = SliceReport::new(NewSliceReport {
            graph_id,
            generation: 1,
            connector,
            dispositions: Vec::new(),
            outputs: Vec::new(),
            diagnostics: Vec::new(),
            sequence: 0,
            publication: None,
        })
        .unwrap();

        assert_eq!(validate_report(&report, &slice), Ok(false));
    }
}
