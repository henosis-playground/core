use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum WaitNode {
    Component(String),
    Input(String),
    ControllerOperation(String),
    Resource(String),
    ExternalInput(String),
    Timer(String),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WaitEdge {
    pub from: WaitNode,
    pub to: WaitNode,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct WaitGraph {
    edges: BTreeMap<WaitNode, BTreeSet<WaitNode>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Quiescence {
    Converged,
    WaitingForTimer,
    ExternallyBlocked,
    Deadlocked,
    Incomplete,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StallReport {
    pub classification: Quiescence,
    pub cycle: Vec<WaitNode>,
    pub edges: Vec<WaitEdge>,
}

impl WaitGraph {
    pub fn add_edge(&mut self, from: WaitNode, to: WaitNode) {
        self.edges.entry(from).or_default().insert(to);
    }

    #[must_use]
    pub fn classify(&self, complete: bool, has_timer: bool) -> StallReport {
        let edges = self
            .edges
            .iter()
            .flat_map(|(from, targets)| {
                targets.iter().map(|to| WaitEdge {
                    from: from.clone(),
                    to: to.clone(),
                })
            })
            .collect::<Vec<_>>();
        if complete {
            return StallReport {
                classification: Quiescence::Converged,
                cycle: Vec::new(),
                edges,
            };
        }
        if has_timer {
            return StallReport {
                classification: Quiescence::WaitingForTimer,
                cycle: Vec::new(),
                edges,
            };
        }
        if self
            .edges
            .values()
            .flatten()
            .any(|node| matches!(node, WaitNode::ExternalInput(_)))
        {
            return StallReport {
                classification: Quiescence::ExternallyBlocked,
                cycle: Vec::new(),
                edges,
            };
        }
        let cycle = self.find_cycle();
        StallReport {
            classification: if cycle.is_empty() {
                Quiescence::Incomplete
            } else {
                Quiescence::Deadlocked
            },
            cycle,
            edges,
        }
    }

    fn find_cycle(&self) -> Vec<WaitNode> {
        for start in self.edges.keys() {
            let mut path = Vec::new();
            let mut positions = BTreeMap::new();
            if let Some(cycle) = self.visit(start, &mut path, &mut positions) {
                return cycle;
            }
        }
        Vec::new()
    }

    fn visit(
        &self,
        node: &WaitNode,
        path: &mut Vec<WaitNode>,
        positions: &mut BTreeMap<WaitNode, usize>,
    ) -> Option<Vec<WaitNode>> {
        if let Some(position) = positions.get(node).copied() {
            let mut cycle = path[position..].to_vec();
            cycle.push(node.clone());
            return Some(cycle);
        }
        positions.insert(node.clone(), path.len());
        path.push(node.clone());
        for target in self.edges.get(node).into_iter().flatten() {
            if let Some(cycle) = self.visit(target, path, positions) {
                return Some(cycle);
            }
        }
        path.pop();
        positions.remove(node);
        None
    }
}
