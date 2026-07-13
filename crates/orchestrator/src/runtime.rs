use std::sync::Arc;

use iddqd::IdHashItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::broadcast;
use types::domain::GraphHistory;
use types::domain::GraphUuid;
use types::domain::SliceReport;

use crate::Orchestrator;
use crate::WatchEvent;

#[derive(Debug)]
pub(crate) struct GraphRuntime {
    pub(crate) history: Mutex<Option<GraphHistory>>,
    pub(crate) reports: RwLock<IdOrdMap<SliceReport>>,
    pub(crate) events: broadcast::Sender<WatchEvent>,
    pub(crate) delivery: Mutex<()>,
}

impl GraphRuntime {
    fn new() -> Self {
        let (events, _) = broadcast::channel(256);
        Self {
            history: Mutex::new(None),
            reports: RwLock::new(IdOrdMap::new()),
            events,
            delivery: Mutex::new(()),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct GraphRuntimeHandle {
    graph_id: GraphUuid,
    runtime: Arc<GraphRuntime>,
}

impl IdHashItem for GraphRuntimeHandle {
    type Key<'a> = GraphUuid;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}

impl Orchestrator {
    pub(crate) async fn runtime(&self, graph_id: GraphUuid) -> Arc<GraphRuntime> {
        let mut runtimes = self.runtimes.lock().await;
        if let Some(handle) = runtimes.get(&graph_id) {
            return Arc::clone(&handle.runtime);
        }
        let runtime = Arc::new(GraphRuntime::new());
        runtimes
            .insert_unique(GraphRuntimeHandle {
                graph_id,
                runtime: Arc::clone(&runtime),
            })
            .expect("graph runtime absence was checked");
        runtime
    }
}
