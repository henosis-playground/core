//! Domain orchestration for graph lifecycle, slices, and connector delivery.

mod delivery;
mod error;
mod graph;
mod report;
mod slice;
mod telemetry;
mod validation;
mod watch;

use std::sync::Arc;

use henosis_db_queries::MetadataDb;
use henosis_journal::Journal;
use henosis_types::ConnectorKey;
use henosis_types::GraphHistory;
use henosis_types::GraphId;
use henosis_types::SliceReport;
use henosis_types::SpecCatalog;
use iddqd::IdHashItem;
use iddqd::IdHashMap;
use iddqd::IdOrdItem;
use iddqd::IdOrdMap;
use iddqd::id_upcast;
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tokio::sync::broadcast;

pub use error::OrchestratorError;
pub use watch::WatchEvent;
pub use watch::WatchParts;
pub use watch::WatchSubscription;

#[derive(Clone, Debug, Deserialize)]
pub struct ConnectorConfig {
    key: ConnectorKey,
    endpoint: String,
    token: String,
}

impl ConnectorConfig {
    #[must_use]
    pub const fn new(key: ConnectorKey, endpoint: String, token: String) -> Self {
        Self {
            key,
            endpoint,
            token,
        }
    }

    #[must_use]
    pub const fn key(&self) -> &ConnectorKey {
        &self.key
    }

    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl IdOrdItem for ConnectorConfig {
    type Key<'a> = &'a ConnectorKey;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        &self.key
    }
}

#[derive(Debug)]
struct GraphRuntime {
    history: Mutex<Option<GraphHistory>>,
    reports: RwLock<IdOrdMap<SliceReport>>,
    events: broadcast::Sender<WatchEvent>,
    delivery: Mutex<()>,
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
struct GraphRuntimeHandle {
    graph_id: GraphId,
    runtime: Arc<GraphRuntime>,
}

impl IdHashItem for GraphRuntimeHandle {
    type Key<'a> = GraphId;

    id_upcast!();

    fn key(&self) -> Self::Key<'_> {
        self.graph_id
    }
}

#[derive(Clone, Debug)]
pub struct Orchestrator {
    journal: Journal,
    metadata: MetadataDb,
    connectors: Arc<IdOrdMap<ConnectorConfig>>,
    specs: Arc<RwLock<SpecCatalog>>,
    runtimes: Arc<Mutex<IdHashMap<GraphRuntimeHandle>>>,
}

impl Orchestrator {
    pub fn new(
        journal: Journal,
        metadata: MetadataDb,
        connectors: Vec<ConnectorConfig>,
    ) -> Result<Self, DuplicateConnector> {
        Ok(Self {
            journal,
            metadata,
            connectors: Arc::new(
                IdOrdMap::from_iter_unique(connectors).map_err(|_| DuplicateConnector)?,
            ),
            specs: Arc::new(RwLock::new(SpecCatalog::default())),
            runtimes: Arc::new(Mutex::new(IdHashMap::new())),
        })
    }

    async fn runtime(&self, graph_id: GraphId) -> Arc<GraphRuntime> {
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

#[derive(Clone, Copy, Debug, thiserror::Error, Eq, PartialEq)]
#[error("connector configuration contains a duplicate key")]
pub struct DuplicateConnector;
