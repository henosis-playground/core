//! Domain orchestration for graph lifecycle, slices, and connector delivery.

mod connector;
mod delivery;
mod error;
mod graph;
mod report;
mod runtime;
mod slice;
mod telemetry;
mod validation;
mod watch;

use std::sync::Arc;

use henosis_db_queries::DbPool;
use henosis_journal::Journal;
use henosis_types::SpecCatalog;
use iddqd::IdHashMap;
use iddqd::IdOrdMap;
use tokio::sync::Mutex;
use tokio::sync::RwLock;

use crate::runtime::GraphRuntimeHandle;

pub(crate) use runtime::GraphRuntime;

pub use connector::ConnectorConfig;
pub use connector::DuplicateConnector;
pub use error::OrchestratorError;
pub use watch::WatchEvent;
pub use watch::WatchParts;
pub use watch::WatchSubscription;

/// Coordinates durable graph state, relational metadata, and connector work.
#[derive(Clone, Debug)]
pub struct Orchestrator {
    journal: Journal,
    metadata: DbPool,
    connectors: Arc<IdOrdMap<ConnectorConfig>>,
    specs: Arc<RwLock<SpecCatalog>>,
    runtimes: Arc<Mutex<IdHashMap<GraphRuntimeHandle>>>,
}

impl Orchestrator {
    pub fn new(
        journal: Journal,
        metadata: DbPool,
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
}
