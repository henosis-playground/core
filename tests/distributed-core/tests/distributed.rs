use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use async_trait::async_trait;
use faultline::Error;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use henosis_app::Application;
use henosis_controller_runtime::ready_report;
use henosis_journal::Journal;
use henosis_journal::is_durable;
use henosis_orchestrator::Command;
use henosis_orchestrator::Core;
use henosis_orchestrator::MaterializedCore;
use henosis_storage::AppendAck;
use henosis_storage::AppendOutcome;
use henosis_storage::AppendRecord;
use henosis_storage::AppendSession;
use henosis_storage::StorageDomainError;
use henosis_storage::StorageEngine;
use henosis_storage::StoredRecord;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use henosis_testkit::ComponentProgram;
use henosis_testkit::ProgramEvaluator;
use henosis_types::BundleRef;
use henosis_types::ComponentIntent;
use henosis_types::ComponentName;
use henosis_types::ComponentRevision;
use henosis_types::Controller;
use henosis_types::ControllerCommand;
use henosis_types::ControllerError;
use henosis_types::ControllerName;
use henosis_types::ControllerPass;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::GraphSourcePolicy;
use henosis_types::NewComponentIntent;
use henosis_types::NewGraphIntent;
use henosis_types::ResourceId;
use henosis_types::ResourceName;
use henosis_types::Stall;
use rand_10::SeedableRng as _;
use serde::Deserialize;
use serde::Serialize;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt as _;
use turmoil::net::TcpListener;
use turmoil::net::TcpStream;

const STORAGE_PORT: u16 = 7400;
const GRAPH: GraphId = GraphId::from_bytes([61; 16]);

fn deterministic_process(seed: u64) -> mad_turmoil::time::SimClocksGuard {
    mad_turmoil::rand::set_rng(rand_10::rngs::StdRng::seed_from_u64(seed));
    mad_turmoil::time::SimClocksGuard::init()
}

#[derive(Deserialize, Serialize)]
enum Request {
    Append {
        stream: String,
        expected: u64,
        records: Vec<Vec<u8>>,
    },
    Read {
        stream: String,
        from: u64,
        limit: usize,
    },
    Tail {
        stream: String,
    },
}

#[derive(Deserialize, Serialize)]
enum Response {
    Appended { start: u64, tail: u64 },
    Conflict { actual: u64 },
    Records(Vec<WireRecord>),
    Tail(u64),
}

#[derive(Clone, Deserialize, Serialize)]
struct WireRecord {
    sequence: u64,
    timestamp: u64,
    body: Vec<u8>,
}

#[derive(Default)]
struct StorageState {
    streams: BTreeMap<String, Vec<WireRecord>>,
    clock: u64,
}

async fn storage_server() -> turmoil::Result {
    let listener = TcpListener::bind(("0.0.0.0", STORAGE_PORT)).await?;
    let state = Arc::new(tokio::sync::Mutex::new(StorageState::default()));
    loop {
        let (mut stream, _) = listener.accept().await?;
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            let result = async {
                let request: Request = read_frame(&mut stream).await?;
                let response = handle_storage(request, &state).await;
                write_frame(&mut stream, &response).await
            }
            .await;
            if result.is_err() {
                let _ = stream.shutdown().await;
            }
        });
    }
}

async fn handle_storage(request: Request, state: &tokio::sync::Mutex<StorageState>) -> Response {
    let mut state = state.lock().await;
    match request {
        Request::Append {
            stream,
            expected,
            records,
        } => {
            let actual = state.streams.get(&stream).map(Vec::len).unwrap_or_default() as u64;
            if actual != expected {
                return Response::Conflict { actual };
            }
            for body in records {
                let sequence = state.streams.get(&stream).map(Vec::len).unwrap_or_default() as u64;
                let timestamp = state.clock;
                state.clock = state.clock.saturating_add(1);
                state
                    .streams
                    .entry(stream.clone())
                    .or_default()
                    .push(WireRecord {
                        sequence,
                        timestamp,
                        body,
                    });
            }
            let tail = state.streams.get(&stream).map(Vec::len).unwrap_or_default() as u64;
            Response::Appended {
                start: actual,
                tail,
            }
        }
        Request::Read {
            stream,
            from,
            limit,
        } => Response::Records(
            state
                .streams
                .get(&stream)
                .into_iter()
                .flatten()
                .skip(from as usize)
                .take(limit)
                .cloned()
                .collect(),
        ),
        Request::Tail { stream } => {
            Response::Tail(state.streams.get(&stream).map(Vec::len).unwrap_or_default() as u64)
        }
    }
}

#[derive(Clone)]
struct NetworkStorage {
    host: &'static str,
}

struct NetworkAppendSession {
    storage: NetworkStorage,
    stream: StreamName,
    poisoned: bool,
}

#[async_trait]
impl AppendSession for NetworkAppendSession {
    async fn append(
        &mut self,
        expected: StreamPosition,
        records: Vec<AppendRecord>,
    ) -> Result<AppendOutcome, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        if self.poisoned {
            return Err(Error::Transient(anyhow::anyhow!(
                "append session is poisoned"
            )));
        }
        let response = self
            .storage
            .request(Request::Append {
                stream: self.stream.as_str().to_owned(),
                expected: expected.sequence(),
                records: records
                    .into_iter()
                    .map(|record| record.body().to_vec())
                    .collect(),
            })
            .await?;
        match response {
            Response::Appended { start, tail } => Ok(AppendOutcome::Acknowledged(AppendAck::new(
                StreamPosition::new(start),
                StreamPosition::new(tail),
            ))),
            Response::Conflict { actual } => {
                self.poisoned = true;
                Err(Error::Domain(StorageDomainError::CasConflict {
                    expected: expected.sequence(),
                    actual,
                }))
            }
            _ => Err(Error::Invariant(anyhow::anyhow!("invalid append response"))),
        }
    }
}

impl NetworkStorage {
    async fn request(
        &self,
        request: Request,
    ) -> Result<Response, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        let mut stream = TcpStream::connect((self.host, STORAGE_PORT))
            .await
            .map_err(|error| {
                Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(
                    anyhow::Error::new(error),
                )
            })?;
        write_frame(&mut stream, &request).await.map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(error)
        })?;
        read_frame(&mut stream).await.map_err(|error| {
            Error::<StorageDomainError, anyhow::Error, anyhow::Error>::Transient(error)
        })
    }
}

#[async_trait]
impl StorageEngine for NetworkStorage {
    async fn open_append_session(
        &self,
        stream: &StreamName,
    ) -> Result<Box<dyn AppendSession>, Error<StorageDomainError, anyhow::Error, anyhow::Error>>
    {
        Ok(Box::new(NetworkAppendSession {
            storage: self.clone(),
            stream: stream.clone(),
            poisoned: false,
        }))
    }

    async fn read(
        &self,
        stream: &StreamName,
        from: StreamPosition,
        limit: usize,
    ) -> Result<Vec<StoredRecord>, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        match self
            .request(Request::Read {
                stream: stream.as_str().to_owned(),
                from: from.sequence(),
                limit,
            })
            .await?
        {
            Response::Records(records) => Ok(records
                .into_iter()
                .map(|record| {
                    StoredRecord::new(
                        stream.clone(),
                        record.sequence,
                        record.timestamp,
                        record.body,
                    )
                })
                .collect()),
            _ => Err(Error::Invariant(anyhow::anyhow!("invalid read response"))),
        }
    }

    async fn tail(
        &self,
        stream: &StreamName,
    ) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
        match self
            .request(Request::Tail {
                stream: stream.as_str().to_owned(),
            })
            .await?
        {
            Response::Tail(tail) => Ok(StreamPosition::new(tail)),
            _ => Err(Error::Invariant(anyhow::anyhow!("invalid tail response"))),
        }
    }

    fn follow(
        &self,
        stream: StreamName,
        from: StreamPosition,
    ) -> BoxStream<
        'static,
        Result<StoredRecord, Error<StorageDomainError, anyhow::Error, anyhow::Error>>,
    > {
        let storage = self.clone();
        Box::pin(async_stream::stream! {
            let mut cursor = from;
            loop {
                match storage.read(&stream, cursor, 100).await {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Ok(records) => {
                        for record in records {
                            cursor = StreamPosition::new(record.sequence().saturating_add(1));
                            yield Ok(record);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        break;
                    }
                }
            }
        })
    }
}

async fn write_frame<T: Serialize>(
    stream: &mut (impl AsyncWrite + Unpin),
    value: &T,
) -> anyhow::Result<()> {
    let bytes = serde_json::to_vec(value)?;
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    stream.flush().await?;
    Ok(())
}

async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut (impl AsyncRead + Unpin),
) -> anyhow::Result<T> {
    let size = stream.read_u32().await? as usize;
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn evaluator_and_bundle() -> (Arc<ProgramEvaluator>, BundleRef) {
    let evaluator = Arc::new(ProgramEvaluator::default());
    let bundle = evaluator.register(ComponentProgram {
        resources: Vec::new(),
        static_outputs: BTreeMap::new(),
    });
    (evaluator, bundle)
}

fn graph(bundle: BundleRef, revision: u8) -> NewGraphIntent {
    let component = ComponentIntent::new(NewComponentIntent {
        name: ComponentName::new("api").unwrap(),
        revision: ComponentRevision::new(format!("{revision:02x}").repeat(32)).unwrap(),
        bundle,
        inputs: Vec::new(),
        outputs: Vec::new(),
        compiled_dependencies: Vec::new(),
        source: None,
    })
    .unwrap();
    NewGraphIntent {
        id: GRAPH,
        components: vec![component],
        source_policy: GraphSourcePolicy::AcceptLocal,
    }
}

fn evaluator_bundle_with_resource() -> (Arc<ProgramEvaluator>, BundleRef) {
    let evaluator = Arc::new(ProgramEvaluator::default());
    let bundle = evaluator.register(ComponentProgram {
        resources: vec![henosis_testkit::ResourceProgram {
            id: ResourceId::from_bytes([72; 16]),
            name: ResourceName::new("service").unwrap(),
            controller: ControllerName::new("test").unwrap(),
            required_values: Vec::new(),
            observed_component_output: None,
        }],
        static_outputs: BTreeMap::new(),
    });
    (evaluator, bundle)
}

struct CountingController {
    calls: Arc<AtomicUsize>,
    name: ControllerName,
}

impl Controller for CountingController {
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match command {
                ControllerCommand::Reconcile(slice) => Ok(ControllerPass::Converged(Some(
                    ready_report(slice, None, Vec::new())
                        .map_err(|error| ControllerError::new(error.to_string()))?,
                ))),
                ControllerCommand::Supersede(_) | ControllerCommand::Retire(_) => {
                    Ok(ControllerPass::Converged(None))
                }
            }
        })
    }
}

struct UnreportedController {
    calls: Arc<AtomicUsize>,
    name: ControllerName,
}

impl Controller for UnreportedController {
    fn name(&self) -> &ControllerName {
        &self.name
    }

    fn execute<'a>(
        &'a self,
        _command: &'a ControllerCommand,
    ) -> BoxFuture<'a, Result<ControllerPass, ControllerError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(ControllerPass::Retryable(
                "process stops before report admission".into(),
            ))
        })
    }
}

async fn append_transition(
    journal: &Journal,
    tail: StreamPosition,
    transition: &henosis_orchestrator::Transition,
) -> Result<StreamPosition, Error<StorageDomainError, anyhow::Error, anyhow::Error>> {
    let events = transition
        .events()
        .iter()
        .filter(|event| is_durable(event))
        .cloned()
        .collect::<Vec<_>>();
    journal
        .append_all(GRAPH, tail, &events)
        .await
        .map(|ack| ack.tail())
}

fn run_case(seed: u64, partition_second_node: bool) -> Vec<String> {
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(seed)
        .simulation_duration(Duration::from_secs(10))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20));
    let mut sim = builder.build();
    sim.host("storage", storage_server);

    let first_outcomes = Arc::clone(&outcomes);
    sim.host("core-a", move || {
        let outcomes = Arc::clone(&first_outcomes);
        async move {
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let journal = Journal::new(storage);
            let (evaluator, bundle) = evaluator_and_bundle();
            let mut core = Core::new(evaluator);
            let created = core
                .handle(Command::CreateGraph(graph(bundle, 1)))
                .await
                .unwrap();
            let tail = append_transition(&journal, StreamPosition::default(), &created).await?;
            tokio::time::sleep(Duration::from_secs(1)).await;
            let update = core
                .handle(Command::UpdateGraph {
                    graph_id: GRAPH,
                    expected_generation: Generation::new(1).unwrap(),
                    components: graph(bundle, 2).components,
                })
                .await
                .unwrap();
            match append_transition(&journal, tail, &update).await {
                Ok(_) => outcomes.lock().unwrap().push("a:won".into()),
                Err(Error::Domain(StorageDomainError::CasConflict { .. })) => {
                    outcomes.lock().unwrap().push("a:lost".into());
                }
                Err(error) => return Err(std::io::Error::other(error.to_string()).into()),
            }
            Ok(())
        }
    });

    let second_outcomes = Arc::clone(&outcomes);
    sim.host("core-b", move || {
        let outcomes = Arc::clone(&second_outcomes);
        async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let journal = Journal::new(storage);
            let (evaluator, bundle) = evaluator_and_bundle();
            let (events, tail) = journal.load_with_tail(GRAPH).await?;
            let mut core = Core::from_materialized(evaluator, MaterializedCore::fold(&events));
            tokio::time::sleep(Duration::from_millis(500)).await;
            let update = core
                .handle(Command::UpdateGraph {
                    graph_id: GRAPH,
                    expected_generation: Generation::new(1).unwrap(),
                    components: graph(bundle, 3).components,
                })
                .await
                .unwrap();
            let result = append_transition(&journal, tail, &update).await;
            if partition_second_node {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            match result {
                Ok(_) => outcomes.lock().unwrap().push("b:won".into()),
                Err(Error::Domain(StorageDomainError::CasConflict { .. })) => {
                    outcomes.lock().unwrap().push("b:lost".into());
                }
                Err(Error::Transient(_)) if partition_second_node => {
                    outcomes.lock().unwrap().push("b:partitioned".into());
                }
                Err(error) => return Err(std::io::Error::other(error.to_string()).into()),
            }
            Ok(())
        }
    });

    if partition_second_node {
        sim.client("faults", async {
            tokio::time::sleep(Duration::from_millis(900)).await;
            turmoil::partition("core-b", "storage");
            tokio::time::sleep(Duration::from_millis(500)).await;
            turmoil::repair("core-b", "storage");
            Ok(())
        });
    }

    let verify_outcomes = Arc::clone(&outcomes);
    sim.client("verifier", async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
        let journal = Journal::new(storage);
        let (events, _) = journal.load_with_tail(GRAPH).await?;
        let first = MaterializedCore::fold(&events);
        let second = MaterializedCore::fold(&events);
        assert_eq!(
            first, second,
            "fold replay must be deterministic across nodes"
        );
        assert_eq!(
            first.graph(GRAPH).unwrap().intent().generation(),
            Generation::new(2).unwrap(),
            "one and only one competing generation may commit"
        );
        let outcomes = verify_outcomes.lock().unwrap();
        assert_eq!(
            outcomes
                .iter()
                .filter(|value| value.ends_with(":won"))
                .count(),
            1
        );
        Ok(())
    });

    sim.run().unwrap();
    let result = outcomes.lock().unwrap().clone();
    result
}

#[test]
fn two_nodes_converge_through_journal_occ() {
    let _deterministic_process = deterministic_process(0);
    let seeds = if std::env::var_os("HENOSIS_EXTENDED_DST").is_some() {
        0..32
    } else {
        0..4
    };
    for seed in seeds {
        run_case(seed, false);
    }
}

#[test]
fn partitioned_node_cannot_overwrite_the_committed_generation() {
    let _deterministic_process = deterministic_process(71);
    run_case(71, true);
}

#[test]
fn same_seed_replays_byte_for_byte() {
    let _deterministic_process = deterministic_process(94);
    let first = serde_json::to_vec(&run_case(94, false)).unwrap();
    let second = serde_json::to_vec(&run_case(94, false)).unwrap();
    assert_eq!(first, second);
}

#[test]
fn remote_creation_wakes_peer_controller_lanes() {
    let _deterministic_process = deterministic_process(91);
    let observed = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(91)
        .simulation_duration(Duration::from_secs(8))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20));
    let mut sim = builder.build();
    sim.host("storage", storage_server);

    let peer_observed = Arc::clone(&observed);
    let peer_calls = Arc::clone(&calls);
    sim.host("core-b", move || {
        let observed = Arc::clone(&peer_observed);
        let calls = Arc::clone(&peer_calls);
        async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let (evaluator, _) = evaluator_bundle_with_resource();
            let name = ControllerName::new("test").unwrap();
            let controller: Arc<dyn Controller> = Arc::new(CountingController {
                calls: Arc::clone(&calls),
                name: name.clone(),
            });
            let app = Application::start(
                evaluator,
                Journal::new(storage),
                BTreeMap::from([(name, controller)]),
            )
            .await?;
            for _ in 0..300 {
                if app.snapshot(GRAPH).await.is_some() {
                    observed.fetch_or(1, Ordering::SeqCst);
                }
                if calls.load(Ordering::SeqCst) > 1 {
                    observed.fetch_or(2, Ordering::SeqCst);
                }
                if observed.load(Ordering::SeqCst) == 3 {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::io::Error::other("peer did not discover and serve remote graph").into())
        }
    });

    sim.host("core-a", || async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
        let (evaluator, bundle) = evaluator_bundle_with_resource();
        let app = Application::start(evaluator, Journal::new(storage), BTreeMap::new()).await?;
        app.apply(Command::CreateGraph(graph(bundle, 1))).await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
        app.apply(Command::UpdateGraph {
            graph_id: GRAPH,
            expected_generation: Generation::new(1).unwrap(),
            components: graph(bundle, 2).components,
        })
        .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    });

    let verified = Arc::clone(&observed);
    sim.client("verifier", async move {
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(
            verified.load(Ordering::SeqCst),
            3,
            "node B must list the remote graph and run its controller lane"
        );
        Ok(())
    });

    sim.run().unwrap();
}

#[test]
fn durable_stall_converges_across_nodes() {
    let _deterministic_process = deterministic_process(92);
    let observed = Arc::new(AtomicUsize::new(0));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(92)
        .simulation_duration(Duration::from_secs(6))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20));
    let mut sim = builder.build();
    sim.host("storage", storage_server);

    let peer_observed = Arc::clone(&observed);
    sim.host("core-b", move || {
        let observed = Arc::clone(&peer_observed);
        async move {
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let (evaluator, _) = evaluator_and_bundle();
            let app = Application::start(evaluator, Journal::new(storage), BTreeMap::new()).await?;
            for _ in 0..300 {
                if app
                    .snapshot(GRAPH)
                    .await
                    .and_then(|state| state.graph(GRAPH).and_then(|graph| graph.stall()).cloned())
                    .is_some()
                {
                    observed.store(1, Ordering::SeqCst);
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::io::Error::other("peer did not observe durable stall").into())
        }
    });

    sim.host("core-a", || async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
        let journal = Journal::new(Arc::clone(&storage));
        let (evaluator, bundle) = evaluator_and_bundle();
        let app = Application::start(evaluator, journal.clone(), BTreeMap::new()).await?;
        app.apply(Command::CreateGraph(graph(bundle, 1))).await?;
        let (_, tail) = journal.load_with_tail(GRAPH).await?;
        journal
            .append(
                GRAPH,
                tail,
                &henosis_types::CoreEvent::StallDetected(Stall::new(
                    GRAPH,
                    Generation::new(1).unwrap(),
                    vec![
                        ComponentName::new("left").unwrap(),
                        ComponentName::new("left").unwrap(),
                    ],
                )),
            )
            .await?;
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    });

    let verified = Arc::clone(&observed);
    sim.client("verifier", async move {
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(verified.load(Ordering::SeqCst), 1);
        Ok(())
    });
    sim.run().unwrap();
}

#[test]
fn process_bounce_mid_report_replays_controller_work() {
    let _deterministic_process = deterministic_process(93);
    let calls = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let mut builder = turmoil::Builder::new();
    builder
        .rng_seed(93)
        .simulation_duration(Duration::from_secs(10))
        .min_message_latency(Duration::from_millis(1))
        .max_message_latency(Duration::from_millis(20));
    let mut sim = builder.build();
    sim.host("storage", storage_server);

    sim.host("core-a", || async {
        let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
        let (evaluator, bundle) = evaluator_bundle_with_resource();
        let app = Application::start(evaluator, Journal::new(storage), BTreeMap::new()).await?;
        app.apply(Command::CreateGraph(graph(bundle, 1))).await?;
        tokio::time::sleep(Duration::from_secs(5)).await;
        Ok(())
    });

    let first_calls = Arc::clone(&calls);
    sim.host("core-b", move || {
        let calls = Arc::clone(&first_calls);
        async move {
            tokio::time::sleep(Duration::from_millis(400)).await;
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let (evaluator, _) = evaluator_bundle_with_resource();
            let name = ControllerName::new("test").unwrap();
            let controller: Arc<dyn Controller> = Arc::new(UnreportedController {
                calls: Arc::clone(&calls),
                name: name.clone(),
            });
            let _app = Application::start(
                evaluator,
                Journal::new(storage),
                BTreeMap::from([(name, controller)]),
            )
            .await?;
            while calls.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Ok(())
        }
    });

    let replay_calls = Arc::clone(&calls);
    let replay_completed = Arc::clone(&completed);
    sim.host("core-c", move || {
        let calls = Arc::clone(&replay_calls);
        let completed = Arc::clone(&replay_completed);
        async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let storage: Arc<dyn StorageEngine> = Arc::new(NetworkStorage { host: "storage" });
            let (evaluator, _) = evaluator_bundle_with_resource();
            let name = ControllerName::new("test").unwrap();
            let controller: Arc<dyn Controller> = Arc::new(CountingController {
                calls: Arc::clone(&calls),
                name: name.clone(),
            });
            let app = Application::start(
                evaluator,
                Journal::new(storage),
                BTreeMap::from([(name, controller)]),
            )
            .await?;
            for _ in 0..300 {
                if app
                    .snapshot(GRAPH)
                    .await
                    .and_then(|state| state.graph(GRAPH).cloned())
                    .is_some_and(|graph| graph.controllers_complete())
                {
                    completed.store(1, Ordering::SeqCst);
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(std::io::Error::other("restarted process did not admit progress").into())
        }
    });

    let verified_calls = Arc::clone(&calls);
    let verified_completed = Arc::clone(&completed);
    sim.client("verifier", async move {
        tokio::time::sleep(Duration::from_secs(5)).await;
        assert!(verified_calls.load(Ordering::SeqCst) >= 2);
        assert_eq!(verified_completed.load(Ordering::SeqCst), 1);
        Ok(())
    });
    sim.run().unwrap();
}
