//! Deterministic simulation and property tests for the core
//! command/state/effect loop.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::num::NonZeroU32;
    use std::sync::Arc;

    use async_trait::async_trait;
    use faultline::Error;
    use futures::StreamExt;
    use futures::future::BoxFuture;
    use henosis_evaluation_engine::BundleSource;
    use henosis_evaluation_engine::EngineConfig;
    use henosis_evaluation_engine::EvaluationEngine;
    use henosis_evaluation_engine::ResourceContract;
    use henosis_evaluation_engine::ResourceRegistry;
    use henosis_journal::Journal;
    use henosis_multi_stream_merge::StreamCatalog;
    use henosis_orchestrator::Command;
    use henosis_orchestrator::ControllerEffect;
    use henosis_orchestrator::Core;
    use henosis_orchestrator::MaterializedCore;
    use henosis_storage::AppendRecord;
    use henosis_storage::MemoryStorage;
    use henosis_storage::StorageDomainError;
    use henosis_storage::StorageEngine;
    use henosis_storage::StreamName;
    use henosis_storage::StreamPosition;
    use henosis_types::BlockedDetail;
    use henosis_types::BundleRef;
    use henosis_types::ComponentInput;
    use henosis_types::ComponentIntent;
    use henosis_types::ComponentName;
    use henosis_types::ComponentOutput;
    use henosis_types::ContentDigest;
    use henosis_types::ControllerCommand;
    use henosis_types::ControllerName;
    use henosis_types::ControllerReport;
    use henosis_types::EvaluationAttempt;
    use henosis_types::EvaluationError;
    use henosis_types::EvaluationRequest;
    use henosis_types::EvaluationResource;
    use henosis_types::Evaluator;
    use henosis_types::Generation;
    use henosis_types::GraphId;
    use henosis_types::GraphName;
    use henosis_types::InputCellState;
    use henosis_types::InputName;
    use henosis_types::KindName;
    use henosis_types::KindVersion;
    use henosis_types::NativeValue;
    use henosis_types::NewBlockedEvaluation;
    use henosis_types::NewCompleteEvaluation;
    use henosis_types::NewComponentIntent;
    use henosis_types::NewControllerReport;
    use henosis_types::NewEvaluationResource;
    use henosis_types::NewGraphIntent;
    use henosis_types::NewPlan;
    use henosis_types::ObservedOutput;
    use henosis_types::ObservedOutputBinding;
    use henosis_types::ObservedOutputKey;
    use henosis_types::OutputAvailability;
    use henosis_types::OutputDeclaration;
    use henosis_types::OutputName;
    use henosis_types::OutputRef;
    use henosis_types::Plan;
    use henosis_types::PublicationId;
    use henosis_types::ResourceDisposition;
    use henosis_types::ResourceDispositionKind;
    use henosis_types::ResourceId;
    use henosis_types::ResourceName;
    use henosis_types::StaticOutput;
    use proptest::prelude::*;

    #[derive(Debug)]
    struct FixedCatalog(Vec<StreamName>);

    #[async_trait]
    impl StreamCatalog for FixedCatalog {
        async fn streams(
            &self,
        ) -> Result<Vec<StreamName>, Error<faultline::Never, anyhow::Error, anyhow::Error>>
        {
            Ok(self.0.clone())
        }
    }

    const REAL_PRODUCER_BUNDLE: &str =
        include_str!("../../../crates/evaluation-engine/fixtures/producer.bundle.js");
    const REAL_CONSUMER_BUNDLE: &str =
        include_str!("../../../crates/evaluation-engine/fixtures/consumer.bundle.js");

    struct FixtureBundleSource {
        bundles: BTreeMap<BundleRef, Arc<[u8]>>,
    }

    impl BundleSource for FixtureBundleSource {
        fn load(&self, bundle: BundleRef) -> BoxFuture<'_, Result<Arc<[u8]>, EvaluationError>> {
            let result = self
                .bundles
                .get(&bundle)
                .cloned()
                .ok_or_else(|| EvaluationError::new("fixture bundle is missing"));
            Box::pin(async move { result })
        }
    }

    struct TestResourceRegistry;

    impl ResourceRegistry for TestResourceRegistry {
        fn validate(
            &self,
            kind: &KindVersion,
            body: &serde_json::Value,
        ) -> Result<ResourceContract, String> {
            if kind.to_string() != "test/item@1" || !body.is_object() {
                return Err("expected a test/item@1 object".to_owned());
            }
            Ok(ResourceContract::new(
                controller_name("test"),
                vec![output_name("result")],
            ))
        }
    }

    #[derive(Debug, Default)]
    struct FakeEvaluator;

    impl Evaluator for FakeEvaluator {
        fn evaluate<'a>(
            &'a self,
            request: EvaluationRequest,
        ) -> BoxFuture<'a, Result<EvaluationAttempt, EvaluationError>> {
            Box::pin(async move {
                let behavior = request.bundle().digest().as_bytes()[0];
                match behavior {
                    1 => producer(&request, 11, "a", "url"),
                    2 => producer(&request, 12, "b", "host"),
                    3 => consumer(&request),
                    4 => block_on_first_input(&request),
                    _ => Err(EvaluationError::new("unknown fake bundle")),
                }
            })
        }
    }

    fn producer(
        request: &EvaluationRequest,
        id_byte: u8,
        resource_name: &str,
        component_output: &str,
    ) -> Result<EvaluationAttempt, EvaluationError> {
        let resource = evaluation_resource(
            request.component(),
            id_byte,
            resource_name,
            &format!("controller-{resource_name}"),
            serde_json::json!({"name": resource_name}),
            vec![OutputDeclaration::new(
                output_name("target"),
                OutputAvailability::Observed,
            )],
        );
        EvaluationAttempt::complete(
            request.snapshot(),
            NewCompleteEvaluation {
                resources: vec![resource],
                outputs: Vec::new(),
                observed_outputs: vec![ObservedOutputBinding::new(
                    output_name(component_output),
                    address(resource_name),
                    output_name("target"),
                )],
                reads: Vec::new(),
            },
        )
        .map_err(|error| EvaluationError::new(error.to_string()))
    }

    fn consumer(request: &EvaluationRequest) -> Result<EvaluationAttempt, EvaluationError> {
        let mut values = Vec::new();
        for cell in request.snapshot().iter() {
            match cell.state() {
                InputCellState::Available(value) => {
                    values.push((cell.name().clone(), value.clone()));
                }
                InputCellState::Blocked => return block_on(request, cell.name()),
                InputCellState::Absent => {}
            }
        }
        let body = serde_json::json!({
            "inputs": values
                .iter()
                .map(|(name, value)| (name.as_str().to_owned(), value.as_json().clone()))
                .collect::<serde_json::Map<_, _>>()
        });
        EvaluationAttempt::complete(
            request.snapshot(),
            NewCompleteEvaluation {
                resources: vec![evaluation_resource(
                    request.component(),
                    13,
                    "consumer",
                    "controller-consumer",
                    body,
                    Vec::new(),
                )],
                outputs: vec![StaticOutput::new(
                    output_name("summary"),
                    NativeValue::new(serde_json::json!("ready")).expect("fixture output is JSON"),
                )],
                observed_outputs: Vec::new(),
                reads: request
                    .snapshot()
                    .iter()
                    .map(|cell| cell.name().clone())
                    .collect(),
            },
        )
        .map_err(|error| EvaluationError::new(error.to_string()))
    }

    fn block_on_first_input(
        request: &EvaluationRequest,
    ) -> Result<EvaluationAttempt, EvaluationError> {
        let input = request
            .snapshot()
            .iter()
            .next()
            .expect("cycle fake has one input");
        block_on(request, input.name())
    }

    fn block_on(
        request: &EvaluationRequest,
        input_name: &InputName,
    ) -> Result<EvaluationAttempt, EvaluationError> {
        let cell = request
            .snapshot()
            .get(input_name)
            .expect("fake reads a declared input");
        EvaluationAttempt::blocked(
            request.snapshot(),
            NewBlockedEvaluation {
                resources: Vec::new(),
                blocked: BlockedDetail::new(
                    input_name.clone(),
                    cell.source().clone(),
                    "reading `.value`",
                    "waiting for the fake controller output",
                ),
                reads: vec![input_name.clone()],
            },
        )
        .map_err(|error| EvaluationError::new(error.to_string()))
    }

    fn evaluation_resource(
        component: &ComponentName,
        id_byte: u8,
        name: &str,
        controller: &str,
        body: serde_json::Value,
        outputs: Vec<OutputDeclaration>,
    ) -> EvaluationResource {
        let canonical = NativeValue::new(body.clone())
            .expect("fixture body is finite JSON")
            .canonical()
            .to_owned();
        EvaluationResource::new(NewEvaluationResource {
            id: resource_id(id_byte),
            component: component.clone(),
            kind: kind(),
            name: resource_name(name),
            controller: controller_name(controller),
            body,
            canonical,
            outputs,
        })
        .expect("fixture resource follows the host protocol")
    }

    fn address(name: &str) -> henosis_types::ResourceAddress {
        henosis_types::ResourceAddress::new(kind(), resource_name(name))
    }

    fn kind() -> KindVersion {
        KindVersion::new(
            KindName::new("test/resource").expect("valid kind"),
            NonZeroU32::new(1).expect("one is non-zero"),
        )
    }

    fn component(
        name: &str,
        behavior: u8,
        inputs: Vec<ComponentInput>,
        outputs: Vec<ComponentOutput>,
    ) -> ComponentIntent {
        ComponentIntent::new(NewComponentIntent {
            name: component_name(name),
            bundle: BundleRef::new(ContentDigest::from_bytes([behavior; 32])),
            inputs,
            outputs,
        })
        .expect("fixture component is valid")
    }

    fn component_bundle(
        name: &str,
        bundle: BundleRef,
        inputs: Vec<ComponentInput>,
        outputs: Vec<ComponentOutput>,
    ) -> ComponentIntent {
        ComponentIntent::new(NewComponentIntent {
            name: component_name(name),
            bundle,
            inputs,
            outputs,
        })
        .expect("fixture component is valid")
    }

    fn observed_component_output(name: &str) -> ComponentOutput {
        ComponentOutput::new(output_name(name), OutputAvailability::Observed, false)
    }

    fn static_component_output(name: &str) -> ComponentOutput {
        ComponentOutput::new(output_name(name), OutputAvailability::Static, false)
    }

    fn input(name: &str, producer: &str, output: &str) -> ComponentInput {
        ComponentInput::new(
            InputName::new(name).expect("valid input"),
            OutputRef::new(component_name(producer), output_name(output)),
            false,
        )
    }

    fn graph(components: Vec<ComponentIntent>) -> NewGraphIntent {
        NewGraphIntent {
            id: graph_id(),
            name: GraphName::new("test-graph").expect("valid graph name"),
            components,
        }
    }

    fn graph_id() -> GraphId {
        GraphId::from_bytes([1; 16])
    }

    fn resource_id(value: u8) -> ResourceId {
        ResourceId::from_bytes([value; 16])
    }

    fn publication_id(value: u8) -> PublicationId {
        PublicationId::from_bytes([value; 16])
    }

    fn component_name(value: &str) -> ComponentName {
        ComponentName::new(value).expect("valid component name")
    }

    fn controller_name(value: &str) -> ControllerName {
        ControllerName::new(value).expect("valid controller name")
    }

    fn resource_name(value: &str) -> ResourceName {
        ResourceName::new(value).expect("valid resource name")
    }

    fn output_name(value: &str) -> OutputName {
        OutputName::new(value).expect("valid output name")
    }

    fn controller_report(
        effect: &ControllerEffect,
        publication: u8,
        value: &str,
    ) -> ControllerReport {
        let ControllerCommand::Reconcile(slice) = effect.command() else {
            panic!("fixture expects a reconcile effect");
        };
        let dispositions = slice
            .resources()
            .iter()
            .map(|resource| ResourceDisposition::new(resource.id(), ResourceDispositionKind::Ready))
            .collect();
        let outputs = slice
            .resources()
            .iter()
            .flat_map(|resource| {
                resource
                    .outputs()
                    .filter(|output| output.availability() == OutputAvailability::Observed)
                    .map(move |output| {
                        ObservedOutput::new(
                            ObservedOutputKey::new(resource.id(), output.name().clone()),
                            NativeValue::new(serde_json::json!(value))
                                .expect("fixture output is JSON"),
                        )
                    })
            })
            .collect::<Vec<_>>();
        ControllerReport::new(NewControllerReport {
            graph_id: slice.graph_id(),
            generation: slice.generation(),
            plan_digest: slice.plan_digest(),
            controller: slice.controller().clone(),
            publication_id: Some(publication_id(publication)),
            dispositions,
            outputs,
        })
        .expect("fake controller emits one atomic holistic report")
    }

    #[tokio::test]
    async fn core_loop_accepts_the_real_isolate_evaluator() {
        let producer_bundle =
            BundleRef::new(ContentDigest::digest(REAL_PRODUCER_BUNDLE.as_bytes()));
        let consumer_bundle =
            BundleRef::new(ContentDigest::digest(REAL_CONSUMER_BUNDLE.as_bytes()));
        let source = Arc::new(FixtureBundleSource {
            bundles: BTreeMap::from([
                (producer_bundle, Arc::from(REAL_PRODUCER_BUNDLE.as_bytes())),
                (consumer_bundle, Arc::from(REAL_CONSUMER_BUNDLE.as_bytes())),
            ]),
        });
        let engine = EvaluationEngine::new(
            source,
            Arc::new(TestResourceRegistry),
            EngineConfig {
                workers: 1,
                ..EngineConfig::default()
            },
        )
        .expect("real evaluator starts");
        let producer = component_bundle(
            "producer",
            producer_bundle,
            Vec::new(),
            vec![static_component_output("value")],
        );
        let consumer = component_bundle(
            "consumer",
            consumer_bundle,
            vec![input("source", "producer", "value")],
            vec![
                static_component_output("summary"),
                observed_component_output("result"),
            ],
        );
        let mut core = Core::new(Arc::new(engine));

        let transition = core
            .handle(Command::CreateGraph(graph(vec![producer, consumer])))
            .await
            .expect("real bundles evaluate through the core seam");
        let plan = core
            .state()
            .graph(graph_id())
            .and_then(|state| state.plan())
            .expect("plan exists");
        assert!(plan.is_complete());
        assert_eq!(plan.resources().len(), 1);
        assert_eq!(
            plan.resources()
                .next()
                .expect("one resource")
                .path()
                .address()
                .to_string(),
            "test/item@1/main"
        );
        assert_eq!(transition.effects().len(), 1);
    }

    #[tokio::test]
    async fn loop_unblocks_after_controller_output() {
        let producer = component(
            "database",
            1,
            Vec::new(),
            vec![observed_component_output("url")],
        );
        let consumer = component(
            "web",
            3,
            vec![input("databaseUrl", "database", "url")],
            vec![static_component_output("summary")],
        );
        let mut core = Core::new(Arc::new(FakeEvaluator));

        let initial = core
            .handle(Command::CreateGraph(graph(vec![producer, consumer])))
            .await
            .expect("graph creation succeeds");
        let initial_plan = core
            .state()
            .graph(graph_id())
            .and_then(|state| state.plan())
            .expect("initial plan exists");
        assert!(!initial_plan.is_complete());
        assert_eq!(initial_plan.resources().len(), 1);
        assert_eq!(initial.effects().len(), 1);

        let report = controller_report(&initial.effects()[0], 21, "postgres.test");
        let after_output = core
            .handle(Command::ReportController(report))
            .await
            .expect("controller output is accepted");
        let final_plan = core
            .state()
            .graph(graph_id())
            .and_then(|state| state.plan())
            .expect("final plan exists");
        assert!(final_plan.is_complete());
        assert_eq!(final_plan.resources().len(), 2);
        assert!(!after_output.effects().is_empty());

        let events = initial
            .events()
            .iter()
            .chain(after_output.events())
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(MaterializedCore::fold(&events), core.state().clone());
    }

    async fn converge(order: [usize; 2]) -> Plan {
        let first = component(
            "first",
            1,
            Vec::new(),
            vec![observed_component_output("url")],
        );
        let second = component(
            "second",
            2,
            Vec::new(),
            vec![observed_component_output("host")],
        );
        let consumer = component(
            "consumer",
            3,
            vec![input("a", "first", "url"), input("b", "second", "host")],
            vec![static_component_output("summary")],
        );
        let mut core = Core::new(Arc::new(FakeEvaluator));
        let initial = core
            .handle(Command::CreateGraph(graph(vec![first, second, consumer])))
            .await
            .expect("graph creation succeeds");
        assert_eq!(initial.effects().len(), 2);
        let controller_order = [
            initial.effects()[0].controller().clone(),
            initial.effects()[1].controller().clone(),
        ];
        let mut available_effects = initial.effects().to_vec();
        for index in order {
            let effect = available_effects
                .iter()
                .find(|effect| effect.controller() == &controller_order[index])
                .expect("level-triggered dispatch includes the pending controller");
            let report = controller_report(
                effect,
                31 + index as u8,
                if index == 0 { "one.test" } else { "two.test" },
            );
            let transition = core
                .handle(Command::ReportController(report))
                .await
                .expect("output arrival succeeds");
            if !transition.effects().is_empty() {
                available_effects = transition.effects().to_vec();
            }
        }
        core.state()
            .graph(graph_id())
            .and_then(|state| state.plan())
            .expect("converged plan exists")
            .clone()
    }

    proptest! {
        #[test]
        fn output_arrival_order_converges(reverse in any::<bool>()) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test runtime builds");
            let order = if reverse { [1, 0] } else { [0, 1] };
            let actual = runtime.block_on(converge(order));
            let expected = runtime.block_on(converge([0, 1]));
            prop_assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn quiescent_cycle_reports_named_stall() {
        let left = component(
            "left",
            4,
            vec![input("rightValue", "right", "value")],
            vec![observed_component_output("value")],
        );
        let right = component(
            "right",
            4,
            vec![input("leftValue", "left", "value")],
            vec![observed_component_output("value")],
        );
        let mut core = Core::new(Arc::new(FakeEvaluator));
        let transition = core
            .handle(Command::CreateGraph(graph(vec![left, right])))
            .await
            .expect("cyclic graph is represented as a runtime stall");
        let stall = core
            .state()
            .graph(graph_id())
            .and_then(|state| state.stall())
            .expect("quiescent blocked cycle is detected");
        let names = stall
            .cycle()
            .iter()
            .map(ComponentName::as_str)
            .collect::<Vec<_>>();
        assert!(names.contains(&"left"));
        assert!(names.contains(&"right"));
        assert!(
            transition
                .events()
                .iter()
                .any(|event| matches!(event, henosis_types::CoreEvent::StallDetected(_)))
        );
    }

    proptest! {
        #[test]
        fn fold_replay_is_deterministic(update_count in 0_u8..16) {
            let only = component("only", 1, Vec::new(), vec![observed_component_output("url")]);
            let initial = henosis_types::GraphIntent::new(graph(vec![only.clone()]))
                .expect("fixture graph is valid");
            let mut events = vec![henosis_types::CoreEvent::GraphCreated(initial.clone())];
            let mut intent = initial;
            for _ in 0..update_count {
                intent = intent.replace_components(vec![only.clone()])
                    .expect("fixture update is valid");
                events.push(henosis_types::CoreEvent::GraphUpdated(intent.clone()));
                events.push(henosis_types::CoreEvent::PlanAccepted {
                    graph_id: graph_id(),
                    plan: Plan::new(NewPlan {
                        generation: intent.generation(),
                        resources: Vec::new(),
                        blocked: Vec::new(),
                    }).expect("empty fixture plan is valid"),
                });
            }
            let full = MaterializedCore::fold(&events);
            let split = events.len() / 2;
            let mut resumed = MaterializedCore::fold(&events[..split]);
            for event in &events[split..] {
                resumed.apply(event);
            }
            prop_assert_eq!(full.clone(), MaterializedCore::fold(&events));
            prop_assert_eq!(full, resumed);
        }
    }

    #[tokio::test]
    async fn storage_cas_journal_replay_and_virtual_merge_hold() {
        let storage = MemoryStorage::default();
        let first = StreamName::new("first").expect("valid stream name");
        let second = StreamName::new("second").expect("valid stream name");
        storage
            .append(
                &first,
                StreamPosition::default(),
                vec![AppendRecord::new(b"one".to_vec())],
            )
            .await
            .expect("initial append succeeds");
        let conflict = storage
            .append(
                &first,
                StreamPosition::default(),
                vec![AppendRecord::new(b"stale".to_vec())],
            )
            .await
            .expect_err("stale compare-and-set is rejected");
        assert!(matches!(
            conflict,
            Error::Domain(StorageDomainError::CasConflict {
                expected: 0,
                actual: 1
            })
        ));
        storage
            .append(
                &second,
                StreamPosition::default(),
                vec![AppendRecord::new(b"two".to_vec())],
            )
            .await
            .expect("independent stream append succeeds");

        let storage: Arc<dyn StorageEngine> = Arc::new(storage);
        let catalog = FixedCatalog(vec![second.clone(), first.clone(), first]);
        let mut merged =
            henosis_multi_stream_merge::subscribe(Arc::clone(&storage), &catalog, &BTreeMap::new())
                .await
                .expect("catalog subscription succeeds");
        let left = merged
            .next()
            .await
            .expect("first virtual record arrives")
            .expect("first source read succeeds");
        let right = merged
            .next()
            .await
            .expect("second virtual record arrives")
            .expect("second source read succeeds");
        assert_eq!(left.virtual_offset(), 0);
        assert_eq!(right.virtual_offset(), 1);
        assert_ne!(left.record().stream(), right.record().stream());

        let journal = Journal::new(storage);
        let intent = henosis_types::GraphIntent::new(graph(vec![component(
            "only",
            1,
            Vec::new(),
            vec![observed_component_output("url")],
        )]))
        .expect("journal fixture graph is valid");
        journal
            .append(
                graph_id(),
                StreamPosition::default(),
                &henosis_types::CoreEvent::GraphCreated(intent.clone()),
            )
            .await
            .expect("journal append succeeds");
        assert_eq!(
            journal
                .load(graph_id())
                .await
                .expect("journal load succeeds"),
            vec![henosis_types::CoreEvent::GraphCreated(intent)]
        );
    }

    #[test]
    fn canonical_json_uses_utf16_key_order_and_fails_closed() {
        let body = serde_json::json!({"\u{e000}": 1, "\u{10000}": 2});
        let value = NativeValue::new(body.clone()).expect("finite JSON canonicalizes");
        assert_eq!(value.canonical(), "{\"𐀀\":2,\"\":1}");
        assert_eq!(
            NativeValue::new(serde_json::json!([1.0, 1e21, 1e-7]))
                .expect("finite JSON canonicalizes")
                .canonical(),
            "[1,1e+21,1e-7]"
        );
        assert!(NativeValue::from_canonical(body, "{}").is_err());
    }

    #[test]
    fn generation_constructor_rejects_zero() {
        assert!(Generation::new(0).is_err());
        let id = graph_id();
        let wire = id.to_string();
        assert!(wire.starts_with("graph_"));
        assert_eq!(wire.parse::<GraphId>().expect("TypeID round trips"), id);
    }
}
