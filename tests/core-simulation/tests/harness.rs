use std::collections::BTreeMap;
use std::sync::Arc;

use faultline::Error;
use henosis_journal::Journal;
use henosis_sim::RealControllerAction;
use henosis_sim::RealControllerWorld;
use henosis_sim::RunStatus;
use henosis_sim::Scenario;
use henosis_sim::SimAction;
use henosis_sim::SimWorld;
use henosis_sim::canonical_resources;
use henosis_sim::run_seed;
use henosis_sim::slow_all_inputs_model;
use henosis_storage::AppendRecord;
use henosis_storage::StorageDomainError;
use henosis_storage::StreamName;
use henosis_storage::StreamPosition;
use henosis_testkit::AppendFault;
use henosis_testkit::ComponentProgram;
use henosis_testkit::MemS2;
use henosis_testkit::ProgramEvaluator;
use henosis_testkit::Quiescence;
use henosis_testkit::ResourceProgram;
use henosis_testkit::Seed;
use henosis_testkit::WaitGraph;
use henosis_testkit::WaitNode;
use henosis_types::ComponentName;
use henosis_types::ControllerName;
use henosis_types::EvaluationRequest;
use henosis_types::EvaluationSnapshot;
use henosis_types::Evaluator;
use henosis_types::Generation;
use henosis_types::GraphId;
use henosis_types::InputCell;
use henosis_types::InputCellState;
use henosis_types::InputName;
use henosis_types::NativeValue;
use henosis_types::OutputName;
use henosis_types::OutputRef;
use henosis_types::ResourceId;
use henosis_types::ResourceName;
use proptest::prelude::*;
use proptest::test_runner::Config;
use proptest_state_machine::ReferenceStateMachine;
use proptest_state_machine::StateMachineTest;
use proptest_state_machine::prop_state_machine;

fn property_cases() -> u32 {
    std::env::var("HENOSIS_PROPTEST_CASES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(32)
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("simulation runtime builds")
}

proptest! {
    #![proptest_config(Config {
        cases: property_cases(),
        max_shrink_iters: 2_048,
        .. Config::default()
    })]

    #[test]
    fn same_seed_twice_replays_byte_for_byte(seed in any::<u64>(), width in 1_u8..5) {
        let scenario = Scenario::bounded(width);
        let first = runtime().block_on(run_seed(Seed::from_u64(seed), &scenario, 64));
        let second = runtime().block_on(run_seed(Seed::from_u64(seed), &scenario, 64));
        prop_assert_eq!(first.trace, second.trace);
        prop_assert_eq!(first.plan, second.plan);
    }

    #[test]
    fn every_fair_arrival_schedule_converges_to_same_plan(seed in any::<u64>(), width in 1_u8..5) {
        let scenario = Scenario::bounded(width);
        let random = runtime().block_on(run_seed(Seed::from_u64(seed), &scenario, 64));
        let canonical = runtime().block_on(run_seed(Seed::from_u64(0), &scenario, 64));
        let slow_model = slow_all_inputs_model(&scenario);
        prop_assert_eq!(random.status, RunStatus::Converged);
        prop_assert_eq!(canonical_resources(&random.plan), slow_model);
        prop_assert_eq!(random.plan, canonical.plan);
        for history in random.presence_history.values() {
            prop_assert!(!history.windows(3).any(|window| window == [true, false, true]));
        }
    }
}

proptest! {
    #![proptest_config(Config {
        cases: property_cases(),
        .. Config::default()
    })]

    #[test]
    fn under_specification_is_monotone_for_fixed_presence_facts(
        available_a in 0_u8..4,
        extra in 0_u8..4,
    ) {
        let available_b = available_a | extra;
        let partial = runtime().block_on(evaluate_monotone_program(available_a));
        let fuller = runtime().block_on(evaluate_monotone_program(available_b));
        prop_assert!(partial.is_subset(&fuller));
    }
}

async fn evaluate_monotone_program(available: u8) -> std::collections::BTreeSet<String> {
    let evaluator = ProgramEvaluator::default();
    let first = InputName::new("first").expect("input name is valid");
    let second = InputName::new("second").expect("input name is valid");
    let bundle = evaluator.register(ComponentProgram {
        resources: vec![ResourceProgram {
            id: ResourceId::from_bytes([55; 16]),
            name: ResourceName::new("monotone").expect("resource name is valid"),
            controller: ControllerName::new("controller-monotone")
                .expect("controller name is valid"),
            required_values: vec![first.clone(), second.clone()],
            observed_component_output: None,
        }],
        static_outputs: BTreeMap::new(),
    });
    let cells = [first, second]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let state = if available & (1 << index) != 0 {
                InputCellState::Available(
                    NativeValue::new(serde_json::json!(index))
                        .expect("generated value is finite JSON"),
                )
            } else {
                InputCellState::Blocked
            };
            InputCell::new(
                name,
                OutputRef::new(
                    ComponentName::new(format!("producer-{index}"))
                        .expect("component name is valid"),
                    OutputName::new("value").expect("output name is valid"),
                ),
                false,
                state,
            )
            .expect("blocked or available is valid for required input")
        })
        .collect();
    let request = EvaluationRequest::new(
        GraphId::from_bytes([44; 16]),
        Generation::new(1).expect("one is valid"),
        ComponentName::new("consumer").expect("component name is valid"),
        bundle,
        EvaluationSnapshot::new(cells).expect("inputs are unique"),
    );
    evaluator
        .evaluate(request)
        .await
        .expect("program evaluation succeeds")
        .resources()
        .iter()
        .map(|resource| resource.resource().body().canonical().to_owned())
        .collect()
}

#[test]
fn late_generation_output_is_rejected_without_contamination() {
    runtime().block_on(async {
        let mut world = SimWorld::new(Seed::from_u64(41), &Scenario::bounded(2)).await;
        world.start_next_generation().await;
        let before = world.canonical_state();
        let stale = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, SimAction::DeliverStale(_)))
            .expect("generation switch retains an old report for a racing delivery");
        world.apply(stale).await;
        assert_eq!(world.canonical_state(), before);
    });
}

#[test]
fn timeout_after_commit_is_resolved_through_the_product_journal() {
    runtime().block_on(async {
        let run = run_seed(Seed::from_u64(19), &Scenario::bounded(1), 32).await;
        let event = run.events.first().expect("simulation emits a graph event");
        let storage = MemS2::default();
        storage.script([AppendFault::CommitThenTimeout]);
        let journal = Journal::new(Arc::new(storage.clone()));
        let graph_id = GraphId::from_bytes([7; 16]);

        let ack = journal
            .append(graph_id, StreamPosition::default(), event)
            .await
            .expect("journal resolves a committed append after timeout");
        assert_eq!(ack.start().sequence(), 0);
        assert_eq!(ack.tail().sequence(), 1);
        assert_eq!(
            journal.load(graph_id).await.expect("replay succeeds"),
            vec![event.clone()]
        );
    });
}

#[test]
fn unknown_append_with_different_stored_bytes_is_a_cas_loss() {
    runtime().block_on(async {
        let run = run_seed(Seed::from_u64(20), &Scenario::bounded(1), 32).await;
        let event = run.events.first().expect("simulation emits a graph event");
        let storage = MemS2::default();
        storage.script([AppendFault::CommitDifferentThenTimeout]);
        let journal = Journal::new(Arc::new(storage));
        let graph_id = GraphId::from_bytes([8; 16]);

        let error = journal
            .append(graph_id, StreamPosition::default(), event)
            .await
            .expect_err("different committed bytes lose the append CAS");
        assert!(matches!(
            error,
            Error::Domain(StorageDomainError::CasConflict {
                expected: 0,
                actual: 1,
            })
        ));
    });
}

#[test]
fn stall_diagnostics_distinguish_external_block_from_internal_cycle() {
    let component = WaitNode::Component("web".to_owned());
    let input = WaitNode::Input("databaseUrl".to_owned());
    let external = WaitNode::ExternalInput("databaseUrl".to_owned());
    let mut blocked = WaitGraph::default();
    blocked.add_edge(component.clone(), input.clone());
    blocked.add_edge(input, external);
    assert_eq!(
        blocked.classify(false, false).classification,
        Quiescence::ExternallyBlocked
    );

    let mut cycle = WaitGraph::default();
    cycle.add_edge(
        component.clone(),
        WaitNode::Component("database".to_owned()),
    );
    cycle.add_edge(WaitNode::Component("database".to_owned()), component);
    let report = cycle.classify(false, false);
    assert_eq!(report.classification, Quiescence::Deadlocked);
    assert!(!report.cycle.is_empty());
    insta::assert_yaml_snapshot!(report, @r###"
    classification: Deadlocked
    cycle:
      - Component: database
      - Component: web
      - Component: database
    edges:
      - from:
          Component: database
        to:
          Component: web
      - from:
          Component: web
        to:
          Component: database
    "###);
}

#[test]
fn chaos_then_healthy_environment_converges_within_bound() {
    runtime().block_on(async {
        let mut world = SimWorld::new(Seed::from_u64(0x5eed), &Scenario::bounded(4)).await;
        let first = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, SimAction::CompleteTarget(_)))
            .expect("chaos phase has work");
        world.apply(first).await;
        let result = world.run(64).await;
        assert_eq!(result.status, RunStatus::Converged);
    });
}

#[derive(Clone, Debug)]
struct StorageModel {
    records: Vec<Vec<u8>>,
    expected: u64,
}

#[derive(Clone, Debug)]
enum StorageTransition {
    Append(u8),
    StaleAppend,
    Read(u8, u8),
}

struct StorageMachine;

impl ReferenceStateMachine for StorageMachine {
    type State = StorageModel;
    type Transition = StorageTransition;

    fn init_state() -> BoxedStrategy<Self::State> {
        Just(StorageModel {
            records: Vec::new(),
            expected: 0,
        })
        .boxed()
    }

    fn transitions(_state: &Self::State) -> BoxedStrategy<Self::Transition> {
        prop_oneof![
            4 => any::<u8>().prop_map(StorageTransition::Append),
            2 => Just(StorageTransition::StaleAppend),
            2 => (any::<u8>(), 0_u8..8).prop_map(|(from, limit)| StorageTransition::Read(from, limit)),
        ]
        .boxed()
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        if let StorageTransition::Append(value) = transition {
            state.records.push(vec![*value]);
            state.expected += 1;
        }
        state
    }
}

struct StorageSut {
    storage: MemS2,
    stream: StreamName,
}

impl StateMachineTest for StorageSut {
    type Reference = StorageMachine;
    type SystemUnderTest = Self;

    fn init_test(_: &StorageModel) -> Self::SystemUnderTest {
        Self {
            storage: MemS2::default(),
            stream: StreamName::new("state-machine").expect("stream name is valid"),
        }
    }

    fn apply(state: Self, reference: &StorageModel, transition: StorageTransition) -> Self {
        match transition {
            StorageTransition::Append(value) => {
                state
                    .storage
                    .append(
                        &state.stream,
                        StreamPosition::new(reference.expected - 1),
                        vec![AppendRecord::new(vec![value])],
                    )
                    .expect("model-current CAS append succeeds");
            }
            StorageTransition::StaleAppend => {
                if reference.expected > 0 {
                    assert!(
                        state
                            .storage
                            .append(
                                &state.stream,
                                StreamPosition::new(reference.expected - 1),
                                vec![AppendRecord::new(vec![255])],
                            )
                            .is_err()
                    );
                }
            }
            StorageTransition::Read(from, limit) => {
                let actual = state
                    .storage
                    .read(
                        &state.stream,
                        StreamPosition::new(u64::from(from)),
                        usize::from(limit),
                    )
                    .into_iter()
                    .map(|record| record.body().to_vec())
                    .collect::<Vec<_>>();
                let expected = reference
                    .records
                    .iter()
                    .skip(usize::from(from))
                    .take(usize::from(limit))
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected);
            }
        }
        state
    }

    fn check_invariants(state: &Self, reference: &StorageModel) {
        assert_eq!(
            state.storage.tail(&state.stream).sequence(),
            reference.expected
        );
        let all = state
            .storage
            .read(&state.stream, StreamPosition::default(), usize::MAX)
            .into_iter()
            .map(|record| record.body().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(all, reference.records);
    }
}

prop_state_machine! {
    #![proptest_config(Config {
        cases: property_cases(),
        max_shrink_iters: 2_048,
        .. Config::default()
    })]

    #[test]
    fn storage_contract_invariants_hold_after_every_transition(sequential 1..32 => StorageSut);
}

#[test]
fn named_rng_streams_do_not_shift_when_another_stream_draws() {
    use henosis_testkit::NamedRng;

    let root = Seed::from_u64(9);
    let mut scheduler_a = NamedRng::new(root, "scheduler");
    let mut failures = NamedRng::new(root, "failures");
    let before = scheduler_a.next_u64();
    for _ in 0..100 {
        let _ = failures.next_u64();
    }
    let after = scheduler_a.next_u64();

    let mut scheduler_b = NamedRng::new(root, "scheduler");
    assert_eq!(before, scheduler_b.next_u64());
    assert_eq!(after, scheduler_b.next_u64());
}

async fn drive_real_controller_passes(world: &mut RealControllerWorld, budget: usize) {
    for _ in 0..budget {
        let Some(action) = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, RealControllerAction::ControllerPass { .. }))
        else {
            break;
        };
        world.apply(action).await;
    }
}

async fn drive_real_controller_reports(world: &mut RealControllerWorld, budget: usize) {
    for _ in 0..budget {
        let Some(action) = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, RealControllerAction::DeliverReport { .. }))
        else {
            break;
        };
        world.apply(action).await;
        drive_real_controller_passes(world, budget).await;
    }
}

#[test]
fn real_controllers_replay_the_same_seed_byte_for_byte() {
    runtime().block_on(async {
        let first = RealControllerWorld::new(Seed::from_u64(0x26), 2)
            .await
            .run(128)
            .await;
        let second = RealControllerWorld::new(Seed::from_u64(0x26), 2)
            .await
            .run(128)
            .await;
        assert_eq!(first.trace, second.trace);
        assert!(first.complete);
        assert_eq!(first.generation.ordinal(), 1);
    });
}

#[test]
fn real_controller_mid_reconcile_supersession_cancels_stale_work() {
    runtime().block_on(async {
        let mut world = RealControllerWorld::new(Seed::from_u64(0x5eed_2601), 2).await;
        for _ in 0..4 {
            let action = world
                .enabled_actions()
                .into_iter()
                .find(|action| matches!(action, RealControllerAction::ControllerPass { .. }))
                .expect("first generation has controller work");
            world.apply(action).await;
        }
        world.start_next_generation(2).await;
        assert_eq!(world.generation().ordinal(), 2);
        drive_real_controller_passes(&mut world, 128).await;
        drive_real_controller_reports(&mut world, 128).await;
        assert!(world.plan_complete());
        assert!(world.all_current_resources_exist());
    });
}

#[test]
fn real_controller_restart_and_apply_then_timeout_still_converge() {
    runtime().block_on(async {
        let mut world = RealControllerWorld::new(Seed::from_u64(0x5eed_2602), 2).await;
        world.script_k8s([henosis_testkit::TargetFault::ApplyThenTimeout]);
        world.script_cloudflare([henosis_testkit::TargetFault::ApplyThenTimeout]);
        world.script_supabase([henosis_testkit::TargetFault::ApplyThenTimeout]);
        for step in 0..96 {
            let Some(action) = world
                .enabled_actions()
                .into_iter()
                .find(|action| !matches!(action, RealControllerAction::DeliverDuplicate { .. }))
            else {
                break;
            };
            world.apply(action).await;
            match step % 3 {
                0 => world.restart_controller("k8s"),
                1 => world.restart_controller("cloudflare"),
                _ => world.restart_controller("supabase"),
            }
        }
        assert!(world.plan_complete());
        assert!(world.all_current_resources_exist());
    });
}

#[test]
fn controller_restart_preserves_publication_deduplication() {
    runtime().block_on(async {
        let mut world = RealControllerWorld::new(Seed::from_u64(0x5eed_2604), 2).await;
        drive_real_controller_passes(&mut world, 128).await;
        let publication = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, RealControllerAction::DeliverReport { .. }))
            .expect("converged controller has a report ready");
        let (controller, generation) = match &publication {
            RealControllerAction::DeliverReport {
                controller,
                generation,
            } => (controller.clone(), *generation),
            _ => unreachable!(),
        };
        world.apply(publication).await;
        world.restart_controller(controller.as_str());
        let before = world.canonical_state();
        let actions_before = world.target_action_count();
        world
            .apply(RealControllerAction::DeliverDuplicate {
                controller,
                generation,
            })
            .await;
        assert_eq!(world.canonical_state(), before);
        assert_eq!(world.target_action_count(), actions_before);
    });
}

#[test]
fn output_publication_racing_retirement_is_fenced_and_cleanup_converges() {
    runtime().block_on(async {
        let mut world = RealControllerWorld::new(Seed::from_u64(0x5eed_2603), 2).await;
        drive_real_controller_passes(&mut world, 128).await;
        let stale_publication = world
            .enabled_actions()
            .into_iter()
            .find(|action| matches!(action, RealControllerAction::DeliverReport { .. }))
            .expect("converged controller has a report ready");
        world.retire().await;
        drive_real_controller_passes(&mut world, 128).await;
        let retired = world.canonical_state();
        world.apply(stale_publication).await;
        assert_eq!(world.canonical_state(), retired);
        assert!(world.no_resources_exist());
    });
}

#[test]
fn crashes_at_each_cleanup_pass_boundary_preserve_all_obligations() {
    runtime().block_on(async {
        for crash_after_passes in 0..=6 {
            let mut world =
                RealControllerWorld::new(Seed::from_u64(0x5eed_2605 + crash_after_passes), 3).await;
            drive_real_controller_passes(&mut world, 128).await;
            drive_real_controller_reports(&mut world, 128).await;
            assert!(world.all_current_resources_exist());

            world.start_next_generation(2).await;
            world.start_next_generation(1).await;
            world.crash_restart_core().await;
            drive_real_controller_passes(&mut world, crash_after_passes as usize).await;
            world.crash_restart_core().await;
            drive_real_controller_passes(&mut world, 128).await;
            drive_real_controller_reports(&mut world, 128).await;

            assert!(world.all_current_resources_exist());
            assert!(
                world.no_superseded_resources_exist(),
                "cleanup leaked after a crash at pass boundary {crash_after_passes}"
            );
        }
    });
}

#[test]
fn crash_mid_retirement_replays_cleanup_until_targets_are_empty() {
    runtime().block_on(async {
        let mut world = RealControllerWorld::new(Seed::from_u64(0x5eed_2610), 2).await;
        drive_real_controller_passes(&mut world, 128).await;
        world.retire().await;
        world.crash_restart_core().await;
        drive_real_controller_passes(&mut world, 128).await;
        assert!(world.no_resources_exist());
    });
}

#[allow(dead_code)]
fn _ordered_collections_are_the_default(_: BTreeMap<String, String>) {}
