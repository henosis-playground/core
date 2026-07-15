//! Hermetic JavaScript evaluation for Henosis component bundles.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::future::Future;
use std::num::NonZeroU32;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use deno_core::JsRuntime;
use deno_core::ModuleLoadOptions;
use deno_core::ModuleLoadResponse;
use deno_core::ModuleLoader;
use deno_core::ModuleResolveResponse;
use deno_core::ModuleSpecifier;
use deno_core::OpState;
use deno_core::ResolutionKind;
use deno_core::RuntimeOptions;
use deno_core::error::CoreError;
use deno_core::error::JsError;
use deno_core::extension;
use deno_core::op2;
use deno_core::serde_v8;
use deno_core::v8;
use deno_error::JsErrorBox;
use futures::FutureExt;
use futures::future::BoxFuture;
use henosis_types::BlockedDetail;
use henosis_types::BundleRef;
use henosis_types::ComponentInput;
use henosis_types::ComponentIntent;
use henosis_types::ComponentName;
use henosis_types::ComponentOutput;
use henosis_types::ControllerName;
use henosis_types::EvaluationAttempt;
use henosis_types::EvaluationError;
use henosis_types::EvaluationRequest;
use henosis_types::EvaluationResource;
use henosis_types::InputCellState;
use henosis_types::InputName;
use henosis_types::KindName;
use henosis_types::KindVersion;
use henosis_types::NativeValue;
use henosis_types::NewBlockedEvaluation;
use henosis_types::NewCompleteEvaluation;
use henosis_types::NewComponentIntent;
use henosis_types::NewEvaluationResource;
use henosis_types::ObservedOutputBinding;
use henosis_types::OutputAvailability;
use henosis_types::OutputDeclaration;
use henosis_types::OutputName;
use henosis_types::OutputRef;
use henosis_types::ResourceAddress;
use henosis_types::ResourceId;
use henosis_types::ResourceName;
use henosis_types::StaticOutput;
use oxc_allocator::Allocator;
use oxc_ast::ast::ArrowFunctionExpression;
use oxc_ast::ast::AwaitExpression;
use oxc_ast::ast::ForOfStatement;
use oxc_ast::ast::Function;
use oxc_ast_visit::Visit;
use oxc_ast_visit::walk;
use oxc_parser::Parser;
use oxc_span::SourceType;
use oxc_syntax::scope::ScopeFlags;
use serde::Deserialize;
use serde::Serialize;

const ENTRY_SPECIFIER: &str = "henosis:component";
const PROTOCOL_VERSION: u32 = 1;

/// Retrieves exact executable bytes for a content-addressed bundle.
pub trait BundleSource: Send + Sync + 'static {
    fn load(&self, bundle: BundleRef) -> BoxFuture<'_, Result<Arc<[u8]>, EvaluationError>>;
}

/// Controller-owned contract for one supported resource kind.
#[derive(Clone, Debug)]
pub struct ResourceContract {
    controller: ControllerName,
    observed_outputs: BTreeSet<OutputName>,
}

impl ResourceContract {
    #[must_use]
    pub fn new(controller: ControllerName, observed_outputs: Vec<OutputName>) -> Self {
        Self {
            controller,
            observed_outputs: observed_outputs.into_iter().collect(),
        }
    }

    #[must_use]
    pub const fn controller(&self) -> &ControllerName {
        &self.controller
    }

    #[must_use]
    pub fn observed_outputs(&self) -> &BTreeSet<OutputName> {
        &self.observed_outputs
    }
}

/// Validates controller-owned resource bodies and supplies their output
/// contract.
pub trait ResourceRegistry: Send + Sync + 'static {
    fn validate(
        &self,
        kind: &KindVersion,
        body: &serde_json::Value,
    ) -> Result<ResourceContract, String>;
}

/// Hard limits and admission bounds for isolate work.
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub workers: usize,
    pub queue_capacity_per_worker: usize,
    pub max_bundle_bytes: usize,
    pub max_output_bytes: usize,
    pub max_heap_bytes: usize,
    pub timeout: Duration,
    pub worker_stack_bytes: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            workers: thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1),
            queue_capacity_per_worker: 8,
            max_bundle_bytes: 2 * 1024 * 1024,
            max_output_bytes: 4 * 1024 * 1024,
            max_heap_bytes: 64 * 1024 * 1024,
            timeout: Duration::from_millis(250),
            worker_stack_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Bounded scheduler whose dedicated threads own all V8 activity.
#[derive(Clone)]
pub struct EvaluationEngine {
    source: Arc<dyn BundleSource>,
    workers: Arc<Vec<mpsc::SyncSender<Job>>>,
    next_worker: Arc<AtomicUsize>,
    config: EngineConfig,
}

impl EvaluationEngine {
    pub fn new(
        source: Arc<dyn BundleSource>,
        registry: Arc<dyn ResourceRegistry>,
        config: EngineConfig,
    ) -> Result<Self, EvaluationError> {
        if config.workers == 0 {
            return Err(EvaluationError::new(
                "evaluation engine requires at least one isolate worker",
            ));
        }
        if config.max_heap_bytes < 8 * 1024 * 1024 {
            return Err(EvaluationError::new(
                "evaluation heap limit must be at least 8 MiB",
            ));
        }

        let mut workers = Vec::with_capacity(config.workers);
        for worker_index in 0..config.workers {
            let (sender, receiver) = mpsc::sync_channel::<Job>(config.queue_capacity_per_worker);
            let worker_registry = Arc::clone(&registry);
            let worker_config = config.clone();
            thread::Builder::new()
                .name(format!("henosis-isolate-{worker_index}"))
                .stack_size(config.worker_stack_bytes)
                .spawn(move || worker_loop(receiver, &worker_registry, &worker_config))
                .map_err(|error| {
                    EvaluationError::new(format!(
                        "failed to start isolate worker {worker_index}: {error}"
                    ))
                })?;
            workers.push(sender);
        }

        Ok(Self {
            source,
            workers: Arc::new(workers),
            next_worker: Arc::new(AtomicUsize::new(0)),
            config,
        })
    }
}

impl henosis_types::Evaluator for EvaluationEngine {
    fn evaluate<'a>(
        &'a self,
        request: EvaluationRequest,
    ) -> BoxFuture<'a, Result<EvaluationAttempt, EvaluationError>> {
        Box::pin(async move {
            let bundle = self.source.load(request.bundle()).await?;
            if bundle.len() > self.config.max_bundle_bytes {
                return Err(EvaluationError::new(format!(
                    "bundle is {} bytes, exceeding the {} byte limit",
                    bundle.len(),
                    self.config.max_bundle_bytes
                )));
            }

            let (response, result) = tokio::sync::oneshot::channel();
            let worker = self.next_worker.fetch_add(1, Ordering::Relaxed) % self.workers.len();
            self.workers[worker]
                .try_send(Job {
                    request,
                    bundle,
                    response,
                })
                .map_err(|error| match error {
                    mpsc::TrySendError::Full(_) => {
                        EvaluationError::new("evaluation worker queue is full")
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        EvaluationError::new("evaluation worker stopped")
                    }
                })?;
            result
                .await
                .map_err(|_| EvaluationError::new("evaluation worker dropped its response"))?
        })
    }
}

/// Read the pure component declaration exported by a bundle without invoking
/// its desire function. Core uses this at graph admission so the CLI does not
/// execute user TypeScript or duplicate the SDK's metadata grammar.
pub fn inspect_bundle(
    bundle: BundleRef,
    source: &[u8],
    config: &EngineConfig,
) -> Result<ComponentIntent, EvaluationError> {
    if source.len() > config.max_bundle_bytes {
        return Err(EvaluationError::new(format!(
            "bundle is {} bytes, exceeding the {} byte limit",
            source.len(),
            config.max_bundle_bytes
        )));
    }
    let source = std::str::from_utf8(source)
        .map_err(|_| EvaluationError::new("bundle is not UTF-8 JavaScript source"))?;
    reject_top_level_await(source)?;
    let dynamic_import_attempted = Rc::new(Cell::new(false));
    let loader = Rc::new(DenyModuleLoader {
        dynamic_import_attempted: Rc::clone(&dynamic_import_attempted),
    });
    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader),
        extensions: vec![henosis_evaluation_runtime::init()],
        create_params: Some(v8::Isolate::create_params().heap_limits(0, config.max_heap_bytes)),
        ..Default::default()
    });
    let timed_out = Arc::new(AtomicBool::new(false));
    let out_of_memory = Arc::new(AtomicBool::new(false));
    let isolate_handle = runtime.v8_isolate().thread_safe_handle();
    let heap_handle = isolate_handle.clone();
    let heap_flag = Arc::clone(&out_of_memory);
    runtime.add_near_heap_limit_callback(move |current, _initial| {
        heap_flag.store(true, Ordering::Release);
        heap_handle.terminate_execution();
        current.saturating_add(1024 * 1024)
    });
    let (cancel_timeout, timeout_cancelled) = mpsc::channel();
    let timeout_handle = isolate_handle;
    let timeout_flag = Arc::clone(&timed_out);
    let timeout = config.timeout;
    let watchdog = thread::spawn(move || {
        if timeout_cancelled.recv_timeout(timeout).is_err() {
            timeout_flag.store(true, Ordering::Release);
            timeout_handle.terminate_execution();
        }
    });

    let result = (|| {
        install_host_boundary(&mut runtime)?;
        let specifier = ModuleSpecifier::parse(ENTRY_SPECIFIER)
            .map_err(|error| EvaluationError::new(format!("invalid entry specifier: {error}")))?;
        let module_id =
            block_on(runtime.load_main_es_module_from_code(&specifier, source.to_owned()))
                .map_err(core_failure("loading component bundle"))?;
        let module_evaluation = runtime.mod_evaluate(module_id);
        match module_evaluation.now_or_never() {
            Some(Ok(())) => {}
            Some(Err(error)) => return Err(core_failure("evaluating component module")(error)),
            None => {
                return Err(EvaluationError::new(
                    "error[HENOSIS_TOP_LEVEL_AWAIT]: top-level await is unavailable in component \
                     bundles",
                ));
            }
        }
        let namespace = runtime
            .get_module_namespace(module_id)
            .map_err(core_failure("reading component exports"))?;
        let metadata = read_metadata(&mut runtime, &namespace)?;
        verify_policy_guards(&mut runtime)?;
        if dynamic_import_attempted.get() {
            return Err(EvaluationError::new(
                "error[HENOSIS_DYNAMIC_IMPORT]: dynamic import is unavailable in component \
                 evaluation",
            ));
        }
        component_intent(bundle, metadata)
    })();
    let _ = cancel_timeout.send(());
    let _ = watchdog.join();
    if out_of_memory.load(Ordering::Acquire) {
        return Err(EvaluationError::new(
            "component metadata inspection exceeded the V8 heap limit",
        ));
    }
    if timed_out.load(Ordering::Acquire) {
        return Err(EvaluationError::new(format!(
            "component metadata inspection exceeded the {:?} execution deadline",
            config.timeout
        )));
    }
    result
}

struct Job {
    request: EvaluationRequest,
    bundle: Arc<[u8]>,
    response: tokio::sync::oneshot::Sender<Result<EvaluationAttempt, EvaluationError>>,
}

fn worker_loop(
    receiver: mpsc::Receiver<Job>,
    registry: &Arc<dyn ResourceRegistry>,
    config: &EngineConfig,
) {
    while let Ok(job) = receiver.recv() {
        let result = evaluate_job(&job.request, &job.bundle, registry.as_ref(), config);
        let _ = job.response.send(result);
    }
}

// === ISOLATE BOUNDARY ===

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct StickyBlockedWire {
    input: String,
    source: String,
    operation: String,
    message: String,
}

#[derive(Default)]
struct StickyBlocked(Option<StickyBlockedWire>);

#[op2]
fn op_henosis_mark_blocked(state: &mut OpState, #[serde] detail: StickyBlockedWire) {
    let sticky = state.borrow_mut::<StickyBlocked>();
    if sticky.0.is_none() {
        sticky.0 = Some(detail);
    }
}

extension!(
    henosis_evaluation_runtime,
    ops = [op_henosis_mark_blocked],
    state = |state| state.put(StickyBlocked::default()),
);

#[derive(Debug)]
struct DenyModuleLoader {
    dynamic_import_attempted: Rc<Cell<bool>>,
}

impl ModuleLoader for DenyModuleLoader {
    fn resolve(
        &self,
        specifier: &str,
        _referrer: &str,
        kind: ResolutionKind,
    ) -> ModuleResolveResponse {
        if matches!(kind, ResolutionKind::MainModule) && specifier == ENTRY_SPECIFIER {
            return ModuleSpecifier::parse(ENTRY_SPECIFIER)
                .map_err(|error| JsErrorBox::generic(error.to_string()));
        }
        if matches!(kind, ResolutionKind::DynamicImport) {
            self.dynamic_import_attempted.set(true);
            return Err(JsErrorBox::generic(format!(
                "error[HENOSIS_DYNAMIC_IMPORT]: dynamic import of {specifier:?} is unavailable in \
                 component evaluation"
            )));
        }
        Err(JsErrorBox::generic(format!(
            "error[HENOSIS_EXTERNAL_IMPORT]: bundle contains unresolved import {specifier:?}; the \
             executable must be one closed ESM file"
        )))
    }

    fn load(
        &self,
        module_specifier: &ModuleSpecifier,
        _maybe_referrer: Option<&deno_core::ModuleLoadReferrer>,
        options: ModuleLoadOptions,
    ) -> ModuleLoadResponse {
        if options.is_dynamic_import {
            self.dynamic_import_attempted.set(true);
        }
        ModuleLoadResponse::Sync(Err(JsErrorBox::generic(format!(
            "error[HENOSIS_EXTERNAL_IMPORT]: module {module_specifier} is not available"
        ))))
    }
}

fn reject_top_level_await(source: &str) -> Result<(), EvaluationError> {
    let allocator = Allocator::default();
    let parsed = Parser::new(&allocator, source, SourceType::mjs()).parse();
    if let Some(diagnostic) = parsed.diagnostics.first() {
        return Err(EvaluationError::new(format!(
            "bundle is not valid ESM JavaScript: {diagnostic:?}"
        )));
    }
    let mut detector = TopLevelAwaitDetector::default();
    detector.visit_program(&parsed.program);
    if detector.found {
        Err(EvaluationError::new(
            "error[HENOSIS_TOP_LEVEL_AWAIT]: top-level await is unavailable in component bundles",
        ))
    } else {
        Ok(())
    }
}

#[derive(Default)]
struct TopLevelAwaitDetector {
    function_depth: usize,
    found: bool,
}

impl<'a> Visit<'a> for TopLevelAwaitDetector {
    fn visit_function(&mut self, function: &Function<'a>, flags: ScopeFlags) {
        self.function_depth += 1;
        walk::walk_function(self, function, flags);
        self.function_depth -= 1;
    }

    fn visit_arrow_function_expression(&mut self, function: &ArrowFunctionExpression<'a>) {
        self.function_depth += 1;
        walk::walk_arrow_function_expression(self, function);
        self.function_depth -= 1;
    }

    fn visit_await_expression(&mut self, expression: &AwaitExpression<'a>) {
        if self.function_depth == 0 {
            self.found = true;
        } else {
            walk::walk_await_expression(self, expression);
        }
    }

    fn visit_for_of_statement(&mut self, statement: &ForOfStatement<'a>) {
        if self.function_depth == 0 && statement.r#await {
            self.found = true;
        }
        walk::walk_for_of_statement(self, statement);
    }
}

fn evaluate_job(
    request: &EvaluationRequest,
    bundle: &[u8],
    registry: &dyn ResourceRegistry,
    config: &EngineConfig,
) -> Result<EvaluationAttempt, EvaluationError> {
    let source = std::str::from_utf8(bundle)
        .map_err(|_| EvaluationError::new("bundle is not UTF-8 JavaScript source"))?;
    reject_top_level_await(source)?;
    let dynamic_import_attempted = Rc::new(Cell::new(false));
    let loader = Rc::new(DenyModuleLoader {
        dynamic_import_attempted: Rc::clone(&dynamic_import_attempted),
    });
    let mut runtime = JsRuntime::new(RuntimeOptions {
        module_loader: Some(loader),
        extensions: vec![henosis_evaluation_runtime::init()],
        create_params: Some(v8::Isolate::create_params().heap_limits(0, config.max_heap_bytes)),
        ..Default::default()
    });

    let timed_out = Arc::new(AtomicBool::new(false));
    let out_of_memory = Arc::new(AtomicBool::new(false));
    let isolate_handle = runtime.v8_isolate().thread_safe_handle();
    let heap_handle = isolate_handle.clone();
    let heap_flag = Arc::clone(&out_of_memory);
    runtime.add_near_heap_limit_callback(move |current, _initial| {
        heap_flag.store(true, Ordering::Release);
        heap_handle.terminate_execution();
        current.saturating_add(1024 * 1024)
    });

    let (cancel_timeout, timeout_cancelled) = mpsc::channel();
    let timeout_handle = isolate_handle;
    let timeout_flag = Arc::clone(&timed_out);
    let timeout = config.timeout;
    let watchdog = thread::spawn(move || {
        if timeout_cancelled.recv_timeout(timeout).is_err() {
            timeout_flag.store(true, Ordering::Release);
            timeout_handle.terminate_execution();
        }
    });

    let result = evaluate_in_runtime(
        &mut runtime,
        request,
        source,
        registry,
        &dynamic_import_attempted,
        config.max_output_bytes,
    );
    let _ = cancel_timeout.send(());
    let _ = watchdog.join();

    if out_of_memory.load(Ordering::Acquire) {
        return Err(EvaluationError::new(
            "component evaluation exceeded the V8 heap limit",
        ));
    }
    if timed_out.load(Ordering::Acquire) {
        return Err(EvaluationError::new(format!(
            "component evaluation exceeded the {:?} execution deadline",
            config.timeout
        )));
    }
    result
}

fn evaluate_in_runtime(
    runtime: &mut JsRuntime,
    request: &EvaluationRequest,
    source: &str,
    registry: &dyn ResourceRegistry,
    dynamic_import_attempted: &Cell<bool>,
    max_output_bytes: usize,
) -> Result<EvaluationAttempt, EvaluationError> {
    install_host_boundary(runtime)?;

    let specifier = ModuleSpecifier::parse(ENTRY_SPECIFIER)
        .map_err(|error| EvaluationError::new(format!("invalid entry specifier: {error}")))?;
    let module_id = block_on(runtime.load_main_es_module_from_code(&specifier, source.to_owned()))
        .map_err(core_failure("loading component bundle"))?;
    let module_evaluation = runtime.mod_evaluate(module_id);
    match module_evaluation.now_or_never() {
        Some(Ok(())) => {}
        Some(Err(error)) => return Err(core_failure("evaluating component module")(error)),
        None => {
            return Err(EvaluationError::new(
                "error[HENOSIS_TOP_LEVEL_AWAIT]: top-level await is unavailable in component \
                 bundles",
            ));
        }
    }

    let namespace = runtime
        .get_module_namespace(module_id)
        .map_err(core_failure("reading component exports"))?;
    let metadata = read_metadata(runtime, &namespace)?;
    validate_metadata(&metadata, request)?;
    let wire_result = invoke_bundle(runtime, &namespace, request)?;
    verify_policy_guards(runtime)?;

    if dynamic_import_attempted.get() {
        return Err(EvaluationError::new(
            "error[HENOSIS_DYNAMIC_IMPORT]: dynamic import is unavailable in component evaluation",
        ));
    }

    let sticky = runtime
        .op_state()
        .borrow()
        .borrow::<StickyBlocked>()
        .0
        .clone();
    let wire_result = apply_sticky_blocked(wire_result, sticky)?;
    let encoded_size = serde_json::to_vec(&wire_result)
        .map_err(|error| EvaluationError::new(format!("failed to measure result: {error}")))?
        .len();
    if encoded_size > max_output_bytes {
        return Err(EvaluationError::new(format!(
            "evaluation result is {encoded_size} bytes, exceeding the {max_output_bytes} byte \
             limit"
        )));
    }

    convert_result(request, &metadata, wire_result, registry)
}

fn block_on<F: Future>(future: F) -> F::Output {
    futures::executor::block_on(future)
}

fn install_host_boundary(runtime: &mut JsRuntime) -> Result<(), EvaluationError> {
    {
        deno_core::scope!(scope, runtime);
        scope
            .get_current_context()
            .set_allow_generation_from_strings(false);
    }
    runtime
        .execute_script("henosis:bootstrap", BOOTSTRAP)
        .map_err(js_failure("installing deterministic runtime policy"))?;
    Ok(())
}

const BOOTSTRAP: &str = r#"
(() => {
  "use strict";
  const markBlocked = globalThis.Deno.core.ops.op_henosis_mark_blocked;
  Object.defineProperty(globalThis, "__henosis_mark_blocked", {
    value(detail) { markBlocked(detail); },
    writable: false,
    configurable: false,
    enumerable: false,
  });
  const forbidden = (name) => function () {
    throw new Error(`error[HENOSIS_NONDETERMINISTIC_API]: ${name} is unavailable in component evaluation`);
  };
  const deterministicDate = forbidden("Date");
  const deterministicNow = forbidden("Date.now");
  const deterministicRandom = forbidden("Math.random");
  Object.defineProperty(deterministicDate, "now", {
    value: deterministicNow, writable: true, configurable: false
  });
  Object.defineProperty(globalThis, "Date", { value: deterministicDate, configurable: false });
  Object.defineProperty(Math, "random", {
    value: deterministicRandom, writable: true, configurable: false
  });
  Object.defineProperty(globalThis, "__henosis_policy", {
    value: Object.freeze({ date: deterministicDate, now: deterministicNow, random: deterministicRandom }),
    writable: false, configurable: false, enumerable: false
  });
  for (const name of [
    "performance", "setTimeout", "setInterval", "clearTimeout", "clearInterval",
    "queueMicrotask", "fetch", "crypto", "WeakRef", "FinalizationRegistry",
    "SharedArrayBuffer", "Atomics", "WebAssembly", "Intl", "eval", "Function",
    "ArrayBuffer", "DataView", "Deno", "process"
  ]) {
    try { delete globalThis[name]; } catch (_) {}
  }
  for (const [prototype, methods] of [
    [String.prototype, ["localeCompare", "toLocaleLowerCase", "toLocaleUpperCase"]],
    [Number.prototype, ["toLocaleString"]],
    [BigInt.prototype, ["toLocaleString"]],
  ]) {
    for (const method of methods) {
      Object.defineProperty(prototype, method, { value: forbidden(method), configurable: false });
    }
  }
  Object.freeze(JSON);
})();
"#;

fn verify_policy_guards(runtime: &mut JsRuntime) -> Result<(), EvaluationError> {
    let value = runtime
        .execute_script(
            "henosis:verify-policy",
            "Date === __henosis_policy.date && Date.now === __henosis_policy.now && Math.random \
             === __henosis_policy.random",
        )
        .map_err(js_failure("verifying deterministic runtime policy"))?;
    deno_core::scope!(scope, runtime);
    let value = v8::Local::new(scope, value);
    if value.is_true() {
        Ok(())
    } else {
        Err(EvaluationError::new(
            "error[HENOSIS_POLICY_TAMPERED]: component modified a deterministic runtime guard",
        ))
    }
}

fn read_metadata(
    runtime: &mut JsRuntime,
    namespace: &v8::Global<v8::Object>,
) -> Result<ComponentMetadataWire, EvaluationError> {
    deno_core::scope!(scope, runtime);
    let namespace = v8::Local::new(scope, namespace);
    let protocol = export(scope, namespace, "protocolVersion")?;
    let protocol: u32 = serde_v8::from_v8(scope, protocol).map_err(|error| {
        EvaluationError::new(format!(
            "bundle export `protocolVersion` is not an integer: {error}"
        ))
    })?;
    if protocol != PROTOCOL_VERSION {
        return Err(EvaluationError::new(format!(
            "unsupported bundle protocol version {protocol}; expected {PROTOCOL_VERSION}"
        )));
    }
    let component = export(scope, namespace, "component")?;
    serde_v8::from_v8(scope, component).map_err(|error| {
        EvaluationError::new(format!(
            "bundle export `component` is not plain JSON-compatible metadata: {error}"
        ))
    })
}

fn invoke_bundle(
    runtime: &mut JsRuntime,
    namespace: &v8::Global<v8::Object>,
    request: &EvaluationRequest,
) -> Result<EvaluationResultWire, EvaluationError> {
    let snapshot = snapshot_wire(request)?;
    deno_core::scope!(scope, runtime);
    let namespace = v8::Local::new(scope, namespace);
    let evaluate = export(scope, namespace, "evaluate")?;
    let evaluate = v8::Local::<v8::Function>::try_from(evaluate)
        .map_err(|_| EvaluationError::new("bundle export `evaluate` is not a function"))?;
    let argument = serde_v8::to_v8(scope, snapshot)
        .map_err(|error| EvaluationError::new(format!("failed to inject snapshot: {error}")))?;
    v8::tc_scope!(let try_catch, scope);
    let receiver = v8::undefined(try_catch).into();
    let result = evaluate.call(try_catch, receiver, &[argument]);
    let Some(result) = result else {
        let message = try_catch
            .exception()
            .and_then(|exception| exception.to_string(try_catch))
            .map(|message| message.to_rust_string_lossy(try_catch))
            .unwrap_or_else(|| "component threw an exception".to_owned());
        return Err(EvaluationError::new(message));
    };
    if result.is_promise() {
        return Err(EvaluationError::new(
            "error[HENOSIS_ASYNC_EVALUATION]: evaluate returned a Promise; components must be \
             synchronous",
        ));
    }
    let wire = serde_v8::from_v8(try_catch, result).map_err(|error| {
        EvaluationError::new(format!(
            "evaluation result is not protocol JSON data: {error}"
        ))
    })?;
    Ok(wire)
}

fn export<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    namespace: v8::Local<'s, v8::Object>,
    name: &str,
) -> Result<v8::Local<'s, v8::Value>, EvaluationError> {
    let key = v8::String::new(scope, name)
        .ok_or_else(|| EvaluationError::new("V8 could not allocate an export name"))?;
    let value = namespace
        .get(scope, key.into())
        .ok_or_else(|| EvaluationError::new(format!("failed to read bundle export `{name}`")))?;
    if value.is_undefined() {
        return Err(EvaluationError::new(format!(
            "bundle is missing required export `{name}`"
        )));
    }
    Ok(value)
}

// === PROTOCOL VALIDATION ===

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ComponentMetadataWire {
    name: String,
    inputs: BTreeMap<String, InputMetadataWire>,
    outputs: BTreeMap<String, OutputMetadataWire>,
}

#[derive(Clone, Debug, Deserialize)]
struct InputMetadataWire {
    component: String,
    output: String,
    optional: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct OutputMetadataWire {
    availability: AvailabilityWire,
    optional: bool,
    schema: SchemaWire,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum AvailabilityWire {
    Static,
    Observed,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum SchemaWire {
    String,
    Url,
    Number,
    Boolean,
    Json,
    Array {
        element: Box<SchemaWire>,
    },
    Object {
        fields: BTreeMap<String, SchemaWire>,
    },
}

fn component_intent(
    bundle: BundleRef,
    metadata: ComponentMetadataWire,
) -> Result<ComponentIntent, EvaluationError> {
    validate_logical_name(&metadata.name, "component name")?;
    let name = ComponentName::new(metadata.name)
        .map_err(|error| EvaluationError::new(format!("invalid component name: {error}")))?;
    let mut inputs = Vec::with_capacity(metadata.inputs.len());
    for (input_name, declaration) in metadata.inputs {
        validate_api_name(&input_name, "input name")?;
        validate_logical_name(&declaration.component, "source component name")?;
        validate_api_name(&declaration.output, "source output name")?;
        inputs.push(ComponentInput::new(
            InputName::new(input_name)
                .map_err(|error| EvaluationError::new(format!("invalid input name: {error}")))?,
            output_ref(&declaration.component, &declaration.output)?,
            declaration.optional,
        ));
    }
    let mut outputs = Vec::with_capacity(metadata.outputs.len());
    for (output_name, declaration) in metadata.outputs {
        validate_api_name(&output_name, "output name")?;
        validate_schema_shape(&declaration.schema)?;
        outputs.push(ComponentOutput::new(
            OutputName::new(output_name)
                .map_err(|error| EvaluationError::new(format!("invalid output name: {error}")))?,
            match declaration.availability {
                AvailabilityWire::Static => OutputAvailability::Static,
                AvailabilityWire::Observed => OutputAvailability::Observed,
            },
            declaration.optional,
        ));
    }
    ComponentIntent::new(NewComponentIntent {
        name,
        bundle,
        inputs,
        outputs,
    })
    .map_err(|error| EvaluationError::new(format!("invalid component metadata: {error}")))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(
    tag = "status",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
enum EvaluationResultWire {
    Complete {
        protocol_version: u32,
        resources: Vec<ResourceWire>,
        outputs: BTreeMap<String, serde_json::Value>,
        observed_outputs: BTreeMap<String, BindingWire>,
        reads: Vec<String>,
    },
    Blocked {
        protocol_version: u32,
        resources: Vec<ResourceWire>,
        blocked: BlockedWire,
        reads: Vec<String>,
    },
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct ResourceWire {
    address: String,
    kind: String,
    name: String,
    body: serde_json::Value,
    canonical: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BindingWire {
    resource: String,
    output: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct BlockedWire {
    code: String,
    input: String,
    source: String,
    operation: String,
    message: String,
}

fn snapshot_wire(request: &EvaluationRequest) -> Result<serde_json::Value, EvaluationError> {
    let mut inputs = serde_json::Map::new();
    for cell in request.snapshot().iter() {
        let value = match cell.state() {
            InputCellState::Available(value) => {
                serde_json::json!({"state": "available", "value": value.as_json()})
            }
            InputCellState::Blocked => serde_json::json!({"state": "blocked"}),
            InputCellState::Absent => serde_json::json!({"state": "absent"}),
        };
        inputs.insert(cell.name().as_str().to_owned(), value);
    }
    Ok(serde_json::json!({
        "protocolVersion": PROTOCOL_VERSION,
        "inputs": inputs,
    }))
}

fn validate_metadata(
    metadata: &ComponentMetadataWire,
    request: &EvaluationRequest,
) -> Result<(), EvaluationError> {
    validate_logical_name(&metadata.name, "component name")?;
    let component = ComponentName::new(metadata.name.clone())
        .map_err(|error| EvaluationError::new(format!("invalid component name: {error}")))?;
    if &component != request.component() {
        return Err(EvaluationError::new(format!(
            "bundle declares component {component}, but request is for {}",
            request.component()
        )));
    }
    if metadata.inputs.len() != request.snapshot().iter().len() {
        return Err(EvaluationError::new(
            "bundle input declarations do not match snapshot cells",
        ));
    }
    for (name, declaration) in &metadata.inputs {
        validate_api_name(name, "input name")?;
        validate_logical_name(&declaration.component, "source component name")?;
        validate_api_name(&declaration.output, "source output name")?;
        let input = InputName::new(name.clone())
            .map_err(|error| EvaluationError::new(format!("invalid input name: {error}")))?;
        let source = output_ref(&declaration.component, &declaration.output)?;
        let cell = request.snapshot().get(&input).ok_or_else(|| {
            EvaluationError::new(format!("snapshot omitted declared input {name:?}"))
        })?;
        if cell.source() != &source || cell.is_optional() != declaration.optional {
            return Err(EvaluationError::new(format!(
                "snapshot cell {name:?} does not agree with bundle metadata"
            )));
        }
        if matches!(cell.state(), InputCellState::Absent) && !declaration.optional {
            return Err(EvaluationError::new(format!(
                "required input {name:?} cannot be absent"
            )));
        }
    }
    for (name, declaration) in &metadata.outputs {
        validate_api_name(name, "output name")?;
        OutputName::new(name.clone())
            .map_err(|error| EvaluationError::new(format!("invalid output name: {error}")))?;
        validate_schema_shape(&declaration.schema)?;
    }
    Ok(())
}

fn validate_schema_shape(schema: &SchemaWire) -> Result<(), EvaluationError> {
    match schema {
        SchemaWire::Array { element } => validate_schema_shape(element),
        SchemaWire::Object { fields } => {
            for child in fields.values() {
                validate_schema_shape(child)?;
            }
            Ok(())
        }
        SchemaWire::String
        | SchemaWire::Url
        | SchemaWire::Number
        | SchemaWire::Boolean
        | SchemaWire::Json => Ok(()),
    }
}

fn apply_sticky_blocked(
    result: EvaluationResultWire,
    sticky: Option<StickyBlockedWire>,
) -> Result<EvaluationResultWire, EvaluationError> {
    let Some(sticky) = sticky else {
        return Ok(result);
    };
    let sticky_wire = BlockedWire {
        code: "HENOSIS_BLOCKED".to_owned(),
        input: sticky.input,
        source: sticky.source,
        operation: sticky.operation,
        message: sticky.message,
    };
    match result {
        EvaluationResultWire::Blocked {
            protocol_version,
            resources,
            blocked,
            reads,
        } => {
            if blocked.input != sticky_wire.input
                || blocked.source != sticky_wire.source
                || blocked.operation != sticky_wire.operation
            {
                return Err(EvaluationError::new(
                    "SDK blocked result disagrees with the sticky host blocked signal",
                ));
            }
            Ok(EvaluationResultWire::Blocked {
                protocol_version,
                resources,
                blocked,
                reads,
            })
        }
        EvaluationResultWire::Complete { mut reads, .. } => {
            if !reads.contains(&sticky_wire.input) {
                reads.push(sticky_wire.input.clone());
                reads.sort();
                reads.dedup();
            }
            Ok(EvaluationResultWire::Blocked {
                protocol_version: PROTOCOL_VERSION,
                resources: Vec::new(),
                blocked: sticky_wire,
                reads,
            })
        }
    }
}

fn convert_result(
    request: &EvaluationRequest,
    metadata: &ComponentMetadataWire,
    wire: EvaluationResultWire,
    registry: &dyn ResourceRegistry,
) -> Result<EvaluationAttempt, EvaluationError> {
    match wire {
        EvaluationResultWire::Complete {
            protocol_version,
            resources,
            outputs,
            observed_outputs,
            reads,
        } => {
            require_protocol(protocol_version)?;
            let (resources, contracts) = convert_resources(request, resources, registry)?;
            let outputs = convert_static_outputs(metadata, outputs)?;
            let observed_outputs =
                convert_observed_outputs(metadata, observed_outputs, &contracts)?;
            ensure_output_completeness(metadata, &outputs, &observed_outputs)?;
            EvaluationAttempt::complete(
                request.snapshot(),
                NewCompleteEvaluation {
                    resources,
                    outputs,
                    observed_outputs,
                    reads: convert_reads(reads)?,
                },
            )
            .map_err(protocol_failure)
        }
        EvaluationResultWire::Blocked {
            protocol_version,
            resources,
            blocked,
            reads,
        } => {
            require_protocol(protocol_version)?;
            if blocked.code != "HENOSIS_BLOCKED" {
                return Err(EvaluationError::new(format!(
                    "blocked result has unknown code {:?}",
                    blocked.code
                )));
            }
            let (resources, _) = convert_resources(request, resources, registry)?;
            let input = InputName::new(blocked.input)
                .map_err(|error| EvaluationError::new(format!("invalid blocked input: {error}")))?;
            let cell = request.snapshot().get(&input).ok_or_else(|| {
                EvaluationError::new("blocked result refers to an undeclared input")
            })?;
            let expected_source = metadata
                .inputs
                .get(input.as_str())
                .map(|input| format!("{}.{}", input.component, input.output))
                .ok_or_else(|| EvaluationError::new("blocked input missing from metadata"))?;
            if blocked.source != expected_source {
                return Err(EvaluationError::new(format!(
                    "blocked source {:?} does not match declared source {:?}",
                    blocked.source, expected_source
                )));
            }
            let detail = BlockedDetail::new(
                input,
                cell.source().clone(),
                blocked.operation,
                blocked.message,
            );
            EvaluationAttempt::blocked(
                request.snapshot(),
                NewBlockedEvaluation {
                    resources,
                    blocked: detail,
                    reads: convert_reads(reads)?,
                },
            )
            .map_err(protocol_failure)
        }
    }
}

fn require_protocol(version: u32) -> Result<(), EvaluationError> {
    if version == PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(EvaluationError::new(format!(
            "result protocol version {version} does not match {PROTOCOL_VERSION}"
        )))
    }
}

fn convert_resources(
    request: &EvaluationRequest,
    resources: Vec<ResourceWire>,
    registry: &dyn ResourceRegistry,
) -> Result<
    (
        Vec<EvaluationResource>,
        BTreeMap<ResourceAddress, ResourceContract>,
    ),
    EvaluationError,
> {
    let mut converted = Vec::with_capacity(resources.len());
    let mut contracts = BTreeMap::new();
    for resource in resources {
        let kind = parse_kind(&resource.kind)?;
        validate_logical_name(&resource.name, "resource name")?;
        let name = ResourceName::new(resource.name.clone())
            .map_err(|error| EvaluationError::new(format!("invalid resource name: {error}")))?;
        let address = ResourceAddress::new(kind.clone(), name.clone());
        if resource.address != address.to_string() {
            return Err(EvaluationError::new(format!(
                "resource address {:?} does not match kind/name {address}",
                resource.address
            )));
        }
        let native = NativeValue::from_canonical(resource.body.clone(), &resource.canonical)
            .map_err(|error| EvaluationError::new(error.to_string()))?;
        let contract = registry
            .validate(&kind, native.as_json())
            .map_err(|error| {
                EvaluationError::new(format!("resource {address} is invalid: {error}"))
            })?;
        if contracts
            .insert(address.clone(), contract.clone())
            .is_some()
        {
            return Err(EvaluationError::new(format!(
                "resource address {address} was emitted more than once"
            )));
        }
        let outputs = contract
            .observed_outputs()
            .iter()
            .cloned()
            .map(|name| OutputDeclaration::new(name, OutputAvailability::Observed))
            .collect();
        converted.push(
            EvaluationResource::new(NewEvaluationResource {
                id: stable_resource_id(request, &address),
                component: request.component().clone(),
                kind,
                name,
                controller: contract.controller().clone(),
                body: resource.body,
                canonical: resource.canonical,
                outputs,
            })
            .map_err(protocol_failure)?,
        );
    }
    Ok((converted, contracts))
}

fn convert_static_outputs(
    metadata: &ComponentMetadataWire,
    outputs: BTreeMap<String, serde_json::Value>,
) -> Result<Vec<StaticOutput>, EvaluationError> {
    let mut converted = Vec::with_capacity(outputs.len());
    for (name, value) in outputs {
        let declaration = metadata.outputs.get(&name).ok_or_else(|| {
            EvaluationError::new(format!("result contains undeclared static output {name:?}"))
        })?;
        if declaration.availability != AvailabilityWire::Static {
            return Err(EvaluationError::new(format!(
                "observed output {name:?} was returned as a static value"
            )));
        }
        validate_schema_value(&declaration.schema, &value, &format!("output {name}"))?;
        let name = OutputName::new(name)
            .map_err(|error| EvaluationError::new(format!("invalid output name: {error}")))?;
        let value = NativeValue::new(value)
            .map_err(|error| EvaluationError::new(format!("invalid output value: {error}")))?;
        converted.push(StaticOutput::new(name, value));
    }
    Ok(converted)
}

fn convert_observed_outputs(
    metadata: &ComponentMetadataWire,
    outputs: BTreeMap<String, BindingWire>,
    contracts: &BTreeMap<ResourceAddress, ResourceContract>,
) -> Result<Vec<ObservedOutputBinding>, EvaluationError> {
    let mut converted = Vec::with_capacity(outputs.len());
    for (name, binding) in outputs {
        let declaration = metadata.outputs.get(&name).ok_or_else(|| {
            EvaluationError::new(format!(
                "result contains undeclared observed output {name:?}"
            ))
        })?;
        if declaration.availability != AvailabilityWire::Observed {
            return Err(EvaluationError::new(format!(
                "static output {name:?} was returned as an observed binding"
            )));
        }
        let address = parse_address(&binding.resource)?;
        let output = OutputName::new(binding.output)
            .map_err(|error| EvaluationError::new(format!("invalid resource output: {error}")))?;
        let contract = contracts.get(&address).ok_or_else(|| {
            EvaluationError::new(format!(
                "observed output {name:?} points at un-emitted resource {address}"
            ))
        })?;
        if !contract.observed_outputs().contains(&output) {
            return Err(EvaluationError::new(format!(
                "resource {address} does not declare observed output {output}"
            )));
        }
        converted.push(ObservedOutputBinding::new(
            OutputName::new(name)
                .map_err(|error| EvaluationError::new(format!("invalid output name: {error}")))?,
            address,
            output,
        ));
    }
    Ok(converted)
}

fn ensure_output_completeness(
    metadata: &ComponentMetadataWire,
    static_outputs: &[StaticOutput],
    observed_outputs: &[ObservedOutputBinding],
) -> Result<(), EvaluationError> {
    let static_names = static_outputs
        .iter()
        .map(|output| output.name().as_str())
        .collect::<BTreeSet<_>>();
    let observed_names = observed_outputs
        .iter()
        .map(|output| output.name().as_str())
        .collect::<BTreeSet<_>>();
    for (name, declaration) in &metadata.outputs {
        let present = match declaration.availability {
            AvailabilityWire::Static => static_names.contains(name.as_str()),
            AvailabilityWire::Observed => observed_names.contains(name.as_str()),
        };
        if !present && !declaration.optional {
            return Err(EvaluationError::new(format!(
                "required {:?} output {name:?} is missing",
                declaration.availability
            )));
        }
    }
    Ok(())
}

fn convert_reads(reads: Vec<String>) -> Result<Vec<InputName>, EvaluationError> {
    reads
        .into_iter()
        .map(|read| {
            InputName::new(read)
                .map_err(|error| EvaluationError::new(format!("invalid read name: {error}")))
        })
        .collect()
}

fn validate_schema_value(
    schema: &SchemaWire,
    value: &serde_json::Value,
    path: &str,
) -> Result<(), EvaluationError> {
    let valid = match schema {
        SchemaWire::String => value.is_string(),
        SchemaWire::Url => value
            .as_str()
            .is_some_and(|value| value.starts_with("https://") || value.starts_with("http://")),
        SchemaWire::Number => value.is_number(),
        SchemaWire::Boolean => value.is_boolean(),
        SchemaWire::Json => true,
        SchemaWire::Array { element } => value.as_array().is_some_and(|values| {
            values
                .iter()
                .all(|value| validate_schema_value(element, value, path).is_ok())
        }),
        SchemaWire::Object { fields } => value.as_object().is_some_and(|object| {
            fields.iter().all(|(name, schema)| {
                object
                    .get(name)
                    .is_some_and(|value| validate_schema_value(schema, value, path).is_ok())
            })
        }),
    };
    if valid {
        Ok(())
    } else {
        Err(EvaluationError::new(format!(
            "{path} does not satisfy its declared schema"
        )))
    }
}

fn validate_logical_name(value: &str, label: &str) -> Result<(), EvaluationError> {
    let mut bytes = value.bytes();
    let valid = value.len() <= 63
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        });
    if valid {
        Ok(())
    } else {
        Err(EvaluationError::new(format!(
            "invalid {label} {value:?}; expected 1-63 lowercase letters, digits, underscores, or \
             hyphens, beginning with a letter"
        )))
    }
}

fn validate_api_name(value: &str, label: &str) -> Result<(), EvaluationError> {
    let mut bytes = value.bytes();
    let valid = value.len() <= 63
        && bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
        && bytes.all(|byte| byte.is_ascii_alphanumeric());
    if valid {
        Ok(())
    } else {
        Err(EvaluationError::new(format!(
            "invalid {label} {value:?}; expected 1-63 ASCII letters or digits, beginning with a \
             letter"
        )))
    }
}

fn validate_kind_name(value: &str) -> Result<(), EvaluationError> {
    let mut segments = value.split('/');
    let valid_segment = |segment: &str| {
        let mut bytes = segment.bytes();
        bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
            && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    };
    let valid = segments.next().is_some_and(valid_segment)
        && segments.next().is_some_and(valid_segment)
        && segments.next().is_none();
    if valid {
        Ok(())
    } else {
        Err(EvaluationError::new(format!(
            "invalid resource kind {value:?}; expected namespace/kind with lowercase letters, \
             digits, and hyphens"
        )))
    }
}

fn output_ref(component: &str, output: &str) -> Result<OutputRef, EvaluationError> {
    Ok(OutputRef::new(
        ComponentName::new(component.to_owned()).map_err(|error| {
            EvaluationError::new(format!("invalid source component name: {error}"))
        })?,
        OutputName::new(output.to_owned()).map_err(|error| {
            EvaluationError::new(format!("invalid source output name: {error}"))
        })?,
    ))
}

fn parse_kind(value: &str) -> Result<KindVersion, EvaluationError> {
    let (name, version) = value.rsplit_once('@').ok_or_else(|| {
        EvaluationError::new(format!("resource kind {value:?} has no version suffix"))
    })?;
    validate_kind_name(name)?;
    let version = version
        .parse::<u32>()
        .ok()
        .and_then(NonZeroU32::new)
        .ok_or_else(|| EvaluationError::new(format!("invalid resource kind version {value:?}")))?;
    let name = KindName::new(name.to_owned())
        .map_err(|error| EvaluationError::new(format!("invalid resource kind: {error}")))?;
    Ok(KindVersion::new(name, version))
}

fn parse_address(value: &str) -> Result<ResourceAddress, EvaluationError> {
    let (kind, name) = value
        .rsplit_once('/')
        .ok_or_else(|| EvaluationError::new(format!("invalid resource address {value:?}")))?;
    Ok(ResourceAddress::new(
        parse_kind(kind)?,
        ResourceName::new(name.to_owned())
            .map_err(|error| EvaluationError::new(format!("invalid resource name: {error}")))?,
    ))
}

fn stable_resource_id(request: &EvaluationRequest, address: &ResourceAddress) -> ResourceId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"henosis-resource-id-v1\0");
    hasher.update(&request.graph_id().into_bytes());
    hasher.update(request.component().as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(address.to_string().as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&hasher.finalize().as_bytes()[..16]);
    ResourceId::from_bytes(bytes)
}

fn protocol_failure(error: impl std::fmt::Display) -> EvaluationError {
    EvaluationError::new(format!("bundle/host protocol failure: {error}"))
}

fn js_failure(context: &'static str) -> impl FnOnce(Box<JsError>) -> EvaluationError {
    move |error| EvaluationError::new(format!("{context}: {error}"))
}

fn core_failure(context: &'static str) -> impl FnOnce(CoreError) -> EvaluationError {
    move |error| EvaluationError::new(format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use henosis_types::ContentDigest;
    use henosis_types::EvaluationSnapshot;
    use henosis_types::Evaluator;
    use henosis_types::Generation;
    use henosis_types::GraphId;
    use henosis_types::InputCell;
    use pretty_assertions::assert_eq;

    const CONSUMER_BUNDLE: &str = include_str!("../fixtures/consumer.bundle.js");
    const BENCHMARK_BACKEND_BUNDLE: &str = include_str!("../fixtures/benchmark/backend.bundle.js");
    const BENCHMARK_SERVICE_PAIR_BUNDLE: &str =
        include_str!("../fixtures/benchmark/service_pair.bundle.js");

    struct MemorySource {
        bundle: Arc<[u8]>,
    }

    impl BundleSource for MemorySource {
        fn load(&self, _bundle: BundleRef) -> BoxFuture<'_, Result<Arc<[u8]>, EvaluationError>> {
            let bundle = Arc::clone(&self.bundle);
            Box::pin(async move { Ok(bundle) })
        }
    }

    struct TestRegistry;

    impl ResourceRegistry for TestRegistry {
        fn validate(
            &self,
            kind: &KindVersion,
            body: &serde_json::Value,
        ) -> Result<ResourceContract, String> {
            if kind.to_string() != "test/item@1" {
                return Err(format!("unsupported kind {kind}"));
            }
            if !body.is_object() {
                return Err("item body must be an object".to_owned());
            }
            Ok(ResourceContract::new(
                ControllerName::new("test").expect("valid controller"),
                vec![OutputName::new("result").expect("valid output")],
            ))
        }
    }

    struct BenchmarkRegistry;

    impl ResourceRegistry for BenchmarkRegistry {
        fn validate(
            &self,
            kind: &KindVersion,
            body: &serde_json::Value,
        ) -> Result<ResourceContract, String> {
            if !body.is_object() {
                return Err(format!("{kind} body must be an object"));
            }
            let (controller, outputs): (&str, &[&str]) = match kind.to_string().as_str() {
                "k8s/object@1" => ("k8s", &[]),
                "cloudflare/worker@1" => (
                    "cloudflare",
                    &["url", "workerName", "deploymentId", "versionId"],
                ),
                other => return Err(format!("unsupported benchmark kind {other}")),
            };
            Ok(ResourceContract::new(
                ControllerName::new(controller).expect("valid controller"),
                outputs
                    .iter()
                    .map(|name| OutputName::new(*name).expect("valid API output"))
                    .collect(),
            ))
        }
    }

    fn bundle_ref(source: &str) -> BundleRef {
        BundleRef::new(ContentDigest::digest(source.as_bytes()))
    }

    fn request(source: &str, component: &str, cells: Vec<InputCell>) -> EvaluationRequest {
        EvaluationRequest::new(
            GraphId::from_bytes([7; 16]),
            Generation::new(1).expect("valid generation"),
            ComponentName::new(component).expect("valid component"),
            bundle_ref(source),
            EvaluationSnapshot::new(cells).expect("valid snapshot"),
        )
    }

    fn input(name: &str, component: &str, output: &str, state: InputCellState) -> InputCell {
        InputCell::new(
            InputName::new(name).expect("valid input"),
            OutputRef::new(
                ComponentName::new(component).expect("valid component"),
                OutputName::new(output).expect("valid output"),
            ),
            false,
            state,
        )
        .expect("valid cell")
    }

    fn available(value: serde_json::Value) -> InputCellState {
        InputCellState::Available(NativeValue::new(value).expect("valid JSON"))
    }

    fn evaluate_direct(
        source: &str,
        request: &EvaluationRequest,
    ) -> Result<EvaluationAttempt, EvaluationError> {
        evaluate_job(
            request,
            source.as_bytes(),
            &TestRegistry,
            &EngineConfig::default(),
        )
    }

    #[test]
    fn api_names_accept_camel_case_while_logical_names_stay_strict() {
        for name in ["apiUrl", "URL2", "restUrl"] {
            validate_api_name(name, "API name").expect("camel-case API name should be valid");
        }
        for name in ["api_url", "api-url", "2api", ""] {
            assert!(validate_api_name(name, "API name").is_err(), "{name:?}");
        }
        validate_logical_name("source_component", "component name")
            .expect("logical names retain underscores");
        assert!(validate_logical_name("sourceComponent", "component name").is_err());
    }

    #[test]
    fn current_sdk_benchmark_bundles_accept_camel_case_api_names() {
        let backend_intent = inspect_bundle(
            bundle_ref(BENCHMARK_BACKEND_BUNDLE),
            BENCHMARK_BACKEND_BUNDLE.as_bytes(),
            &EngineConfig::default(),
        )
        .expect("core admission should inspect current SDK metadata");
        assert_eq!(
            backend_intent
                .inputs()
                .map(|input| input.name().as_str())
                .collect::<Vec<_>>(),
            vec!["databaseUrl", "tunnelHost"]
        );

        let service_pair = evaluate_job(
            &request(BENCHMARK_SERVICE_PAIR_BUNDLE, "service_pair", Vec::new()),
            BENCHMARK_SERVICE_PAIR_BUNDLE.as_bytes(),
            &BenchmarkRegistry,
            &EngineConfig::default(),
        )
        .expect("current CLI/SDK service-pair bundle should evaluate");
        let output_names = service_pair
            .complete_result()
            .expect("service pair completes")
            .outputs()
            .map(|output| output.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(output_names, vec!["apiUrl", "webUrl"]);

        let backend = evaluate_job(
            &request(
                BENCHMARK_BACKEND_BUNDLE,
                "backend",
                vec![
                    input(
                        "databaseUrl",
                        "database",
                        "restUrl",
                        InputCellState::Blocked,
                    ),
                    input(
                        "tunnelHost",
                        "supabase_tunnel",
                        "hostname",
                        InputCellState::Blocked,
                    ),
                ],
            ),
            BENCHMARK_BACKEND_BUNDLE.as_bytes(),
            &BenchmarkRegistry,
            &EngineConfig::default(),
        )
        .expect("current CLI/SDK backend bundle should report suspense");
        assert_eq!(
            backend
                .blocked_result()
                .expect("backend is blocked")
                .blocked()
                .input()
                .as_str(),
            "databaseUrl"
        );
    }

    #[test]
    fn actual_cli_bundle_blocks_then_completes() {
        let blocked_request = request(
            CONSUMER_BUNDLE,
            "consumer",
            vec![input(
                "source",
                "producer",
                "value",
                InputCellState::Blocked,
            )],
        );
        let blocked = evaluate_direct(CONSUMER_BUNDLE, &blocked_request)
            .expect("real bundle should return blocked");
        assert_eq!(
            blocked
                .blocked_result()
                .expect("blocked result")
                .blocked()
                .input()
                .as_str(),
            "source"
        );
        assert_eq!(
            blocked
                .reads()
                .iter()
                .map(InputName::as_str)
                .collect::<Vec<_>>(),
            vec!["source"]
        );

        let complete_request = request(
            CONSUMER_BUNDLE,
            "consumer",
            vec![input(
                "source",
                "producer",
                "value",
                available(serde_json::json!("ready")),
            )],
        );
        let complete = evaluate_direct(CONSUMER_BUNDLE, &complete_request)
            .expect("real bundle should complete");
        assert_eq!(complete.resources().len(), 1);
        assert_eq!(
            complete.resources()[0].address().to_string(),
            "test/item@1/main"
        );
        assert_eq!(
            complete
                .reads()
                .iter()
                .map(InputName::as_str)
                .collect::<Vec<_>>(),
            vec!["source"]
        );
        let result = complete.complete_result().expect("complete result");
        assert_eq!(result.outputs().count(), 1);
        assert_eq!(result.observed_outputs().count(), 1);
    }

    #[test]
    fn fresh_isolates_produce_byte_identical_results() {
        let request = request(
            CONSUMER_BUNDLE,
            "consumer",
            vec![input(
                "source",
                "producer",
                "value",
                available(serde_json::json!("ready")),
            )],
        );
        let first = evaluate_direct(CONSUMER_BUNDLE, &request).expect("first evaluation");
        let second = evaluate_direct(CONSUMER_BUNDLE, &request).expect("second evaluation");
        assert_eq!(
            serde_json::to_vec(&first).expect("serialize first"),
            serde_json::to_vec(&second).expect("serialize second")
        );
    }

    #[tokio::test]
    async fn evaluator_trait_runs_on_the_bounded_worker_pool() {
        let source = Arc::new(MemorySource {
            bundle: Arc::from(CONSUMER_BUNDLE.as_bytes()),
        });
        let engine = EvaluationEngine::new(
            source,
            Arc::new(TestRegistry),
            EngineConfig {
                workers: 1,
                ..EngineConfig::default()
            },
        )
        .expect("engine starts");
        let request = request(
            CONSUMER_BUNDLE,
            "consumer",
            vec![input(
                "source",
                "producer",
                "value",
                available(serde_json::json!("ready")),
            )],
        );
        let result = engine.evaluate(request).await.expect("pool evaluation");
        assert!(result.complete_result().is_some());
    }

    #[test]
    fn sticky_blocked_signal_overrides_catch_and_swallow() {
        let source = r#"
          export const protocolVersion = 1;
          export const component = {
            name: "consumer",
            inputs: { value: { component: "producer", output: "value", optional: false } },
            outputs: {}
          };
          export function evaluate() {
            try {
              __henosis_mark_blocked({
                input: "value", source: "producer.value", operation: "reading `.value`",
                message: "blocked even when swallowed"
              });
              throw new Error("sentinel");
            } catch (_) {}
            return { protocolVersion: 1, status: "complete", resources: [], outputs: {}, observedOutputs: {}, reads: [] };
          }
        "#;
        let request = request(
            source,
            "consumer",
            vec![input("value", "producer", "value", InputCellState::Blocked)],
        );
        let result = evaluate_direct(source, &request).expect("sticky signal is a blocked result");
        assert_eq!(
            result
                .blocked_result()
                .expect("blocked")
                .blocked()
                .message(),
            "blocked even when swallowed"
        );
    }

    #[test]
    fn rejects_top_level_await() {
        let source = r#"
          await Promise.resolve();
          export const protocolVersion = 1;
          export const component = { name: "hostile", inputs: {}, outputs: {} };
          export function evaluate() {
            return { protocolVersion: 1, status: "complete", resources: [], outputs: {}, observedOutputs: {}, reads: [] };
          }
        "#;
        let error = evaluate_direct(source, &request(source, "hostile", Vec::new()))
            .expect_err("top-level await must fail");
        assert!(error.to_string().contains("HENOSIS_TOP_LEVEL_AWAIT"));
    }

    #[test]
    fn rejects_returned_promises() {
        let source = minimal_bundle("return Promise.resolve({});");
        let error = evaluate_direct(&source, &request(&source, "hostile", Vec::new()))
            .expect_err("promise must fail");
        assert!(error.to_string().contains("HENOSIS_ASYNC_EVALUATION"));
    }

    #[test]
    fn hostile_ambient_apis_fail_closed_with_diagnostics() {
        for (name, body, expected) in [
            ("Date.now", "Date.now();", "HENOSIS_NONDETERMINISTIC_API"),
            (
                "Math.random",
                "Math.random();",
                "HENOSIS_NONDETERMINISTIC_API",
            ),
            (
                "fetch",
                "fetch('https://example.test');",
                "fetch is not defined",
            ),
            (
                "dynamic import",
                "import('data:text/javascript,export default 1').catch(() => {});",
                "HENOSIS_DYNAMIC_IMPORT",
            ),
        ] {
            let source = minimal_bundle(body);
            let error =
                evaluate_direct(&source, &request(&source, "hostile", Vec::new())).expect_err(name);
            assert!(
                error.to_string().contains(expected),
                "{name} diagnostic was: {error}"
            );
        }
    }

    #[test]
    fn canonical_body_mismatch_fails_closed() {
        let source = r#"
          export const protocolVersion = 1;
          export const component = { name: "hostile", inputs: {}, outputs: {} };
          export function evaluate() {
            return {
              protocolVersion: 1,
              status: "complete",
              resources: [{ address: "test/item@1/main", kind: "test/item@1", name: "main", body: { a: 1 }, canonical: "{\"a\":2}" }],
              outputs: {}, observedOutputs: {}, reads: []
            };
          }
        "#;
        let error = evaluate_direct(source, &request(source, "hostile", Vec::new()))
            .expect_err("canonical mismatch must fail");
        assert!(error.to_string().contains("canonical JSON"));
    }

    #[test]
    fn timeout_is_a_host_failure() {
        let source = minimal_bundle("for (;;) {}");
        let request = request(&source, "hostile", Vec::new());
        let error = evaluate_job(
            &request,
            source.as_bytes(),
            &TestRegistry,
            &EngineConfig {
                timeout: Duration::from_millis(20),
                ..EngineConfig::default()
            },
        )
        .expect_err("infinite loop must time out");
        assert!(error.to_string().contains("execution deadline"));
    }

    fn minimal_bundle(body: &str) -> String {
        format!(
            r#"
              export const protocolVersion = 1;
              export const component = {{ name: "hostile", inputs: {{}}, outputs: {{}} }};
              export function evaluate(snapshot) {{
                {body}
                return {{ protocolVersion: 1, status: "complete", resources: [], outputs: {{}}, observedOutputs: {{}}, reads: [] }};
              }}
            "#
        )
    }
}
