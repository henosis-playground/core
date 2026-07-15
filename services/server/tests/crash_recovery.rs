use std::fs;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use base64::Engine as _;
use henosis_journal::Journal;
use henosis_journal::S2Storage;
use henosis_types::ContentDigest;
use henosis_types::CoreEvent;
use henosis_types::GraphId;
use serde_json::Value;

const DATABASE_BUNDLE: &[u8] =
    include_bytes!("../../../crates/evaluation-engine/fixtures/benchmark/database.bundle.js");
const BACKEND_BUNDLE: &str =
    include_str!("../../../crates/evaluation-engine/fixtures/benchmark/backend.bundle.js");
const FRONTEND_BUNDLE: &[u8] =
    include_bytes!("../../../crates/evaluation-engine/fixtures/benchmark/frontend.bundle.js");

#[test]
fn server_replays_midflight_graph_after_sigkill() {
    if std::env::var("HENOSIS_S2_CRASH_TEST").as_deref() != Ok("1") {
        eprintln!("skipped: set HENOSIS_S2_CRASH_TEST=1 with s2-lite running");
        return;
    }

    let account_endpoint = test_env("HENOSIS_TEST_S2_ACCOUNT_ENDPOINT", "http://127.0.0.1:4480");
    let basin_endpoint = test_env("HENOSIS_TEST_S2_BASIN_ENDPOINT", "http://127.0.0.1:4480");
    let basin = test_env("HENOSIS_TEST_S2_BASIN", "henosis-core-test");
    let access_token = test_env("HENOSIS_TEST_S2_ACCESS_TOKEN", "local");
    let bundle_root = PathBuf::from(test_env(
        "HENOSIS_TEST_BUNDLE_ROOT",
        "/tmp/henosis-core-crash-test-bundles",
    ));
    fs::create_dir_all(&bundle_root).expect("create persistent crash-test bundle root");

    let backend_bundle = BACKEND_BUNDLE
        .replace(
            "databaseUrl: input.required(database_default.outputs.restUrl),\n    tunnelHost: \
             input.required(tunnel_default.outputs.hostname)",
            "databaseUrl: input.required(database_default.outputs.restUrl)",
        )
        .replace(
            "SUPABASE_REST_URL: inputs.databaseUrl.value,\n        SUPABASE_TUNNEL_HOST: \
             inputs.tunnelHost.value",
            "SUPABASE_REST_URL: inputs.databaseUrl.value",
        );
    assert!(!backend_bundle.contains("inputs.tunnelHost"));

    let database_digest = install_bundle(&bundle_root, DATABASE_BUNDLE);
    let backend_digest = install_bundle(&bundle_root, backend_bundle.as_bytes());
    let frontend_digest = install_bundle(&bundle_root, FRONTEND_BUNDLE);
    let graph_id = unique_graph_id();

    let first_port = free_port();
    let mut first = spawn_server(
        first_port,
        &bundle_root,
        &access_token,
        &account_endpoint,
        &basin_endpoint,
        &basin,
    );
    wait_ready(first_port);
    let first_url = format!("http://127.0.0.1:{first_port}");
    let created = rpc(
        &first_url,
        "CreateGraph",
        serde_json::json!({
            "graphId": graph_id.to_string(),
            "components": [
                component("database", database_digest),
                component("backend", backend_digest),
                component("frontend", frontend_digest),
            ],
        }),
    );
    assert_eq!(created.pointer("/status/generation"), Some(&Value::from(1)));

    let before = wait_for_status(&first_url, graph_id, |status| {
        !status
            .get("outputs")
            .and_then(Value::as_array)
            .is_none_or(Vec::is_empty)
            && status
                .pointer("/plan/blocked")
                .and_then(Value::as_array)
                .is_some_and(|blocked| !blocked.is_empty())
    });
    let durable_before = durable_status(&before);
    first.kill().expect("SIGKILL first server");
    first.wait().expect("reap killed first server");

    let second_port = free_port();
    let mut second = spawn_server(
        second_port,
        &bundle_root,
        &access_token,
        &account_endpoint,
        &basin_endpoint,
        &basin,
    );
    wait_ready(second_port);
    let second_url = format!("http://127.0.0.1:{second_port}");
    let after = rpc(
        &second_url,
        "GetGraph",
        serde_json::json!({"graphId": graph_id.to_string()}),
    );
    assert_eq!(durable_status(&after["status"]), durable_before);

    let listed = rpc(
        &second_url,
        "ListGraphs",
        serde_json::json!({"includeRetired": false}),
    );
    assert!(listed["graphs"].as_array().is_some_and(|graphs| {
        graphs
            .iter()
            .any(|graph| graph["graphId"] == graph_id.to_string())
    }));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build journal inspection runtime");
    runtime.block_on(async {
        let storage = S2Storage::connect(
            access_token.clone(),
            &account_endpoint,
            &basin_endpoint,
            &basin,
        )
        .expect("connect journal inspector");
        let journal = Journal::new(std::sync::Arc::new(storage));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let events = journal.load(graph_id).await.expect("load graph journal");
            let publications = events
                .iter()
                .filter(|event| matches!(event, CoreEvent::OutputsPublished(publication) if publication.controller().as_str() == "supabase"))
                .count();
            if publications == 1 {
                break;
            }
            assert!(Instant::now() < deadline, "supabase publication was not recovered");
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
        let events = journal.load(graph_id).await.expect("reload graph journal");
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CoreEvent::OutputsPublished(publication) if publication.controller().as_str() == "supabase"))
                .count(),
            1,
            "generation-fenced publication must not duplicate after resume"
        );
    });

    let retired = rpc(
        &second_url,
        "RetireGraph",
        serde_json::json!({
            "graphId": graph_id.to_string(),
            "expectedGeneration": 1,
        }),
    );
    assert_eq!(retired.pointer("/status/retired"), Some(&Value::Bool(true)));
    let listed = rpc(
        &second_url,
        "ListGraphs",
        serde_json::json!({"includeRetired": false}),
    );
    assert!(!listed["graphs"].as_array().is_some_and(|graphs| {
        graphs
            .iter()
            .any(|graph| graph["graphId"] == graph_id.to_string())
    }));

    second.kill().expect("stop second server");
    second.wait().expect("reap second server");
}

fn component(name: &str, digest: ContentDigest) -> Value {
    serde_json::json!({
        "name": name,
        "bundleDigest": base64::engine::general_purpose::STANDARD.encode(digest.as_bytes()),
    })
}

fn install_bundle(root: &Path, source: &[u8]) -> ContentDigest {
    let digest = ContentDigest::digest(source);
    let directory = root.join(digest.to_string());
    fs::create_dir_all(&directory).expect("create content-addressed bundle directory");
    fs::write(directory.join("module.js"), source).expect("write bundle fixture");
    digest
}

fn unique_graph_id() -> GraphId {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after epoch")
        .as_nanos();
    let mut bytes = nanos.to_le_bytes();
    bytes[0] ^= std::process::id() as u8;
    GraphId::from_bytes(bytes)
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral port")
        .local_addr()
        .expect("read ephemeral port")
        .port()
}

fn spawn_server(
    port: u16,
    bundle_root: &Path,
    access_token: &str,
    account_endpoint: &str,
    basin_endpoint: &str,
    basin: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_henosis-core-server"))
        .env("HENOSIS_BIND", format!("127.0.0.1:{port}"))
        .env("HENOSIS_BUNDLE_ROOT", bundle_root)
        .env(
            "HENOSIS_DEPLOY_REMOTE",
            bundle_root.join("unused-deploy.git"),
        )
        .env("S2_ACCESS_TOKEN", access_token)
        .env("S2_ACCOUNT_ENDPOINT", account_endpoint)
        .env("S2_BASIN_ENDPOINT", basin_endpoint)
        .env("S2_BASIN", basin)
        .env("RUST_LOG", "henosis=error")
        .env_remove("HENOSIS_CLOUDFLARE_LIVE")
        .spawn()
        .expect("spawn real core server")
}

fn wait_ready(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while TcpListener::bind(("127.0.0.1", port)).is_ok() {
        assert!(Instant::now() < deadline, "server did not bind in time");
        thread::sleep(Duration::from_millis(25));
    }
}

fn rpc(base: &str, method: &str, body: Value) -> Value {
    let response = reqwest::blocking::Client::new()
        .post(format!("{base}/henosis.v1.GraphService/{method}"))
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .expect("send ConnectRPC request");
    let status = response.status();
    let bytes = response.bytes().expect("read ConnectRPC response");
    assert!(
        status.is_success(),
        "{method} failed with {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    serde_json::from_slice(&bytes).expect("decode ConnectRPC JSON response")
}

fn wait_for_status(base: &str, graph_id: GraphId, predicate: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let response = rpc(
            base,
            "GetGraph",
            serde_json::json!({"graphId": graph_id.to_string()}),
        );
        let status = response["status"].clone();
        if predicate(&status) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "graph did not reach the crash point"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn durable_status(status: &Value) -> Value {
    serde_json::json!({
        "generation": status["generation"],
        "plan": status["plan"],
        "outputs": status["outputs"],
        "stallCycle": status["stallCycle"],
        "retired": status["retired"],
        "components": status["components"],
        "sourcePolicy": status["sourcePolicy"],
    })
}

fn test_env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}
