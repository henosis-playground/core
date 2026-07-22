use std::fs;
use std::io::Read as _;
use std::net::TcpListener;
use std::path::Path;
use std::path::PathBuf;
use std::process::Child;
use std::process::Command;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use base64::Engine as _;
use henosis_app::ArtifactRequirement;
use henosis_app::BUNDLE_FORMAT_VERSION;
use henosis_app::BundleManifestV1;
use henosis_app::BundlerIdentity;
use henosis_app::CompiledDependencyManifest;
use henosis_app::RUNTIME_API_VERSION;
use henosis_app::WorkloadArtifactKind;
use henosis_evaluation_engine::EngineConfig;
use henosis_evaluation_engine::inspect_bundle_contract;
use henosis_types::BundleRef;
use henosis_types::ContentDigest;
use henosis_types::GraphId;
use henosis_types::OutputAvailability;
use serde_json::Value;
use sha2::Digest as _;
use sha2::Sha256;

const RECOVERY_BUNDLE: &[u8] = br#"
export const protocolVersion = 1;
export const component = {
  name: "app",
  revision: "0000000000000000000000000000000000000000000000000000000000000000",
  inputs: {},
  outputs: { version: { availability: "static", optional: false, schema: { kind: "string" } } },
  compiledDependencies: []
};
export const bundleContract = {
  declaredCapabilities: [],
  configFiles: [],
  artifactRequirements: []
};
export function evaluate() {
  return {
    protocolVersion: 1,
    status: "complete",
    resources: [],
    outputs: { version: "v1" },
    observedOutputs: {},
    reads: []
  };
}
"#;

#[test]
#[ignore = "requires s2-lite; run `just test-s2-crash`"]
fn server_recovers_on_fresh_machine_from_s2_and_durable_bundle_store() {
    assert_eq!(
        std::env::var("HENOSIS_S2_CRASH_TEST").as_deref(),
        Ok("1"),
        "run this integration test through `just test-s2-crash`"
    );

    let account_endpoint = test_env("HENOSIS_TEST_S2_ACCOUNT_ENDPOINT", "http://127.0.0.1:4480");
    let basin_endpoint = test_env("HENOSIS_TEST_S2_BASIN_ENDPOINT", "http://127.0.0.1:4480");
    let basin = test_env("HENOSIS_TEST_S2_BASIN", "henosis-core-test");
    let access_token = test_env("HENOSIS_TEST_S2_ACCESS_TOKEN", "local");
    let bundle_root = PathBuf::from(test_env(
        "HENOSIS_TEST_BUNDLE_ROOT",
        "/tmp/henosis-core-crash-test-bundles",
    ));
    fs::create_dir_all(&bundle_root).expect("create crash-test bundle ingress");
    let durable_bundle_root = bundle_root.with_extension("durable");
    fs::create_dir_all(&durable_bundle_root).expect("create durable crash-test bundle store");

    let bundle_digest = install_bundle(&bundle_root, RECOVERY_BUNDLE);
    let graph_id = unique_graph_id();

    let first_port = free_port();
    let mut first = spawn_server(
        first_port,
        &bundle_root,
        &durable_bundle_root,
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
            "components": [component("app", bundle_digest)],
        }),
    );
    assert_eq!(
        created.pointer("/status/generation"),
        Some(&Value::String("1".to_owned()))
    );

    let before = rpc(
        &first_url,
        "GetGraph",
        serde_json::json!({"graphId": graph_id.to_string()}),
    );
    let durable_before = durable_status(&before["status"]);
    first.kill().expect("SIGKILL first server");
    first.wait().expect("reap killed first server");
    fs::remove_dir_all(&bundle_root).expect("wipe first machine bundle ingress");
    fs::create_dir_all(&bundle_root).expect("create empty fresh-machine bundle ingress");

    let second_port = free_port();
    let mut second = spawn_server(
        second_port,
        &bundle_root,
        &durable_bundle_root,
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
    let resumed_watch = watch_first(&second_url, graph_id, 0);
    assert_eq!(
        durable_status(&resumed_watch["status"]),
        durable_status(&after["status"]),
        "a reconnecting watcher receives the current replayed snapshot"
    );

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
    let inspected = inspect_bundle_contract(
        BundleRef::new(ContentDigest::digest(source)),
        source,
        &EngineConfig::default(),
    )
    .expect("inspect crash-test bundle");
    let compiled_dependencies = inspected
        .intent
        .compiled_dependencies()
        .map(|dependency| CompiledDependencyManifest {
            component: dependency.component().as_str().to_owned(),
            revision: dependency.revision().as_str().to_owned(),
            outputs: dependency
                .outputs()
                .map(|(name, output)| {
                    let availability = match output.availability() {
                        OutputAvailability::Static => "static",
                        OutputAvailability::Observed => "observed",
                    };
                    (
                        name.as_str().to_owned(),
                        serde_json::json!({
                            "availability": availability,
                            "optional": output.is_optional(),
                            "schema": output.schema(),
                        }),
                    )
                })
                .collect(),
            consumed_outputs: dependency
                .consumed_outputs()
                .map(|name| name.as_str().to_owned())
                .collect(),
        })
        .collect();
    let artifact_requirements = inspected
        .contract
        .artifact_requirements
        .iter()
        .map(|requirement| ArtifactRequirement {
            component: requirement.component.clone(),
            input: requirement.input.clone(),
            kind: match requirement.kind.as_str() {
                "cloudflare-worker" => WorkloadArtifactKind::CloudflareWorker,
                "static-assets" => WorkloadArtifactKind::StaticAssets,
                other => panic!("unsupported fixture artifact kind {other}"),
            },
            path: requirement.path.clone(),
            source_path: PathBuf::new(),
            line: 0,
            column: 0,
        })
        .collect();
    let executable_sha256 = format!("{:x}", Sha256::digest(source));
    let manifest = BundleManifestV1 {
        format_version: BUNDLE_FORMAT_VERSION,
        component: inspected.intent.name().as_str().to_owned(),
        component_revision: inspected.intent.revision().as_str().to_owned(),
        module_format: "esm".to_owned(),
        entrypoint: "module.js".to_owned(),
        executable_sha256,
        runtime_api_version: RUNTIME_API_VERSION.to_owned(),
        bundler: BundlerIdentity {
            name: "crash-test".to_owned(),
            version: "1".to_owned(),
            config_hash: "test".to_owned(),
            executable_sha256: "test".to_owned(),
        },
        dependency_lock_hash: None,
        sdk_package_hashes: std::collections::BTreeMap::new(),
        declared_capabilities: inspected.contract.declared_capabilities,
        compiled_dependencies,
        config_files: Vec::new(),
        artifact_requirements,
    };
    let bundle_id = manifest
        .identity()
        .expect("compute fixture bundle identity");
    let digest = ContentDigest::from_bytes(parse_hex_digest(&bundle_id));
    let directory = root.join(&bundle_id);
    fs::create_dir_all(&directory).expect("create content-addressed bundle directory");
    fs::write(directory.join("module.js"), source).expect("write bundle fixture");
    fs::write(
        directory.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).expect("encode fixture manifest"),
    )
    .expect("write fixture manifest");
    digest
}

fn parse_hex_digest(value: &str) -> [u8; 32] {
    assert_eq!(value.len(), 64);
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = u8::from_str_radix(std::str::from_utf8(pair).expect("hex is UTF-8"), 16)
            .expect("bundle identity is hex");
    }
    bytes
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
    durable_bundle_root: &Path,
    access_token: &str,
    account_endpoint: &str,
    basin_endpoint: &str,
    basin: &str,
) -> Child {
    Command::new(env!("CARGO_BIN_EXE_henosis-core-server"))
        .env("HENOSIS_BIND", format!("127.0.0.1:{port}"))
        .env("HENOSIS_BUNDLE_ROOT", bundle_root)
        .env("HENOSIS_DURABLE_BUNDLE_ROOT", durable_bundle_root)
        .env(
            "HENOSIS_DEPLOY_REMOTE",
            bundle_root.join("unused-deploy.git"),
        )
        .env("S2_ACCESS_TOKEN", access_token)
        .env("S2_ACCOUNT_ENDPOINT", account_endpoint)
        .env("S2_BASIN_ENDPOINT", basin_endpoint)
        .env("S2_BASIN", basin)
        .env("RUST_LOG", "henosis=error")
        .env("HENOSIS_CLOUDFLARE_LIVE", "1")
        .env(
            "HENOSIS_ARTIFACT_ROOT",
            durable_bundle_root.join("artifacts"),
        )
        .env("HENOSIS_SUPABASE_LIVE", "1")
        .env("HENOSIS_SUPABASE_HOST", "127.0.0.1")
        .env("HENOSIS_SUPABASE_PORT", "5432")
        .env("HENOSIS_SUPABASE_USER", "postgres")
        .env("HENOSIS_SUPABASE_DATABASE", "postgres")
        .env(
            "HENOSIS_SUPABASE_PASSWORD_FILE",
            bundle_root.join("unused-supabase-password"),
        )
        .env("HENOSIS_SUPABASE_API_URL", "http://127.0.0.1:4484")
        .env(
            "HENOSIS_SUPABASE_DATABASE_URL_REF",
            "test://supabase/database",
        )
        .env("HENOSIS_SUPABASE_ANON_KEY_REF", "test://supabase/anon")
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

fn watch_first(base: &str, graph_id: GraphId, after_sequence: u64) -> Value {
    let request = serde_json::to_vec(&serde_json::json!({
        "graphId": graph_id.to_string(),
        "afterSequence": after_sequence.to_string(),
    }))
    .expect("encode watch request");
    let mut envelope = Vec::with_capacity(request.len() + 5);
    envelope.push(0);
    envelope.extend_from_slice(&(request.len() as u32).to_be_bytes());
    envelope.extend_from_slice(&request);
    let mut response = reqwest::blocking::Client::new()
        .post(format!("{base}/henosis.v1.GraphService/WatchGraph"))
        .header("content-type", "application/connect+json")
        .header("connect-protocol-version", "1")
        .body(envelope)
        .send()
        .expect("open ConnectRPC watch");
    assert!(response.status().is_success());
    let mut header = [0_u8; 5];
    response
        .read_exact(&mut header)
        .expect("read first watch envelope header");
    assert_eq!(header[0], 0, "first watch envelope is a data message");
    let length = u32::from_be_bytes(header[1..].try_into().expect("four-byte length")) as usize;
    let mut body = vec![0_u8; length];
    response
        .read_exact(&mut body)
        .expect("read first watch envelope body");
    serde_json::from_slice(&body).expect("decode first watch response")
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
