#!/usr/bin/env bash
set -euo pipefail

CORE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HENOSIS_ROOT=$(cd "$CORE_ROOT/../.." && pwd)
BOT_ROOT="$HENOSIS_ROOT/repos/bot"
PLATFORM_ROOT="$HENOSIS_ROOT/repos/platform"
COMPOSE_FILE="$HENOSIS_ROOT/infra/docker-compose.yml"
MODE=${1:-offline}
if [[ "$MODE" == --live ]]; then
  DEMO_ROOT=${HENOSIS_D26_LIVE_DEMO_ROOT:-/tmp/henosis-d26-live-demo}
else
  DEMO_ROOT=${HENOSIS_D26_DEMO_ROOT:-/tmp/henosis-d26-demo}
fi
CORE_PORT=${HENOSIS_D26_CORE_PORT:-4581}
S2_PORT=${HENOSIS_D26_S2_PORT:-4580}
CORE_URL="http://127.0.0.1:$CORE_PORT"
GRAPH=graph_070w3ge1r70w3ge1r70w3ge1r7
GIT_SYNC_GRAPH=graph_081040g2081040g2081040g208
COMPOSE_PROJECT=henosis-d26-demo

rm -rf "$DEMO_ROOT"
mkdir -p "$DEMO_ROOT"
TRANSCRIPT="$DEMO_ROOT/transcript.txt"
exec > >(tee "$TRANSCRIPT") 2>&1

server_pid=
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down >/dev/null 2>&1 || true
}
trap cleanup EXIT

run() {
  printf '\n$'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

if [[ "$MODE" == --live ]]; then
  BENCHMARK="$PLATFORM_ROOT/examples/benchmark"
  BUILD_ROOT="$DEMO_ROOT/workers"
  mkdir -p "$BUILD_ROOT/backend" "$BUILD_ROOT/frontend"
  printf 'Henosis D26 live Cloudflare demo\n'
  printf 'Safety: explicit --live opt-in; only henosis-* Workers are mutated and retired.\n'
  printf 'Artifact lane: Wrangler compiles benchmark TypeScript before the controller; the controller receives only content digests and fetches verified bytes.\n'
  run wrangler deploy "$BENCHMARK/workers/backend.ts" --dry-run --outdir "$BUILD_ROOT/backend" --name henosis-artifact-build-backend --compatibility-date 2026-07-15
  run wrangler deploy "$BENCHMARK/workers/frontend.ts" --dry-run --outdir "$BUILD_ROOT/frontend" --name henosis-artifact-build-frontend --compatibility-date 2026-07-15
  ARTIFACT_ROOT="$DEMO_ROOT/artifacts"
  mapfile -t DIGESTS < <(python - \
    "$BUILD_ROOT/backend/backend.js" \
    "$BUILD_ROOT/frontend/frontend.js" \
    "$ARTIFACT_ROOT" <<'PY'
import hashlib
import json
import pathlib
import sys

backend, frontend, root = map(pathlib.Path, sys.argv[1:])
index = b'''<!doctype html>
<html lang="en"><meta charset="utf-8"><title>Henosis benchmark</title>
<body>Henosis benchmark frontend</body></html>
'''
assets = json.dumps({
    "format": "henosis-static-assets-v1",
    "files": {"index.html": list(index)},
}, sort_keys=True, separators=(",", ":")).encode()
for content in (backend.read_bytes(), frontend.read_bytes(), assets):
    digest = hashlib.sha256(content).hexdigest()
    destination = root / "sha256" / digest
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(content)
    print(f"sha256:{digest}")
PY
  )
  run env \
    HENOSIS_CLOUDFLARE_LIVE=1 \
    HENOSIS_CLOUDFLARE_ARTIFACT_ROOT="$ARTIFACT_ROOT" \
    HENOSIS_CLOUDFLARE_BACKEND_DIGEST="${DIGESTS[0]}" \
    HENOSIS_CLOUDFLARE_FRONTEND_DIGEST="${DIGESTS[1]}" \
    HENOSIS_CLOUDFLARE_FRONTEND_ASSETS_DIGEST="${DIGESTS[2]}" \
    cargo +nightly-2026-06-09 test --manifest-path "$CORE_ROOT/Cargo.toml" \
      -p henosis-controller-cloudflare live_benchmark_workers_upload_serve_publish_and_retire \
      -- --ignored --nocapture
  printf '\nLive demo complete. Transcript: %s\n' "$TRANSCRIPT"
  exit 0
fi

printf 'Henosis D26 end-to-end demo\n'
printf 'Honest targets: k8s=file:// bare Git; supabase=fake output transport; cloudflare=recorded/fake transport (no live credentials).\n'
printf 'Storage note: s2-lite is started and health-checked; this closing-pass server wiring keeps its live materialization in memory.\n'

run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d --wait s2-lite
run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" ps s2-lite

run cargo build --manifest-path "$BOT_ROOT/Cargo.toml" -p henosis-cli
run cargo build --manifest-path "$CORE_ROOT/Cargo.toml" -p henosis-core-server -p henosis-frontend-git-sync

BUNDLES="$PLATFORM_ROOT/examples/benchmark/.henosis/bundles"
rm -rf "$BUNDLES"
run "$BOT_ROOT/target/debug/henosis" debug bundle "$PLATFORM_ROOT/examples/benchmark" --output "$BUNDLES"

DEPLOY_REMOTE="$DEMO_ROOT/deploy.git"
run git init --bare --initial-branch=main "$DEPLOY_REMOTE"

printf '\n$ HENOSIS_BIND=127.0.0.1:%s HENOSIS_BUNDLE_ROOT=%q HENOSIS_DEPLOY_REMOTE=%q %q\n' \
  "$CORE_PORT" "$BUNDLES" "$DEPLOY_REMOTE" "$CORE_ROOT/target/debug/henosis-core-server"
HENOSIS_BIND="127.0.0.1:$CORE_PORT" \
HENOSIS_BUNDLE_ROOT="$BUNDLES" \
HENOSIS_DEPLOY_REMOTE="$DEPLOY_REMOTE" \
RUST_LOG=henosis=info \
  "$CORE_ROOT/target/debug/henosis-core-server" >"$DEMO_ROOT/core.log" 2>&1 &
server_pid=$!
for _ in $(seq 1 120); do
  if (echo >"/dev/tcp/127.0.0.1/$CORE_PORT") 2>/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$DEMO_ROOT/core.log"
    exit 1
  fi
  sleep 0.25
done

run "$BOT_ROOT/target/debug/henosis" deploy "$PLATFORM_ROOT/examples/benchmark" --graph "$GRAPH" --core "$CORE_URL" --create --demo-targets

printf '\nKubernetes file:// publication\n'
run git --git-dir="$DEPLOY_REMOTE" for-each-ref '--format=%(refname:short)' refs/heads
run git --git-dir="$DEPLOY_REMOTE" ls-tree -r --name-only "refs/heads/env/$GRAPH"

INTENT_REMOTE="$DEMO_ROOT/intent.git"
INTENT_WORK="$DEMO_ROOT/intent-work"
run git init --bare --initial-branch=main "$INTENT_REMOTE"
run git init --initial-branch=main "$INTENT_WORK"
run git -C "$INTENT_WORK" config user.name 'Henosis Agent'
run git -C "$INTENT_WORK" config user.email henosis-agent@users.noreply.github.com
run git -C "$INTENT_WORK" config commit.gpgsign false
mkdir -p "$INTENT_WORK/henosis/graphs"
python - "$BUNDLES/manifest.json" "$INTENT_WORK/henosis/graphs/$GIT_SYNC_GRAPH.toml" "$GIT_SYNC_GRAPH" <<'PY'
import base64
import json
import pathlib
import sys

manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
bundle = next(item for item in manifest["bundles"] if item["component"] == "service_pair")
digest = base64.b64encode(bytes.fromhex(bundle["bundle_id"])).decode()
pathlib.Path(sys.argv[2]).write_text(
    f'''schema = 1\ngraph = "{sys.argv[3]}"\nname = "git-sync-demo"\ngeneration = 0\n\n'''
    f'''[[components]]\nname = "service_pair"\nbundleDigest = "{digest}"\n'''
)
PY
printf '\nEdited pin file before frontend sync\n'
printf '\n$ git -C %q diff --no-index /dev/null %q\n' "$INTENT_WORK" "henosis/graphs/$GIT_SYNC_GRAPH.toml"
git -C "$INTENT_WORK" diff --no-index /dev/null "henosis/graphs/$GIT_SYNC_GRAPH.toml" || true
run git -C "$INTENT_WORK" add .
run git -C "$INTENT_WORK" commit -m 'Pin D26 demo graph'
run git -C "$INTENT_WORK" remote add origin "$INTENT_REMOTE"
run git -C "$INTENT_WORK" push origin main
run "$CORE_ROOT/target/debug/henosis-frontend-git-sync" "$INTENT_REMOTE" "$CORE_URL"

printf '\nGit-sync acknowledgement written through the public GraphService\n'
run git --git-dir="$INTENT_REMOTE" show "main:henosis/graphs/$GIT_SYNC_GRAPH.toml"
run env --chdir="$PLATFORM_ROOT/examples/benchmark" \
  "$BOT_ROOT/target/debug/henosis" status --graph "$GIT_SYNC_GRAPH" --core "$CORE_URL" --demo-targets

printf '\nDemo complete. Transcript: %s\n' "$TRANSCRIPT"
