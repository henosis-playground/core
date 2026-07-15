#!/usr/bin/env bash
set -euo pipefail

CORE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HENOSIS_ROOT=$(cd "$CORE_ROOT/../.." && pwd)
BOT_ROOT="$HENOSIS_ROOT/repos/bot"
PLATFORM_ROOT="$HENOSIS_ROOT/repos/platform"
COMPOSE_FILE="$HENOSIS_ROOT/infra/docker-compose.yml"
DEMO_ROOT=${HENOSIS_D26_DEMO_ROOT:-/tmp/henosis-d26-demo}
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

printf 'Henosis D26 end-to-end demo\n'
printf 'Honest targets: k8s=file:// bare Git; supabase=fake output transport; cloudflare=recorded/fake transport (no live credentials).\n'
printf 'Storage note: s2-lite is started and health-checked; this closing-pass server wiring keeps its live materialization in memory.\n'

run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d --wait s2-lite
run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" ps s2-lite

run cargo build --manifest-path "$BOT_ROOT/Cargo.toml" -p henosis-cli
run cargo build --manifest-path "$CORE_ROOT/Cargo.toml" -p henosis-core-server -p henosis-frontend-git-sync

BUNDLES="$DEMO_ROOT/bundles"
run "$BOT_ROOT/target/debug/henosis" bundle "$PLATFORM_ROOT/examples/benchmark" --output "$BUNDLES"

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

run "$BOT_ROOT/target/debug/henosis" submit "$GRAPH" --manifest "$BUNDLES/manifest.json" --core "$CORE_URL" --demo-targets
run "$BOT_ROOT/target/debug/henosis" watch "$GRAPH" --core "$CORE_URL" --demo-targets

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
run "$BOT_ROOT/target/debug/henosis" status "$GIT_SYNC_GRAPH" --core "$CORE_URL" --demo-targets

printf '\nDemo complete. Transcript: %s\n' "$TRANSCRIPT"
