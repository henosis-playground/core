#!/usr/bin/env bash
set -euo pipefail

CORE_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
HENOSIS_ROOT=$(cd "$CORE_ROOT/../.." && pwd)
BOT_ROOT="$HENOSIS_ROOT/repos/bot"
PLATFORM_ROOT="$HENOSIS_ROOT/repos/platform"
BENCHMARK="$PLATFORM_ROOT/examples/benchmark"
COMPOSE_FILE="$HENOSIS_ROOT/infra/docker-compose.yml"
STATE_ROOT=${HENOSIS_SHOWCASE_ROOT:-/tmp/henosis-d26-showcase}
CORE_PORT=${HENOSIS_SHOWCASE_CORE_PORT:-4681}
S2_PORT=${HENOSIS_SHOWCASE_S2_PORT:-4680}
CORE_URL="http://127.0.0.1:$CORE_PORT"
GRAPH=${HENOSIS_SHOWCASE_GRAPH:-graph_070w3ge1r70w3ge1r70w3ge1r7}
COMPOSE_PROJECT=henosis-d26-showcase
CLI="$BOT_ROOT/target/debug/henosis"
SERVER="$CORE_ROOT/target/debug/henosis-core-server"
DEPLOY_REMOTE="$STATE_ROOT/deploy.git"
PID_FILE="$STATE_ROOT/core.pid"
URLS_FILE="$STATE_ROOT/urls.env"
TRANSCRIPT="$STATE_ROOT/transcript.txt"
MODE=${1:-up}

run() {
  printf '\n$'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

server_is_running() {
  [[ -f "$PID_FILE" ]] && kill -0 "$(<"$PID_FILE")" 2>/dev/null
}

wait_for_core() {
  for _ in $(seq 1 120); do
    if (echo >"/dev/tcp/127.0.0.1/$CORE_PORT") 2>/dev/null; then
      return 0
    fi
    if ! server_is_running; then
      printf 'core server exited during startup\n'
      if [[ -f "$STATE_ROOT/core.log" ]]; then
        tail -n 80 "$STATE_ROOT/core.log"
      fi
      return 1
    fi
    sleep 0.25
  done
  printf 'core server did not listen on %s\n' "$CORE_URL"
  return 1
}

extract_output() {
  local reference=$1
  local status_file=$2
  python - "$reference" "$status_file" <<'PY'
import pathlib
import sys

needle = f"output {sys.argv[1]} = "
for line in pathlib.Path(sys.argv[2]).read_text().splitlines():
    if needle in line:
        print(line.split(needle, 1)[1])
        break
else:
    raise SystemExit(f"missing {sys.argv[1]} in status output")
PY
}

curl_until_ready() {
  local url=$1
  local output=
  printf '\n$ curl --fail --silent --show-error %q\n' "$url"
  for _ in $(seq 1 30); do
    if output=$(curl --fail --silent --show-error "$url" 2>&1); then
      printf '%s\n' "$output"
      return 0
    fi
    sleep 1
  done
  printf '%s\n' "$output" >&2
  return 1
}

showcase_up() {
  if server_is_running; then
    printf 'showcase is already running (pid %s); run `just showcase-down` first\n' "$(<"$PID_FILE")" >&2
    exit 1
  fi

  rm -rf "$STATE_ROOT"
  mkdir -p "$STATE_ROOT"
  exec > >(tee "$TRANSCRIPT") 2>&1

  printf 'Henosis D26 SHOWCASE\n'
  printf 'One graph: Cloudflare Workers LIVE; Kubernetes -> local bare Git; Supabase -> FAKE (explicitly labeled).\n'
  printf 'Safety: live Cloudflare is opt-in here and the controller enforces henosis-* managed names.\n'
  printf 'Storage note: s2-lite is started and health-checked; the current showcase server still materializes graph state in memory.\n'
  printf 'Teardown is intentionally separate: run `just showcase-down`.\n'

  for command in docker git curl wrangler cargo; do
    command -v "$command" >/dev/null || { printf 'missing required command: %s\n' "$command" >&2; exit 1; }
  done

  run wrangler whoami
  run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" up -d --wait s2-lite
  run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" ps s2-lite
  run cargo build --manifest-path "$BOT_ROOT/Cargo.toml" -p henosis-cli
  run cargo build --manifest-path "$CORE_ROOT/Cargo.toml" -p henosis-core-server

  rm -rf "$BENCHMARK/.henosis"
  run git init --bare --initial-branch=main "$DEPLOY_REMOTE"

  printf '\n$ HENOSIS_CLOUDFLARE_LIVE=1 HENOSIS_BIND=127.0.0.1:%s HENOSIS_BUNDLE_ROOT=%q HENOSIS_ARTIFACT_ROOT=%q HENOSIS_DEPLOY_REMOTE=%q %q\n' \
    "$CORE_PORT" "$BENCHMARK/.henosis/bundles" "$BENCHMARK/.henosis/artifacts" "$DEPLOY_REMOTE" "$SERVER"
  HENOSIS_CLOUDFLARE_LIVE=1 \
  HENOSIS_BIND="127.0.0.1:$CORE_PORT" \
  HENOSIS_BUNDLE_ROOT="$BENCHMARK/.henosis/bundles" \
  HENOSIS_ARTIFACT_ROOT="$BENCHMARK/.henosis/artifacts" \
  HENOSIS_DEPLOY_REMOTE="$DEPLOY_REMOTE" \
  RUST_LOG=henosis=info \
    nohup "$SERVER" >"$STATE_ROOT/core.log" 2>&1 &
  printf '%s\n' "$!" >"$PID_FILE"
  wait_for_core

  printf '\nDeploying the benchmark graph. Watch for named blocking, observations, re-evaluation, and ready.\n'
  run env --chdir="$BENCHMARK" "$CLI" deploy . --graph "$GRAPH" --core "$CORE_URL" --create

  STATUS_FILE="$STATE_ROOT/status.txt"
  printf '\n$ %q status --graph %q --core %q\n' "$CLI" "$GRAPH" "$CORE_URL"
  env --chdir="$BENCHMARK" "$CLI" status --graph "$GRAPH" --core "$CORE_URL" | tee "$STATUS_FILE"
  BACKEND_URL=$(extract_output backend.outputs.url "$STATUS_FILE")
  FRONTEND_URL=$(extract_output frontend.outputs.url "$STATUS_FILE")
  printf 'BACKEND_URL=%q\nFRONTEND_URL=%q\n' "$BACKEND_URL" "$FRONTEND_URL" >"$URLS_FILE"

  printf '\nLive URLs\n'
  printf '  backend:  %s\n' "$BACKEND_URL"
  printf '  frontend: %s\n' "$FRONTEND_URL"
  printf '\nFrontend response (the frontend fetches the backend using the observed graph value)\n'
  curl_until_ready "$FRONTEND_URL/showcase?from=frontend"

  printf '\nKubernetes publication containing the same graph-fed backend URL\n'
  run git --git-dir="$DEPLOY_REMOTE" for-each-ref '--format=%(refname:short)' refs/heads
  run git --git-dir="$DEPLOY_REMOTE" grep -n -C 4 -F "$BACKEND_URL" "refs/heads/env/$GRAPH"

  printf '\nSHOWCASE READY and left running.\n'
  printf 'Transcript: %s\n' "$TRANSCRIPT"
  printf 'Teardown:   cd %s && just showcase-down\n' "$CORE_ROOT"
}

showcase_down() {
  mkdir -p "$STATE_ROOT"
  exec > >(tee -a "$TRANSCRIPT") 2>&1
  printf '\nHenosis D26 SHOWCASE teardown\n'

  if server_is_running; then
    if [[ -f "$URLS_FILE" ]]; then
      # shellcheck disable=SC1090
      source "$URLS_FILE"
    fi
    run env --chdir="$BENCHMARK" "$CLI" retire --graph "$GRAPH" --core "$CORE_URL"

    if [[ -n "${BACKEND_URL:-}" && -n "${FRONTEND_URL:-}" ]]; then
      printf '\nVerifying Cloudflare cleanup (both URLs must stop returning success)\n'
      for url in "$FRONTEND_URL" "$BACKEND_URL"; do
        code=000
        for _ in $(seq 1 30); do
          code=$(curl --silent --output /dev/null --write-out '%{http_code}' "$url" || true)
          [[ "$code" != 2* && "$code" != 3* ]] && break
          sleep 1
        done
        printf '  %s -> HTTP %s after retire\n' "$url" "$code"
        if [[ "$code" == 2* || "$code" == 3* ]]; then
          printf 'Cloudflare cleanup verification failed for %s\n' "$url" >&2
          exit 1
        fi
      done
    fi

    kill "$(<"$PID_FILE")" 2>/dev/null || true
    wait "$(<"$PID_FILE")" 2>/dev/null || true
    rm -f "$PID_FILE"
  else
    printf 'core server is not running; skipping graph retire\n'
  fi

  run env S2_LITE_PORT="$S2_PORT" docker compose -p "$COMPOSE_PROJECT" -f "$COMPOSE_FILE" down
  printf 'SHOWCASE DOWN\nTranscript: %s\n' "$TRANSCRIPT"
}

case "$MODE" in
  up) showcase_up ;;
  down) showcase_down ;;
  *) printf 'usage: %s [up|down]\n' "$0" >&2; exit 2 ;;
esac
