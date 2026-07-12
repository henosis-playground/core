# core

The Henosis graph orchestrator.

## Server configuration

`henosis-core-server` serves `henosis.v1.GraphService` and
`henosis.v1.ConnectorCallbackService` over ConnectRPC. It reads:

- `DATABASE_URL`, or `CORE_POSTGRES_PASSWORD_FILE` for the Compose database;
- `S2_ACCESS_TOKEN`, `S2_ACCOUNT_ENDPOINT`, `S2_BASIN_ENDPOINT`, and `S2_BASIN`;
- `HENOSIS_AUTH_TOKENS_JSON`, a non-empty JSON array of accepted bearer tokens;
- `HENOSIS_CONNECTORS_JSON`, a JSON map from connector key to `{ "endpoint", "token" }`;
- `HENOSIS_LISTEN`, defaulting to `0.0.0.0:8080`.

Graph content is read exclusively from S2. PostgreSQL contains only connector delivery
checkpoints, display labels, and authentication material.

This workspace contains the graph lifecycle domain, its S2 journal and PostgreSQL metadata
layers, committed ConnectRPC protocol bindings, and service shell. Protocol sources and Buf config
live in `proto/` and the repository root; run `just proto` after changing them.

## Layout

<!-- LINT.IfChange(layout_rules) -->
- `crates/` — library crates and reusable supporting crates
- `tests/` — workspace member crates that build integration and end-to-end test binaries
- `crates/workspace-hack/` — cargo-hakari dependency unification (auto-generated, do not edit)
- Reusable test harnesses, fixtures, and helpers belong in `crates/`, not `tests/`
- Other binary categories should live in their own top-level directories, such as `services/` or `tools/`
<!-- LINT.ThenChange(//AGENTS.md:layout_rules) -->

## Commands

Use `just` to discover and run common tasks:

<!-- LINT.IfChange(command_recipes) -->
- `just lint` — run all lints (fmt, clippy, deny, pre-commit). Always run after making changes.
- `just test` — run all tests with optimized third-party dependencies
- `just doc` — build docs
<!-- LINT.ThenChange(//AGENTS.md:command_recipes) -->
