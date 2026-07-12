export RUST_BACKTRACE := env_var_or_default("RUST_BACKTRACE", "short")

default:
  just --list

clean:
    cargo clean

fmt *flags:
    cargo fmt --all {{ flags }}
    taplo fmt
    yamlfmt .

check-fmt:
    cargo fmt --all -- --check
    taplo fmt --check
    yamlfmt -lint .

clippy *flags:
    cargo clippy --workspace --all-features --all-targets {{ flags }} -- -D warnings --allow deprecated

check-deny:
    cargo deny --all-features check

check-pre-commit:
    prek run --all-files

# Checks source format/lint/compatibility and that committed bindings match the protos.
check-proto:
    #!/usr/bin/env bash
    set -euo pipefail
    buf format --diff --exit-code
    buf lint
    buf breaking --exclude-imports --against proto/henosis-v1-baseline.binpb.gz
    before="$(mktemp -d)"
    trap 'rm -rf "$before"' EXIT
    cp -R crates/proto/src/generated "$before/generated"
    buf generate
    cargo fmt -p henosis-proto
    diff -ru "$before/generated" crates/proto/src/generated

# Runs all lints (fmt, clippy, deny, protobuf, pre-commit hooks)
lint: check-fmt clippy check-deny check-proto check-pre-commit

test *flags:
    cargo nextest run --cargo-profile testing --no-tests=pass {{ flags }}

# Apply root db/migrations and verify the generated Rust schema is unchanged.
db-migrate *flags:
    diesel migration run --locked-schema {{ flags }}

doc *flags:
    RUSTDOCFLAGS="--cfg docsrs" cargo doc --all-features --no-deps --document-private-items --keep-going {{ flags }}

# Regenerates the committed Rust bindings from the root proto module.
proto:
    buf generate
    cargo fmt -p henosis-proto

[private]
_assert-clean:
    {{ if `test -z "$(git status --porcelain --untracked-files=no)" && echo clean || echo dirty` == "dirty" {
        error("working tree is dirty — commit or stash changes first")
    } else {
        ""
    } }}

# Auto-fix formatting and lint warnings (requires clean working tree)
fix: _assert-clean
    just fmt
    cargo fix --workspace --allow-dirty --allow-staged
    cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged

changelog:
    git cliff -o CHANGELOG.md

hakari:
    cargo hakari manage-deps --yes
    cargo hakari generate
    taplo fmt crates/workspace-hack/Cargo.toml
