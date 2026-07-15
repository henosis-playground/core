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

# Runs all lints (fmt, clippy, deny, pre-commit hooks)
lint: check-fmt clippy check-deny check-pre-commit

test *flags:
    cargo nextest run --cargo-profile testing --no-tests=pass {{ flags }}

doc *flags:
    RUSTDOCFLAGS="--cfg docsrs" cargo doc --all-features --no-deps --document-private-items --keep-going {{ flags }}

# Runs the clean local D26 bundle → core → controllers → Git/frontend demo.
demo-d26:
    ./demo/d26.sh

# Builds benchmark workload artifacts and reconciles them against live Cloudflare.
demo-d26-live:
    ./demo/d26.sh --live

# Runs the live D26 showcase and leaves the graph, Workers, core, and s2-lite up.
showcase:
    ./demo/showcase.sh up

# Retires the showcase graph, verifies Cloudflare cleanup, and stops local services.
showcase-down:
    ./demo/showcase.sh down

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
