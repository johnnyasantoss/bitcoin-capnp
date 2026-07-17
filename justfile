# bitcoin-capnp development commands
# Requires: capnpc (cap'n proto), nightly Rust toolchain
set dotenv-load

export RUST_LOG := env("RUST_LOG", "debug")
export RUST_BACKTRACE := env("RUST_BACKTRACE", "full")
export SOCKET := env("SOCKET", "../bitcoin/testnet4/node.sock")

_default:
    @just --list

# Builds the lib
build:
    cargo build

# Verify library compiles
check:
    cargo check --all

# Build the logger example
build-example EXAMPLE="logger":
    cargo build --example {{ EXAMPLE }}

# Build the logger example
run-example EXAMPLE="logger" *ARGS="":
    cargo run --example {{ EXAMPLE }} -- {{ SOCKET }} {{ ARGS }}

# Runs the interactive TUI
run-tui:
    cargo run --manifest-path examples/tui/Cargo.toml -- {{ SOCKET }}

# Generate docs and verify no broken doc links
doc:
    cargo doc --no-deps --open

# Watch mode — re-check on file changes (requires bacon)
watch:
    bacon -s check-all

# Lint all file
lint:
    tombi lint
    cargo clippy
