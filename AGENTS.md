# bitcoin-ipc

Rust Cap'n Proto IPC client for Bitcoin Core / Knots. Consumers: p2poolv2,
Stratum V2, any software that needs to talk to a Bitcoin node.

Migration from `sv2-bitcoin-core` in progress — see `docs/001-dep-cleanup.md`
and `docs/002-repurpose.md` before touching code.

## Build

Requires `capnpc` (from cap'n proto install) to generate `src/gen/*.rs` from
`capnp/*.capnp` at build time.

## Run

Library crate — no binary. Only entrypoint:

```
cargo run --example logger /path/to/bitcoin/node.sock
```

## Quirks

- Uses `tokio::task::spawn_local` — consumers must wrap runtime in
  `tokio::task::LocalSet` (see `examples/logger.rs:21`).
- `src/gen/` is gitignored except `src/gen/mod.rs` — never commit generated
  capnp code.
- Requires nightly Rust.
- No tests, no CI, no lint config.

## Code style

Follow the same conventions as p2poolv2 (`../p2poolv2/AGENTS.md`):

- `thiserror::Error` for all error types. No manual `Display`/`Error` impls.
- `///` doc comments on public API types and methods. No inline `//` comments
  unless logic is genuinely surprising.
- Prefer `file.rs:line` references over copying code into this file.
- Merge small related modules rather than creating many tiny files.
