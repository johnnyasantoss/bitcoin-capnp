# bitcoin-ipc

Rust Cap'n Proto IPC client for Bitcoin Core / Knots. Consumers: p2poolv2,
Stratum V2, any software that needs to talk to a Bitcoin node.

Migration from `sv2-bitcoin-core` to `bitcoin-ipc` complete — see
`docs/001-dep-cleanup.md` and `docs/002-repurpose.md` for history.

## Build

Requires `capnpc` (from cap'n proto install) to generate `src/gen/*.rs` from
`capnp/*.capnp` at build time.

## Verify

- After public API or doc changes, run `cargo test` — not just `cargo check`.
  Doc examples in `///` comments are doctests; `check` does not compile them.
- Run `cargo fmt` before every commit. Unformatted code is a review blocker.

## Run

Library crate — no binary. Example entrypoints:

```
cargo run --example logger /path/to/bitcoin/node.sock
cargo run --example echo   /path/to/bitcoin/node.sock
```

## Docs

- Plans for new features, examples, and architecture changes live under
  `docs/` as numbered files: `NNN-short-slug.md`.

## Quirks

- Uses `tokio::task::spawn_local` — consumers must wrap runtime in
  `tokio::task::LocalSet` (see `examples/logger.rs:21`).
- `src/gen/` is gitignored except `src/gen/mod.rs` — never commit generated
  capnp code.
- Requires nightly Rust.

## Code style

- Avoid deep nesting. Extract inner closures and nested logic into named
  functions. A flat call stack reads faster than four levels of indentation.
- `thiserror::Error` for all error types. No manual `Display`/`Error` impls.
- `///` doc comments on ALL public API items — structs, enums, enum variants,
  methods, functions, modules. No inline `//` comments unless logic is
  genuinely surprising.
- Prefer `file.rs:line` references over copying code into this file.
- Merge small related modules rather than creating many tiny files.
- No `.unwrap()`. Use `.expect("fluent message")` — even in examples. The
  message should read as natural English, not a robotic label.
- `pub(crate)` for internal plumbing (`client.rs`, `libmp.rs`). `pub` only for
  consumer-facing API modules. Internal abstractions like `PublicClient`,
  `ContextBuilder`, and `IntoCapnp` stay `pub(crate)` — they are not part of
  the public API surface.
- Use traits for abstractions, not macros. A `macro_rules!` that is purely a
  mechanical list of type paths is acceptable — but logic must live in trait
  default methods.
- Per-request cancellation (`send_cancellable`) over client-level kill switches.
  A `CancellationToken` passed to a single RPC stops only that call.
