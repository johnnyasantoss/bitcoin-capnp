# bitcoin-ipc

Rust client library for Bitcoin Core's Cap'n Proto IPC (libmultiprocess)
interface.

Works with Bitcoin Core, Knots, and compatible implementations. For consumers
(p2poolv2, Stratum V2, any software that needs to talk to a Bitcoin node).

## Build

Requires `capnpc` (from [Cap'n Proto](https://capnproto.org/install.html)) to
generate `src/gen/*.rs` from `capnp/*.capnp` at build time.

## Run

Library crate — no binary target. Only entrypoint:

```
cargo run --example logger /path/to/bitcoin/node.sock
```
