//! Wraps types from Bitcoin Core's `capnp/proxy.capnp`.
//! Do not modify — upstream schema changes belong in Bitcoin Core repository.
//!
//! Re-exports the `Thread` and `Context` types from the auto-generated
//! Cap'n Proto code with Rust-native documentation.

use crate::generated::proxy_capnp;

/// Cap'n Proto execution context passed as a parameter from client to server.
///
/// Contains thread handles that route method calls to the correct server thread.
/// See `capnp/proxy.capnp` for the upstream schema.
pub use proxy_capnp::context;
