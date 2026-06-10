//! Conversions between Rust-native types and Cap'n Proto builders.

/// Converts a Rust-native options struct into its capnp Builder equivalent.
///
/// `B` is the target capnp builder type (e.g. `BlockCreateOptions::Builder<'_>`).
pub(crate) trait IntoCapnp<B> {
    fn apply(&self, builder: &mut B);
}
