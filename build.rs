use std::env;
use std::path::PathBuf;

fn main() {
    let gen_dir = PathBuf::from(
        env::var("CARGO_MANIFEST_DIR")
            .expect("CARGO_MANIFEST_DIR was not defined by the cargo environment!"),
    )
    .join("src")
    .join("generated")
    .to_str()
    .unwrap()
    .to_string();

    let out_path = PathBuf::from(gen_dir);

    // Ensure rebuild when any capnp file changes
    println!("cargo:rerun-if-changed=capnp");

    // Also trigger rebuild if build.rs itself changes
    println!("cargo:rerun-if-changed=build.rs");

    capnpc::CompilerCommand::new()
        .src_prefix("capnp")
        .file("capnp/chain.capnp")
        .file("capnp/common.capnp")
        .file("capnp/echo.capnp")
        .file("capnp/handler.capnp")
        .file("capnp/init.capnp")
        .file("capnp/mining.capnp")
        .file("capnp/proxy.capnp")
        .file("capnp/rpc.capnp")
        .output_path(out_path)
        .run()
        .expect("capnpc compilation failed");
}
