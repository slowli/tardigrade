//! Build script for the server.

use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let protos = &[
        "proto/tardigrade/service.proto",
        "proto/tardigrade/channels.proto",
        "proto/tardigrade/test.proto",
    ];

    tonic_build::configure()
        .file_descriptor_set_path(out_dir.join("tardigrade_descriptor.bin"))
        .build_client(false)
        .compile(protos, &["proto"])
        .unwrap();
}
