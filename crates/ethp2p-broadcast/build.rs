fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/broadcast.proto");
    println!("cargo:rerun-if-changed=proto/rs.proto");
    prost_build::Config::new()
        .compile_protos(&["proto/broadcast.proto", "proto/rs.proto"], &["proto/"])
}
