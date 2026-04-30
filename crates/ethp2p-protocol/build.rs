fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/protocol.proto");
    prost_build::Config::new().compile_protos(&["proto/protocol.proto"], &["proto/"])
}
