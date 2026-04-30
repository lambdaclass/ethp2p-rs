fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto/broadcast.proto");
    prost_build::Config::new().compile_protos(&["proto/broadcast.proto"], &["proto/"])
}
