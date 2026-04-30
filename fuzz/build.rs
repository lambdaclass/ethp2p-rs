//! Build script for `ethp2p-fuzz`.
//!
//! When the `goref-shim` feature is enabled, invokes
//! `go build -buildmode=c-archive` to produce `libgoref.a` from
//! `goref/`, then emits the cargo link directives that statically link
//! it into the fuzz binaries. When the feature is disabled (the
//! default), this build script is a no-op.

fn main() {
    println!("cargo:rerun-if-changed=goref/");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_GOREF_SHIM");

    #[cfg(feature = "goref-shim")]
    build_goref();
}

#[cfg(feature = "goref-shim")]
fn build_goref() {
    use std::path::PathBuf;
    use std::process::Command;

    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let goref_dir = manifest_dir.join("goref");
    let go_mod = goref_dir.join("go.mod");

    if !go_mod.exists() {
        eprintln!(
            "\nethp2p-fuzz: `goref-shim` feature is enabled but the shim source is missing.\n\
             Expected: {go_mod}\n\
             See {readme} for the C ABI the shim must export.\n",
            go_mod = go_mod.display(),
            readme = goref_dir.join("README.md").display(),
        );
        std::process::exit(1);
    }

    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let archive = out_dir.join("libgoref.a");

    let status = Command::new("go")
        .arg("build")
        .arg("-buildmode=c-archive")
        .arg("-o")
        .arg(&archive)
        .arg(".")
        .current_dir(&goref_dir)
        .status()
        .expect("failed to invoke `go build` for goref shim; is Go installed?");

    if !status.success() {
        eprintln!("ethp2p-fuzz: `go build` for goref shim failed");
        std::process::exit(1);
    }

    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=goref");

    // cgo on darwin needs these system frameworks linked.
    if std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default() == "macos" {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }
}
