use std::path::{Path, PathBuf};
use std::process::ExitCode;

use serde::Deserialize;
use sha2::{Digest, Sha256};

const USAGE: &str = "xtask — repository automation for ethp2p-rs

USAGE:
    cargo xtask <COMMAND>

COMMANDS:
    check-protos    Verify vendored .proto files match upstream hashes

OPTIONS:
    -h, --help    Print this message
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        None | Some("-h" | "--help") => {
            print!("{USAGE}");
            ExitCode::SUCCESS
        }
        Some("check-protos") => check_protos(),
        Some(other) => {
            eprintln!("unknown subcommand: {other}");
            eprint!("\n{USAGE}");
            ExitCode::from(2)
        }
    }
}

#[derive(Debug, Deserialize)]
struct HashesConfig {
    proto: Vec<ProtoEntry>,
}

#[derive(Debug, Deserialize)]
struct ProtoEntry {
    path: String,
    sha256: String,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("xtask crate must have a parent")
        .to_path_buf()
}

fn check_protos() -> ExitCode {
    let root = workspace_root();
    let hashes_path = root.join("xtask").join("proto-hashes.toml");
    let toml_text = match std::fs::read_to_string(&hashes_path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed to read {}: {e}", hashes_path.display());
            return ExitCode::from(1);
        }
    };
    let cfg: HashesConfig = match toml::from_str(&toml_text) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("failed to parse {}: {e}", hashes_path.display());
            return ExitCode::from(1);
        }
    };

    let mut errors: usize = 0;
    for entry in &cfg.proto {
        let abs = root.join(&entry.path);
        match std::fs::read(&abs) {
            Ok(bytes) => {
                let actual = hex::encode(Sha256::digest(&bytes));
                if actual == entry.sha256 {
                    println!("ok   {}", entry.path);
                } else {
                    eprintln!("DRIFT {}", entry.path);
                    eprintln!("  expected sha256 = {}", entry.sha256);
                    eprintln!("  actual   sha256 = {actual}");
                    errors += 1;
                }
            }
            Err(e) => {
                eprintln!("MISSING {} ({e})", entry.path);
                errors += 1;
            }
        }
    }

    if errors == 0 {
        println!("\nall {} vendored proto hashes match", cfg.proto.len());
        ExitCode::SUCCESS
    } else {
        eprintln!("\n{errors} proto hash mismatch(es)");
        ExitCode::from(1)
    }
}
