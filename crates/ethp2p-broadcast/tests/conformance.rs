//! Codec conformance corpus driver.
//!
//! Walks `conformance/corpus/codec/`, parses each `<name>.yaml`,
//! encodes the typed message via `prost`, and asserts byte-equality
//! against the corresponding `<name>.bytes.hex`.
//!
//! Set `ETHP2P_REGEN_CODEC_CORPUS=1` to write fresh `.bytes.hex` files
//! instead of asserting (used after editing a YAML or after slice 2's
//! Go oracle replaces the regression anchor with a Go-validated
//! golden).

use std::path::{Path, PathBuf};

use ethp2p_broadcast::pb::{bcast, chunk, sess, Bcast, Sess};
use prost::Message;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum CorpusEntry {
    BcastHandshake {
        version: u32,
        channels: Vec<String>,
        peer_id: String,
    },
    SessOpen {
        channel: String,
        message_id: String,
        preamble_hex: String,
        initial_update_hex: String,
    },
    ChunkHeader {
        channel: String,
        message_id: String,
        chunk_id_hex: String,
        data_length: u32,
    },
}

impl CorpusEntry {
    fn encode(&self) -> Vec<u8> {
        match self {
            Self::BcastHandshake {
                version,
                channels,
                peer_id,
            } => {
                let bcast = Bcast {
                    message: Some(bcast::Message::PeerHandshake(bcast::Handshake {
                        version: *version,
                        channels: channels.clone(),
                        peer_id: peer_id.clone(),
                    })),
                };
                let mut out = Vec::with_capacity(bcast.encoded_len());
                bcast.encode(&mut out).unwrap();
                out
            }
            Self::SessOpen {
                channel,
                message_id,
                preamble_hex,
                initial_update_hex,
            } => {
                let sess = Sess {
                    frame: Some(sess::Frame::SessionOpen(sess::Open {
                        channel: channel.clone(),
                        message_id: message_id.clone(),
                        preamble: hex::decode(preamble_hex).expect("preamble_hex is valid hex"),
                        initial_update: hex::decode(initial_update_hex)
                            .expect("initial_update_hex is valid hex"),
                    })),
                };
                let mut out = Vec::with_capacity(sess.encoded_len());
                sess.encode(&mut out).unwrap();
                out
            }
            Self::ChunkHeader {
                channel,
                message_id,
                chunk_id_hex,
                data_length,
            } => {
                let header = chunk::Header {
                    channel: channel.clone(),
                    message_id: message_id.clone(),
                    chunk_id: hex::decode(chunk_id_hex).expect("chunk_id_hex is valid hex"),
                    data_length: *data_length,
                };
                let mut out = Vec::with_capacity(header.encoded_len());
                header.encode(&mut out).unwrap();
                out
            }
        }
    }
}

fn corpus_dir() -> PathBuf {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    crate_dir
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .join("conformance/corpus/codec")
}

#[test]
fn corpus_byte_equality() {
    let regen = std::env::var("ETHP2P_REGEN_CODEC_CORPUS").is_ok();
    let dir = corpus_dir();

    let mut yaml_paths: Vec<_> = std::fs::read_dir(&dir)
        .expect("corpus dir exists")
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "yaml"))
        .collect();
    yaml_paths.sort();

    assert!(!yaml_paths.is_empty(), "corpus is empty");

    let mut failures = Vec::new();
    for yaml in &yaml_paths {
        let entry: CorpusEntry =
            serde_yaml::from_str(&std::fs::read_to_string(yaml).unwrap()).unwrap();
        let encoded = entry.encode();
        let hex_path = yaml.with_extension("bytes.hex");

        if regen {
            std::fs::write(&hex_path, format!("{}\n", hex::encode(&encoded))).unwrap();
            println!("regenerated {}", hex_path.display());
            continue;
        }

        let expected_hex = std::fs::read_to_string(&hex_path)
            .unwrap_or_else(|e| panic!("missing hex anchor at {}: {e}", hex_path.display()));
        let expected = hex::decode(expected_hex.trim())
            .unwrap_or_else(|e| panic!("invalid hex in {}: {e}", hex_path.display()));

        if encoded != expected {
            failures.push(format!(
                "{}: got {} bytes, expected {} bytes\n  got     = {}\n  expected = {}",
                yaml.file_name().unwrap().to_string_lossy(),
                encoded.len(),
                expected.len(),
                hex::encode(&encoded),
                hex::encode(&expected),
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "conformance mismatch:\n{}",
        failures.join("\n")
    );
}
