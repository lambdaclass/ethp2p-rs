use ethp2p_protocol::pb::{Protocol, Selector};
use prost::Message;

#[test]
fn selector_encodes_non_empty() {
    let selector = Selector {
        protocol: Protocol::Bcast as i32,
    };
    let mut buf = Vec::new();
    selector.encode(&mut buf).unwrap();
    assert!(!buf.is_empty(), "encoded selector must not be empty");
}

#[test]
fn protocol_enum_variants_match_proto_definition() {
    assert_eq!(Protocol::Unspecified as i32, 0);
    assert_eq!(Protocol::Bcast as i32, 1);
    assert_eq!(Protocol::Sess as i32, 2);
    assert_eq!(Protocol::Chunk as i32, 3);
}

#[test]
fn selector_roundtrip() {
    for proto in [
        Protocol::Unspecified,
        Protocol::Bcast,
        Protocol::Sess,
        Protocol::Chunk,
    ] {
        let original = Selector {
            protocol: proto as i32,
        };
        let mut buf = Vec::new();
        original.encode(&mut buf).unwrap();
        let decoded = Selector::decode(buf.as_slice()).unwrap();
        assert_eq!(decoded, original);
    }
}
