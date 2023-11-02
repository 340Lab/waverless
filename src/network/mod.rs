pub mod p2p;
pub mod p2p_client;
pub mod p2p_quic;
pub mod serial;
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/kv.rs"));
}
