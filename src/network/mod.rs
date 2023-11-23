pub mod msg_pack;
pub mod p2p;
pub mod p2p_quic;

pub mod proto {
    pub mod kv {
        include!(concat!(env!("OUT_DIR"), "/kv.rs"));
    }
    pub mod raft {
        include!(concat!(env!("OUT_DIR"), "/raft.rs"));
    }

    pub mod sche {
        include!(concat!(env!("OUT_DIR"), "/sche.rs"));
    }
}
