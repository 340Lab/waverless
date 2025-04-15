pub mod http_handler;
pub mod m_p2p;
pub mod m_p2p_quic;
pub mod msg_pack;
pub mod rpc_model;

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

    pub mod metric {
        include!(concat!(env!("OUT_DIR"), "/metric.rs"));
    }
    pub mod remote_sys {
        include!(concat!(env!("OUT_DIR"), "/remote_sys.rs"));
    }
    include!(concat!(env!("OUT_DIR"), "/data.rs"));
}
