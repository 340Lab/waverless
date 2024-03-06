use downcast_rs::{impl_downcast, Downcast};

use super::{
    m_p2p::MsgId,
    proto::{self, kv::KvResponse},
};

macro_rules! count_modules {
    ($module:ty) => {1u32};
    ($module:ty,$($modules:ty),+) => {1u32 + count_modules!($($modules),+)};
}

// 定义宏，用于生成 MsgPack trait 的实现
macro_rules! define_msg_ids {
    ($module:ty) => {
        impl MsgPack for $module {
            fn msg_id(&self) -> MsgId {
                0
            }
        }
    };
    ($module:ty,$($modules:ty),+) => {
        impl MsgPack for $module {
            fn msg_id(&self) -> MsgId {
                count_modules!($($modules),+)
            }
        }
        define_msg_ids!($($modules),+);
    };
    // ($($module:ty),+) => {
    //     $(
    //         impl MsgPack for $module {
    //             fn msg_id(&self) -> MsgId {
    //                 count_modules!($module)
    //             }
    //         }
    //     )*
    // };
}

// pub struct MsgCoder<M: prost::Message> {}

pub trait MsgPack: prost::Message + Downcast {
    fn msg_id(&self) -> MsgId;
    // fn construct_from_raw_mem(bytes: Bytes) {}
}

impl_downcast!(MsgPack);

define_msg_ids!(
    proto::raft::VoteRequest,
    proto::raft::VoteResponse,
    proto::raft::AppendEntriesRequest,
    proto::raft::AppendEntriesResponse,
    proto::sche::DistributeTaskReq,
    proto::sche::DistributeTaskResp,
    proto::metric::RscMetric,
    proto::kv::KvRequests,
    proto::kv::KvResponses
);

pub trait RPCReq: MsgPack + Default {
    type Resp: MsgPack + Default;
}

impl RPCReq for proto::raft::VoteRequest {
    type Resp = proto::raft::VoteResponse;
}

impl RPCReq for proto::raft::AppendEntriesRequest {
    type Resp = proto::raft::AppendEntriesResponse;
}

impl RPCReq for proto::sche::DistributeTaskReq {
    type Resp = proto::sche::DistributeTaskResp;
}

impl RPCReq for proto::kv::KvRequests {
    type Resp = proto::kv::KvResponses;
}

pub trait KvResponseExt {
    fn new_lock(lock_id: u32) -> KvResponse;
    fn new_common(kvs: Vec<proto::kv::KvPair>) -> KvResponse;
    fn lock_id(&self) -> Option<u32>;
    fn common_kvs(&self) -> Option<&Vec<proto::kv::KvPair>>;
}

impl KvResponseExt for KvResponse {
    fn new_common(kvs: Vec<proto::kv::KvPair>) -> KvResponse {
        KvResponse {
            resp: Some(proto::kv::kv_response::Resp::CommonResp(
                proto::kv::kv_response::KvResponse { kvs },
            )),
        }
    }
    fn new_lock(lock_id: u32) -> KvResponse {
        KvResponse {
            resp: Some(proto::kv::kv_response::Resp::LockId(lock_id)),
        }
    }
    fn lock_id(&self) -> Option<u32> {
        match self.resp.as_ref().unwrap() {
            proto::kv::kv_response::Resp::CommonResp(_) => None,
            proto::kv::kv_response::Resp::LockId(id) => Some(*id),
        }
    }
    fn common_kvs(&self) -> Option<&Vec<proto::kv::KvPair>> {
        match self.resp.as_ref().unwrap() {
            proto::kv::kv_response::Resp::CommonResp(resp) => Some(&resp.kvs),
            proto::kv::kv_response::Resp::LockId(_) => None,
        }
    }
}

// impl MsgId for raft::prelude::Message {
//     fn msg_id(&self) -> u32 {
//         0
//     }
// }
// impl MsgPack for raft::prelude::Message {
//     fn msg_id() -> u32 {
//         0
//     }
// }
