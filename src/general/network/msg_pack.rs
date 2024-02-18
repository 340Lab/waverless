use downcast_rs::{impl_downcast, Downcast};

use super::{p2p::MsgId, proto};

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
    proto::kv::MetaKvRequest,
    proto::kv::MetaKvResponse,
    proto::sche::MakeSchePlanReq,
    proto::sche::MakeSchePlanResp,
    proto::sche::DistributeTaskReq,
    proto::sche::DistributeTaskResp,
    proto::metric::RscMetric
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

impl RPCReq for proto::kv::MetaKvRequest {
    type Resp = proto::kv::MetaKvResponse;
}

impl RPCReq for proto::sche::MakeSchePlanReq {
    type Resp = proto::sche::MakeSchePlanResp;
}

impl RPCReq for proto::sche::DistributeTaskReq {
    type Resp = proto::sche::DistributeTaskResp;
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
