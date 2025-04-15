
use downcast_rs::{impl_downcast, Downcast};

use super::{
    m_p2p::MsgId,
    proto::{self},
};

macro_rules! count_modules {
    ($module:ty) => {1u32};
    ($module:ty,$($modules:ty),+) => {1u32 + count_modules!($($modules),+)};
}

// 定义宏，用于生成 MsgPack trait 的实现
macro_rules! define_msg_ids {
    (($module:ty,$arg:ident,$verify:block)) => {
        impl MsgPack for $module {
            fn msg_id(&self) -> MsgId {
                0
            }
            fn verify(&self)->bool{
                let $arg=self;
                $verify
            }
        }
    };
    (($module:ty,$arg:ident,$verify:block),$(($modules:ty,$args:ident,$verifies:block)),+) => {
        impl MsgPack for $module {
            fn msg_id(&self) -> MsgId {
                count_modules!($($modules),+)
            }
            fn verify(&self)->bool{
                let $arg=self;
                $verify
            }
        }
        define_msg_ids!($(($modules,$args,$verifies)),+);
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
    fn verify(&self) -> bool;
}

impl_downcast!(MsgPack);

define_msg_ids!(
    (proto::raft::VoteRequest, _pack, { true }),
    (proto::raft::VoteResponse, _pack, { true }),
    (proto::raft::AppendEntriesRequest, _pack, { true }),
    (proto::raft::AppendEntriesResponse, _pack, { true }),
    (proto::sche::DistributeTaskReq, _pack, { true }),
    (proto::sche::DistributeTaskResp, _pack, { true }),
    (proto::metric::RscMetric, _pack, { true }),
    (proto::kv::KvRequests, pack, {
        for r in &pack.requests {
            let r: &proto::kv::KvRequest = r;
            let Some(op) = r.op.as_ref() else {
                return false;
            };
            match op {
                proto::kv::kv_request::Op::Set(kv_put_request) => {
                    if kv_put_request.kv.is_none() {
                        return false;
                    }
                }
                proto::kv::kv_request::Op::Get(kv_get_request) => {
                    if kv_get_request.range.is_none() {
                        return false;
                    }
                }
                proto::kv::kv_request::Op::Delete(kv_delete_request) => {
                    if kv_delete_request.range.is_none() {
                        return false;
                    }
                }
                proto::kv::kv_request::Op::Lock(kv_lock_request) => {
                    if kv_lock_request.range.is_none() {
                        return false;
                    }
                }
            }
        }
        true
    }),
    (proto::kv::KvResponses, _pack, { true }),
    (proto::remote_sys::GetDirContentReq, _pack, { true }),
    (proto::remote_sys::GetDirContentResp, _pack, { true }),
    (proto::remote_sys::RunCmdReq, _pack, { true }),
    (proto::remote_sys::RunCmdResp, _pack, { true }),
    (proto::DataVersionScheduleRequest, pack, {
        pack.context.is_some()
    }),
    (proto::DataVersionScheduleResponse, pack, {
        pack.cache_plan.is_some()
    }),
    (proto::WriteOneDataRequest, _pack, { true }),
    (proto::WriteOneDataResponse, _pack, { true }),
    (proto::DataMetaUpdateRequest, _pack, { true }),
    (proto::DataMetaUpdateResponse, _pack, { true }),
    (proto::DataMetaGetRequest, _pack, { true }),
    (proto::DataMetaGetResponse, _pack, { true }),
    (proto::GetOneDataRequest, _pack, { true }),
    (proto::GetOneDataResponse, _pack, { true }),
    (proto::kv::KvLockRequest, pack, {
        match pack.read_0_write_1_unlock_2 {
            0 | 1 | 2 => true,
            _ => false,
        }
    }),
    (proto::kv::KvLockResponse, _pack, { true }) // (proto::kv::KvLockWaitAcquireNotifyRequest, _pack, { true }),
                                                 // (proto::kv::KvLockWaitAcquireNotifyResponse, _pack, { true })
                                                 // (proto::DataDeleteRequest, _pack, { true }),
                                                 // (proto::DataDeleteResponse, _pack, { true })
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

impl RPCReq for proto::remote_sys::GetDirContentReq {
    type Resp = proto::remote_sys::GetDirContentResp;
}

impl RPCReq for proto::remote_sys::RunCmdReq {
    type Resp = proto::remote_sys::RunCmdResp;
}

impl RPCReq for proto::DataVersionScheduleRequest {
    type Resp = proto::DataVersionScheduleResponse;
}

impl RPCReq for proto::WriteOneDataRequest {
    type Resp = proto::WriteOneDataResponse;
}

impl RPCReq for proto::DataMetaUpdateRequest {
    type Resp = proto::DataMetaUpdateResponse;
}

impl RPCReq for proto::DataMetaGetRequest {
    type Resp = proto::DataMetaGetResponse;
}

impl RPCReq for proto::GetOneDataRequest {
    type Resp = proto::GetOneDataResponse;
}

impl RPCReq for proto::kv::KvLockRequest {
    type Resp = proto::kv::KvLockResponse;
}

// impl RPCReq for proto::kv::KvLockWaitAcquireNotifyRequest {
//     type Resp = proto::kv::KvLockWaitAcquireNotifyResponse;
// }
// impl RPCReq for proto::DataDeleteRequest {
//     type Resp = proto::DataDeleteResponse;
// }

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
