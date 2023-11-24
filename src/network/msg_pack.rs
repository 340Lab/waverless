use downcast_rs::{impl_downcast, Downcast};

use super::{p2p::MsgId, proto};

// pub struct MsgCoder<M: prost::Message> {}

pub trait MsgPack: prost::Message + Downcast {
    fn msg_id(&self) -> MsgId;
}

impl_downcast!(MsgPack);

impl MsgPack for proto::raft::VoteRequest {
    fn msg_id(&self) -> MsgId {
        0
    }
}

impl MsgPack for proto::raft::VoteResponse {
    fn msg_id(&self) -> MsgId {
        1
    }
}

impl MsgPack for proto::raft::AppendEntriesRequest {
    fn msg_id(&self) -> MsgId {
        2
    }
}

impl MsgPack for proto::raft::AppendEntriesResponse {
    fn msg_id(&self) -> MsgId {
        3
    }
}

impl MsgPack for proto::kv::MetaKvRequest {
    fn msg_id(&self) -> MsgId {
        4
    }
}

impl MsgPack for proto::kv::MetaKvResponse {
    fn msg_id(&self) -> MsgId {
        5
    }
}

impl MsgPack for proto::sche::UserRequest {
    fn msg_id(&self) -> MsgId {
        6
    }
}

impl MsgPack for proto::sche::UserResponse {
    fn msg_id(&self) -> MsgId {
        7
    }
}

impl MsgPack for proto::metric::RscMetric {
    fn msg_id(&self) -> MsgId {
        8
    }
}

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

impl RPCReq for proto::sche::UserRequest {
    type Resp = proto::sche::UserResponse;
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
