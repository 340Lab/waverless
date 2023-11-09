

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

pub trait RPCReq: MsgPack {
    type Resp: MsgPack;
}

impl RPCReq for proto::raft::VoteRequest {
    type Resp = proto::raft::VoteResponse;
}

impl RPCReq for proto::raft::AppendEntriesRequest {
    type Resp = proto::raft::AppendEntriesResponse;
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
