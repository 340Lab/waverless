use super::proto;
use crate::{kv::KeyRange, sys::NodeID};

// pub struct MsgCoder<M: prost::Message> {}

pub trait MsgPack: prost::Message + Default {
    fn msg_id() -> u32;
}

// impl MsgId for raft::prelude::Message {
//     fn msg_id(&self) -> u32 {
//         0
//     }
// }
impl MsgPack for raft::prelude::Message {
    fn msg_id() -> u32 {
        0
    }
}
