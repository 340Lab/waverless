use crate::{result::WSResult, sys::LogicalModule};
use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};

use super::network::proto;

pub struct SetOptions {}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvOps {
    Get,
    Set,
    Delete,
}

#[async_trait]
pub trait KVInterface: LogicalModule {
    async fn get(&self, key_range: proto::kv::KeyRange) -> WSResult<Vec<proto::kv::KvPair>>;
    async fn set(
        &self,
        kvs: Vec<proto::kv::KvPair>,
        opts: SetOptions,
    ) -> WSResult<Vec<proto::kv::kv_response::KvPairOpt>>;
}

impl SetOptions {
    pub fn new() -> SetOptions {
        SetOptions {}
    }
}

#[async_trait]
pub trait KVNode: KVInterface + Downcast {
    async fn ready(&self) -> bool;
}
impl_downcast!(KVNode);
