use crate::{
    network::proto::kv::{kv_response::KvPairOpt, KeyRange, KvPair},
    result::WSResult,
    sys::LogicalModule,
};
use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};

pub struct SetOptions {}

#[async_trait]
pub trait KVInterface: LogicalModule {
    async fn get(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>>;
    async fn set(&self, kvs: Vec<KvPair>, opts: SetOptions) -> WSResult<Vec<KvPairOpt>>;
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
