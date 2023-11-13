use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};

use super::kv_client::KVClient;

pub struct SetOptions {
    pub consistent: bool,
}

impl SetOptions {
    pub fn new() -> SetOptions {
        SetOptions { consistent: false }
    }
    pub fn set_consistent(mut self, consistent: bool) -> Self {
        self.consistent = consistent;
        self
    }
}

#[async_trait]
pub trait KVNode: KVClient + Downcast {
    async fn ready(&self) -> bool;
}
impl_downcast!(KVNode);
