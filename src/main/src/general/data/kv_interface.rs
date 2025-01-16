use crate::{
    general::network::proto,
    result::WSResult,
    sys::{LogicalModule, NodeID},
};
use async_trait::async_trait;

pub struct KvOptions {
    spec_node: Option<NodeID>,
}

impl KvOptions {
    pub fn new() -> KvOptions {
        KvOptions { spec_node: None }
    }

    pub fn spec_node(&self) -> Option<NodeID> {
        self.spec_node
    }

    pub fn with_spec_node(mut self, node: NodeID) -> KvOptions {
        self.spec_node = Some(node);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KvOps {
    Get,
    Set,
    Delete,
}

#[async_trait]
pub trait KvInterface: LogicalModule {
    async fn call(
        &self,
        kv: proto::kv::KvRequests,
        opt: KvOptions,
    ) -> WSResult<proto::kv::KvResponses>;
}
