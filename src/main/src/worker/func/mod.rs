pub mod m_instance_manager;
pub mod owned;
pub mod shared;
pub mod wasm_host_funcs;

use crate::{
    general::{
        m_appmeta_manager::{AppType, FnMeta},
        network::http_handler::ReqId,
    },
    result::WSResult,
};
use async_trait::async_trait;
use enum_as_inner::EnumAsInner;
use tokio::task::JoinHandle;

use self::{
    owned::wasm::WasmInstance,
    shared::{process::ProcessInstance, SharedInstance},
};

#[derive(EnumAsInner)]
pub enum OwnedInstance {
    WasmInstance(WasmInstance),
}

pub enum Instance {
    Owned(OwnedInstance),
    Shared(SharedInstance),
}
impl From<OwnedInstance> for Instance {
    fn from(v: OwnedInstance) -> Self {
        Self::Owned(v)
    }
}

impl From<SharedInstance> for Instance {
    fn from(v: SharedInstance) -> Self {
        Self::Shared(v)
    }
}

impl From<ProcessInstance> for Instance {
    fn from(v: ProcessInstance) -> Self {
        Self::Shared(SharedInstance(v))
    }
}

#[async_trait]
impl InstanceTrait for Instance {
    fn instance_name(&self) -> String {
        match self {
            Instance::Owned(v) => v.instance_name(),
            Instance::Shared(v) => v.instance_name(),
        }
    }
    async fn execute(&self, fn_ctx: &mut FnExeCtx) -> WSResult<Option<String>> {
        match self {
            Instance::Owned(v) => v.execute(fn_ctx).await,
            Instance::Shared(v) => v.execute(fn_ctx).await,
        }
    }
}

pub enum NewJavaInstanceConfig {}

#[async_trait]
pub trait InstanceTrait {
    fn instance_name(&self) -> String;
    async fn execute(&self, fn_ctx: &mut FnExeCtx) -> WSResult<Option<String>>;
}

#[derive(Clone, Debug)]
pub enum EventCtx {
    Http(String),
    KvSet { key: Vec<u8>, opeid: Option<u32> },
}

impl EventCtx {
    pub fn take_prev_kv_opeid(&mut self) -> Option<u32> {
        match self {
            EventCtx::KvSet { opeid, .. } => opeid.take(),
            _ => None,
        }
    }
}

pub struct FnExeCtx {
    pub app: String,
    pub app_type: AppType,
    pub func: String,
    pub func_meta: FnMeta,
    pub req_id: ReqId,
    pub event_ctx: EventCtx,
    pub res: Option<String>,
    /// remote scheduling tasks
    pub sub_waiters: Vec<JoinHandle<()>>, // pub trigger_node: NodeID,
}

impl FnExeCtx {
    pub fn empty_http(&self) -> bool {
        match &self.event_ctx {
            EventCtx::Http(str) => str.len() == 0,
            _ => false,
        }
    }
    /// call this when you are sure it's a http event
    pub fn http_str_unwrap(&self) -> String {
        match &self.event_ctx {
            EventCtx::Http(str) => str.to_owned(),
            _ => panic!("not a http event"),
        }
    }
}
