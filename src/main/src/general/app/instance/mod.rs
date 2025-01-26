pub mod m_instance_manager;

use super::app_native::NativeAppInstance;
use super::app_shared::SharedInstance;
use super::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use crate::general::app::app_owned::wasm::WasmInstance;
use crate::general::app::app_shared::process::ProcessInstance;
use crate::result::WSResult;
use async_trait::async_trait;
use enum_as_inner::EnumAsInner;

#[derive(EnumAsInner)]
pub enum OwnedInstance {
    WasmInstance(WasmInstance),
}

pub enum Instance {
    Owned(OwnedInstance),
    Shared(SharedInstance),
    Native(NativeAppInstance),
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
            Instance::Native(v) => v.instance_name(),
        }
    }
    async fn execute(&self, fn_ctx: &mut FnExeCtxAsync) -> WSResult<Option<String>> {
        match self {
            Instance::Owned(v) => v.execute(fn_ctx).await,
            Instance::Shared(v) => v.execute(fn_ctx).await,
            Instance::Native(v) => v.execute(fn_ctx).await,
        }
    }

    fn execute_sync(&self, fn_ctx: &mut FnExeCtxSync) -> WSResult<Option<String>> {
        match self {
            Instance::Owned(v) => v.execute_sync(fn_ctx),
            Instance::Shared(v) => v.execute_sync(fn_ctx),
            Instance::Native(v) => v.execute_sync(fn_ctx),
        }
    }
}

pub enum NewJavaInstanceConfig {}

#[async_trait]
pub trait InstanceTrait {
    fn instance_name(&self) -> String;
    async fn execute(&self, fn_ctx: &mut FnExeCtxAsync) -> WSResult<Option<String>>;
    fn execute_sync(&self, fn_ctx: &mut FnExeCtxSync) -> WSResult<Option<String>>;
}
