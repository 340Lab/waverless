pub mod wasm;
pub mod wasm_host_funcs;

use crate::general::app::instance::InstanceTrait;
use crate::general::app::instance::OwnedInstance;
use crate::general::app::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use crate::result::WSResult;
use async_trait::async_trait;

use super::instance::m_instance_manager::InstanceManager;

#[async_trait]
impl InstanceTrait for OwnedInstance {
    fn instance_name(&self) -> String {
        match self {
            OwnedInstance::WasmInstance(v) => v.instance_name(),
        }
    }
    async fn execute(
        &self,
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> WSResult<Option<String>> {
        match self {
            OwnedInstance::WasmInstance(v) => v.execute(instman, fn_ctx).await,
        }
    }

    fn execute_sync(
        &self,
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxSync,
    ) -> WSResult<Option<String>> {
        match self {
            OwnedInstance::WasmInstance(v) => v.execute_sync(instman, fn_ctx),
        }
    }
}
