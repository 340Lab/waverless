pub mod wasm;
pub mod wasm_host_funcs;

use crate::general::app::instance::InstanceTrait;
use crate::general::app::instance::OwnedInstance;
use crate::general::app::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use crate::result::{WSResult, WsFuncError};
use async_trait::async_trait;

#[async_trait]
impl InstanceTrait for OwnedInstance {
    fn instance_name(&self) -> String {
        match self {
            OwnedInstance::WasmInstance(v) => v.instance_name(),
        }
    }
    async fn execute(&self, fn_ctx: &mut FnExeCtxAsync) -> WSResult<Option<String>> {
        match self {
            OwnedInstance::WasmInstance(v) => v.execute(fn_ctx).await,
        }
    }

    fn execute_sync(&self, fn_ctx: &mut FnExeCtxSync) -> WSResult<Option<String>> {
        match self {
            OwnedInstance::WasmInstance(v) => v.execute_sync(fn_ctx),
        }
    }
}
