pub mod wasm;

use async_trait::async_trait;

use super::{FnExeCtx, InstanceTrait, OwnedInstance};
use crate::result::WSResult;

#[async_trait]
impl InstanceTrait for OwnedInstance {
    fn instance_name(&self) -> String {
        match self {
            OwnedInstance::WasmInstance(v) => v.instance_name(),
        }
    }
    async fn execute(&self, fn_ctx: &mut FnExeCtx) -> WSResult<Option<String>> {
        match self {
            OwnedInstance::WasmInstance(v) => v.execute(fn_ctx).await,
        }
    }
}
