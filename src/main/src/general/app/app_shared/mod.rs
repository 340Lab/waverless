pub mod java;
pub mod process;
pub mod process_instance_man_related;
pub mod process_rpc;

use crate::general::app::instance::InstanceTrait;
use crate::general::app::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use async_trait::async_trait;

use super::instance::m_instance_manager::InstanceManager;

pub struct SharedInstance(pub process::ProcessInstance);

impl From<process::ProcessInstance> for SharedInstance {
    fn from(v: process::ProcessInstance) -> Self {
        Self(v)
    }
}

#[async_trait]
impl InstanceTrait for SharedInstance {
    fn instance_name(&self) -> String {
        self.0.instance_name()
    }
    async fn execute(
        &self,
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> crate::result::WSResult<Option<String>> {
        self.0.execute(instman, fn_ctx).await
    }
    fn execute_sync(
        &self,
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxSync,
    ) -> crate::result::WSResult<Option<String>> {
        self.0.execute_sync(instman, fn_ctx)
    }
}
