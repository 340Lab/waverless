pub mod java;
pub mod process;
pub mod process_instance_man_related;
pub mod process_rpc;

use crate::general::app::instance::InstanceTrait;
use crate::general::app::m_executor::FnExeCtx;
use async_trait::async_trait;

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
    async fn execute(&self, fn_ctx: &mut FnExeCtx) -> crate::result::WSResult<Option<String>> {
        self.0.execute(fn_ctx).await
    }
}
