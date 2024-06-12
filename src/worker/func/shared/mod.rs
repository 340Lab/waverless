use async_trait::async_trait;

use super::InstanceTrait;

pub mod java;
pub mod process;
pub mod process_instance_man_related;
pub mod process_rpc;

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
        fn_ctx: &mut crate::worker::func::FnExeCtx,
    ) -> crate::result::WSResult<Option<String>> {
        self.0.execute(fn_ctx).await
    }
}
