pub mod app_checkpoint;

use std::collections::HashMap;

use super::instance::m_instance_manager::InstanceManager;
use super::{
    AffinityPattern, AffinityRule, AppMeta, AppType, DataAccess, DataEventTrigger, FnMeta,
    KeyPattern, NodeTag,
};
use crate::general::app::instance::{Instance, InstanceTrait};
use crate::general::app::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use crate::general::data::m_data_general::DATA_UID_PREFIX_APP_META;
use crate::new_map;
use crate::result::{WSResult, WsFuncError};
use app_checkpoint::FunctionAppCheckpoint;
use async_trait::async_trait;

pub trait NativeAppFunc: Sized {
    async fn execute(
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> WSResult<Option<String>>;
}

pub struct NativeAppInstance {
    _dummy_private: (), // avoid empty struct
}

impl NativeAppInstance {
    pub fn new() -> Self {
        Self { _dummy_private: () }
    }
}

#[async_trait]
impl InstanceTrait for NativeAppInstance {
    // don't need instance name
    fn instance_name(&self) -> String {
        "native_app_dummy_instance".to_string()
    }
    async fn execute(
        &self,
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> WSResult<Option<String>> {
        // Native apps don't support async execution
        // Err(WsFuncError::UnsupportedAppType.into())
        let res = match fn_ctx.app_name() {
            "app_checkpoint" => match fn_ctx.func_name() {
                // "checkpointable" => app_checkpoint::checkpointable(fn_ctx),
                "checkpoint" => Some(FunctionAppCheckpoint::execute(instman, fn_ctx).await?),
                _ => None,
            },
            _ => {
                return Err(WsFuncError::AppNotFound {
                    app: fn_ctx.app_name().to_string(),
                }
                .into())
            }
        };

        let Some(res) = res else {
            return Err(WsFuncError::FuncNotFound {
                app: fn_ctx.app_name().to_string(),
                func: fn_ctx.func_name().to_string(),
            }
            .into());
        };

        Ok(res)
    }

    fn execute_sync(
        &self,
        _instman: &InstanceManager,
        _fn_ctx: &mut FnExeCtxSync,
    ) -> WSResult<Option<String>> {
        // For now, just return None as native apps don't produce results
        todo!()
        // Ok(None)
    }
}

impl From<NativeAppInstance> for Instance {
    fn from(v: NativeAppInstance) -> Self {
        Self::Native(v)
    }
}

pub fn native_apps() -> HashMap<String, AppMeta> {
    let mut nativeapps = HashMap::new();
    // https://fvd360f8oos.feishu.cn/wiki/GGUnw0H1diVoHSkgm3vcMhtbnjI
    // app_checkpoint:
    //     checkpointable:
    //         inner_dataset:
    //         app_{}:
    //         - get
    //     checkpoint:
    //         inner_dataset:
    //         app_{}:
    //         - trigger_by_write:
    //             condition: checkpointable
    //         - get
    let _ = nativeapps.insert(
        "app_checkpoint".to_string(),
        AppMeta::new(
            AppType::Native,
            new_map!(HashMap {
                "checkpointable".to_string() => FnMeta {
                    sync_async: super::FnSyncAsyncSupport::Sync,
                    calls: vec![],
                    data_accesses: Some(new_map!(HashMap {
                        KeyPattern(DATA_UID_PREFIX_APP_META.to_string()) => DataAccess {
                            get: true,
                            set: false,
                            delete: false,
                            event: None,
                        }
                    })),
                    affinity: Some(AffinityRule {
                        tags: vec![NodeTag::Worker],
                        nodes: AffinityPattern::All,
                    }),
                },
                "checkpoint".to_string() => FnMeta {
                    sync_async: super::FnSyncAsyncSupport::Async,
                    calls: vec![],
                    data_accesses: Some(new_map!(HashMap {
                        KeyPattern(DATA_UID_PREFIX_APP_META.to_string()) => DataAccess {
                            get: true,
                            set: false,
                            delete: false,
                            event: Some(DataEventTrigger::WriteWithCondition {
                                condition: "checkpointable".to_string(),
                            }),
                        }
                    })),
                    affinity: Some(AffinityRule {
                        tags: vec![NodeTag::Worker],
                        nodes: AffinityPattern::All,
                    }),
                },
            }),
        ),
    );

    nativeapps
}
