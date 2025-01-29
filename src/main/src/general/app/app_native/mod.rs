pub mod app_checkpoint;

use std::collections::HashMap;

use super::{
    AffinityPattern, AffinityRule, AppMeta, AppType, DataAccess, DataEventTrigger, FnMeta,
    KeyPattern, NodeTag,
};
use crate::general::app::instance::{Instance, InstanceTrait};
use crate::general::app::m_executor::{FnExeCtxAsync, FnExeCtxSync};
use crate::general::data::m_data_general::DATA_UID_PREFIX_APP_META;
use crate::new_map;
use crate::result::{WSResult, WsFuncError};
use async_trait::async_trait;

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
    async fn execute(&self, _fn_ctx: &mut FnExeCtxAsync) -> WSResult<Option<String>> {
        // Native apps don't support async execution
        Err(WsFuncError::UnsupportedAppType.into())
    }

    fn execute_sync(&self, _fn_ctx: &mut FnExeCtxSync) -> WSResult<Option<String>> {
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
