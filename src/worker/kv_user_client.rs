use std::time::Duration;

use crate::{
    general::{
        kv_interface::{KvInterface, KvOptions},
        network::{
            p2p::RPCCaller,
            proto::kv::{KvRequests, KvResponses},
        },
    },
    result::WSResult,
    sys::{KvUserClientView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
// use crossbeam_skiplist::SkipMap;

use ws_derive::LogicalModule;

// use super::super::kv_interface::KvInterface;

#[derive(LogicalModule)]
pub struct KvUserClient {
    // testmap: SkipMap<Vec<u8>, Vec<u8>>,
    pub view: KvUserClientView,
    rpc_caller_kv: RPCCaller<KvRequests>,
}

#[async_trait]
impl LogicalModule for KvUserClient {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        unsafe {
            *(&*KV_USER_CLIENT as *const Option<KvUserClientView>
                as *mut Option<KvUserClientView>) =
                Some(KvUserClientView::new(args.logical_modules_ref.clone()));
        }
        Self {
            // testmap: SkipMap::new(),
            view: KvUserClientView::new(args.logical_modules_ref.clone()),
            rpc_caller_kv: RPCCaller::default(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        self.rpc_caller_kv.regist(self.view.p2p());

        let all = vec![];

        Ok(all)
    }
}

// #[async_trait]
// impl KvInterface for RaftKvNode {
//     async fn get(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {
//         // get data position from master
//         // get data from position
//     }
//     async fn set(&self, kvs: Vec<KvPair>, _opts: SetOptions) -> WSResult<Vec<KvPairOpt>> {
//         // get which function calling this, decide middle data position by the consumer function postion.
//     }
// }

lazy_static::lazy_static! {
    static ref KV_USER_CLIENT: Option<KvUserClientView>=None;
    // static ref RECENT_Kv_CACHE: Cache<i32, Vec<u8>>=Cache::<i32, Vec<u8>>::builder()
    //     .time_to_live(Duration::from_secs(10))
    //     .weigher(|_key, value| -> u32 { value.len().try_into().unwrap_or(u32::MAX) })
    //     // This cache will hold up to 32MiB of values.
    //     .max_capacity(32 * 1024 * 1024)
    //     .build();
    // static ref NEXT_CACHE_ID: AtomicI32=AtomicI32::new(0);
}

pub fn kv_user_client() -> &'static KvUserClient {
    let res = &*KV_USER_CLIENT as *const Option<KvUserClientView> as *mut Option<KvUserClientView>;
    unsafe { (*res).as_ref().unwrap().kv_user_client() }
}

#[async_trait]
impl KvInterface for KvUserClient {
    async fn call(&self, req: KvRequests, opt: KvOptions) -> WSResult<KvResponses> {
        if let Some(node_id) = opt.spec_node() {
            self.rpc_caller_kv
                .call(
                    self.view.p2p(),
                    node_id,
                    req,
                    Some(Duration::from_secs(60 * 30)),
                )
                .await
        } else {
            // 1. dicide placement position
            // 2. send data to the position
            self.rpc_caller_kv
                .call(
                    self.view.p2p(),
                    self.view.p2p().nodes_config.get_master_node(),
                    req,
                    Some(Duration::from_secs(60 * 30)),
                )
                .await
        }
    }
}

impl KvUserClient {
    // pub async fn get_by_app_fn(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {}
    // pub async fn set_by_app_fn(
    //     &self,
    //     mut kvs: Vec<KvPair>,
    //     _opts: KvOptions,
    //     fn_ctx: &FunctionCtx,
    // ) -> WSResult<Vec<KvPairOpt>> {
    //     tracing::info!("app called kv set");
    //     let app_meta_man = self.view.instance_manager().app_meta_manager.read().await;

    //     // make sure each key set is valid
    //     let patterns = {
    //         let appmeta = app_meta_man.get_app_meta(&fn_ctx.app).unwrap();
    //         let fnmeta = appmeta.get_fn_meta(&fn_ctx.func).unwrap();

    //         let mut patterns = vec![];
    //         for kv in kvs.iter() {
    //             if let Some(pattern) = fnmeta.match_key(&kv.key, KvOps::Set) {
    //                 patterns.push(pattern);
    //             } else {
    //                 // no pattern matched
    //                 return Err(WsPermissionErr::AccessKeyPermissionDenied {
    //                     app: fn_ctx.app.clone(),
    //                     func: fn_ctx.func.clone(),
    //                     access_key: TryUtf8VecU8(kv.key.clone()),
    //                 }
    //                 .into());
    //             }
    //         }
    //         patterns
    //     };

    //     // TODO: handle errors
    //     for (kv, pattern) in kvs.iter_mut().zip(&patterns) {
    //         tracing::info!("app called kv set really");
    //         // one pattern may trigger multiple functions
    //         kv_event::check_and_handle_event(
    //             fn_ctx,
    //             pattern,
    //             self,
    //             &*app_meta_man,
    //             KvOps::Set,
    //             std::mem::take(kv),
    //         )
    //         .await;
    //     }

    //     Ok(vec![])
    //     // // common plan
    //     // self.set(kvs, SetOptions::new()).await
    // }
}
