use crate::{
    general::{
        kv_interface::{KVInterface, KvOps, SetOptions},
        network::proto::kv::{kv_response::KvPairOpt, KeyRange, KvPair},
    },
    result::{WSResult, WsPermissionErr},
    sys::{KvUserClientView, LogicalModule, LogicalModuleNewArgs},
    util::{JoinHandleWrapper, TryUtf8VecU8},
    worker::executor::FunctionCtx,
};
use async_trait::async_trait;
// use crossbeam_skiplist::SkipMap;

use ws_derive::LogicalModule;

use super::function_event::kv_event;

// use super::super::kv_interface::KVInterface;

#[derive(LogicalModule)]
pub struct KvUserClient {
    // testmap: SkipMap<Vec<u8>, Vec<u8>>,
    pub view: KvUserClientView,
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
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        Ok(all)
    }
}

// #[async_trait]
// impl KVInterface for RaftKVNode {
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
    // static ref RECENT_KV_CACHE: Cache<i32, Vec<u8>>=Cache::<i32, Vec<u8>>::builder()
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
impl KVInterface for KvUserClient {
    async fn get(&self, _key_range: KeyRange) -> WSResult<Vec<KvPair>> {
        // if let Some(res) = self.testmap.get(&key_range.start) {
        //     Ok(vec![KvPair {
        //         key: res.key().clone(),
        //         value: res.value().clone(),
        //     }])
        // } else
        {
            Ok(vec![])
        }
    }
    async fn set(&self, _kvs: Vec<KvPair>, _opts: SetOptions) -> WSResult<Vec<KvPairOpt>> {
        // for kv in kvs {
        //     let _ = self.testmap.insert(kv.key, kv.value);
        // }
        Ok(vec![])
    }
}

impl KvUserClient {
    // pub async fn get_by_app_fn(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {}
    pub async fn set_by_app_fn(
        &self,
        mut kvs: Vec<KvPair>,
        _opts: SetOptions,
        fn_ctx: &FunctionCtx,
    ) -> WSResult<Vec<KvPairOpt>> {
        tracing::info!("app called kv set");
        let app_meta_man = self.view.instance_manager().app_meta_manager.read().await;
        // let key = kvs[0].key.clone();
        let patterns = {
            let appmeta = app_meta_man.get_app_meta(&fn_ctx.app).unwrap();
            let fnmeta = appmeta.get_fn_meta(&fn_ctx.func).unwrap();

            // make sure each key set is valid
            let mut patterns = vec![];
            for kv in kvs.iter() {
                if let Some(pattern) = fnmeta.match_key(&kv.key, KvOps::Set) {
                    patterns.push(pattern);
                } else {
                    // no pattern matched
                    return Err(WsPermissionErr::AccessKeyPermissionDenied {
                        app: fn_ctx.app.clone(),
                        func: fn_ctx.func.clone(),
                        access_key: TryUtf8VecU8(kv.key.clone()),
                    }
                    .into());
                }
            }
            patterns
        };

        // TODO: handle errors
        for (kv, pattern) in kvs.iter_mut().zip(&patterns) {
            tracing::info!("app called kv set really");
            // one pattern may trigger multiple functions
            kv_event::check_and_handle_event(
                fn_ctx,
                pattern,
                self,
                &*app_meta_man,
                KvOps::Set,
                std::mem::take(kv),
            )
            .await;
        }

        Ok(vec![])
        // // common plan
        // self.set(kvs, SetOptions::new()).await
    }
}
