use crate::{
    kv::kv_interface::{KVInterface, SetOptions},
    network::proto::{
        self,
        kv::{kv_response::KvPairOpt, KeyRange, KvPair},
    },
    result::WSResult,
    schedule::container_manager::KvEventType,
    sys::{DynKvClientView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
// use crossbeam_skiplist::SkipMap;

use ws_derive::LogicalModule;

// use super::super::kv_interface::KVInterface;

#[derive(LogicalModule)]
pub struct DynKvClient {
    // testmap: SkipMap<Vec<u8>, Vec<u8>>,
    pub view: DynKvClientView,
}

#[async_trait]
impl LogicalModule for DynKvClient {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        unsafe {
            *(&*DYN_KV_CLIENT as *const Option<DynKvClientView> as *mut Option<DynKvClientView>) =
                Some(DynKvClientView::new(args.logical_modules_ref.clone()));
        }
        Self {
            // testmap: SkipMap::new(),
            view: DynKvClientView::new(args.logical_modules_ref.clone()),
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
    static ref DYN_KV_CLIENT: Option<DynKvClientView>=None;
    // static ref RECENT_KV_CACHE: Cache<i32, Vec<u8>>=Cache::<i32, Vec<u8>>::builder()
    //     .time_to_live(Duration::from_secs(10))
    //     .weigher(|_key, value| -> u32 { value.len().try_into().unwrap_or(u32::MAX) })
    //     // This cache will hold up to 32MiB of values.
    //     .max_capacity(32 * 1024 * 1024)
    //     .build();
    // static ref NEXT_CACHE_ID: AtomicI32=AtomicI32::new(0);
}
pub fn dyn_kv_client<'a>() -> &'a DynKvClient {
    let res = &*DYN_KV_CLIENT as *const Option<DynKvClientView> as *mut Option<DynKvClientView>;
    unsafe { (*res).as_ref().unwrap().dyn_kv_client() }
}

#[async_trait]
impl KVInterface for DynKvClient {
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

impl DynKvClient {
    // pub async fn get_by_app_fn(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {}
    pub async fn set_by_app_fn(
        &self,
        kvs: Vec<KvPair>,
        _opts: SetOptions,
        app: &str,
        func: &str,
    ) -> WSResult<Vec<KvPairOpt>> {
        let app_metas = self.view.container_manager().app_metas.read().await;
        let key = kvs[0].key.clone();
        if let Some(ks) = app_metas.get_fn_key(app, func) {
            if let Some(find) = ks
                .iter()
                .find(|k| std::str::from_utf8(&*key).unwrap().contains(&k.matcher()))
            {
                if let Some(event) = app_metas.event_kv.get(&find.matcher()) {
                    tracing::info!("event {:?} trigger by kv_set from {}.{}", event, app, func);
                    if event.ty == KvEventType::Set {
                        let p2p = self.view.container_manager().view.p2p();
                        match p2p
                            .call_rpc(
                                p2p.nodes_config.get_master_node(),
                                proto::sche::FnEventScheRequest {
                                    app: app.to_owned(),
                                    func: func.to_owned(),
                                    with_new_kv_size: 0,
                                },
                            )
                            .await
                        {
                            Ok(schecmd) => {
                                tracing::info!("rpc call res: {:?}", schecmd);
                                // 1. sync set kv
                                // TODO: use local kv client to store on target node
                                if let Ok(_res) = self.set(kvs, SetOptions::new()).await {
                                    // 2. sche
                                    let event = event.clone();
                                    let view = self.view.clone();
                                    let _ = tokio::spawn(async move {
                                        if let Err(err) = view
                                            .p2p()
                                            .call_rpc(
                                                schecmd.target_node,
                                                proto::sche::ScheReq {
                                                    app: event.app.clone(),
                                                    func: event.func.clone(),
                                                    k: std::str::from_utf8(&*key)
                                                        .unwrap()
                                                        .to_owned(),
                                                },
                                            )
                                            .await
                                        {
                                            tracing::error!("sche sub fn failed with err: {}", err);
                                        }
                                    });
                                }

                                return Ok(vec![]);
                            }
                            Err(e) => {
                                tracing::error!("rpc call error: {:?}", e);
                            }
                        }
                    }
                }
            }
        }

        // common plan
        self.set(kvs, SetOptions::new()).await
    }
}
