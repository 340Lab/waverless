use std::{
    collections::HashMap,
    sync::{atomic::AtomicU32, Arc},
};

use async_trait::async_trait;
use parking_lot::RwLock;
use tokio::sync::Notify;
use ws_derive::LogicalModule;

use crate::{
    general::{
        m_appmeta_manager::{
            fn_event::{EventTriggerInfo},
            AppMetaManager,
        },
        m_kv_store_engine::{KeyTypeKv, KeyTypeKvPosition, KvStoreEngine},
        network::{
            m_p2p::{P2PModule, RPCHandler, RPCResponsor, TaskId},
            msg_pack::KvResponseExt,
            proto::{
                self,
                kv::{KvRequests, KvResponse, KvResponses},
            },
        },
    },
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};

use super::m_master::Master;

logical_module_view_impl!(MasterKvView);
logical_module_view_impl!(MasterKvView, p2p, P2PModule);
logical_module_view_impl!(MasterKvView, appmeta_manager, AppMetaManager);
logical_module_view_impl!(MasterKvView, master, Option<Master>);
logical_module_view_impl!(MasterKvView, master_kv, Option<MasterKv>);
logical_module_view_impl!(MasterKvView, kv_store_engine, KvStoreEngine);

#[derive(LogicalModule)]
pub struct MasterKv {
    lock_notifiers: RwLock<HashMap<Vec<u8>, (NodeID, u32, Arc<Notify>)>>,
    view: MasterKvView,
    kv_ope_id_allocator: AtomicU32,
    // kv_ope_notify: tokio::sync::RwLock<HashMap<u32, Arc<Notify>>>,
    kv_ope_notify: RwLock<HashMap<u32, Arc<Notify>>>,

    rpc_handler: RPCHandler<proto::kv::KvRequests>,
}

#[async_trait]
impl LogicalModule for MasterKv {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            lock_notifiers: RwLock::new(HashMap::new()),
            rpc_handler: RPCHandler::default(),
            view: MasterKvView::new(args.logical_modules_ref.clone()),
            kv_ope_id_allocator: AtomicU32::new(0),
            // kv_ope_notify: tokio::sync::RwLock::new(HashMap::new()),
            kv_ope_notify: RwLock::new(HashMap::new()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.view.clone();
        self.rpc_handler
            .regist(self.view.p2p(), move |responsor, reqs| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.master_kv().handle_kv_requests(reqs, responsor).await;
                });
                Ok(())
            });

        Ok(vec![])
    }
}

impl MasterKv {
    // for each operation, find it's sub-trigger func
    async fn collect_event_infos(
        &self,
        _reqs: &proto::kv::KvRequests,
    ) -> Vec<Option<EventTriggerInfo>> {
        // let metas = self.view.appmeta_manager().meta.read().await;
        // reqs.requests
        //     .iter()
        //     .map(|req| fn_event::try_match_kv_event(&metas, req, &reqs.app, &reqs.func))
        //     .collect()
        vec![]
    }

    async fn handle_kv_requests(
        &self,
        reqs: proto::kv::KvRequests,
        responsor: RPCResponsor<KvRequests>,
    ) {
        if reqs.prev_kv_opeid >= 0 {
            let mut _hold_not_arc = None;
            let noted = {
                let hold_nots = self.kv_ope_notify.read();
                hold_nots.get(&(reqs.prev_kv_opeid as u32)).map(|v| {
                    _hold_not_arc = Some(v.clone());
                    _hold_not_arc.as_ref().unwrap().notified()
                })
            };

            if let Some(noted) = noted {
                noted.await
            }
        }
        let mut kv_responses = KvResponses { responses: vec![] };
        // pre-collect each operation's event trigger info
        let trigger = self.collect_event_infos(&reqs).await;
        let mut kv_opeid = None;
        for (req, event) in reqs.requests.into_iter().zip(trigger) {
            let mut sub_tasks = vec![];
            // if with event
            if let Some(mut trigger) = event {
                let app_fns = std::mem::take(&mut trigger.trigger_appfns);

                for (app, func) in app_fns {
                    if kv_opeid.is_none() {
                        kv_opeid = Some(
                            self.kv_ope_id_allocator
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                        );
                        assert!(self
                            .kv_ope_notify
                            .write()
                            .insert(kv_opeid.unwrap(), Notify::new().into())
                            .is_none());
                    }
                    let trigger_data = trigger.to_trigger(kv_opeid.unwrap());
                    let view = self.view.clone();
                    // schedule sub tasks parallelly
                    sub_tasks.push(tokio::spawn(async move {
                        view.master()
                            .schedule_one_trigger(app, func, trigger_data)
                            .await;
                    }));
                }
            }
            kv_responses.responses.push(match req.op.unwrap() {
                proto::kv::kv_request::Op::Set(set) => {
                    self.handle_kv_set(set, responsor.node_id()).await
                }
                proto::kv::kv_request::Op::Get(get) => self.handle_kv_get(get).await,
                proto::kv::kv_request::Op::Delete(delete) => self.handle_kv_delete(delete).await,
                proto::kv::kv_request::Op::Lock(lock) => {
                    self.handle_kv_lock(lock, responsor.node_id(), responsor.task_id())
                        .await
                } // notify sub tasks to run because data's persisted
            });
            tracing::debug!("notify all waiting kv operations");
            // notify all waiting kv operations
            if let Some(opeid) = kv_opeid {
                if let Some(notify) = self.kv_ope_notify.write().remove(&opeid) {
                    notify.notify_waiters();
                } else {
                    panic!("fatal logical error, kv opeid:{} not found", opeid);
                }
            }
            // make sure each task is triggered
            for task in sub_tasks {
                task.await.unwrap();
            }
        }

        if let Err(err) = responsor.send_resp(kv_responses).await {
            tracing::error!("handle kv requests error:{}", err);
        };
    }
    async fn handle_kv_set(
        &self,
        set: proto::kv::kv_request::KvPutRequest,
        _from: NodeID,
    ) -> KvResponse {
        tracing::debug!("handle_kv_set:{:?}", set.kv.as_ref().map(|v| &v.key));

        if let Some(kv) = set.kv {
            self.view
                .kv_store_engine()
                .set(KeyTypeKv(&kv.key), &kv.value);
            self.view.kv_store_engine().set(
                KeyTypeKvPosition(&kv.key),
                &self.view.p2p().nodes_config.this_node(),
            );

            self.view.kv_store_engine().flush();
        }

        KvResponse::new_common(vec![])
    }
    async fn handle_kv_get(&self, get: proto::kv::kv_request::KvGetRequest) -> KvResponse {
        tracing::debug!("handle_kv_get:{:?}", get);
        let mut kvs = vec![];
        if let Some(v) = self
            .view
            .kv_store_engine()
            .get(KeyTypeKv(&get.range.as_ref().unwrap().start))
        {
            kvs.push(proto::kv::KvPair {
                key: get.range.unwrap().start,
                value: v.clone(),
            });
        }
        KvResponse::new_common(kvs)
    }
    async fn handle_kv_delete(&self, delete: proto::kv::kv_request::KvDeleteRequest) -> KvResponse {
        tracing::debug!("handle_kv_delete:{:?}", delete);
        // let res = self
        //     .kv_map
        //     .write()
        //     .remove(&delete.range.as_ref().unwrap().start);
        self.view
            .kv_store_engine()
            .del(KeyTypeKvPosition(&delete.range.as_ref().unwrap().start));
        self.view
            .kv_store_engine()
            .del(KeyTypeKv(&delete.range.as_ref().unwrap().start));
        self.view.kv_store_engine().flush();
        // let mut kvs = vec![];
        // if let Some(v) = res {
        //     kvs.push(proto::kv::KvPair {
        //         key: delete.range.unwrap().start,
        //         value: v,
        //     });
        // }
        KvResponse::new_common(vec![])
    }
    async fn handle_kv_lock(
        &self,
        lock: proto::kv::kv_request::KvLockRequest,
        from: NodeID,
        task: TaskId,
    ) -> KvResponse {
        tracing::debug!("handle_kv_lock:{:?}", lock);
        let mut notify_last = None;
        loop {
            if let Some(&release_id) = lock.release_id.get(0) {
                tracing::debug!("unlock:{:?}", release_id);
                // valid unlock:
                // - is the owner
                // - match verify id
                let mut is_owner = false;
                let mut write = self.lock_notifiers.write();
                if let Some((nodeid, real_release_id, _)) =
                    write.get(&lock.range.as_ref().unwrap().start)
                {
                    if *nodeid == from && *real_release_id == release_id {
                        is_owner = true;
                    }
                }
                if is_owner {
                    tracing::debug!("unlock success");
                    let (_, _, notify) = write.remove(&lock.range.as_ref().unwrap().start).unwrap();
                    notify.notify_one();
                    return KvResponse::new_common(vec![]);
                }
            } else {
                // get, just get the lock
                // the key creator will be the owner of the lock
                let mut notify = None;
                {
                    let mut write = self.lock_notifiers.write();
                    let notify_to_insert = if let Some(notify) = notify_last.take() {
                        notify
                    } else {
                        Arc::new(Notify::new())
                    };
                    let _ = write
                        .entry(lock.range.as_ref().unwrap().start.clone())
                        .and_modify(|v| {
                            tracing::debug!("lock already exists");
                            notify = Some(v.2.clone());
                        })
                        .or_insert_with(|| {
                            tracing::debug!("lock not exists, preempt");
                            (from, task, notify_to_insert)
                        });
                }
                // didn't get the lock
                if let Some(notify) = notify {
                    notify_last = Some(notify);
                    tracing::debug!("wait for other to release");
                    // wait for release
                    notify_last.as_ref().unwrap().notified().await;
                    continue;
                } else {
                    return KvResponse::new_lock(task);
                }
            }
        }
    }
}
