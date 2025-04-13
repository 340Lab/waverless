use crate::general::network::proto_ext::ProtoExtDataItem;
use crate::{
    general::{
        data::{
            m_data_general::{
                new_data_unique_id_fn_kv, DataGeneral, DataItemIdx, DataSetMetaV2, GetOrDelDataArg,
                GetOrDelDataArgType,
                dataitem::DataItemArgWrapper
            },
            m_dist_lock::DistLock,
        },
        network::{
            m_p2p::{P2PModule, RPCCaller},
            proto::{
                self,
                kv::{KvRequests, KvResponse, KvResponses},
            },
            proto_ext::ProtoExtKvResponse,
        },
    },
    logical_module_view_impl,
    result::{WSError, WSResult, WSResultExt, WsDataError},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use std::collections::HashMap;
use ws_derive::LogicalModule;

logical_module_view_impl!(KvUserClientView);
logical_module_view_impl!(KvUserClientView, p2p, P2PModule);
logical_module_view_impl!(KvUserClientView, data_general, DataGeneral);
logical_module_view_impl!(KvUserClientView, dist_lock, DistLock);
logical_module_view_impl!(KvUserClientView, kv_user_client, Option<KvUserClient>);

#[derive(LogicalModule)]
pub struct KvUserClient {
    // testmap: SkipMap<Vec<u8>, Vec<u8>>,
    view: KvUserClientView,
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

// pub fn kv_user_client() -> &'static KvUserClient {
//     let res = &*KV_USER_CLIENT as *const Option<KvUserClientView> as *mut Option<KvUserClientView>;
//     unsafe { (*res).as_ref().unwrap().kv_user_client() }
// }

// #[async_trait]
// impl KvInterface for KvUserClient {
//     async fn call(&self, req: KvRequests, opt: KvOptions) -> WSResult<KvResponses> {
//         if let Some(node_id) = opt.spec_node() {
//             self.rpc_caller_kv
//                 .call(
//                     self.view.p2p(),
//                     node_id,
//                     req,
//                     Some(Duration::from_secs(60 * 30)),
//                 )
//                 .await
//         } else {
//             // 1. dicide placement position
//             // 2. send data to the position
//             self.rpc_caller_kv
//                 .call(
//                     self.view.p2p(),
//                     self.view.p2p().nodes_config.get_master_node(),
//                     req,
//                     Some(Duration::from_secs(60 * 30)),
//                 )
//                 .await
//         }
//     }
// }

impl KvUserClient {
    pub async fn kv_requests(
        &self,
        app_name: &str,
        func_name: &str,
        reqs: proto::kv::KvRequests,
        // responsor: RPCResponsor<KvRequests>,
    ) -> WSResult<proto::kv::KvResponses> {
        let mut kv_responses = KvResponses { responses: vec![] };
        // pre-collect each operation's event trigger info

        // let mut kv_opeid = None;
        for req in reqs.requests.into_iter() {
            // let mut sub_tasks = vec![];
            let response = match req.op.unwrap() {
                proto::kv::kv_request::Op::Set(set) => {
                    Some(self.handle_kv_set(app_name, func_name, set).await)
                }
                proto::kv::kv_request::Op::Get(get) => Some(self.handle_kv_get(get).await),
                proto::kv::kv_request::Op::Delete(delete) => {
                    Some(self.handle_kv_delete(delete).await)
                }
                proto::kv::kv_request::Op::Lock(lock) => {
                    let req = if lock.release_id.len() > 0 {
                        proto::kv::KvLockRequest {
                            key: lock.range.unwrap().start,
                            read_0_write_1_unlock_2: 2,
                            release_id: lock.release_id[0],
                        }
                    } else {
                        proto::kv::KvLockRequest {
                            key: lock.range.unwrap().start,
                            read_0_write_1_unlock_2: if lock.read_or_write { 0 } else { 1 },
                            release_id: 0,
                        }
                    };

                    let ok = match self.view.dist_lock().lock(req).await {
                        Ok(ok) => ok,
                        Err(err) => {
                            tracing::warn!("kv_requests lock err:{:?}", err);
                            return Err(err);
                        }
                    };

                    if lock.release_id.len() > 0 {
                        //unlocked
                        Some(proto::kv::KvResponse {
                            resp: Some(proto::kv::kv_response::Resp::LockId(if ok.success {
                                lock.release_id[0]
                            } else {
                                0
                            })),
                        })
                    } else {
                        // locked
                        Some(proto::kv::KvResponse {
                            resp: Some(proto::kv::kv_response::Resp::LockId(ok.release_id)),
                        })
                    }

                    // Some(ok)
                    // self.handle_kv_lock(lock, responsor.node_id(), responsor.task_id())
                    //     .await
                } // notify sub tasks to run because data's persisted
            };
            if let Some(response) = response {
                kv_responses.responses.push(response);
            }

            // // make sure each task is triggered
            // for task in sub_tasks {
            //     task.await.unwrap();
            // }
        }

        Ok(kv_responses)
    }

    async fn handle_kv_set(
        &self,
        app_name: &str,
        func_name: &str,
        set: proto::kv::kv_request::KvPutRequest,
    ) -> KvResponse {
        let proto::kv::KvPair { key, value } = set.kv.unwrap();
        let cur_node = self.view.p2p().nodes_config.this_node();
        tracing::debug!("handle_kv_set: key: {:?}", key);

        let data_general = self.view.data_general();
        //返回结果未处理 曾俊
        if let Err(e) = data_general
            .write_data(
                new_data_unique_id_fn_kv(&key),
                //原代码：
                // vec![proto::DataItem {
                //     data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(value)),
                // }],
                //修改后封装成要求的DataItemArgWrapper类型 tmpzipfile设置为Uninitialized状态   在DataItemArgWrapper结构体中添加了一个new方法         曾俊   
             vec![DataItemArgWrapper::new(value)],
                Some((
                    cur_node,
                    proto::DataOpeType::Write,
                    proto::data_schedule_context::OpeRole::FuncCall(proto::DataOpeRoleFuncCall {
                        app_func: format!("{}/{}", app_name, func_name),
                        node_id: cur_node,
                    }),
                )),
            )
            .await{
                tracing::error!("Failed to write data: {}", e);
            }
        // .todo_handle("This part of the code needs to be implemented.");
        KvResponse::new_common(vec![])
    }

    fn convert_get_data_res_to_kv_response(
        key: Vec<u8>,
        uid: Vec<u8>,
        _meta: DataSetMetaV2,
        splits: HashMap<DataItemIdx, proto::DataItem>,
    ) -> WSResult<Vec<proto::kv::KvPair>> {
        tracing::debug!("convert_get_data_res_to_kv_response uid: {:?}, split keys: {:?}", uid, splits.keys().collect::<Vec<_>>());
        if splits.len() != 1 {
            return Err(WSError::WsDataError(
                WsDataError::KvGotWrongSplitCountAndIdx {
                    unique_id: uid.clone(),
                    idx: splits.keys().cloned().collect(),
                },
            ));
        }

        let (idx, data_item) = splits.into_iter().next().unwrap();
        if idx != 0 {
            return Err(WSError::WsDataError(
                WsDataError::KvGotWrongSplitCountAndIdx {
                    unique_id: uid.clone(),
                    idx: vec![idx],
                },
            ));
        }

        let data_item_dispatch = data_item.data_item_dispatch.unwrap();
        let raw_bytes = match data_item_dispatch {
            proto::data_item::DataItemDispatch::RawBytes(value) => value,
            _ => {
                return Err(WSError::WsDataError(WsDataError::KvDeserializeErr {
                    unique_id: uid,
                    context: format!(
                        "data_item_dispatch({}) is not RawBytes",
                        proto::DataItem {
                            data_item_dispatch: Some(data_item_dispatch),
                        }
                        .to_string(),
                    ),
                }))
            }
        };

        Ok(vec![proto::kv::KvPair {
            key: key,
            value: raw_bytes,
        }])
    }

    async fn handle_kv_get(&self, get: proto::kv::kv_request::KvGetRequest) -> KvResponse {
        tracing::debug!("handle_kv_get:{:?}", get);

        let data_general = self.view.data_general();
        let uid = new_data_unique_id_fn_kv(&get.range.as_ref().unwrap().start);
        let got = data_general
            .get_or_del_data(GetOrDelDataArg {
                meta: None,
                unique_id: uid.clone(),
                ty: GetOrDelDataArgType::All,
            })
            .await;

        let got = match got {
            Ok((meta, splits)) => match Self::convert_get_data_res_to_kv_response(
                get.range.unwrap().start,
                uid,
                meta,
                splits,
            ) {
                Ok(res) => res,
                Err(err) => {
                    tracing::warn!("get kv data error:{:?}", err);
                    vec![]
                }
            },
            Err(WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid })) => {
                tracing::debug!("get kv data not found, uid({:?})", uniqueid);
                vec![]
            }
            Err(err) => {
                tracing::warn!("get kv data error:{:?}", err);
                vec![]
            }
        };
        KvResponse::new_common(got)
    }
    async fn handle_kv_delete(&self, delete: proto::kv::kv_request::KvDeleteRequest) -> KvResponse {
        tracing::debug!("handle_kv_delete:{:?}", delete);

        let data_general = self.view.data_general();
        let uid = new_data_unique_id_fn_kv(&delete.range.as_ref().unwrap().start);
        let deleted = data_general
            .get_or_del_data(GetOrDelDataArg {
                meta: None,
                unique_id: uid.clone(),
                ty: GetOrDelDataArgType::Delete,
            })
            .await;

        let deleted = match deleted {
            Ok((deleted_meta, deleted_splits)) => match Self::convert_get_data_res_to_kv_response(
                delete.range.unwrap().start,
                uid,
                deleted_meta,
                deleted_splits,
            ) {
                Ok(res) => res,
                Err(err) => {
                    tracing::warn!("delete kv data error:{:?}", err);
                    vec![]
                }
            },
            Err(WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid })) => {
                tracing::debug!("delete kv data not found, uid({:?})", uniqueid);
                vec![]
            }
            Err(err) => {
                tracing::warn!("delete kv data error:{:?}", err);
                vec![]
            }
        };
        KvResponse::new_common(deleted)
    }
    // async fn handle_kv_lock(
    //     &self,
    //     lock: proto::kv::kv_request::KvLockRequest,
    //     from: NodeID,
    //     task: TaskId,
    // ) -> KvResponse {
    //     KvResponse::new_common(deleted)
    //     // tracing::debug!("handle_kv_lock:{:?}", lock);
    //     // let mut notify_last = None;
    //     // loop {
    //     //     if let Some(&release_id) = lock.release_id.get(0) {
    //     //         tracing::debug!("unlock:{:?}", release_id);
    //     //         // valid unlock:
    //     //         // - is the owner
    //     //         // - match verify id
    //     //         let mut is_owner = false;
    //     //         // let mut write = self.lock_notifiers.write();
    //     //         if let Some((nodeid, real_release_id, _)) =
    //     //             write.get(&lock.range.as_ref().unwrap().start)
    //     //         {
    //     //             if *nodeid == from && *real_release_id == release_id {
    //     //                 is_owner = true;
    //     //             }
    //     //         }
    //     //         if is_owner {
    //     //             tracing::debug!("unlock success");
    //     //             let (_, _, notify) = write.remove(&lock.range.as_ref().unwrap().start).unwrap();
    //     //             notify.notify_one();
    //     //             return KvResponse::new_common(vec![]);
    //     //         }
    //     //     } else {
    //     //         // get, just get the lock
    //     //         // the key creator will be the owner of the lock
    //     //         let mut notify = None;
    //     //         {
    //     //             let mut write = self.lock_notifiers.write();
    //     //             let notify_to_insert = if let Some(notify) = notify_last.take() {
    //     //                 notify
    //     //             } else {
    //     //                 Arc::new(Notify::new())
    //     //             };
    //     //             let _ = write
    //     //                 .entry(lock.range.as_ref().unwrap().start.clone())
    //     //                 .and_modify(|v| {
    //     //                     tracing::debug!("lock already exists");
    //     //                     notify = Some(v.2.clone());
    //     //                 })
    //     //                 .or_insert_with(|| {
    //     //                     tracing::debug!("lock not exists, preempt");
    //     //                     (from, task, notify_to_insert)
    //     //                 });
    //     //         }
    //     //         // didn't get the lock
    //     //         if let Some(notify) = notify {
    //     //             notify_last = Some(notify);
    //     //             tracing::debug!("wait for other to release");
    //     //             // wait for release
    //     //             notify_last.as_ref().unwrap().notified().await;
    //     //             continue;
    //     //         } else {
    //     //             return KvResponse::new_lock(task);
    //     //         }
    //     //     }
    //     // }
    // }
}

#[cfg(test)]
mod test {
    
    use std::{time::Duration};

    use super::KvUserClientView;
    use crate::general::{
        network::{
            proto::{
                self,
                kv::{KvRequest, KvRequests},
            },
            proto_ext::KvRequestExt,
        },
        test_utils,
    };

    #[tokio::test(flavor = "multi_thread")]
    async fn test_kv_user_client() {
        let (_hold, _sys1, sys2) = test_utils::get_test_sys().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
        let view = KvUserClientView::new(sys2);
        let app = "test_app";
        let func = "test_func";
        let test_key = "test_key";
        let test_value = "test_value";

        // first time get should be none
        {
            let res = view
                .kv_user_client()
                .kv_requests(
                    app,
                    func,
                    KvRequests {
                        app: app.to_owned(),
                        func: func.to_owned(),
                        prev_kv_opeid: -1,
                        requests: vec![KvRequest::new_get(test_key.as_bytes().to_owned())],
                    },
                )
                .await
                .unwrap();
            assert!(res.responses.len() == 1);
            match res.responses[0].resp.clone().unwrap() {
                proto::kv::kv_response::Resp::CommonResp(kv_response) => {
                    assert!(kv_response.kvs.len() == 0);
                }
                proto::kv::kv_response::Resp::LockId(_) => panic!(),
            }
            tracing::debug!("first time get is none");
        }

        // (insert and get then delete twice) *3
        for _ in 0..3 {
            let res = view
                .kv_user_client()
                .kv_requests(
                    app,
                    func,
                    KvRequests {
                        app: app.to_owned(),
                        func: func.to_owned(),
                        prev_kv_opeid: -1,
                        requests: vec![KvRequest::new_set(proto::kv::KvPair {
                            key: test_key.as_bytes().to_owned(),
                            value: test_value.as_bytes().to_owned(),
                        })],
                    },
                )
                .await
                .unwrap();
            assert!(res.responses.len() == 1);
            match res.responses[0].resp.clone().unwrap() {
                proto::kv::kv_response::Resp::CommonResp(kv_response) => {
                    assert!(kv_response.kvs.len() == 0);
                    // assert_eq!(str::from_utf8(&kv_response.kvs[0].key).unwrap(), test_key);
                    // assert_eq!(
                    //     str::from_utf8(&kv_response.kvs[0].value).unwrap(),
                    //     test_value
                    // );
                }
                proto::kv::kv_response::Resp::LockId(_) => panic!(),
            }
            tracing::debug!("set success");

            // get after set
            let res = view
                .kv_user_client()
                .kv_requests(
                    app,
                    func,
                    KvRequests {
                        app: app.to_owned(),
                        func: func.to_owned(),
                        prev_kv_opeid: -1,
                        requests: vec![KvRequest::new_get(test_key.as_bytes().to_owned())],
                    },
                )
                .await
                .unwrap();
            assert!(res.responses.len() == 1);
            match res.responses[0].resp.clone().unwrap() {
                proto::kv::kv_response::Resp::CommonResp(kv_response) => {
                    assert_eq!(kv_response.kvs.len(), 1);
                    assert!(kv_response.kvs[0].key == test_key.as_bytes().to_owned());
                    assert!(kv_response.kvs[0].value == test_value.as_bytes().to_owned());
                }
                proto::kv::kv_response::Resp::LockId(_) => panic!(),
            }
            tracing::debug!("get after set success");

            // delete after get
            let res = view
                .kv_user_client()
                .kv_requests(
                    app,
                    func,
                    KvRequests {
                        app: app.to_owned(),
                        func: func.to_owned(),
                        prev_kv_opeid: -1,
                        requests: vec![KvRequest::new_delete(test_key.as_bytes().to_owned())],
                    },
                )
                .await
                .unwrap();
            assert!(res.responses.len() == 1);
            match res.responses[0].resp.clone().unwrap() {
                proto::kv::kv_response::Resp::CommonResp(kv_response) => {
                    assert!(kv_response.kvs[0].key == test_key.as_bytes().to_owned());
                    assert!(kv_response.kvs[0].value == test_value.as_bytes().to_owned());
                }
                proto::kv::kv_response::Resp::LockId(_) => panic!(),
            }
            tracing::debug!("delete after get success");

            // delete again will be none
            let res = view
                .kv_user_client()
                .kv_requests(
                    app,
                    func,
                    KvRequests {
                        app: app.to_owned(),
                        func: func.to_owned(),
                        prev_kv_opeid: -1,
                        requests: vec![KvRequest::new_delete(test_key.as_bytes().to_owned())],
                    },
                )
                .await
                .unwrap();
            assert!(res.responses.len() == 1);
            match res.responses[0].resp.clone().unwrap() {
                proto::kv::kv_response::Resp::CommonResp(kv_response) => {
                    assert!(kv_response.kvs.len() == 0);
                }
                proto::kv::kv_response::Resp::LockId(_) => panic!(),
            }
            tracing::debug!("delete again is none");
        }
    }
}
