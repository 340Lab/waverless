

use async_trait::async_trait;
use ws_derive::LogicalModule;

use super::KVClient;
use crate::{
    kv::{dist_kv::SetOptions},
    network::proto::{
        self,
        kv::{kv_response::KvPairOpt, KeyRange, KvPair, KvResponse},
    },
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, MetaKVClientView},
    util::JoinHandleWrapper,
};

#[derive(LogicalModule)]
pub struct MetaKVClient {
    view: MetaKVClientView,
}

#[async_trait]
impl LogicalModule for MetaKVClient {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let view = MetaKVClientView::new(args.logical_modules_ref.clone());
        MetaKVClient { view }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];
        let view = self.view.clone();
        self.view
            .p2p()
            .regist_rpc::<proto::kv::MetaKvRequest, _>(move |nid, _p2p, taskid, req| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    if let Some(meta) = view.meta_kv() {
                        if let Some(mut req) = req.request {
                            let req_op = req.op.take().unwrap_or_else(|| {
                                tracing::error!(
                                    "requset invalid, request from nid: {}, req:{:?}",
                                    nid,
                                    req
                                );
                                panic!();
                            });
                            match req_op {
                                proto::kv::kv_request::Op::Get(get) => {
                                    let range = get.range.unwrap();
                                    match  meta.get(range.clone()).await{
                                        Ok(res)=>{
                                            view.p2p().send_resp(nid, taskid, proto::kv::MetaKvResponse{
                                                response: Some(KvResponse{
                                                    kvs:res.into_iter().map(|v|{
                                                        KvPairOpt{
                                                           kv: Some(v), 
                                                        }
                                                    }).collect()
                                                }),
                                            }).await.expect("send meta kv get response error");
                                        }
                                        Err(err)=>{
                                            tracing::error!("rpc meta kv get request handle error, request from nid: {}, range:{:?}, err:{:?}", nid, range,err);
                                        }
                                    }
                                }
                                proto::kv::kv_request::Op::Set(put) => {

                                    match  meta.set(put.kvs.unwrap().kvs,SetOptions { consistent: true }).await{
                                        Ok(res)=>{
                                            view.p2p().send_resp(nid, taskid, proto::kv::MetaKvResponse{
                                                response: Some(KvResponse{
                                                    kvs:res
                                                }),
                                            }).await.expect("send meta kv set response error");
                                        }
                                        Err(err)=>{
                                            tracing::error!("rpc meta kv set request handle error, request from nid: {}, err:{:?}", nid,err);
                                        }
                                    }
                                }
                                _ => {
                                    tracing::error!(
                                        "not implement, request from nid: {}, req:{:?}",
                                        nid,
                                        req_op
                                    );
                                }
                            }
                        } else {
                            tracing::error!(
                                "requset invalid, request from nid: {}, req:{:?}",
                                nid,
                                req
                            );
                        }
                    } else {
                        // don't response
                        tracing::error!(
                            "meta kv node not found, request from nid: {}, req:{:?}",
                            nid,
                            req
                        );
                    }
                });
                Ok(())
            });
        

        // // test meta kv
        // if self.view.p2p().nodes_config.this.0==1{
        //     let view=self.view.clone();
        //     let _=tokio::spawn(async move{
        //         if let Some(meta)=view.meta_kv(){
        //             let mut last_set=0;
        //             loop{
        //                 if meta.ready().await{
        //                     tracing::info!("try test once");
        //                     let value=format!("test value: {}", last_set).as_bytes().to_owned();
        //                     let _=meta.set(vec![KvPair{
        //                         key: "test".as_bytes().to_owned(),
        //                         value:value.clone(),
        //                     }], SetOptions { consistent: true }).await.unwrap();
        //                     tokio::time::sleep(Duration::from_secs(1)).await;
        //                     let res=meta.get(KeyRange { start:  "test".as_bytes().to_owned(), end: None }).await.unwrap();
        //                     assert!(res.len()==1);
        //                     assert!(res[0].value==value);

        //                     tracing::info!("test once success with result: {}", String::from_utf8_lossy(&*res[0].value) );
        //                     last_set+=1;
        //                 }
        //                 tokio::time::sleep(Duration::from_secs(5)).await;
        //             }
        //         }
        //     });
        // }
        

        Ok(all)
    }
}

#[async_trait]
impl KVClient for MetaKVClient {
    async fn get(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {
        // if this is a meta kv node, directly call meta_kv
        if let Some(meta_kv) = self.view.meta_kv() {
            meta_kv.get(key_range).await
        }
        // else, rpc to remote meta kv node
        else {
            // select on meta kv node
            panic!("not implement, all nodes were meta kv node");
            // self.view.p2p().call_rpc(node_id, req) 
        }
    }
    async fn set(
        &self,
        kvs: Vec<KvPair>,
        opts: SetOptions,
    ) -> WSResult<Vec<KvPairOpt>> {
        // if this is a meta kv node, directly call meta_kv
        if let Some(meta_kv) = self.view.meta_kv() {
            meta_kv.set(kvs,opts).await
        }
        // else, rpc to remote meta kv node
        else {
            // select on meta kv node
            panic!("not implement, all nodes were meta kv node");
            // self.view.p2p().call_rpc(node_id, req) 
        }
    }
}
