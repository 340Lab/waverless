use std::{mem::ManuallyDrop, sync::Arc};

use crate::{
    network::proto,
    result::WSResult,
    sys::{ExecutorView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use wasmedge_sdk::{r#async::AsyncState, Vm, WasmValue};
use ws_derive::LogicalModule;

#[derive(LogicalModule)]
pub struct Executor {
    view: ExecutorView,
}

#[async_trait]
impl LogicalModule for Executor {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: ExecutorView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let view = self.view.clone();
        self.view
            .p2p()
            .regist_rpc::<proto::sche::ScheReq, _>(move |responser, r| {
                tracing::info!("rpc recv: {:?}", r);
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.executor().execute_sche_req(r).await;

                    if let Err(err) = responser.send_resp(proto::sche::ScheResp {}).await {
                        tracing::error!("send sche resp failed with err: {}", err);
                    }
                });
                Ok(())
            });
        Ok(vec![])
    }
}

impl Executor {
    pub async fn execute_http_app(&self, app: &str) {
        let event_index = self.view.container_manager().app_metas.read().await;
        if let Some(func) = event_index.event_http_app.get(app) {
            self.execute(app, func).await;
        }
    }
    pub async fn execute_sche_req(&self, sche_req: proto::sche::ScheReq) {
        let vm = self
            .view
            .container_manager()
            .load_container(&sche_req.app)
            .await;

        let _ = self
            .view
            .container_manager()
            .instance_running_function
            .write()
            .insert(
                vm.instance_names()[0].clone(),
                Arc::new((sche_req.app.to_owned(), sche_req.func.to_owned())),
            );

        let app_metas = self.view.container_manager().app_metas.read().await;
        if let Some(app_meta) = app_metas.app_metas.get(&sche_req.app) {
            if let Some(_func) = app_meta.fns.get(&sche_req.func) {
                // according to func input, construct params

                // fn get_value_of_kv<'a>(
                //     pattern: &str,
                //     kvs: &'a Vec<proto::kv::KvPair>,
                // ) -> Option<&'a Vec<u8>> {
                //     if let Some(kv) = kvs
                //         .iter()
                //         .find(|kv| std::str::from_utf8(&kv.key).unwrap().contains(pattern))
                //     {
                //         // use mem::take to avoid clone
                //         Some(&kv.value)
                //     } else {
                //         None
                //     }
                // }

                // fn get_key_of_kv<'a>(
                //     pattern: &str,
                //     kvs: &'a Vec<proto::kv::KvPair>,
                // ) -> Option<&'a Vec<u8>> {
                //     if let Some(kv) = kvs
                //         .iter()
                //         .find(|kv| std::str::from_utf8(&kv.key).unwrap().contains(pattern))
                //     {
                //         // use mem::take to avoid clone
                //         Some(&kv.key)
                //     } else {
                //         None
                //     }
                // }

                // if !fetch_kv_if_not_exist(
                //     &sche_req,
                //     self.view.dyn_kv_client(),
                //     &mut kvs,
                //     &sche_req.k,
                // )
                // .await
                // {
                //     return;
                // }

                // tracing::info!(
                //     "{}.{} consuming kv prepared {:?}",
                //     sche_req.app,
                //     sche_req.func,
                //     kvs
                // );

                fn prepare_vec_in_vm(vm: &Vm, v: &[u8]) -> (i32, i32) {
                    let ptr = vm
                        .run_func(
                            Some(&vm.instance_names()[0]),
                            "allocate",
                            vec![WasmValue::from_i32(v.len() as i32)],
                        )
                        .unwrap()[0]
                        .to_i32();
                    let mut mem = ManuallyDrop::new(unsafe {
                        Vec::from_raw_parts(
                            vm.named_module(&vm.instance_names()[0])
                                .unwrap()
                                .memory("memory")
                                .unwrap()
                                .data_pointer_mut(ptr as u32, v.len() as u32)
                                .unwrap(),
                            v.len(),
                            v.len(),
                        )
                    });
                    mem.copy_from_slice(v);
                    (ptr, v.len() as i32)
                }

                let (ptr, len) = prepare_vec_in_vm(&vm, &sche_req.k.as_bytes());
                let params = vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)];
                // if let Some(inputs) = &func.input {
                //     for input in inputs {
                //         match input {
                //             crate::config::FnInputYaml::Key { key: pattern } => {
                //                 if let Some(key) = get_key_of_kv(
                //                     &KeyPattern::new(pattern.to_owned()).matcher(),
                //                     &mut kvs,
                //                 ) {
                //                     let (ptr, len) = prepare_vec_in_vm(&vm, &*key);
                //                     params.push(WasmValue::from_i32(ptr));
                //                     params.push(WasmValue::from_i32(len));
                //                 } else {
                //                     tracing::error!(
                //                         "{}.{} needed kv with pattern {} not fount",
                //                         sche_req.app,
                //                         sche_req.func,
                //                         pattern
                //                     );
                //                     return;
                //                 }
                //             }
                //             crate::config::FnInputYaml::Value { value: pattern } => {
                //                 if let Some(value) = get_value_of_kv(
                //                     &KeyPattern::new(pattern.to_owned()).matcher(),
                //                     &mut kvs,
                //                 ) {
                //                     let (ptr, len) = prepare_vec_in_vm(&vm, &*value);
                //                     params.push(WasmValue::from_i32(ptr));
                //                     params.push(WasmValue::from_i32(len));
                //                 } else {
                //                     tracing::error!(
                //                         "{}.{} needed kv with pattern {} not fount",
                //                         sche_req.app,
                //                         sche_req.func,
                //                         pattern
                //                     );
                //                     return;
                //                 }
                //             }
                //         }
                //     }
                // }

                let _ = vm
                    .run_func_async(
                        &AsyncState::new(),
                        Some(&vm.instance_names()[0]),
                        sche_req.func,
                        params,
                    )
                    .await
                    .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));
            } else {
                tracing::warn!("app {} func {} not found", sche_req.app, sche_req.func);
                return;
            }
        } else {
            tracing::warn!("app {} not found", sche_req.app);
            return;
        };

        self.view
            .container_manager()
            .finish_using(&sche_req.app, vm)
            .await
    }
    pub async fn execute(&self, app: &str, func: &str) {
        let vm = self.view.container_manager().load_container(app).await;

        let _ = self
            .view
            .container_manager()
            .instance_running_function
            .write()
            .insert(
                vm.instance_names()[0].clone(),
                Arc::new((app.to_owned(), func.to_owned())),
            );

        let _ = vm
            .run_func_async(
                &AsyncState::new(),
                Some(&vm.instance_names()[0]),
                func,
                None,
            )
            .await
            .unwrap_or_else(|_| panic!("vm instance names {:?}", vm.instance_names()));
        // vm
        // })
        // .await
        // .unwrap();

        self.view.container_manager().finish_using(app, vm).await
    }
}
