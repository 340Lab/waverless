use super::{utils, utils::m_kv_user_client, HostFuncRegister};
use crate::general::network::proto::{
    self,
    kv::{KeyRange, KvPair, KvRequest, KvRequests, KvResponses},
};
use crate::general::network::proto_ext::ProtoExtKvResponse;
use moka::sync::Cache;
use std::{sync::atomic::AtomicI32, time::Duration};
#[cfg(target_os = "macos")]
use wasmer::{imports, Function, FunctionType, Imports};

#[cfg(target_os = "linux")]
use wasmedge_sdk::{
    async_host_function, error::HostFuncError, host_function, Caller, ImportObjectBuilder,
    NeverType, WasmValue,
};

// fn kv_set(kptr: *const u8, klen: i32, v: *const u8, vlen: i32);
// type KvSetArgs = (i32, i32, i32, i32);
// #[cfg_attr(target_os = "linux", async_host_function)]
// async fn kv_set_async<T>(
//     // caller: CallingFrame,
//     // args: Vec<WasmValue>,
//     caller: Caller,
//     args: Vec<WasmValue>,
//     _ctx: *mut T,
// ) -> Result<Vec<WasmValue>, HostFuncError> {
//     let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32()).to_owned();
//     let value = utils::u8slice(&caller, args[2].to_i32(), args[3].to_i32()).to_owned();
//     let cur_app_fn = utils::current_app_fn_ctx(&caller);
//     kv_user_client()
//         .set_by_app_fn(KvPair { key, value }, KvOptions::new(), unsafe {
//             cur_app_fn.0.as_ref()
//         })
//         .await;
//     Ok(vec![])
// }
// #[cfg(target_os = "macos")]
// fn kv_set(k_ptr: i32, k_len: i32, v_ptr: i32, v_len: i32) {
//     // let
// }

// fn kv_get_len(kptr: *const u8, klen: i32, vlen: &mut i32, id: &mut i32);
// type KvGetLenArgs = (i32, i32, i32, i32);
// #[cfg_attr(target_os = "linux", async_host_function)]
// async fn kv_get_len_async<T>(
//     // caller: CallingFrame,
//     // args: Vec<WasmValue>,
//     caller: Caller,
//     args: Vec<WasmValue>,
//     _ctx: *mut T,
// ) -> Result<Vec<WasmValue>, HostFuncError> {
//     let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
//     let len = utils::mutref::<i32>(&caller, args[2].to_i32());
//     let id = utils::mutref::<i32>(&caller, args[3].to_i32());
//     if let Ok(mut res) = kv_user_client()
//         .get(KeyRange {
//             start: key.to_owned(),
//             end: vec![],
//         })
//         .await
//     {
//         if res.len() > 0 {
//             let KvPair { key: _, value } = res.pop().unwrap();
//             let newid = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
//             if newid < 0 {
//                 NEXT_CACHE_ID.store(0, std::sync::atomic::Ordering::Relaxed);
//             }
//             let newid = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
//             *len = value.len() as i32;
//             *id = newid;
//             RECENT_Kv_CACHE.insert(newid, value);
//         } else {
//             *len = 0;
//         }
//     } else {
//         *len = 0;
//     }

//     // kv_user_client()
//     Ok(vec![])
// }

#[cfg(target_os = "macos")]
fn kv_get_len(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    util::call_async_from_sync(kv_get_len_async(caller, args, std::ptr::null_mut::<()>()))
}

// fn kv_get(id: i32, vlen:  *const u8);
// type KvGetArgs = (i32, i32);
// #[cfg_attr(target_os = "linux", host_function)]
// fn kv_get(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
//     let id = args[0].to_i32();
//     if let Some(res) = RECENT_Kv_CACHE.remove(&id) {
//         let retslice = utils::mutu8sclice(&caller, args[1].to_i32(), res.len() as i32).unwrap();
//         retslice.copy_from_slice(res.as_slice());
//     }

//     Ok(vec![])
// }

lazy_static::lazy_static! {

    static ref RECENT_KV_CACHE: Cache<i32, KvResponses>=Cache::builder()
        .time_to_live(Duration::from_secs(10))
        // This cache will hold up to 32MiB of values.
        .max_capacity(10240)
        .build();
    static ref NEXT_CACHE_ID: AtomicI32=AtomicI32::new(0);
}

const SET_ID: usize = 1;
const GET_ID: usize = 2;
const LOCK_ID: usize = 3;
const DELETE_ID: usize = 4;

type KvBatchOpe = (i32, i32, i32);
#[cfg_attr(target_os = "linux", async_host_function)]
async fn kv_batch_ope<T>(
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    let opes_arg_ptr = args[0].to_i32();
    let opes_arg_len = args[1].to_i32();
    let opes_id = utils::mutref::<i32>(&caller, args[2].to_i32());
    let args = utils::i32slice(&caller, opes_arg_ptr, opes_arg_len);
    let func_ctx = unsafe {
        #[cfg(feature = "unsafe-log")]
        tracing::debug!("current_app_fn_ctx begin");
        let res = utils::current_app_fn_ctx(&caller).0.as_mut();
        #[cfg(feature = "unsafe-log")]
        tracing::debug!("current_app_fn_ctx end");
        res
    };

    // request and response mem position
    let ope_cnt = args[0];
    let mut requests: Vec<KvRequest> = vec![];
    let mut cur_idx = 1;
    // tracing::debug!("args:{:?}", args);
    // Construct the requests
    for _ in 0..ope_cnt {
        let ope_type = args[cur_idx];
        match ope_type as usize {
            // set
            SET_ID => {
                let key = utils::u8slice(&caller, args[cur_idx + 1], args[cur_idx + 2]);
                let value = utils::u8slice(&caller, args[cur_idx + 3], args[cur_idx + 4]);
                requests.push(KvRequest {
                    op: Some(proto::kv::kv_request::Op::Set(
                        proto::kv::kv_request::KvPutRequest {
                            kv: Some(KvPair {
                                key: key.to_owned(),
                                value: value.to_owned(),
                            }),
                        },
                    )),
                });
                cur_idx += 5;
            }
            // get
            GET_ID => {
                let key = utils::u8slice(&caller, args[cur_idx + 1], args[cur_idx + 2]);
                // tracing::debug!("ptr:{} len:{} get key:{:?}", args[cur_idx + 1], args[cur_idx + 2],key);
                requests.push(KvRequest {
                    op: Some(proto::kv::kv_request::Op::Get(
                        proto::kv::kv_request::KvGetRequest {
                            range: Some(KeyRange {
                                start: key.to_owned(),
                                end: vec![],
                            }),
                        },
                    )),
                });
                cur_idx += 4;
            }
            // lock
            LOCK_ID => {
                let key = utils::u8slice(&caller, args[cur_idx + 1], args[cur_idx + 2]);
                // // first bit
                // let read_or_write = args[cur_idx + 3] & 1 == 1;
                // <0 means get
                let release_id = args[cur_idx + 3];
                requests.push(KvRequest {
                    op: Some(proto::kv::kv_request::Op::Lock(
                        proto::kv::kv_request::KvLockRequest {
                            read_or_write: false,
                            release_id: if release_id < 0 {
                                vec![]
                            } else {
                                vec![release_id as u32]
                            },
                            range: Some(KeyRange {
                                start: key.to_owned(),
                                end: vec![],
                            }),
                        },
                    )),
                });
                cur_idx += 5;
            }
            DELETE_ID => {
                let key = utils::u8slice(&caller, args[cur_idx + 1], args[cur_idx + 2]);
                requests.push(KvRequest {
                    op: Some(proto::kv::kv_request::Op::Delete(
                        proto::kv::kv_request::KvDeleteRequest {
                            range: Some(KeyRange {
                                start: key.to_owned(),
                                end: vec![],
                            }),
                        },
                    )),
                });
                cur_idx += 3;
            }
            _ => {
                panic!("not implemented, reqs{:?},{:X}", requests, ope_type);
            }
        }
    }
    // tracing::debug!("requests:{:?}", requests);
    match m_kv_user_client()
        .kv_requests(
            &func_ctx.app,
            &func_ctx.func,
            KvRequests {
                requests,
                app: func_ctx.app.clone(),
                func: func_ctx.func.clone(),
                prev_kv_opeid: func_ctx
                    .event_ctx
                    .take_prev_kv_opeid()
                    .map_or(-1, |v| v as i64),
            },
            // KvOptions::new(),
        )
        .await
    {
        Ok(res) => {
            let id = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // Write back the results to wasm runtime
            let mut cur_idx = 1;
            let mut resps = res.responses.iter();
            // Construct the requests
            for _ in 0..ope_cnt {
                let ope_type = args[cur_idx];
                match ope_type as usize {
                    // set
                    SET_ID => {
                        let _ = resps.next().unwrap();
                        cur_idx += 5;
                    }
                    // get
                    GET_ID => {
                        let kvs = resps.next().unwrap().common_kvs().unwrap();
                        // get len
                        *utils::mutref::<i32>(&caller, args[cur_idx + 3]) = if kvs.len() > 0 {
                            kvs.get(0).unwrap().value.len() as i32
                        } else {
                            -1
                        };
                        cur_idx += 4;
                    }
                    // lock
                    LOCK_ID => {
                        if let Some(lockid) = resps.next().unwrap().lock_id() {
                            // lock id is allocated by the remote when call the lock
                            *utils::mutref::<u32>(&caller, args[cur_idx + 4]) = lockid;
                        } else {
                            // unlock, no response
                        }
                        cur_idx += 5;
                    }
                    DELETE_ID => {
                        let _ = resps.next().unwrap();
                        cur_idx += 3;
                    }
                    _ => {
                        panic!("not implemented");
                    }
                }
            }
            RECENT_KV_CACHE.insert(id, res);
            *opes_id = id;
        }
        Err(err) => {
            tracing::error!("kv batch ope error:{}", err);
        }
    }
    Ok(vec![])
}

#[host_function]
fn kv_batch_res(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let id = args[0].to_i32();

    if let Some(res) = RECENT_KV_CACHE.get(&id) {
        let args_ptr = args[1].to_i32();
        let args_len = args[2].to_i32();
        let args = utils::i32slice(&caller, args_ptr, args_len);
        let mut cur_idx = 0;
        while cur_idx < args.len() {
            let ope_idx = args[cur_idx];
            if let Some(res) = res.responses.get(ope_idx as usize) {
                if let Some(kvs) = res.common_kvs() {
                    if let Some(kv) = kvs.get(0) {
                        let slice =
                            utils::mutu8sclice(&caller, args[cur_idx + 1], kv.value.len() as i32)
                                .unwrap();
                        slice.copy_from_slice(kvs.get(0).unwrap().value.as_slice());
                    }
                } else if let Some(_lock_id) = res.lock_id() {
                    // do nothing
                } else {
                    panic!("not implemented");
                }
            }
            cur_idx += 2;
        }
    }
    Ok(vec![])
}

pub(super) struct KvFuncsRegister;

impl HostFuncRegister for KvFuncsRegister {
    fn register(&self, builder: ImportObjectBuilder) -> ImportObjectBuilder {
        builder
            .with_async_func::<KvBatchOpe, (), NeverType>("kv_batch_ope", kv_batch_ope, None)
            .unwrap()
            .with_func::<KvBatchOpe, (), NeverType>("kv_batch_res", kv_batch_res, None)
            .unwrap()
        // .with_async_func::<KvGetLenArgs, (), NeverType>("kv_get_len", kv_get_len_async, None)
        // .unwrap()
        // .with_func::<KvGetArgs, (), NeverType>("kv_get", kv_get, None)
        // .unwrap()
    }
}
