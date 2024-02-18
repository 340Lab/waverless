use super::{utils, HostFuncRegister};
use crate::general::{
    kv_interface::{KVInterface, SetOptions},
    network::proto::kv::{KeyRange, KvPair},
};
use crate::worker::kv_user_client::kv_user_client;
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
type KvSetArgs = (i32, i32, i32, i32);
#[cfg_attr(target_os = "linux", async_host_function)]
async fn kv_set_async<T>(
    // caller: CallingFrame,
    // args: Vec<WasmValue>,
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32()).to_owned();
    let value = utils::u8slice(&caller, args[2].to_i32(), args[3].to_i32()).to_owned();
    let cur_app_fn = utils::current_app_fn_ctx(&caller);
    kv_user_client()
        .set_by_app_fn(vec![KvPair { key, value }], SetOptions {}, unsafe {
            cur_app_fn.0.as_ref()
        })
        .await;
    Ok(vec![])
}
#[cfg(target_os = "macos")]
fn kv_set(k_ptr: i32, k_len: i32, v_ptr: i32, v_len: i32) {
    // let
}

lazy_static::lazy_static! {

    static ref RECENT_KV_CACHE: Cache<i32, Vec<u8>>=Cache::<i32, Vec<u8>>::builder()
        .time_to_live(Duration::from_secs(10))
        .weigher(|_key, value| -> u32 { value.len().try_into().unwrap_or(u32::MAX) })
        // This cache will hold up to 32MiB of values.
        .max_capacity(32 * 1024 * 1024)
        .build();
    static ref NEXT_CACHE_ID: AtomicI32=AtomicI32::new(0);
}
// fn kv_get_len(kptr: *const u8, klen: i32, vlen: &mut i32, id: &mut i32);
type KvGetLenArgs = (i32, i32, i32, i32);
#[cfg_attr(target_os = "linux", async_host_function)]
async fn kv_get_len_async<T>(
    // caller: CallingFrame,
    // args: Vec<WasmValue>,
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
    let len = utils::mutref::<i32>(&caller, args[2].to_i32());
    let id = utils::mutref::<i32>(&caller, args[3].to_i32());
    if let Ok(mut res) = kv_user_client()
        .get(KeyRange {
            start: key.to_owned(),
            end: vec![],
        })
        .await
    {
        if res.len() > 0 {
            let KvPair { key: _, value } = res.pop().unwrap();
            let newid = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if newid < 0 {
                NEXT_CACHE_ID.store(0, std::sync::atomic::Ordering::Relaxed);
            }
            let newid = NEXT_CACHE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *len = value.len() as i32;
            *id = newid;
            RECENT_KV_CACHE.insert(newid, value);
        } else {
            *len = 0;
        }
    } else {
        *len = 0;
    }

    // kv_user_client()
    Ok(vec![])
}

#[cfg(target_os = "macos")]
fn kv_get_len(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    util::call_async_from_sync(kv_get_len_async(caller, args, std::ptr::null_mut::<()>()))
}

// fn kv_get(id: i32, vlen:  *const u8);
type KvGetArgs = (i32, i32);
#[cfg_attr(target_os = "linux", host_function)]
fn kv_get(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let id = args[0].to_i32();
    if let Some(res) = RECENT_KV_CACHE.remove(&id) {
        let retslice = utils::mutu8sclice(&caller, args[1].to_i32(), res.len() as i32).unwrap();
        retslice.copy_from_slice(res.as_slice());
    }

    Ok(vec![])
}

pub(super) struct KvFuncsRegister;

impl HostFuncRegister for KvFuncsRegister {
    fn register(&self, builder: ImportObjectBuilder) -> ImportObjectBuilder {
        builder
            .with_async_func::<KvSetArgs, (), NeverType>("kv_set", kv_set_async, None)
            .unwrap()
            .with_async_func::<KvGetLenArgs, (), NeverType>("kv_get_len", kv_get_len_async, None)
            .unwrap()
            .with_func::<KvGetArgs, (), NeverType>("kv_get", kv_get, None)
            .unwrap()
    }
}
