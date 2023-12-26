use std::{sync::atomic::AtomicI32, time::Duration};

use crate::{
    fs::fs,
    kv::{
        dyn_kv::client::dyn_kv_client,
        kv_interface::{KVInterface, SetOptions},
    },
    network::proto::kv::{KeyRange, KvPair},
};
use moka::sync::Cache;
use wasmedge_sdk::{
    async_host_function, error::HostFuncError, host_function, Caller, ImportObject,
    ImportObjectBuilder, NeverType, WasmValue,
};

mod utils {
    use std::sync::Arc;

    use wasmedge_sdk::Caller;

    use crate::kv::dyn_kv::client::dyn_kv_client;

    pub fn u8slice<'a>(caller: &Caller, ptr: i32, len: i32) -> &'a [u8] {
        // tracing::info!("getting u8 slice");
        let mem = caller
            .memory(0)
            .unwrap()
            .data_pointer(ptr as u32, len as u32)
            .unwrap();
        let res = unsafe { std::slice::from_raw_parts(mem, len as usize) };
        // tracing::info!("got u8 slice");
        res
    }

    pub fn mutu8sclice<'a>(caller: &Caller, ptr: i32, len: i32) -> Option<&'a mut [u8]> {
        if let Ok(mem) = caller
            .memory(0)
            .unwrap()
            .data_pointer_mut(ptr as u32, len as u32)
        {
            Some(unsafe { std::slice::from_raw_parts_mut(mem, len as usize) })
        } else {
            None
        }
    }

    pub fn mutref<'a, T: Sized>(caller: &Caller, ptr: i32) -> &'a mut T {
        unsafe {
            &mut *(caller
                .memory(0)
                .unwrap()
                .data_pointer_mut(ptr as u32, std::mem::size_of::<T>() as u32)
                .unwrap() as *mut T)
        }
    }

    pub fn current_app_fn(caller: &Caller) -> Arc<(String, String)> {
        dyn_kv_client()
            .view
            .container_manager()
            .instance_running_function
            .read()
            .get(&caller.instance().unwrap().name().unwrap())
            .unwrap()
            .clone()
    }
}

// fn kv_set(kptr: *const u8, klen: i32, v: *const u8, vlen: i32);
type KvSetArgs = (i32, i32, i32, i32);
#[async_host_function]
async fn kv_set<T>(
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32()).to_owned();
    let value = utils::u8slice(&caller, args[2].to_i32(), args[3].to_i32()).to_owned();
    let cur_app_fn = utils::current_app_fn(&caller);
    // tracing::info!(
    //     "host called: kv_set {} {}",
    //     std::str::from_utf8(&*key).unwrap(),
    //     std::str::from_utf8(&*value).unwrap()
    // );
    dyn_kv_client()
        .set_by_app_fn(
            vec![KvPair { key, value }],
            SetOptions {},
            &cur_app_fn.0,
            &cur_app_fn.1,
        )
        .await;
    // let dyn_kv_client = dyn_kv_client();

    Ok(vec![])
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
#[async_host_function]
async fn kv_get_len<T>(
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    let key = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
    let len = utils::mutref::<i32>(&caller, args[2].to_i32());
    let id = utils::mutref::<i32>(&caller, args[3].to_i32());
    if let Ok(mut res) = dyn_kv_client()
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

    // dyn_kv_client()
    Ok(vec![])
}

// fn kv_get(id: i32, vlen:  *const u8);
type KvGetArgs = (i32, i32);
#[host_function]
fn kv_get(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let id = args[0].to_i32();
    if let Some(res) = RECENT_KV_CACHE.remove(&id) {
        let retslice = utils::mutu8sclice(&caller, args[1].to_i32(), res.len() as i32).unwrap();
        retslice.copy_from_slice(res.as_slice());
    }

    Ok(vec![])
}

// fname_ptr, fname_len, fd_ptr
type OpenFileArgs = (i32, i32, i32);
#[host_function]
fn open_file(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let fname = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
    let res = utils::mutref::<i32>(&caller, args[2].to_i32());
    *res = fs().open_file(std::str::from_utf8(fname).unwrap()).unwrap();

    Ok(vec![])
}

// fd, data, len, offset, retlen_ptr
type ReadFileArgs = (i32, i32, i32, i32, i32);
#[async_host_function]
async fn read_file_at<T>(
    caller: Caller,
    args: Vec<WasmValue>,
    _ctx: *mut T,
) -> Result<Vec<WasmValue>, HostFuncError> {
    tokio::task::spawn_blocking(move || {
        let fd = args[0].to_i32();
        let data = utils::mutu8sclice(&caller, args[1].to_i32(), args[2].to_i32()).unwrap();
        let offset = args[3].to_i32();
        let retlen = utils::mutref::<i32>(&caller, args[4].to_i32());
        *retlen = fs().read_file_at(fd, offset, data).unwrap() as i32;
    })
    .await
    .unwrap();

    Ok(vec![])
}

pub fn new_import_obj() -> ImportObject {
    ImportObjectBuilder::new()
        .with_async_func::<KvSetArgs, (), NeverType>("kv_set", kv_set, None)
        .unwrap()
        .with_async_func::<KvGetLenArgs, (), NeverType>("kv_get_len", kv_get_len, None)
        .unwrap()
        .with_func::<KvGetArgs, (), NeverType>("kv_get", kv_get, None)
        .unwrap()
        .with_func::<OpenFileArgs, (), NeverType>("open_file", open_file, None)
        .unwrap()
        .with_async_func::<ReadFileArgs, (), NeverType>("read_file_at", read_file_at, None)
        .unwrap()
        .build::<NeverType>("env", None)
        .unwrap()
}
