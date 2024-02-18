use super::{utils, HostFuncRegister};
use crate::general::fs::fs;

#[cfg(target_os = "macos")]
use wasmer::{imports, Function, FunctionType, Imports};

#[cfg(target_os = "linux")]
use wasmedge_sdk::{
    async_host_function, error::HostFuncError, host_function, Caller, ImportObjectBuilder,
    NeverType, WasmValue,
};

// fname_ptr, fname_len, fd_ptr
type OpenFileArgs = (i32, i32, i32);
#[cfg_attr(target_os = "linux", host_function)]
fn open_file(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let fname = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
    let res = utils::mutref::<i32>(&caller, args[2].to_i32());
    *res = fs().open_file(std::str::from_utf8(fname).unwrap()).unwrap();

    Ok(vec![])
}

fn read_file_at(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let fd = args[0].to_i32();
    let data = utils::mutu8sclice(&caller, args[1].to_i32(), args[2].to_i32()).unwrap();
    let offset = args[3].to_i32();
    let retlen = utils::mutref::<i32>(&caller, args[4].to_i32());
    *retlen = fs().read_file_at(fd, offset, data).unwrap() as i32;

    Ok(vec![])
}
// fd, data, len, offset, retlen_ptr
type ReadFileArgs = (i32, i32, i32, i32, i32);
#[cfg_attr(target_os = "linux", async_host_function)]
async fn read_file_at_async<T>(
    // caller: CallingFrame,
    // args: Vec<WasmValue>,
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
#[cfg(target_os = "macos")]
fn read_file_at(fd: i32, data_ptr: i32, data_len: i32, offset: i32, retlen_ptr: i32) {
    let data = utils::mutu8sclice(&caller, args[1].to_i32(), args[2].to_i32()).unwrap();
    let offset = args[3].to_i32();
    let retlen = utils::mutref::<i32>(&caller, args[4].to_i32());
    *retlen = fs().read_file_at(fd, offset, data).unwrap() as i32;
}

pub(super) struct FsFuncsRegister;

impl HostFuncRegister for FsFuncsRegister {
    fn register(&self, builder: ImportObjectBuilder) -> ImportObjectBuilder {
        builder
            .with_async_func::<ReadFileArgs, (), NeverType>(
                "read_file_at",
                read_file_at_async,
                None,
            )
            .unwrap()
            .with_func::<OpenFileArgs, (), NeverType>("open_file", open_file, None)
            .unwrap()
    }
}
