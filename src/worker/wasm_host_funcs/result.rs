use super::{utils, HostFuncRegister};

#[cfg(target_os = "macos")]
use wasmer::{imports, Function, FunctionType, Imports};

#[cfg(target_os = "linux")]
use wasmedge_sdk::{
    error::HostFuncError, host_function, Caller, ImportObjectBuilder, NeverType, WasmValue,
};

// fname_ptr, fname_len, fd_ptr
type WriteResultArgs = (i32, i32);
#[cfg_attr(target_os = "linux", host_function)]
fn write_result(caller: Caller, args: Vec<WasmValue>) -> Result<Vec<WasmValue>, HostFuncError> {
    let fname = utils::u8slice(&caller, args[0].to_i32(), args[1].to_i32());
    unsafe { utils::current_app_fn_ctx(&caller).0.as_mut() }.res =
        Some(std::str::from_utf8(fname).unwrap().to_string());

    Ok(vec![])
}

pub(super) struct ResultFuncsRegister;

impl HostFuncRegister for ResultFuncsRegister {
    fn register(&self, builder: ImportObjectBuilder) -> ImportObjectBuilder {
        builder
            .with_func::<WriteResultArgs, (), NeverType>("write_result", write_result, None)
            .unwrap()
    }
}
