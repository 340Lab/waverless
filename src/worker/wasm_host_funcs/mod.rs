#[cfg(target_os = "linux")]
use wasmedge_sdk::{ImportObject, ImportObjectBuilder, NeverType};
mod fs;
mod kv;

use fs::FsFuncsRegister;
use kv::KvFuncsRegister;

mod utils {

    use std::ptr::NonNull;

    use wasmedge_sdk::{Caller, CallingFrame, Instance, Memory};

    use crate::{
        util::SendNonNull,
        worker::{executor::FunctionCtx, kv_user_client::kv_user_client},
    };

    trait WasmCtx {
        fn memory(&self, idx: u32) -> Option<Memory>;
        fn instance(&self) -> Option<&Instance>;
    }

    impl WasmCtx for Caller {
        fn memory(&self, idx: u32) -> Option<Memory> {
            self.memory(idx)
        }
        fn instance(&self) -> Option<&Instance> {
            self.instance()
        }
    }

    impl WasmCtx for CallingFrame {
        fn memory(&self, idx: u32) -> Option<Memory> {
            self.memory(idx)
        }
        fn instance(&self) -> Option<&Instance> {
            self.instance()
        }
    }

    pub fn u8slice<'a>(caller: &impl WasmCtx, ptr: i32, len: i32) -> &'a [u8] {
        // tracing::debug!("u8slice ptr: {}, len: {}", ptr, len);
        let mem = caller
            .memory(0)
            .unwrap()
            .data_pointer(ptr as u32, len as u32)
            .unwrap();
        let res = unsafe { std::slice::from_raw_parts(mem, len as usize) };
        res
    }

    pub fn i32slice<'a>(caller: &impl WasmCtx, ptr: i32, len: i32) -> &'a [i32] {
        let mem = caller
            .memory(0)
            .unwrap()
            .data_pointer(ptr as u32, len as u32)
            .unwrap();
        unsafe { std::slice::from_raw_parts(mem as *const i32, len as usize) }
    }

    pub fn mutu8sclice<'a>(caller: &impl WasmCtx, ptr: i32, len: i32) -> Option<&'a mut [u8]> {
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

    pub fn mutref<'a, T: Sized>(caller: &impl WasmCtx, ptr: i32) -> &'a mut T {
        unsafe {
            &mut *(caller
                .memory(0)
                .unwrap()
                .data_pointer_mut(ptr as u32, std::mem::size_of::<T>() as u32)
                .unwrap() as *mut T)
        }
    }

    pub fn current_app_fn_ctx(caller: &impl WasmCtx) -> SendNonNull<FunctionCtx> {
        let app_fn = SendNonNull(NonNull::from(
            kv_user_client()
                .view
                .instance_manager()
                .instance_running_function
                .read()
                .get(&caller.instance().unwrap().name().unwrap())
                .unwrap(),
        ));
        app_fn
    }
}

// #[cfg(target_os = "macos")]
// pub fn new_import_obj(store: &mut Store) -> Imports {
//     imports! {
//         "env" => {
//             "kv_set" => Function::new_typed(store, kv_set),
//             "kv_get_len" => Function::new_typed(store, kv_get_len(caller, args)),
//             "kv_get" => kv_get,
//             "open_file" => open_file,
//             "read_file_at" => read_file_at,
//         }
//     }
// }

trait HostFuncRegister {
    fn register(&self, builder: ImportObjectBuilder) -> ImportObjectBuilder;
}

#[cfg(target_os = "linux")]
pub fn new_import_obj() -> ImportObject {
    let builder = ImportObjectBuilder::new();
    let builder = KvFuncsRegister {}.register(builder);
    let builder = FsFuncsRegister {}.register(builder);

    builder.build::<NeverType>("env", None).unwrap()
}
