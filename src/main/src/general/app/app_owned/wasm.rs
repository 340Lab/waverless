use crate::general::app::app_owned::wasm_host_funcs;
use crate::general::app::instance::m_instance_manager::InstanceManager;
use crate::general::app::instance::InstanceTrait;
use crate::general::app::instance::OwnedInstance;
use crate::general::app::m_executor::{EventCtx, FnExeCtxAsync, FnExeCtxBase, FnExeCtxSync};
use crate::result::{WSResult, WsFuncError};
use async_trait::async_trait;
use std::{mem::ManuallyDrop, path::Path};
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    r#async::AsyncState,
    Module, VmBuilder,
};
use wasmedge_sdk::{Vm, WasmValue};

#[cfg(target_os = "linux")]
pub type WasmInstance = Vm;

impl EventCtx {
    pub fn conv_to_wasm_params(&self, vm: &WasmInstance) -> Vec<WasmValue> {
        fn prepare_vec_in_vm(vm: &WasmInstance, v: &[u8]) -> (i32, i32) {
            let vm_ins = vm.instance_name();
            let ptr = vm
                .run_func(
                    Some(&vm_ins),
                    "allocate",
                    vec![WasmValue::from_i32(v.len() as i32)],
                )
                .unwrap_or_else(|err| {
                    panic!("err:{:?} vm instance names:{:?}", err, vm.instance_names())
                })[0]
                .to_i32();
            let mut mem = ManuallyDrop::new(unsafe {
                Vec::from_raw_parts(
                    vm.named_module(&vm_ins)
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
        match self {
            EventCtx::Http(text) => {
                // if text.len() == 0 {
                //     return vec![];
                // }
                let (ptr, len) = prepare_vec_in_vm(vm, text.as_bytes());
                vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)]
            }
            // (EventCtx::KvSet { key, .. }) => {
            //     let (ptr, len) = prepare_vec_in_vm(vm, &key);
            //     vec![WasmValue::from_i32(ptr), WasmValue::from_i32(len)]
            // }
            e => panic!("not support event ctx and fn arg: {:?}", e),
        }
    }
}

#[async_trait]
impl InstanceTrait for WasmInstance {
    fn instance_name(&self) -> String {
        self.instance_names()
            .into_iter()
            .filter(|v| &*v != "env" && &*v != "wasi_snapshot_preview1")
            .next()
            .unwrap()
    }
    async fn execute(
        &self,
        _instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> WSResult<Option<String>> {
        #[cfg(target_os = "linux")]
        {
            let mut final_err = None;
            let mut instance_name_for_first_time_init = None;
            if self.active_module().is_err() {
                instance_name_for_first_time_init = Some(self.instance_name())
            }

            // retry loop
            let mut params = fn_ctx.event_ctx().conv_to_wasm_params(&self);
            for turn in 0..2 {
                let func = fn_ctx.func().to_owned();
                let Err(err) = self
                    .run_func_async(
                        &AsyncState::new(),
                        instance_name_for_first_time_init.as_ref().map(|s| &**s),
                        func,
                        std::mem::take(&mut params),
                    )
                    .await
                else {
                    final_err = None;
                    break;
                };

                fn is_func_type_mismatch(err: &wasmedge_sdk::error::WasmEdgeError) -> bool {
                    match &*err {
                        wasmedge_sdk::error::WasmEdgeError::Core(
                            wasmedge_sdk::error::CoreError::Execution(
                                wasmedge_sdk::error::CoreExecutionError::FuncTypeMismatch,
                            ),
                        ) => true,
                        _ => false,
                    }
                }

                if turn == 0 && fn_ctx.empty_http() && is_func_type_mismatch(&err) {
                    fn_ctx.set_result(None);
                    continue;
                } else {
                    tracing::error!("run func failed with err: {}", err);
                    final_err = Some(err);
                    break;
                }
            }
            if let Some(err) = final_err {
                Err(WsFuncError::WasmError(err).into())
            } else {
                Ok(fn_ctx.take_result())
            }
        }
    }

    /// WASM instances don't support synchronous execution
    /// See [`FnExeCtxSyncAllowedType`] for supported types (currently only Native)
    fn execute_sync(
        &self,
        _instman: &InstanceManager,
        _fn_ctx: &mut FnExeCtxSync,
    ) -> WSResult<Option<String>> {
        Err(WsFuncError::UnsupportedAppType.into())
    }
}

// pub fn new_java_instance(_config: NewJavaInstanceConfig) -> ProcessInstance {}

pub fn new_wasm_instance(
    file_dir: impl AsRef<Path>,
    instance_name: &str,
    id: u64,
) -> OwnedInstance {
    let config = ConfigBuilder::new(CommonConfigOptions::default())
        .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
        .build()
        .expect("failed to create config");
    let module = Module::from_file(
        Some(&config),
        file_dir
            .as_ref()
            .join(format!("apps/{}/app.wasm", instance_name)),
    )
    .unwrap();
    let import = wasm_host_funcs::new_import_obj();
    let vm = VmBuilder::new()
        .with_config(config)
        // .with_wasi_context(WasiContext::default())
        .build()
        .unwrap_or_else(|err| panic!("failed to create vm: {:?}", err));
    let vm = vm.register_import_module(import).unwrap();
    let vm = vm
        .register_module(Some(&format!("{}{}", instance_name, id)), module)
        .unwrap();
    return OwnedInstance::WasmInstance(vm);
}

// #[cfg(target_os = "macos")]
// impl Clone for WasmInstance {
//     fn clone(&self) -> Self {
//         tracing::warn!("cloning wasm instance, which is not supposed to happen");
//         Self {
//             instance: self.instance.clone(),
//             store: None,
//         }
//     }
// }
pub trait WasmEngine {
    fn run_function(
        &self,
        instance: &mut WasmInstance,
        func_name: &str,
        args: Vec<WasmValue>,
    ) -> WSResult<Vec<WasmValue>>;
}

#[cfg(target_os = "macos")]
pub struct WasmerEngine;

#[cfg(target_os = "macos")]
impl WasmEngine for WasmerEngine {
    fn run_function(
        &self,
        instance: &mut WasmInstance,
        func_name: &str,
        args: Vec<WasmValue>,
    ) -> WSResult<Vec<WasmValue>> {
        instance
            .instance
            .exports
            .get_function(func_name)
            .unwrap()
            .call(instance.store.as_mut().unwrap(), &*args)
            .map(|v| {
                let v = ManuallyDrop::new(v);
                unsafe { Vec::from_raw_parts(v.as_ptr() as *mut WasmValue, v.len(), v.len()) }
            })
            .map_err(|err| crate::result::WSError::NotImplemented)
    }
}
