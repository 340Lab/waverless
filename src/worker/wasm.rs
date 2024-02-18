use crate::result::WSResult;

#[cfg(target_os = "macos")]
use std::mem::ManuallyDrop;
#[cfg(target_os = "macos")]
// pub type WasmInstance = (wasmer::Instance, wasmer::Store);
pub struct WasmInstance {
    pub instance: wasmer::Instance,
    pub store: Option<wasmer::Store>,
}
#[cfg(target_os = "macos")]
impl Clone for WasmInstance {
    fn clone(&self) -> Self {
        tracing::warn!("cloning wasm instance, which is not supposed to happen");
        Self {
            instance: self.instance.clone(),
            store: None,
        }
    }
}
#[cfg(target_os = "macos")]
pub type WasmValue = wasmer::Value;

#[cfg(target_os = "linux")]
pub type WasmInstance = wasmedge_sdk::Vm;
#[cfg(target_os = "linux")]
pub type WasmValue = wasmedge_sdk::WasmValue;

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
