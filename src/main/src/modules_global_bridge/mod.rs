use std::future::Future;

use crate::result::WSResult;
use crate::sys::LogicalModules;
use crate::sys::LogicalModulesRef;

pub mod process_func;

lazy_static::lazy_static! {
    static ref MODULES: Option<LogicalModulesRef>=None;
}

tokio::task_local! {
    static MODULES_REF: LogicalModulesRef;
}

pub fn try_get_modules_ref() -> WSResult<LogicalModulesRef> {
    let mut res=Err(WSError::WsRuntimeErr(WsRuntimeErr::ModulesRefOutofLifetime));
    MODULES_REF.try_with(|m|{
        res=Ok(m.clone());
    });
    res
}

pub fn modules_ref_scope(modules_ref: LogicalModulesRef,future: impl Future) {
    MODULES_REF.scope(modules_ref,future);
}

fn modules() -> &'static LogicalModules {
    #[cfg(feature = "unsafe-log")]
    tracing::debug!("modules begin");
    let res = unsafe {
        (&*MODULES.as_ref().unwrap().inner.as_ptr())
            .as_ref()
            .unwrap()
    };
    #[cfg(feature = "unsafe-log")]
    tracing::debug!("modules end");
    res
}

pub fn set_singleton_modules(modules: LogicalModulesRef) {
    // *utils::MODULES = Some(modules);
    tracing::debug!("set_singleton_modules begin");
    unsafe {
        *(&*MODULES as *const _ as *mut _) = Some(modules);
    };
    tracing::debug!("set_singleton_modules end");
}
