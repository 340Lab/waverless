use crate::sys::{LogicalModules, LogicalModulesRef};

pub mod process_func;

lazy_static::lazy_static! {
    static ref MODULES: Option<LogicalModulesRef>=None;
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
