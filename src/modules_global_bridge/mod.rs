use crate::sys::{LogicalModules, LogicalModulesRef};

pub mod process_func;

lazy_static::lazy_static! {
    static ref MODULES: Option<LogicalModulesRef>=None;
}

fn modules() -> &'static LogicalModules {
    unsafe {
        (&*MODULES.as_ref().unwrap().inner.as_ptr())
            .as_ref()
            .unwrap()
    }
}

pub fn set_singleton_modules(modules: LogicalModulesRef) {
    // *utils::MODULES = Some(modules);
    unsafe {
        *(&*MODULES as *const _ as *mut _) = Some(modules);
    }
}
