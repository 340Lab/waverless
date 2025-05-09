use crate::result::WSError; //虞光勇修改，修改内容：增加use crate::result::WSError;来导入 WSError。
use crate::result::WSResult;
use crate::result::WsRuntimeErr; //虞光勇修改，修改内容：增加use crate::result::WsRuntimeErr;来导入 WsRuntimeErr。
use crate::sys::LogicalModules;
use crate::sys::LogicalModulesRef;
use std::future::Future;

pub mod process_func;

lazy_static::lazy_static! {
    static ref MODULES: Option<LogicalModulesRef>=None;
}

tokio::task_local! {
    static MODULES_REF: LogicalModulesRef;
}

// pub fn try_get_modules_ref() -> WSResult<LogicalModulesRef> {
//     //没有处理try_wth的错误返回    曾俊
//     // let mut res=Err(WSError::WsRuntimeErr(WsRuntimeErr::ModulesRefOutofLifetime));
//     // MODULES_REF.try_with(|m|{
//     //     res=Ok(m.clone());
//     // });
//     // res

//     MODULES_REF.try_with(|m| {
//         // 克隆 m 并返回 Ok 结果
//         Ok(m.clone())
//     })
//     // 如果 try_with 失败，则返回相应的错误
//     .map_err(|_e| WSError::WsRuntimeErr(WsRuntimeErr::ModulesRefOutofLifetime))?
// }

//没有处理scope的返回值    曾俊
// pub fn modules_ref_scope(modules_ref: LogicalModulesRef,future: impl Future) {
//     MODULES_REF.scope(modules_ref,future);
// }
pub async fn modules_ref_scope<F>(modules_ref: LogicalModulesRef, future: F)
where
    F: Future + 'static,
{
    MODULES_REF
        .scope(modules_ref, async move {
            let _ = future.await;
        })
        .await;
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
