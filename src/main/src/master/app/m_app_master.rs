use crate::general::app::AppMetaManager;
use crate::logical_module_view_impl;
use crate::master::app::fddg::FDDGMgmt;
use crate::result::WSResult;
use crate::sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef};
use crate::util::JoinHandleWrapper;
use async_trait::async_trait;
use ws_derive::LogicalModule;

logical_module_view_impl!(MasterAppMgmtView);
// access general app
logical_module_view_impl!(MasterAppMgmtView, appmeta_manager, AppMetaManager);

#[derive(LogicalModule)]
pub struct MasterAppMgmt {
    view: MasterAppMgmtView,
    pub fddg: FDDGMgmt,
}

#[async_trait]
impl LogicalModule for MasterAppMgmt {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MasterAppMgmtView::new(args.logical_modules_ref.clone()),
            fddg: FDDGMgmt::new(),
        }
    }

    async fn init(&self) -> WSResult<()> {
        self.load_apps().await?;
        Ok(())
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
}

impl MasterAppMgmt {
    async fn load_apps(&self) -> WSResult<()> {
        // load app triggers to fddg
        // - for each native apps
        for (app_name, app_meta) in &self.view.appmeta_manager().native_apps {
            for (fn_name, fn_meta) in app_meta.fns.iter() {
                self.fddg
                    .add_fn_trigger((&app_name, app_meta.app_type), (&fn_name, &fn_meta))?;
            }
        }

        // - for each existing apps

        Ok(())
    }
}
