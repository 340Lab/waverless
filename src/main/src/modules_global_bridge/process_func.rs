use crate::general::app::app_shared::process_rpc::ProcessRpc;
use crate::general::app::instance::m_instance_manager::InstanceManager;
use crate::general::app::AppMetaManager;

pub trait ModulesGlobalBrigeInstanceManager: Sized + 'static {
    unsafe fn global_m_instance_manager() -> &'static InstanceManager;
}

impl ModulesGlobalBrigeInstanceManager for ProcessRpc {
    unsafe fn global_m_instance_manager() -> &'static InstanceManager {
        super::modules().instance_manager()
    }
}

pub trait ModulesGlobalBrigeAppMetaManager: Sized + 'static {
    unsafe fn global_m_app_meta_manager() -> &'static AppMetaManager;
}

impl ModulesGlobalBrigeAppMetaManager for ProcessRpc {
    unsafe fn global_m_app_meta_manager() -> &'static AppMetaManager {
        &super::modules().appmeta_manager
    }
}
