use crate::{
    general::m_appmeta_manager::AppMetaManager,
    worker::func::{m_instance_manager::InstanceManager, shared::process_rpc::ProcessRpc},
};

pub trait ModulesGlobalBrigeInstanceManager: Sized + 'static {
    unsafe fn global_m_instance_manager() -> Option<&'static InstanceManager>;
}

impl ModulesGlobalBrigeInstanceManager for ProcessRpc {
    unsafe fn global_m_instance_manager() -> Option<&'static InstanceManager> {
        super::modules().instance_manager.as_ref()
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
