use std::time::Duration;

use crate::{
    general::m_appmeta_manager::AppType,
    worker::func::{
        m_instance_manager::{EachAppCache, InstanceManager},
        shared::java,
    },
};

use super::{process::ProcessInstance, SharedInstance};

impl InstanceManager {
    pub async fn update_checkpoint(&self, app_name: &str) {
        let Some(instance) = self.app_instances.get(app_name) else {
            tracing::warn!("app not found when update checkpoint, {}", app_name);
            return;
        };
        let Some(SharedInstance(ref proc_ins)) = instance.value().as_shared() else {
            tracing::warn!("app not found when update checkpoint, {}", app_name);
            return;
        };
        // state 2 connecting, make others wait
        {
            proc_ins.before_checkpoint();
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
        // take snap shot
        {
            tracing::debug!("taking snapshot for app: {}", app_name);
            match proc_ins.app_type {
                AppType::Jar => java::take_snapshot(app_name, self.view.os()).await,
                AppType::Wasm => unreachable!(),
            }
        }
        // recover by criu
        {
            tokio::time::sleep(Duration::from_secs(3)).await;
            tracing::debug!("restart app after snapshot: {}", app_name);
            java::cold_start(app_name, self.view.os());
        }
    }

    /// # Panics
    /// We call it when we alreay know it's a process
    ///
    /// So panics will happen if the previous logic is wrong
    pub fn get_process_instance(&self, app_type: &AppType, app: &str) -> ProcessInstance {
        let instance = self.app_instances.get_or_insert_with(app.to_owned(), || {
            // Cold start
            match app_type {
                AppType::Jar => {
                    let instance = ProcessInstance::new(app.to_owned(), AppType::Jar);
                    let _ = self.app_instances.insert(
                        app.to_owned(),
                        EachAppCache::Shared(SharedInstance(instance.clone())),
                    );
                    // Q2: what if when the verify comes before the insert?
                    // we should insert the instance before cold start
                    java::cold_start(app, self.view.os());

                    // TODO Q1: instance lives forever?
                    // maybe start ttl when verified

                    EachAppCache::Shared(instance.into())
                }
                AppType::Wasm => unreachable!("wasm only support owned instance"),
            }
        });

        return match instance.value() {
            // if it's a process instance, we just return it
            EachAppCache::Owned(_) => {
                unreachable!("not a process instance")
            }
            EachAppCache::Shared(shared) => shared.0.clone(),
        };
    }
}
