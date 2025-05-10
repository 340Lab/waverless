use std::time::Duration;

use tokio::process::Command;

use crate::general::app::instance::m_instance_manager::EachAppCache;
use crate::{
    general::app::app_shared::java,
    general::app::app_shared::process::ProcessInstance,
    general::app::app_shared::SharedInstance,
    general::app::instance::m_instance_manager::InstanceManager,
    general::app::AppType,
    result::{WSResult, WsFuncError},
};

impl InstanceManager {
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
                    {
                        // let view = self.view.clone();
                        // let app = app.to_owned();
                        // let instance = instance.clone();

                        let p = java::JavaColdStart::direct_start()
                            .cold_start(&app, self.view.os())
                            .unwrap();
                        instance.bind_process(p);
                    }

                    // TODO Q1: instance lives forever?
                    // maybe start ttl when verified

                    EachAppCache::Shared(instance.into())
                }
                AppType::Wasm => panic!("wasm only support owned instance"),
                AppType::Native => panic!("native only support owned instance"),
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
