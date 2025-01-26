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
    pub async fn update_checkpoint(&self, app_name: &str, restart: bool) -> WSResult<()> {
        async fn debug_port_left() {
            tracing::debug!("debug port left");
            // only for test

            let _ = Command::new("lsof")
                .arg("-i:8080")
                .spawn()
                .expect("lsof failed")
                .wait()
                .await
                .unwrap();
        }
        let Some(instance) = self.app_instances.get(app_name) else {
            tracing::warn!("InstanceNotFound when update checkpoint, {}", app_name);
            return Err(WsFuncError::InstanceNotFound(app_name.to_owned()).into());
        };
        let Some(SharedInstance(ref proc_ins)) = instance.value().as_shared() else {
            tracing::warn!("InstanceTypeNotMatch when update checkpoint, {}", app_name);
            return Err(WsFuncError::InstanceTypeNotMatch {
                app: app_name.to_owned(),
                want: "shared".to_owned(),
            }
            .into());
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
                AppType::Wasm | AppType::Native => {
                    panic!("wasm/native can't take snapshot")
                }
            }
        }
        // recover by criu
        // tokio::time::sleep(Duration::from_secs(3)).await;

        tracing::debug!("restart app after snapshot: {}", app_name);
        let res = java::JavaColdStart::mksure_checkpoint(self.view.os().app_path(app_name))
            .await
            .cold_start(app_name, self.view.os());
        let p = match res {
            Err(e) => {
                tracing::warn!("cold start failed: {:?}", e);
                return Err(e);
            }
            Ok(ok) => ok,
        };
        // just update the process in old instance; because the old is dead;
        // let pid = java::wait_for_pid(app_name).await?;
        proc_ins.bind_process(p);
        let _ = proc_ins.wait_for_verify().await;
        tracing::debug!("wait_for_verify done1");
        if !restart {
            tracing::debug!("don't restart after checkpoint, kill it");

            let _ = proc_ins.kill().await;
            debug_port_left().await;
            // remove instance
            let _ = self.app_instances.remove(app_name);
        }

        Ok(())
    }

    pub async fn make_checkpoint_for_app(&self, app: &str) -> WSResult<()> {
        tracing::debug!("make checkpoint for app: {}", app);
        let p = self.get_process_instance(&AppType::Jar, app);
        let _ = p.wait_for_verify().await;
        tracing::debug!("wait_for_verify done2");
        tokio::time::sleep(Duration::from_secs(3)).await;

        self.update_checkpoint(app, false).await?;
        Ok(())
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
