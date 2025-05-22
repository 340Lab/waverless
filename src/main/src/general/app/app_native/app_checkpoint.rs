// use std::process::Command;
use std::time::Duration;

use futures::TryFutureExt;
use tokio::process::Command;

use super::NativeAppFunc;
use crate::general::app::app_shared::{java, SharedInstance};
use crate::general::app::{AppType, InstanceManager};
use crate::general::data::m_data_general::parse_appname_from_data_uid;
use crate::{
    general::app::m_executor::{EventCtx, FnExeCtxAsync},
    result::{WSResult, WsFuncError},
};

// pub fn function_checkpoint(fn_ctx: &mut FnExeCtxAsync) -> WSResult<Option<String>> {}

pub struct FunctionAppCheckpoint;

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
        // remove app checkpoint-dir
        let app_dir = self.view.os().app_path(app);
        let _ = std::fs::remove_dir_all(app_dir.join("checkpoint-dir"));
        let _ = std::fs::remove_file(app_dir.join("checkpoint.log"));

        let p = self.get_process_instance(&AppType::Jar, app);
        let _ = p.wait_for_verify().await;
        tracing::debug!("wait_for_verify done2");
        tokio::time::sleep(Duration::from_secs(3)).await;

        self.update_checkpoint(app, false).await?;
        Ok(())
    }
}

impl NativeAppFunc for FunctionAppCheckpoint {
    async fn execute(
        instman: &InstanceManager,
        fn_ctx: &mut FnExeCtxAsync,
    ) -> WSResult<Option<String>> {
        tracing::debug!("native app FunctionAppCheckpoint");
        // first we get the data unique id, and read it
        match fn_ctx.event_ctx_mut() {
            EventCtx::KvSet { key, .. } => {
                tracing::debug!("native app FunctionAppCheckpoint kv set triggered");
                let Some(appname) = parse_appname_from_data_uid(key) else {
                    return Err(WsFuncError::FuncTriggerAppInvalid {
                        key: key.to_vec(),
                        appmeta: None,
                        context: "native app FunctionAppCheckpoint the trigger app key of app_checkpoint is invalid".to_string(),
                    }
                    .into());
                };
                let appmeta_ = instman
                    .view
                    .appmeta_manager()
                    .get_app_meta(&appname)
                    .await
                    .map_err(|err| {
                        tracing::error!("native app FunctionAppCheckpoint get app meta failed to checkpoint err: {:?}", err);
                        err
                    })?;
                let Some((appmeta, Some(datameta))) = appmeta_ else {
                    return Err(WsFuncError::FuncTriggerAppInvalid {
                        key: key.to_vec(),
                        appmeta: Some((appname.to_string(), appmeta_.clone())),
                        context: "native app FunctionAppCheckpoint app meta not found".to_string(),
                    }
                    .into());
                };
                tracing::debug!(
                    "native app FunctionAppCheckpoint load appmeta done, load app file start"
                );
                instman
                    .view
                    .appmeta_manager()
                    .load_app_file(&appname, datameta)
                    .await
                    .map_err(|err| {
                        tracing::error!("native app FunctionAppCheckpoint load app file failed to checkpoint err: {:?}", err);
                        err
                    })?;
                tracing::debug!("native app FunctionAppCheckpoint load app file done");
                // start checkpoint
                if appmeta.app_type == AppType::Jar {
                    instman
                        .make_checkpoint_for_app(&appname)
                        .await
                        .map_err(|err| {
                            tracing::error!("native app FunctionAppCheckpoint make checkpoint for app failed: {:?}", err);
                            err
                        })?;
                }
            }
            _ => {
                tracing::debug!(
                    "native app FunctionAppCheckpoint::execute not supported http calling"
                );
                return Err(WsFuncError::InvalidTriggerForAppFunction {
                    app: fn_ctx.app_name().to_string(),
                    func: fn_ctx.func_name().to_string(),
                    trigger_type: fn_ctx.event_ctx().clone(),
                }
                .into());
            }
        };

        // then we start checkpoint

        Ok(None)
    }
}
