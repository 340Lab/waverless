use std::{path::PathBuf, str::from_utf8, time::Duration};

use tokio::process::{self, Command};

use crate::{
    general::m_os::{OperatingSystem, OsProcessType},
    result::{WSError, WSResult, WsFuncError},
};

use super::process::PID;

pub(crate) struct JavaColdStart {
    _dummy_private: (),
}

impl JavaColdStart {
    pub(crate) async fn mksure_checkpoint(appdir: PathBuf) -> Self {
        let mut i = 0;
        loop {
            // if dir not exist, continue
            if !appdir.join("checkpoint-dir").exists() {
                continue;
            }

            let checkpoint_dir = appdir.join("checkpoint-dir");

            // let lsres = Command::new("ls")
            //     .arg("-l")
            //     .arg(checkpoint_dir.to_str().unwrap())
            //     .output()
            //     .await
            //     .expect("ls failed");

            // tracing::debug!("ls checkpoint-dir output: {:?}", lsres);

            let res = Command::new("lsof")
                .arg("+D") // check all process with files in checkpoint-dir
                .arg(checkpoint_dir.to_str().unwrap())
                .output()
                .await
                .expect("lsof failed");

            tracing::debug!("lsof checkpoint-dir output: {:?}", res);

            let output = from_utf8(&res.stdout).expect("failed to parse output to string");
            if output == "" {
                break;
            }

            let sleep_time = match i {
                0 => 1000,
                1 => 500,
                _ => 200,
            };
            tokio::time::sleep(Duration::from_millis(sleep_time)).await;
            i += 1;
        }

        Self { _dummy_private: () }
    }

    pub fn direct_start() -> Self {
        Self { _dummy_private: () }
    }

    pub(crate) fn cold_start(self, app: &str, os: &OperatingSystem) -> WSResult<process::Child> {
        tracing::debug!("java cold start {}", app);
        let p = os.start_process(OsProcessType::JavaApp(app.to_owned()));
        Ok(p)
    }
}

pub(super) async fn find_pid(app: &str) -> WSResult<PID> {
    let res = Command::new("jcmd")
        .arg("-l")
        .output()
        .await
        .map_err(|e| WSError::from(WsFuncError::InstanceProcessStartFailed(e)))?;
    let res = from_utf8(&res.stdout).expect("failed to parse output to string");
    let res = res.split(|x| x == '\n').collect::<Vec<_>>();
    tracing::debug!("jcmd output: {:?}", res);
    let err = || Err(WsFuncError::InstanceJavaPidNotFound(app.to_owned()).into());
    let Some(res) = res
        .iter()
        .filter(|x| x.contains(&format!("--appName={}", app)))
        .next()
    else {
        return err();
    };
    let Some(res) = res.split(|x| x == ' ').next() else {
        return err();
    };
    let Ok(pid) = res.parse::<PID>() else {
        return err();
    };
    Ok(pid)
}

pub async fn take_snapshot(app: &str, os: &OperatingSystem) {
    let res = os
        .start_process(OsProcessType::JavaCheckpoints(app.to_owned()))
        .wait()
        .await
        .unwrap();
    assert!(res.success());
}
