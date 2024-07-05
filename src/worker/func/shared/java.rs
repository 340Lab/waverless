use std::str::from_utf8;

use tokio::process::{self, Command};

use crate::{
    general::m_os::{OperatingSystem, OsProcessType},
    result::{WSError, WSResult, WsFuncError},
};

use super::process::PID;

pub(super) fn cold_start(app: &str, os: &OperatingSystem) -> WSResult<process::Child> {
    tracing::debug!("java cold start {}", app);
    let p = os.start_process(OsProcessType::JavaApp(app.to_owned()));
    // .filter(|x| x.starts_with(app))
    // .next()
    // .expect("no pid found")
    // .split(|x| x == ' ')
    // .next()
    // .expect("no pid found")
    // .parse()
    // .expect("failed to parse pid");
    Ok(p)
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

pub(super) async fn take_snapshot(app: &str, os: &OperatingSystem) {
    let res = os
        .start_process(OsProcessType::JavaCheckpoints(app.to_owned()))
        .wait()
        .await
        .unwrap();
    assert!(res.success());
}
