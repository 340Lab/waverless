use crate::general::m_os::{OperatingSystem, OsProcessType};

pub(super) fn cold_start(app: &str, os: &OperatingSystem) {
    tracing::debug!("java cold start {}", app);
    let _ = os.start_process(OsProcessType::JavaApp(app.to_owned()));
}

pub(super) async fn take_snapshot(app: &str, os: &OperatingSystem) {
    let _ = os
        .start_process(OsProcessType::JavaCheckpoints(app.to_owned()))
        .wait()
        .await;
}
