use crate::general::m_os::{OperatingSystem, OsProcessType};

pub(super) fn cold_start(app: &str, os: &OperatingSystem) {
    os.start_process(OsProcessType::JavaApp(app.to_owned()));
}
