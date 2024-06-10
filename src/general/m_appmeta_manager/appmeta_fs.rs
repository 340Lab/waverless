use std::path::{Path, PathBuf};

use tokio::fs;

use super::{AppType, View};

pub struct AppMetaFs {
    view: View,
}

impl AppMetaFs {
    pub(super) fn new(view: View) -> Self {
        Self { view }
    }

    pub fn concat_app_dir(&self, app: &str) -> PathBuf {
        let sys_dir = &self.view.os().file_path;
        let app_dir = Path::new(sys_dir).join("apps").join(app);
        app_dir
    }

    /// {sys_dir} contains apps dir
    /// {sys_dir}/apps contains apps dir named by appid
    /// {sys_dir}/apps/{appid} contains app.yml and app.xxx
    /// currently support .jar or .wasm
    pub async fn get_app_type(&self, app: &str) -> Option<AppType> {
        let sys_dir = &self.view.os().file_path;
        let app_dir = Path::new(sys_dir).join("apps").join(app);

        // Check for .jar file
        let jar_path = app_dir.join("app.jar");
        if fs::metadata(&jar_path).await.is_ok() {
            return Some(AppType::Jar);
        }

        // Check for .wasm file
        let wasm_path = app_dir.join("app.wasm");
        if fs::metadata(&wasm_path).await.is_ok() {
            return Some(AppType::Wasm);
        }

        // Return None if neither file is found
        tracing::warn!("failed to find at path: {:?}", app_dir);
        None
    }
}
