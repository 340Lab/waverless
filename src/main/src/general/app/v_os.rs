use super::{AppMeta, AppMetaYaml, AppType, View};
use crate::result::{WSResult, WsFuncError};
use std::path::{Path, PathBuf};
use tokio::fs;
use uuid::Uuid;

pub struct AppMetaVisitOs {
    view: View,
}

impl AppMetaVisitOs {
    pub(super) fn new(view: View) -> Self {
        Self { view }
    }

    pub fn crac_file_path(&self) -> PathBuf {
        self.view
            .os()
            .file_path
            .clone()
            .join("apps")
            .join("crac_config")
    }

    pub fn concat_app_dir(&self, app: &str) -> PathBuf {
        self.view.os().file_path.clone().join("apps").join(app)
    }

    pub fn app_dir(&self) -> PathBuf {
        self.view.os().file_path.clone().join("apps")
    }

    pub async fn read_app_meta(&self, app: &str) -> WSResult<AppMeta> {
        let app_dir = self.concat_app_dir(app);
        let yml_dir = app_dir.join("app.yml");
        let ymlcontent = fs::read_to_string(yml_dir).await;
        let ymlcontent = match ymlcontent {
            Err(e) => {
                tracing::warn!("app pack conf read invalid, {:?}", e);
                return Err(WsFuncError::AppPackConfReadInvalid(e).into());
            }
            Ok(ok) => ok,
        };
        let yml = serde_yaml::from_str::<AppMetaYaml>(&ymlcontent);
        let yml = match yml {
            Err(e) => {
                tracing::warn!("app pack conf decode invalid, {:?}", e);
                return Err(WsFuncError::AppPackConfDecodeErr(e).into());
            }
            Ok(ok) => ok,
        };
        AppMeta::new_from_yaml(yml, app, self).await
    }

    pub async fn get_app_type_in_dir(&self, app_dir: impl AsRef<Path>) -> WSResult<AppType> {
        // Check for .jar file
        let jar_path = app_dir.as_ref().join("app.jar");
        if fs::metadata(&jar_path).await.is_ok() {
            return Ok(AppType::Jar);
        }

        // Check for .wasm file
        let wasm_path = app_dir.as_ref().join("app.wasm");
        if fs::metadata(&wasm_path).await.is_ok() {
            return Ok(AppType::Wasm);
        }

        // Return None if neither file is found
        tracing::warn!("failed to find at path: {:?}", app_dir.as_ref());
        Err(WsFuncError::AppPackNoExe.into())
    }

    /// {sys_dir} contains apps dir
    /// {sys_dir}/apps contains apps dir named by appid
    /// {sys_dir}/apps/{appid} contains app.yml and app.xxx
    /// currently support .jar or .wasm
    pub async fn get_app_type(&self, app: &str) -> WSResult<AppType> {
        let sys_dir = &self.view.os().file_path;
        let app_dir = Path::new(sys_dir).join("apps").join(app);

        self.get_app_type_in_dir(app_dir).await
    }

    pub async fn prepare_tmp_app_dir(&self, app: &str) -> String {
        let sys_dir = &self.view.os().file_path;
        let app_dir = Path::new(sys_dir).join("apps");
        let tmp_app = format!("{}_tmp{}", app, Uuid::new_v4());
        let tmp_dir = app_dir.join(&tmp_app);
        // clean tmp
        let _ = fs::remove_dir_all(&tmp_dir).await;
        fs::create_dir_all(&tmp_dir).await.unwrap();
        tmp_app
    }
}
