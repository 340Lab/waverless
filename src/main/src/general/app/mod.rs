pub mod fn_event;
mod http;
mod v_os;

use self::v_os::AppMetaVisitOs;
use crate::{general::network::proto, result::WSResultExt, worker::m_executor::Executor};
use crate::{general::network::proto_ext::ProtoExtDataItem, util::VecExt};
use crate::{
    general::{
        data::{
            kv_interface::KvOps,
            m_data_general::{DataGeneral, DATA_UID_PREFIX_APP_META},
            m_kv_store_engine::{KeyTypeServiceList, KvAdditionalConf, KvStoreEngine},
        },
        m_os::OperatingSystem,
        network::{
            http_handler::HttpHandler,
            m_p2p::P2PModule,
            proto::{data_schedule_context::OpeRole, DataOpeRoleUploadApp},
        },
    },
    result::{WSError, WsDataError},
};
use crate::{
    logical_module_view_impl,
    master::m_master::Master,
    result::{ErrCvt, WSResult, WsFuncError},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::{self, JoinHandleWrapper},
    worker::func::m_instance_manager::InstanceManager,
};

use async_trait::async_trait;
use axum::body::Bytes;
use enum_as_inner::EnumAsInner;
use serde::{Deserialize, Deserializer, Serialize};
use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap},
    fs,
    io::Cursor,
    path::Path,
};
use tokio::sync::RwLock;

use ws_derive::LogicalModule;

use super::data::m_data_general::{GetOrDelDataArg, GetOrDelDataArgType};

logical_module_view_impl!(View);
logical_module_view_impl!(View, os, OperatingSystem);
logical_module_view_impl!(View, kv_store_engine, KvStoreEngine);
logical_module_view_impl!(View, http_handler, Box<dyn HttpHandler>);
logical_module_view_impl!(View, appmeta_manager, AppMetaManager);
logical_module_view_impl!(View, p2p, P2PModule);
logical_module_view_impl!(View, master, Option<Master>);
logical_module_view_impl!(View, instance_manager, Option<InstanceManager>);
logical_module_view_impl!(View, data_general, DataGeneral);
logical_module_view_impl!(View, executor, Option<Executor>);

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum FnEventYaml {
    HttpFn { http_fn: () },
    HttpApp { http_app: () },
    KvSet { kv_set: usize },
}

#[derive(PartialEq, Eq)]
pub enum FnEvent {
    HttpFn,
    HttpApp,
    KvSet(usize),
}

impl From<FnEventYaml> for FnEvent {
    fn from(yaml: FnEventYaml) -> Self {
        match yaml {
            FnEventYaml::HttpFn { http_fn: _ } => Self::HttpFn,
            FnEventYaml::HttpApp { http_app: _ } => Self::HttpApp,
            FnEventYaml::KvSet { kv_set } => Self::KvSet(kv_set),
        }
    }
}

// #[derive(Debug, Serialize, Deserialize)]
// #[serde(untagged)]
// pub enum FnArgYaml {
//     KvKey { kv_key: usize },
//     HttpText { http_text: () },
// }

// #[derive(Debug)]
// pub enum FnArg {
//     KvKey(usize),
//     HttpText,
// }

// impl From<FnArgYaml> for FnArg {
//     fn from(yaml: FnArgYaml) -> Self {
//         match yaml {
//             FnArgYaml::KvKey { kv_key } => Self::KvKey(kv_key),
//             FnArgYaml::HttpText { http_text: _ } => Self::HttpText,
//         }
//     }
// }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpMethod {
    Get,
    Post,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpCall {
    Direct,
    Indirect,
}

#[derive(Debug, EnumAsInner, Clone, Serialize, Deserialize)]
pub enum FnCallMeta {
    Http { method: HttpMethod, call: HttpCall },
    Rpc,
}

#[derive(Debug)]
pub struct FnMetaYaml {
    /// key to operations
    pub calls: Vec<FnCallMeta>,
    pub kvs: Option<BTreeMap<String, Vec<String>>>,
}

impl<'de> Deserialize<'de> for FnMetaYaml {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut map = serde_yaml::Value::deserialize(deserializer)?;
        let map = map
            .as_mapping_mut()
            .ok_or_else(|| serde::de::Error::custom("not a map"))?;
        // let calls = map.remove("calls").ok_or_else(|| serde::de::Error::missing_field("calls"))?;
        let mut calls = vec![];
        fn parse_http_call<'de, D: Deserializer<'de>>(
            map: &serde_yaml::Value,
        ) -> Result<HttpCall, D::Error> {
            let map = map
                .as_mapping()
                .ok_or_else(|| serde::de::Error::custom("not a map"))?;
            let call = map
                .get("call")
                .ok_or_else(|| serde::de::Error::missing_field("call"))?;
            let call = call
                .as_str()
                .ok_or_else(|| serde::de::Error::custom("not a string"))?;
            let call = if call == "direct" {
                HttpCall::Direct
            } else if call == "indirect" {
                HttpCall::Indirect
            } else {
                return Err(serde::de::Error::custom("invalid call type"));
            };
            Ok(call)
        }
        if let Some(v) = map.get("http.get") {
            let call = parse_http_call::<D>(v)?;
            calls.push(FnCallMeta::Http {
                method: HttpMethod::Get,
                call,
            });
        }
        if let Some(v) = map.get("http.post") {
            let call = parse_http_call::<D>(v)?;
            calls.push(FnCallMeta::Http {
                method: HttpMethod::Post,
                call,
            });
        }
        if let Some(_v) = map.get("rpc") {
            calls.push(FnCallMeta::Rpc);
        }

        let kvs = map.remove("kvs");
        let kvs = if let Some(kvs) = kvs {
            serde_yaml::from_value(kvs).map_err(serde::de::Error::custom)?
        } else {
            None
        };

        tracing::debug!("FnMetaYaml constructed, calls:{:?}", calls);
        Ok(Self { calls, kvs })
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct KeyPattern(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvMeta {
    set: bool,
    get: bool,
    delete: bool,
    pub pattern: KeyPattern,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FnMeta {
    pub calls: Vec<FnCallMeta>,
    // pub event: Vec<FnEvent>,
    // pub args: Vec<FnArg>,
    pub kvs: Option<Vec<KvMeta>>,
}

#[derive(Debug, Deserialize)]
pub struct AppMetaYaml {
    pub fns: HashMap<String, FnMetaYaml>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AppType {
    Jar,
    Wasm,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AppMeta {
    pub app_type: AppType,
    fns: HashMap<String, FnMeta>,
    cache_contains_http_fn: Option<bool>,
}

impl AppMeta {
    pub fn contains_http_fn(&self) -> bool {
        if let Some(v) = self.cache_contains_http_fn {
            return v;
        }
        let res = self
            .fns
            .iter()
            .any(|(_, fnmeta)| fnmeta.allow_http_call().is_some());
        unsafe {
            #[cfg(feature = "unsafe-log")]
            tracing::debug!("http_handler begin");
            let _ = util::non_null(&self.cache_contains_http_fn)
                .as_mut()
                .replace(res);
            #[cfg(feature = "unsafe-log")]
            tracing::debug!("http_handler end");
        }
        res
    }
}

// #[derive(Debug, Serialize, Deserialize)]
// pub struct AppMetaService {
//     actions: Vec<Action>,
//     node: NodeID,
//     app_dir: String,
// }

pub struct AppMetas {
    tmp_app_metas: HashMap<String, AppMeta>,
    pattern_2_app_fn: HashMap<String, Vec<(String, String)>>,
}

// impl FnEvent {
//     pub fn match_kv_ope(&self, ope: KvOps) -> bool {
//         match self {
//             Self::KvSet(_) => ope == KvOps::Set,
//             Self::HttpApp => false,
//         }
//     }
// }

impl AppMetaYaml {
    pub fn read(apps_dir: impl AsRef<Path>, appname: &str) -> AppMetaYaml {
        let file_path = apps_dir.as_ref().join(format!("{}/app.yaml", appname));
        let file = std::fs::File::open(file_path).unwrap_or_else(|err| {
            tracing::debug!("open config file failed, err: {:?}", err);

            let file_path = apps_dir.as_ref().join(format!("{}/app.yml", appname));
            std::fs::File::open(file_path).unwrap_or_else(|err| {
                panic!("open config file failed, err: {:?}", err);
            })
        });
        serde_yaml::from_reader(file).unwrap_or_else(|e| {
            panic!("parse yaml config file failed, err: {:?}", e);
        })
    }
    // // return true if key set is valid
    // pub fn check_key_set(&self, key: &str) -> bool {
    //     self.fns
    //         .iter()
    //         .any(|(_, fn_meta)| {
    //             if let Some(kvs)=&fn_meta.kvs{
    //                 kvs.iter().any(|(k, _)| key.contains(k))
    //             }else{
    //                 false
    //             })
    // }
}

impl FnMeta {
    pub fn allow_rpc_call(&self) -> bool {
        self.calls.iter().any(|v| match v {
            FnCallMeta::Rpc => true,
            _ => false,
        })
    }
    pub fn allow_http_call(&self) -> Option<HttpMethod> {
        self.calls.iter().find_map(|v| match v {
            FnCallMeta::Http { method, call: _ } => Some(method.clone()),
            _ => None,
        })
    }

    pub fn match_key(&self, key: &[u8], ope: KvOps) -> Option<KeyPattern> {
        let key = if let Ok(key) = std::str::from_utf8(key) {
            key
        } else {
            return None;
        };
        if let Some(kvs) = &self.kvs {
            for kv in kvs {
                if kv.pattern.match_key(key) {
                    match ope {
                        KvOps::Get => {
                            if kv.get {
                                return Some(kv.pattern.clone());
                            }
                        }
                        KvOps::Set => {
                            if kv.set {
                                return Some(kv.pattern.clone());
                            }
                        }
                        KvOps::Delete => {
                            if kv.delete {
                                return Some(kv.pattern.clone());
                            }
                        }
                    }
                    tracing::info!("allow ope {:?}, cur ope:{:?}", kv, ope);
                }
            }
            // tracing::info!("no key pattern matched for key: {}", key);
        }

        None
    }

    pub fn try_get_kv_meta_by_index(&self, index: usize) -> Option<&KvMeta> {
        if let Some(kvs) = &self.kvs {
            return kvs.get(index);
        }
        None
    }

    // / index should be valid
    // fn get_kv_meta_by_index_unwrap(&self, index: usize) -> &KvMeta {
    //     self.try_get_kv_meta_by_index(index).unwrap()
    // }
    // /// get event related kvmeta matches operation
    // pub fn get_event_kv(&self, ope: KvOps, event: &FnEvent) -> Option<&KvMeta> {
    //     match event {
    //         FnEvent::KvSet(kv_set) => {
    //             if ope == KvOps::Set {
    //                 return Some(self.get_kv_meta_by_index_unwrap(*kv_set));
    //             }
    //         }
    //         FnEvent::HttpApp => {}
    //     }
    //     None
    // }

    // / find kv event trigger with match the `pattern` and `ope`
    // pub fn find_will_trigger_kv_event(&self, _pattern: &KeyPattern, _ope: KvOps) -> Option<&KvMeta> {
    //     unimplemented!()
    //     // self.event.iter().find_map(|event| {
    //     //     match event {
    //     //         FnEvent::HttpApp => {}
    //     //         FnEvent::KvSet(key_index) => {
    //     //             if ope == KvOps::Set {
    //     //                 let res = self.get_kv_meta_by_index_unwrap(*key_index);
    //     //                 if res.pattern == *pattern {
    //     //                     return Some(res);
    //     //                 }
    //     //             }
    //     //         }
    //     //         FnEvent::HttpFn => {}
    //     //     }
    //     //     None
    //     // })
    // }
}

impl KeyPattern {
    pub fn new(input: String) -> Self {
        Self(input)
    }
    // match {} for any words
    // "xxxx_{}_{}" matches "xxxx_abc_123"
    // “xxxx{}{}" matches "xxxxabc123"
    pub fn match_key(&self, key: &str) -> bool {
        let re = self.0.replace("{}", "[a-zA-Z0-9]+");
        // let pattern_len = re.len();
        // tracing::info!("len:{}", re.len());
        let re = regex::Regex::new(&re).unwrap();
        if let Some(len) = re.find(key) {
            tracing::info!(
                "match key: {} with pattern: {} with len {} {} ",
                key,
                self.0,
                len.len(),
                key.len()
            );
            len.len() == key.len()
        } else {
            tracing::info!("not match key: {} with pattern: {}", key, self.0);
            false
        }
    }
    // pub fn matcher(&self) -> String {

    //     // let re = Regex::new(r"(.+)\{\}").unwrap();

    //     // if let Some(captured) = re.captures(&*self.0) {
    //     //     if let Some(capture_group) = captured.get(1) {
    //     //         let result = capture_group.as_str();
    //     //         // println!("Result: {}", result);
    //     //         return result.to_owned();
    //     //     }
    //     // }

    //     // self.0.clone()
    // }
}

impl From<FnMetaYaml> for FnMeta {
    fn from(yaml: FnMetaYaml) -> Self {
        let kvs = if let Some(kvs) = yaml.kvs {
            Some(
                kvs.into_iter()
                    .map(|(key, ops)| {
                        let mut set = false;
                        let mut get = false;
                        let mut delete = false;
                        for op in ops {
                            if op == "set" {
                                set = true;
                            } else if op == "get" {
                                get = true;
                            } else if op == "delete" {
                                delete = true;
                            } else {
                                panic!("invalid operation: {}", op);
                            }
                        }
                        // TODO: check key pattern
                        KvMeta {
                            delete,
                            set,
                            get,
                            pattern: KeyPattern::new(key),
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };
        let res = Self {
            calls: yaml.calls,
            kvs,
        };
        // assert!(res.check_kv_valid());
        res
    }
}

// impl From<AppMetaYaml> for AppMeta {
//     fn from(yaml: AppMetaYaml) -> Self {
//         let fns = yaml
//             .fns
//             .into_iter()
//             .map(|(fnname, fnmeta)| (fnname, fnmeta.into()))
//             .collect();
//         Self { fns }
//     }
// }

impl AppMeta {
    pub async fn new(
        metayaml: AppMetaYaml,
        app_name: &str,
        meta_fs: &AppMetaVisitOs,
    ) -> WSResult<Self> {
        let fns = metayaml
            .fns
            .into_iter()
            .map(|(fnname, fnmeta)| {
                let fnmeta = fnmeta.into();
                (fnname, fnmeta)
            })
            .collect();
        let app_type = meta_fs.get_app_type(app_name).await?;
        Ok(Self {
            app_type,
            fns,
            cache_contains_http_fn: None,
        })
    }
    pub fn fns(&self) -> Vec<String> {
        self.fns.iter().map(|(fnname, _)| fnname.clone()).collect()
    }
    pub fn get_fn_meta(&self, fnname: &str) -> Option<&FnMeta> {
        self.fns.get(fnname)
    }
    // pub fn http_trigger_fn(&self) -> Option<&str> {
    //     self.fns.iter().find_map(|(fnname, fnmeta)| {
    //         if fnmeta.event.iter().any(|e| e == &FnEvent::HttpApp) {
    //             Some(fnname.as_str())
    //         } else {
    //             None
    //         }
    //     })
    // }
}

lazy_static::lazy_static! {
    static ref VIEW: Option<View> = None;
}
fn view() -> &'static View {
    tracing::debug!("get view");
    let res = unsafe { util::non_null(&*VIEW).as_ref().as_ref().unwrap() };
    tracing::debug!("get view end");
    res
}

#[derive(LogicalModule)]
pub struct AppMetaManager {
    meta: RwLock<AppMetas>,
    pub fs_layer: AppMetaVisitOs,
    view: View,
    // app_meta_list_lock: Mutex<()>,
}

#[async_trait]
impl LogicalModule for AppMetaManager {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let view = View::new(args.logical_modules_ref.clone());
        unsafe {
            #[cfg(feature = "unsafe-log")]
            tracing::debug!("app man view begin");
            let _ = util::non_null(&*VIEW).as_mut().replace(view.clone());
            #[cfg(feature = "unsafe-log")]
            tracing::debug!("app man view end");
        }
        let fs_layer = AppMetaVisitOs::new(view.clone());
        Self {
            meta: RwLock::new(AppMetas {
                tmp_app_metas: HashMap::new(),
                pattern_2_app_fn: HashMap::new(),
            }),
            view,
            fs_layer,
            // app_meta_list_lock: Mutex::new(()),
        }
    }
    async fn init(&self) -> WSResult<()> {
        let mut router = self.view.http_handler().building_router();

        let take = router.option_mut().take().unwrap();
        let take = http::binds(take, self.view.clone());
        let _ = router.option_mut().replace(take);
        // .route("/appman/upload", post(handler2))

        Ok(())
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        self.meta
            .write()
            .await
            .load_all_app_meta(&self.view.os().file_path, &self.fs_layer)
            .await?;
        Ok(vec![])
    }
}

impl AppMetas {
    // pub fn new() -> Self {
    //     Self {
    //         app_metas: HashMap::new(),
    //         pattern_2_app_fn: HashMap::new(),
    //     }
    // }
    // pub async fn set_tmp_appmeta(&self, )
    fn get_tmp_app_meta(&self, app: &str) -> Option<AppMeta> {
        self.tmp_app_metas.get(app).cloned()
    }

    pub fn get_pattern_triggers(
        &self,
        pattern: impl Borrow<str>,
    ) -> Option<&Vec<(String, String)>> {
        self.pattern_2_app_fn.get(pattern.borrow())
    }
    async fn load_all_app_meta(
        &mut self,
        file_dir: impl AsRef<Path>,
        meta_fs: &AppMetaVisitOs,
    ) -> WSResult<()> {
        if !file_dir.as_ref().join("apps").exists() {
            fs::create_dir_all(file_dir.as_ref().join("apps")).unwrap();
            return Ok(());
        }
        let entries =
            fs::read_dir(file_dir.as_ref().join("apps")).map_err(|e| ErrCvt(e).to_ws_io_err())?;

        // 遍历文件夹中的每个条目
        for entry in entries {
            // 获取目录项的 Result<DirEntry, io::Error>
            let entry = entry.map_err(|e| ErrCvt(e).to_ws_io_err())?;
            // 获取目录项的文件名
            let file_name = entry.file_name();
            // dir name is the app name
            let app_name = file_name.to_str().unwrap().to_owned();

            // allow spec files
            if entry.file_type().unwrap().is_file() {
                let allowed_files = vec!["crac_config"];
                assert!(allowed_files
                    .contains(&&*(*entry.file_name().as_os_str().to_string_lossy()).to_owned()));
                continue;
            }

            // allow only dir
            assert!(entry.file_type().unwrap().is_dir());

            // read app config yaml
            let meta_yaml = {
                let apps_dir = file_dir.as_ref().join("apps");
                let file_name_str = app_name.clone();
                tokio::task::spawn_blocking(move || AppMetaYaml::read(apps_dir, &*file_name_str))
                    .await
                    .unwrap()
            };

            // transform
            let meta = AppMeta::new(meta_yaml, &app_name, meta_fs).await.unwrap();

            //TODO: build and checks
            // - build up key pattern to app fn

            // for (fnname, fnmeta) in &meta.fns {
            //     for event in &fnmeta.event {
            //         match event {
            //             // not kv event, no key pattern
            //             FnEvent::HttpFn => {}
            //             FnEvent::HttpApp => {}
            //             FnEvent::KvSet(key_index) => {
            //                 let kvmeta = fnmeta.try_get_kv_meta_by_index(*key_index).unwrap();
            //                 self.pattern_2_app_fn
            //                     .entry(kvmeta.pattern.0.clone())
            //                     .or_insert_with(Vec::new)
            //                     .push((app_name.clone(), fnname.clone()));
            //             }
            //         }
            //     }
            // }
            let _ = self.tmp_app_metas.insert(app_name, meta);
        }
        Ok(())
    }
}

impl AppMetaManager {
    async fn construct_tmp_app(&self, tmpapp: &str) -> WSResult<AppMeta> {
        // 1.meta
        // let appdir = self.fs_layer.concat_app_dir(app);
        let appmeta = self.fs_layer.read_app_meta(tmpapp).await?;

        // TODO: 2.check project dir
        // 3. if java, take snapshot
        if let AppType::Jar = appmeta.app_type {
            let _ = self
                .meta
                .write()
                .await
                .tmp_app_metas
                .insert(tmpapp.to_owned(), appmeta.clone());
            tracing::debug!("record app meta to make checkpoint {}", tmpapp);
            self.view
                .instance_manager()
                .make_checkpoint_for_app(tmpapp)
                .await?;
            self.view
                .instance_manager()
                .drap_app_instances(tmpapp)
                .await;
            // remove app_meta
            tracing::debug!("checkpoint made, remove app meta {}", tmpapp);
            let _ = self
                .meta
                .write()
                .await
                .tmp_app_metas
                .remove(tmpapp)
                .unwrap_or_else(|| {
                    panic!("remove app meta failed, app: {}", tmpapp);
                });
        }

        Ok(appmeta)
    }
    pub async fn app_available(&self, app: &str) -> WSResult<bool> {
        match self
            .view
            .data_general()
            .get_or_del_datameta_from_master(
                format!("{}{}", DATA_UID_PREFIX_APP_META, app).as_bytes(),
                false,
            )
            .await
        {
            Err(err) => match err {
                WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid }) => {
                    tracing::debug!(
                        "app meta not found, app: {}",
                        std::str::from_utf8(&*uniqueid).unwrap()
                    );
                    Ok(false)
                }
                _ => Err(err),
            },
            Ok(_) => Ok(true),
        }
    }

    // call inner AppMetas.get_app_meta
    pub async fn get_app_meta(&self, app: &str) -> WSResult<Option<AppMeta>> {
        if let Some(res) = self.meta.read().await.get_tmp_app_meta(app) {
            return Ok(Some(res));
        }

        // self.app_metas.get(app)
        let meta = view()
            .data_general()
            .get_or_del_data(GetOrDelDataArg {
                meta: None,
                unique_id: format!("{}{}", DATA_UID_PREFIX_APP_META, app).into(),
                ty: GetOrDelDataArgType::PartialOne { idx: 0 },
            })
            .await;

        // only one data item
        let (_, meta): (_, proto::DataItem) = match meta {
            Ok((_, datas)) => datas.into_iter().next().unwrap(),
            Err(err) => match err {
                WSError::WsDataError(WsDataError::DataSetNotFound { uniqueid }) => {
                    tracing::debug!(
                        "get_app_meta not exist, uniqueid: {:?}",
                        std::str::from_utf8(&*uniqueid)
                    );
                    return Ok(None);
                }
                _ => {
                    tracing::warn!("get_app_meta failed with err {:?}", err);
                    return Err(err);
                }
            },
        };

        let proto::DataItem {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(metabytes)),
        } = meta
        else {
            return Err(WsFuncError::InvalidAppMetaDataItem {
                app: app.to_owned(),
            }
            .into());
        };

        let meta = bincode::deserialize_from::<_, AppMeta>(Cursor::new(&metabytes));
        let meta = match meta {
            Err(e) => {
                tracing::warn!(
                    "meta decode failed with data:{:?}, err:{:?}",
                    metabytes.limit_range_debug(0..100),
                    e
                );
                return Err(WsFuncError::InvalidAppMetaDataItem {
                    app: app.to_owned(),
                }
                .into());
            }
            Ok(meta) => meta,
        };
        Ok(Some(meta))
    }

    pub async fn app_uploaded(&self, appname: String, data: Bytes) -> WSResult<()> {
        // 1. tmpapp name & dir
        // TODO: fobidden tmpapp public access
        // let tmpapp = format!("tmp{}", Uuid::new_v4()); //appname.clone();
        let tmpapp = format!("{}", appname);
        let tmpappdir = self.fs_layer.concat_app_dir(&tmpapp);
        let tmpapp = tmpapp.clone();

        // 2. unzip app pack
        let tmpappdir2 = tmpappdir.clone();
        // remove old dir&app
        if let Some(_) = self.meta.write().await.tmp_app_metas.remove(&tmpapp) {
            tracing::debug!("remove old app meta {}", tmpapp);
        }
        let ins = self.view.instance_manager().app_instances.remove(&tmpapp);
        if let Some(ins) = ins {
            ins.value().kill().await;
            tracing::debug!("remove old app instance {}", tmpapp);
        }

        if tmpappdir2.exists() {
            // remove old app
            fs::remove_dir_all(&tmpappdir2).unwrap();
        }
        let res = tokio::task::spawn_blocking(move || {
            let data = data.to_vec();
            zip_extract::extract(Cursor::new(data), &tmpappdir2, false)
        })
        .await
        .unwrap();

        match res {
            Ok(res) => res,
            Err(err) => {
                tracing::warn!("unzip failed, err: {:?}", err);
                let _ = fs::remove_dir_all(&tmpappdir);
                return Err(WsFuncError::AppPackFailedZip(err).into());
            }
        };

        // 3. check meta
        let res = self.construct_tmp_app(&tmpapp).await;
        let appmeta = match res {
            Err(e) => {
                let _ = fs::remove_dir_all(&tmpappdir);
                tracing::warn!("construct app failed, err {:?}", e);
                return Err(e);
            }
            Ok(appmeta) => appmeta,
        };

        // 4. zip tmp dir to memory
        let zipfiledata = {
            tracing::debug!("zip tmp dir to memory");
            // if let Ok(direntries) = fs::read_dir(tmpappdir.join("checkpoint-dir")) {
            //     for f in direntries {
            //         tracing::debug!(
            //             "file in checkpoint-dir: {:?}",
            //             f.map(|v| v.file_name().to_str().unwrap().to_owned())
            //         );
            //     }
            // }
            let view = self.view.clone();
            tokio::task::spawn_blocking(move || {
                view.os()
                    .zip_dir_2_data(&tmpappdir, zip::CompressionMethod::Deflated)
            })
            .await
            .unwrap()
        }?;

        // remove temp dir
        // let _ = fs::remove_dir_all(&tmpappdir).map_err(|e| WSError::from(WsIoErr::Io(e)))?;

        // 3. broadcast meta and appfile
        let write_data_id = format!("{}{}", DATA_UID_PREFIX_APP_META, appname);
        let write_datas = vec![
            proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(
                    bincode::serialize(&appmeta).unwrap(),
                )),
            },
            proto::DataItem {
                data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(
                    proto::FileData {
                        file_name_opt: format!("apps/{}", appname),
                        is_dir_opt: true,
                        file_content: zipfiledata,
                    },
                )),
            },
        ];
        tracing::debug!(
            "2broadcast meta and appfile, datasetid: {}, datas: {:?}",
            write_data_id,
            write_datas
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
        );
        self.view
            .data_general()
            .write_data(
                write_data_id,
                write_datas,
                // vec![
                //     DataMeta {
                //         cache: DataModeCache::AlwaysInMem as i32,
                //         distribute: DataModeDistribute::BroadcastRough as i32,
                //     },
                //     DataMeta {
                //         cache: DataModeCache::AlwaysInFs as i32,
                //         distribute: DataModeDistribute::BroadcastRough as i32,
                //     },
                // ],
                Some((
                    self.view.p2p().nodes_config.this_node(),
                    proto::DataOpeType::Write,
                    OpeRole::UploadApp(DataOpeRoleUploadApp {}),
                )),
            )
            .await?;
        tracing::debug!("app uploaded");
        Ok(())
    }

    pub fn set_app_meta_list(&self, list: Vec<String>) {
        self.view
            .kv_store_engine()
            .set(
                KeyTypeServiceList,
                &serde_json::to_string(&list).unwrap().into(),
                false,
            )
            .todo_handle();
    }
    pub fn get_app_meta_list(&self) -> Vec<String> {
        let res = self
            .view
            .kv_store_engine()
            .get(&KeyTypeServiceList, false, KvAdditionalConf {})
            .map(|(_version, list)| list)
            .unwrap_or_else(|| {
                return vec![];
            });
        serde_json::from_slice(&res).unwrap_or_else(|e| {
            tracing::warn!("parse app meta list failed, err: {:?}", e);
            vec![]
        })
    }

    // pub fn get_app_meta_basicinfo_list(&self) -> Vec<ServiceBasic> {
    //     let apps = self.get_app_meta_list();
    //     apps.into_iter()
    //         .map(|app| {
    //             let service = self.get_app_meta_service(&app).unwrap();
    //             ServiceBasic {
    //                 name: app,
    //                 node: format!("{}", service.node),
    //                 dir: service.app_dir,
    //                 actions: service.actions,
    //             }
    //         })
    //         .collect()
    // }

    // pub fn get_app_meta_service(&self, app_name: &str) -> Option<AppMetaService> {
    //     let Some(res) = self
    //         .view
    //         .kv_store_engine()
    //         .get(KeyTypeServiceMeta(app_name.as_bytes()))
    //     else {
    //         return None;
    //     };
    //     serde_json::from_slice(&res).map_or_else(
    //         |e| {
    //             tracing::warn!("parse service meta failed, err: {:?}", e);
    //             None
    //         },
    //         |v| Some(v),
    //     )
    // }

    // pub fn set_app_meta_service(&self, app_name: &str, service: AppMetaService) {
    //     self.view.kv_store_engine().set(
    //         KeyTypeServiceMeta(app_name.as_bytes()),
    //         &serde_json::to_string(&service).unwrap().into(),
    //     );
    // }

    // // node id is valid before call this function
    // pub async fn add_service(&self, req: AddServiceReq) -> AddServiceResp {
    //     // // check conflict service
    //     // if self.get_app_meta_service(&req.service.name).is_some() {
    //     //     return AddServiceResp::Fail {
    //     //         msg: format!("service {} already exist", req.service.name),
    //     //     };
    //     // }

    //     // get the target node
    //     let Ok(nodeid) = req.service.node.parse::<NodeID>() else {
    //         return AddServiceResp::Fail {
    //             msg: "node id should be number".to_owned(),
    //         };
    //     };
    //     if !self.view.p2p().nodes_config.node_exist(nodeid) {
    //         return AddServiceResp::Fail {
    //             msg: format!("node {nodeid} not exist"),
    //         };
    //     }

    //     // call and return if rpc failed
    //     let res = match self
    //         .view
    //         .os()
    //         .remote_get_dir_content_caller
    //         .call(
    //             self.view.p2p(),
    //             nodeid,
    //             GetDirContentReq {
    //                 path: req.service.dir.clone(),
    //             },
    //             None,
    //         )
    //         .await
    //     {
    //         Ok(res) => res,
    //         Err(e) => {
    //             return AddServiceResp::Fail {
    //                 msg: format!("call remote_get_dir_content_caller failed, err: {:?}", e),
    //             };
    //         }
    //     };

    //     // return if remote failed
    //     let _res = match res.dispatch.unwrap() {
    //         super::network::proto::remote_sys::get_dir_content_resp::Dispatch::Fail(fail) => {
    //             return AddServiceResp::Fail { msg: fail.error };
    //         }
    //         super::network::proto::remote_sys::get_dir_content_resp::Dispatch::Ok(res) => res,
    //     };

    //     // add to appmeta list
    //     {
    //         let _mu = self.app_meta_list_lock.lock();
    //         let mut appmeta_list = self.get_app_meta_list();
    //         appmeta_list.push(req.service.name.clone());
    //         let mut dup = HashSet::new();
    //         let appmeta_list = appmeta_list
    //             .into_iter()
    //             .filter(|v| dup.insert(v.clone()))
    //             .collect();
    //         self.set_app_meta_list(appmeta_list);
    //         self.set_app_meta_service(
    //             &req.service.name,
    //             AppMetaService {
    //                 actions: req.service.actions,
    //                 node: nodeid,
    //                 app_dir: req.service.dir,
    //             },
    //         );
    //     }
    //     AddServiceResp::Succ {}
    // }
    // pub async fn run_service_action(&self, req: RunServiceActionReq) -> RunServiceActionResp {
    //     if !req.sync {
    //         return RunServiceActionResp::Fail {
    //             msg: "unsuppot async mode".to_owned(),
    //         };
    //     }

    //     // sync logic
    //     // check service and action
    //     let service = match self.get_app_meta_service(&req.service) {
    //         Some(service) => service,
    //         None => {
    //             return RunServiceActionResp::Fail {
    //                 msg: format!("service {} not exist", req.service),
    //             };
    //         }
    //     };

    //     // check action valid
    //     let Some(action) = service.actions.iter().find(|v| v.cmd == req.action_cmd) else {
    //         return RunServiceActionResp::Fail {
    //             msg: format!("action {} not exist", req.action_cmd),
    //         };
    //     };

    //     // handle rpc fail
    //     let res = match self
    //         .view
    //         .os()
    //         .remote_run_cmd_caller
    //         .call(
    //             self.view.p2p(),
    //             service.node,
    //             RunCmdReq {
    //                 cmd: action.cmd.clone(),
    //                 workdir: service.app_dir,
    //             },
    //             Some(Duration::from_secs(10)),
    //         )
    //         .await
    //     {
    //         Ok(res) => res,
    //         Err(err) => {
    //             return RunServiceActionResp::Fail {
    //                 msg: format!("call remote_run_cmd_caller failed, err: {:?}", err),
    //             };
    //         }
    //     };

    //     // handle cmd fail
    //     let res = match res.dispatch.unwrap() {
    //         super::network::proto::remote_sys::run_cmd_resp::Dispatch::Ok(res) => res,
    //         super::network::proto::remote_sys::run_cmd_resp::Dispatch::Err(err) => {
    //             return RunServiceActionResp::Fail {
    //                 msg: format!("remote run cmd failed: {}", err.error),
    //             }
    //         }
    //     };

    //     RunServiceActionResp::Succ { output: res.output }
    // }
}

#[cfg(test)]
mod test {
    use crate::util;

    use super::*;
    #[test]
    fn test_key_pattern() {
        util::test_tracing_start();
        let pattern = KeyPattern::new("xxxx_{}_{}".to_owned());
        assert!(pattern.match_key("xxxx_abc_123"));
    }
}
