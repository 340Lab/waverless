use crate::{
    config::AppMetaYaml,
    host_funcs,
    network::proto,
    result::{ErrCvt, WSResult},
    sys::{ContainerManagerView, LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use regex::Regex;
use std::{
    collections::{HashMap, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, RwLock};
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    Module, Vm, VmBuilder,
};
use ws_derive::LogicalModule;

pub struct LRUCache<R> {
    capacity: usize,
    cache: HashMap<String, R>,
    lru: VecDeque<String>,
}

impl<R> LRUCache<R> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            cache: HashMap::new(),
            lru: VecDeque::new(),
        }
    }

    pub fn get(&mut self, key: &str) -> Option<R> {
        if let Some(v) = self.cache.remove(key) {
            self.lru.retain(|k| k != key);
            // self.lru.push_front(key.to_string());
            Some(v)
        } else {
            None
        }
    }

    pub fn put(&mut self, key: &str, value: R) {
        if self.cache.len() == self.capacity {
            if let Some(k) = self.lru.pop_back() {
                let _ = self.cache.remove(&k);
            }
        }
        let _ = self.cache.insert(key.to_string(), value);
        self.lru.push_front(key.to_string());
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KvEventType {
    Set,
    // New,
    // Delete,
    // Change,
}

#[derive(Clone, Debug)]
pub struct KvEventDef {
    pub ty: KvEventType,
    pub app: String,
    pub func: String,
}

pub struct KeyPattern(String);

impl KeyPattern {
    pub fn new(input: String) -> Self {
        Self(input)
    }
    pub fn matcher(&self) -> String {
        let re = Regex::new(r"(.+)\{\}").unwrap();

        if let Some(captured) = re.captures(&*self.0) {
            if let Some(capture_group) = captured.get(1) {
                let result = capture_group.as_str();
                // println!("Result: {}", result);
                return result.to_owned();
            }
        }

        self.0.clone()
    }
}

#[derive(Default)]
pub struct AppMetas {
    /// app name 2 trigger function
    pub event_http_app: HashMap<String, String>,
    /// key/keyprefix 2 listening functions
    pub event_kv: HashMap<String, KvEventDef>,
    /// appname,fnname - kvs
    // pub fns_key: HashMap<(String, String), Vec<KeyPattern>>,
    pub app_metas: HashMap<String, AppMetaYaml>,
}

impl AppMetas {
    pub fn get_fn_key(&self, app: &str, fnname: &str) -> Option<Vec<KeyPattern>> {
        if let Some(func) = self.app_metas.get(app).and_then(|m| m.fns.get(fnname)) {
            if let Some(kv) = &func.kv {
                return Some(kv.iter().map(|k| KeyPattern::new(k.clone())).collect());
            }
        }
        None
    }
}

struct EachAppCache {
    cache: moka::sync::Cache<u64, Vm>,
    next_instance_id: AtomicU64,
    using: AtomicU64,
    getting: Notify,
}
impl EachAppCache {
    pub fn new() -> Self {
        Self {
            cache: moka::sync::CacheBuilder::new(100)
                .time_to_live(Duration::from_secs(60))
                .build(),
            next_instance_id: AtomicU64::new(0),
            using: AtomicU64::new(0),
            getting: Notify::new(),
        }
    }
    pub async fn get(&self, file_dir: impl AsRef<Path>, container_name: &str) -> Vm {
        loop {
            let using = self.getting.notified();

            if self.using.fetch_add(1, Ordering::Relaxed) >= 100 {
                // wait for put
                using.await;
            } else {
                break;
            }
        }

        if let Some(a) = self.cache.iter().next() {
            if let Some(a) = self.cache.remove(&*a.0) {
                return a;
            }
        }
        {
            tracing::info!("new vm");
            let config = ConfigBuilder::new(CommonConfigOptions::default())
                .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
                .build()
                .expect("failed to create config");
            let module = Module::from_file(
                Some(&config),
                file_dir
                    .as_ref()
                    .join(format!("apps/{}/app.wasm", container_name)),
            )
            .unwrap();
            // self.cache.lock().await.put(container_name, module.clone());
            let import = host_funcs::new_import_obj();
            let vm = VmBuilder::new()
                .with_config(config)
                .build()
                .expect("failed to create vm")
                .register_import_module(import)
                .unwrap()
                .register_module(
                    Some(&format!(
                        "{}",
                        self.next_instance_id.fetch_add(1, Ordering::Relaxed)
                    )),
                    module,
                )
                .unwrap();
            return vm;
        }
    }
    pub fn put(&self, value: Vm) {
        self.cache
            .insert(self.next_instance_id.fetch_add(1, Ordering::Relaxed), value);
        let _ = self.using.fetch_sub(1, Ordering::Relaxed);
        self.getting.notify_waiters();
    }
}

#[derive(LogicalModule)]
pub struct ContainerManager {
    // cache: Mutex<LRUCache<Vm>>,
    using_map: SkipMap<String, EachAppCache>,
    file_dir: PathBuf,
    pub app_metas: RwLock<AppMetas>,
    /// instance addr 2 running function
    pub instance_running_function: parking_lot::RwLock<HashMap<String, Arc<(String, String)>>>,
    pub next_instance_id: AtomicU64,
    pub view: ContainerManagerView,
    // exe_view: ExecutorView,
}

#[async_trait]
impl LogicalModule for ContainerManager {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            // cache: Mutex::new(LRUCache::new(10)),
            using_map: SkipMap::new(),
            file_dir: args.nodes_config.file_dir.clone(),
            app_metas: RwLock::new(AppMetas::default()),
            instance_running_function: parking_lot::RwLock::new(HashMap::new()),
            next_instance_id: AtomicU64::new(0),
            view: ContainerManagerView::new(args.logical_modules_ref.clone()),
            // exe_view: ExecutorView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        self.view
            .p2p()
            .regist_rpc_send::<proto::sche::FnEventScheRequest>();

        self.load_all_app_meta().await?;
        Ok(vec![])
    }
}

impl ContainerManager {
    async fn apply_app_meta(&self, app_name: &str, app_meta: AppMetaYaml) {
        tracing::info!("load app meta {}", app_name);
        let mut app_metas = self.app_metas.write().await;

        // TODO: handle app config change
        // TODO: handle conflict event
        for (fname, fmeta) in &app_meta.fns {
            for e in fmeta.event.iter() {
                match e {
                    crate::config::FnEventYaml::HttpApp { .. } => {
                        let _ = app_metas
                            .event_http_app
                            .insert(app_name.to_owned(), fname.clone());
                    }
                    crate::config::FnEventYaml::KvSet { kv_set } => {
                        let p = KeyPattern(kv_set.clone());
                        let _ = app_metas.event_kv.insert(
                            p.matcher(),
                            KvEventDef {
                                ty: KvEventType::Set,
                                app: app_name.to_owned(),
                                func: fname.clone(),
                            },
                        );
                    }
                }
            }
        }
        let _ = app_metas.app_metas.insert(app_name.to_owned(), app_meta);
    }
    pub async fn load_all_app_meta(&self) -> WSResult<()> {
        let entries =
            fs::read_dir(self.file_dir.join("apps")).map_err(|e| ErrCvt(e).to_ws_io_err())?;

        // 遍历文件夹中的每个条目
        for entry in entries {
            // 获取目录项的 Result<DirEntry, io::Error>
            let entry = entry.map_err(|e| ErrCvt(e).to_ws_io_err())?;
            // 获取目录项的文件名
            let file_name = entry.file_name();
            // 将文件名转换为字符串
            let file_name_str = file_name.to_str().unwrap().to_owned();
            assert!(entry.file_type().unwrap().is_dir());
            let res = {
                let apps_dir = self.file_dir.join("apps");
                let file_name_str = file_name_str.clone();
                tokio::task::spawn_blocking(move || AppMetaYaml::read(apps_dir, &*file_name_str))
                    .await
                    .unwrap()
            };
            self.apply_app_meta(&file_name_str, res).await;
        }
        Ok(())
    }
    pub async fn finish_using(&self, container_name: &str, vm: Vm) {
        self.using_map
            .get_or_insert(container_name.to_owned(), EachAppCache::new())
            .value()
            .put(vm);
    }
    pub async fn load_container(&self, container_name: &str) -> Vm {
        // let lock = self
        //     .using_map
        //     .get_or_insert(container_name.to_owned(), Mutex::new(()).into())
        //     .value()
        //     .clone();
        self.using_map
            .get_or_insert(container_name.to_owned(), EachAppCache::new())
            .value()
            .get(&self.file_dir, container_name)
            .await
    }
}
