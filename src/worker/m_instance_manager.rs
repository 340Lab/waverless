use super::{m_executor::FunctionCtx, wasm::WasmInstance};
use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
    worker::wasm_host_funcs, // worker::host_funcs,
};
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::sync::Notify;
use ws_derive::LogicalModule;

#[cfg(target_os = "macos")]
use wasmer::{imports, Instance, Module, Store};

#[cfg(target_os = "linux")]
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    Module, VmBuilder,
};

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

// #[derive(Default)]
// pub struct AppMetas {
//     /// app name 2 trigger function
//     // pub event_http_app: HashMap<String, String>,
//     /// key/keyprefix 2 listening functions
//     // pub event_kv: HashMap<String, KvEventDef>,
//     /// appname,fnname - kvs
//     // pub fns_key: HashMap<(String, String), Vec<KeyPattern>>,
//     pub app_metas: HashMap<String, AppMetaYaml>,
// }

// impl AppMetas {
//     // pub fn get_fn_key(&self, app: &str, fnname: &str) -> Option<Vec<KeyPattern>> {
//     //     if let Some(func) = self.app_metas.get(app).and_then(|m| m.fns.get(fnname)) {
//     //         if let Some(kv) = &func.kv {
//     //             return Some(kv.iter().map(|k| KeyPattern::new(k.clone())).collect());
//     //         }
//     //     }
//     //     None
//     // }
// }

struct EachAppCache {
    cache: moka::sync::Cache<u64, WasmInstance>,
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
    pub async fn get(&self, file_dir: impl AsRef<Path>, instance_name: &str) -> WasmInstance {
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
                    .join(format!("apps/{}/app.wasm", instance_name)),
            )
            .unwrap();
            let import = wasm_host_funcs::new_import_obj();
            let vm = VmBuilder::new()
                .with_config(config)
                // .with_wasi_context(WasiContext::default())
                .build()
                .unwrap_or_else(|err| panic!("failed to create vm: {:?}", err));
            let vm = vm.register_import_module(import).unwrap();
            let vm = vm
                .register_module(
                    Some(&format!(
                        "{}{}",
                        instance_name,
                        self.next_instance_id.fetch_add(1, Ordering::Relaxed)
                    )),
                    module,
                )
                .unwrap();
            return vm;
        }
    }
    pub fn put(&self, value: WasmInstance) {
        self.cache
            .insert(self.next_instance_id.fetch_add(1, Ordering::Relaxed), value);
        let _ = self.using.fetch_sub(1, Ordering::Relaxed);
        self.getting.notify_waiters();
    }
}

#[derive(LogicalModule)]
pub struct InstanceManager {
    // cache: Mutex<LRUCache<Vm>>,
    using_map: SkipMap<String, EachAppCache>,
    file_dir: PathBuf,
    /// instance addr 2 running function
    pub instance_running_function: parking_lot::RwLock<HashMap<String, FunctionCtx>>,
    pub next_instance_id: AtomicU64,
}

#[async_trait]
impl LogicalModule for InstanceManager {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            using_map: SkipMap::new(),
            file_dir: args.nodes_config.file_dir.clone(),
            instance_running_function: parking_lot::RwLock::new(HashMap::new()),
            next_instance_id: AtomicU64::new(0),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
}

impl InstanceManager {
    // async fn apply_app_meta(&self, app_name: &str, app_meta: AppMetaYaml) -> WSResult<()> {
    //     tracing::info!("load app meta {}", app_name);
    //     let mut app_metas = self.app_metas.write().await;

    //     let _ = app_metas.insert(app_name.to_owned(), app_meta.into());
    //     Ok(())
    // }

    pub async fn finish_using(&self, instance_name: &str, vm: WasmInstance) {
        self.using_map
            .get_or_insert(instance_name.to_owned(), EachAppCache::new())
            .value()
            .put(vm);
    }
    pub async fn load_instance(&self, instance_name: &str) -> WasmInstance {
        // let lock = self
        //     .using_map
        //     .get_or_insert(instance_name.to_owned(), Mutex::new(()).into())
        //     .value()
        //     .clone();
        self.using_map
            .get_or_insert(instance_name.to_owned(), EachAppCache::new())
            .value()
            .get(&self.file_dir, instance_name)
            .await
    }
}
