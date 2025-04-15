use super::shared::process_rpc::ProcessRpc;
use super::{owned::wasm, shared::SharedInstance, FnExeCtx, Instance, OwnedInstance};
use crate::general::m_os::OperatingSystem;
use crate::general::network::rpc_model;
use crate::sys::LogicalModulesRef;
use crate::{
    general::m_appmeta_manager::AppType, // worker::host_funcs,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use crate::{logical_module_view_impl, util};
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use enum_as_inner::EnumAsInner;
use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    ptr::NonNull,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
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

#[derive(Clone)]
struct InstanceCell(Arc<Option<OwnedInstance>>);
// impl Clone for InstanceCell {
//     fn clone(&self) -> Self {
//         Self(None)
//     }
// }

impl From<OwnedInstance> for InstanceCell {
    fn from(a: OwnedInstance) -> Self {
        Self(Arc::new(Some(a)))
    }
}

pub struct OwnedEachAppCache {
    cache: moka::sync::Cache<u64, InstanceCell>,
    next_instance_id: AtomicU64,
    using: AtomicU64,
    getting: Notify,
}
impl OwnedEachAppCache {
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
    pub async fn get(&self, file_dir: impl AsRef<Path>, instance_name: &str) -> OwnedInstance {
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
                return unsafe { util::non_null(&*a.0).as_mut().take().unwrap() };
            }
        }

        wasm::new_wasm_instance(
            file_dir,
            instance_name,
            self.next_instance_id.fetch_add(1, Ordering::Relaxed),
        )
    }
    pub fn put(&self, value: OwnedInstance) {
        self.cache.insert(
            self.next_instance_id.fetch_add(1, Ordering::Relaxed),
            value.into(),
        );
        let _ = self.using.fetch_sub(1, Ordering::Relaxed);
        self.getting.notify_waiters();
    }
}

#[derive(EnumAsInner)]
pub enum EachAppCache {
    Owned(OwnedEachAppCache),
    Shared(SharedInstance),
}

impl EachAppCache {
    pub async fn kill(&self) {
        match self {
            Self::Owned(_owned) => {}
            Self::Shared(p) => {
                let _ = p.0.kill().await;
            }
        }
    }
}

impl From<OwnedEachAppCache> for EachAppCache {
    fn from(a: OwnedEachAppCache) -> Self {
        Self::Owned(a)
    }
}

#[derive(LogicalModule)]
pub struct InstanceManager {
    // cache: Mutex<LRUCache<Vm>>,
    pub app_instances: SkipMap<String, EachAppCache>,
    file_dir: PathBuf,
    /// instance addr 2 running function
    pub instance_running_function: parking_lot::RwLock<HashMap<String, UnsafeFunctionCtx>>,
    pub next_instance_id: AtomicU64,
    pub view: InstanceManagerView,
}

logical_module_view_impl!(InstanceManagerView);
logical_module_view_impl!(InstanceManagerView, os, OperatingSystem);

pub struct UnsafeFunctionCtx(pub NonNull<FnExeCtx>);

unsafe impl Send for UnsafeFunctionCtx {}
unsafe impl Sync for UnsafeFunctionCtx {}

#[async_trait]
impl LogicalModule for InstanceManager {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        // std::env::set_var("JAVA_HOME", "/usr/crac_jdk");

        Self {
            app_instances: SkipMap::new(),
            file_dir: args.nodes_config.file_dir.clone(),
            instance_running_function: parking_lot::RwLock::new(HashMap::new()),
            next_instance_id: AtomicU64::new(0),
            view: InstanceManagerView::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![rpc_model::spawn::<ProcessRpc>(
            self.file_dir
                .join("agent.sock")
                .to_str()
                .unwrap()
                .to_string(),
        )
        .into()])
    }
}

impl InstanceManager {
    // async fn apply_app_meta(&self, app_name: &str, app_meta: AppMetaYaml) -> WSResult<()> {
    //     tracing::info!("load app meta {}", app_name);
    //     let mut app_metas = self.app_metas.write().await;

    //     let _ = app_metas.insert(app_name.to_owned(), app_meta.into());
    //     Ok(())
    // }

    pub async fn finish_using(&self, instance_name: &str, instance: Instance) {
        match instance {
            Instance::Owned(v) => {
                self.app_instances
                    .get_or_insert(instance_name.to_owned(), OwnedEachAppCache::new().into())
                    .value()
                    .as_owned()
                    .expect("supposed to be owned, just inserted in prev line")
                    .put(v);
            }
            Instance::Shared(v) => drop(v),
        }
    }
    pub async fn load_instance(&self, app_type: &AppType, instance_name: &str) -> Instance {
        match &app_type {
            AppType::Jar => self.get_process_instance(app_type, instance_name).into(),
            AppType::Wasm => self
                .app_instances
                .get_or_insert(instance_name.to_owned(), OwnedEachAppCache::new().into())
                .value()
                .as_owned()
                .expect("wasm is supposed to be owned, just inserted in prev line")
                .get(&self.file_dir, instance_name)
                .await
                .into(),
        }
    }
    pub async fn drap_app_instances(&self, app: &str) {
        let _inss = self.app_instances.remove(app);
        // if let Some(inss) = inss {
        //     match inss.value() {
        //         EachAppCache::Owned(owned) => {
        //             // for (_, v) in owned.cache.iter() {
        //             //     if let Some(v) = v.0.as_ref() {
        //             //         v.drop();
        //             //     }
        //             // }
        //         }
        //         EachAppCache::Shared(_) => {}
        //     }
        // }
    }
}
