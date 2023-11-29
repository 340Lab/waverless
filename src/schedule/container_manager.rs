use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    sync::Arc,
};

use crossbeam_skiplist::SkipMap;
use tokio::sync::{Mutex, OwnedMutexGuard};
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    Module, Vm, VmBuilder,
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

pub struct ContainerManager {
    cache: Mutex<LRUCache<Vm>>,
    using_map: SkipMap<String, Arc<Mutex<()>>>,
    file_dir: PathBuf,
}

impl ContainerManager {
    pub fn new(file_dir: impl AsRef<Path>) -> Self {
        Self {
            cache: Mutex::new(LRUCache::new(10)),
            using_map: SkipMap::new(),
            file_dir: file_dir.as_ref().to_owned(),
        }
    }
    pub async fn finish_using<'a>(
        &'a self,
        containername: &str,
        vm: Vm,
        holding: OwnedMutexGuard<()>,
    ) {
        self.cache.lock().await.put(containername, vm);
        drop(holding)
    }
    pub async fn load_container(&self, container_name: &str) -> (Vm, OwnedMutexGuard<()>) {
        // let lock = self
        //     .using_map
        //     .get_or_insert(container_name.to_owned(), Mutex::new(()).into())
        //     .value()
        //     .clone();
        let holding = self
            .using_map
            .get_or_insert(container_name.to_owned(), Mutex::new(()).into())
            .value()
            .clone()
            .lock_owned()
            .await;
        let config = ConfigBuilder::new(CommonConfigOptions::default())
            .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
            .build()
            .expect("failed to create config");

        // 1. 从缓存加载容器
        if let Some(vm) = self.cache.lock().await.get(container_name) {
            (vm, holding)
        }
        // 2. 从磁盘加载容器
        else {
            tracing::info!("new vm");
            let module = Module::from_file(
                Some(&config),
                self.file_dir
                    .join(format!("/apps/imgs/{}.wasm", container_name)),
            )
            .unwrap();
            // self.cache.lock().await.put(container_name, module.clone());

            (
                VmBuilder::new()
                    .with_config(config)
                    .build()
                    .expect("failed to create vm")
                    .register_module(Some(container_name), module)
                    .unwrap(),
                holding,
            )
        }
    }
}
