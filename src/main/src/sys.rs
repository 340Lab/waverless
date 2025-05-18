use crate::general::app::app_owned::wasm_host_funcs;
use crate::general::app::instance::m_instance_manager::InstanceManager;
use crate::general::app::m_executor::Executor;
use crate::general::data::m_kv_user_client::KvUserClient;
use crate::{
    config::NodesConfig,
    general::{
        app::AppMetaManager,
        data::{
            m_data_general::DataGeneral, m_dist_lock::DistLock, m_kv_store_engine::KvStoreEngine,
        },
        m_metric_publisher::MetricPublisher,
        m_os::OperatingSystem,
        network::{http_handler::HttpHandlerDispatch, m_p2p::P2PModule},
    },
    master::{
        app::m_app_master::MasterAppMgmt, data::m_data_master::DataMaster, m_master::Master,
        m_metric_observor::MetricObservor,
    },
    modules_global_bridge, util,
    worker::m_worker::WorkerCore,
};
use crate::{result::WSResult, util::JoinHandleWrapper};
use async_trait::async_trait;
use std::sync::{Arc, Weak};
use tokio::sync::Mutex;

pub struct Sys {
    logical_modules: Arc<Option<LogicalModules>>,
    sub_tasks: Mutex<Vec<JoinHandleWrapper>>,
}

impl Drop for Sys {
    fn drop(&mut self) {
        tracing::info!("drop sys");
    }
}

impl Sys {
    pub fn new(config: NodesConfig) -> Sys {
        // chdir to file_path
        // std::env::set_current_dir(&config.file_dir)
        //     .unwrap_or_else(|err| panic!("failed to start sys at {}", config.file_dir));
        tracing::info!("Running at dir: {:?}", config.file_dir);

        Sys {
            logical_modules: LogicalModules::new(config),
            sub_tasks: Vec::new().into(),
        }
    }

    pub fn new_logical_modules_ref(&self) -> LogicalModulesRef {
        LogicalModulesRef::new(self.logical_modules.clone())
    }

    pub async fn wait_for_end(&mut self) {
        if let Err(err) = (*self.logical_modules).as_ref().unwrap().start(self).await {
            panic!("start logical nodes error: {:?}", err);
        }
        tracing::info!("modules all started, waiting for end");
        for task in self.sub_tasks.lock().await.iter_mut() {
            task.join().await;
        }
    }

    #[cfg(test)]
    pub async fn test_start_all(&self) -> LogicalModulesRef {
        if let Err(err) = (*self.logical_modules).as_ref().unwrap().start(self).await {
            panic!("start logical nodes error: {:?}", err);
        }
        assert!(self.logical_modules.is_some());
        LogicalModulesRef {
            inner: Arc::downgrade(&self.logical_modules),
        }
    }
    #[cfg(test)]
    pub async fn test_directly_wait_for_end(&mut self) {
        for task in self.sub_tasks.lock().await.iter_mut() {
            task.join().await;
        }
    }
}

pub type NodeID = u32;

#[derive(Clone)]
pub struct LogicalModuleNewArgs {
    pub logical_modules_ref: LogicalModulesRef,
    pub parent_name: String,
    pub btx: BroadcastSender,
    pub logical_models: Option<Weak<LogicalModules>>,
    pub nodes_config: NodesConfig,
}

impl LogicalModuleNewArgs {
    pub fn expand_parent_name(&mut self, this_name: &str) {
        let name = format!("{}::{}", self.parent_name, this_name);
        self.parent_name = name;
    }
}

#[async_trait]
pub trait LogicalModule: Send + Sync + 'static {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized;
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>>;

    async fn init(&self) -> WSResult<()> {
        Ok(())
    }
    // async fn listen_async_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
    // fn listen_sync_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
}

#[derive(Clone, Debug)]
pub enum BroadcastMsg {
    SysEnd,
}

pub type BroadcastSender = tokio::sync::broadcast::Sender<BroadcastMsg>;

#[derive(Clone)]
pub struct LogicalModulesRef {
    pub inner: Weak<Option<LogicalModules>>,
}

impl LogicalModulesRef {
    pub fn new(inner: Arc<Option<LogicalModules>>) -> LogicalModulesRef {
        let inner = Arc::downgrade(&inner);
        LogicalModulesRef { inner }
    }
}
// impl LogicalModulesRef {
//     fn setup(&mut self, modules: Arc<LogicalModules>) {
//         self.inner = Some(Arc::downgrade(&modules));
//     }
// }

#[macro_export]
macro_rules! logical_module_view_impl {
    ($module:ident,$module_name:ident,Option<$type:ty>) => {
        impl $module {
            pub fn $module_name(&self) -> &$type {
                unsafe {
                    #[cfg(feature="unsafe-log")]
                    tracing::debug!("unsafe ptr begin");
                    let res = &(*self.inner.inner.as_ptr())
                        .as_ref()
                        .unwrap()
                        .$module_name
                        .as_ref()
                        .unwrap();
                    #[cfg(feature="unsafe-log")]
                    tracing::debug!("unsafe ptr end");

                    let _: &dyn Send = res;
                    res
                }
            }
        }
    };
    ($module:ident,$module_name:ident,$type:ty) => {
        impl $module {
            pub fn $module_name(&self) -> &$type {
                camelpaste::paste! {
                    // let tag=stringify!($module_name) ;
                    #[cfg(feature="unsafe-log")]
                    tracing::debug!("unsafe ptr begin2 {}",tag);

                    let res = unsafe { &(*self.inner.inner.as_ptr()).as_ref().unwrap().$module_name };

                    #[cfg(feature="unsafe-log")]
                    tracing::debug!("unsafe ptr end2 {}",tag);

                    // 编译期校验 $type 是Send的类型
                    let _: &dyn Send = res;

                    res
                }
            }
        }
    };
    ($module:ident) => {
        #[derive(Clone)]
        pub struct $module {
            inner: LogicalModulesRef,
        }
        impl $module {
            pub fn new(inner: LogicalModulesRef) -> Self {
                $module { inner }
            }
            pub fn copy_module_ref(&self) -> LogicalModulesRef {
                self.inner.clone()
            }
            // fn setup(&mut self, modules: Arc<LogicalModules>) {
            //     self.inner.setup(modules);
            // }
        }

        // unsafe send
        unsafe impl Send for $module {}
    };
}

macro_rules! init_module_opt {
    ($self:ident,$sys:ident,$opt:ident) => {
        // unsafe {
        //     let mu = ($self as *const LogicalModules) as *mut LogicalModules;
        //     (*mu).start_cnt += 1;
        // }
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        if let Some($opt) = $self.$opt.as_ref() {
            $opt.init().await?;
        }
    };
}

macro_rules! init_module {
    ($self:ident,$sys:ident,$opt:ident) => {
        $self.$opt.init().await?;
    };
    ($self:ident,$sys:ident,$opt:ident,Option<$type:ty>) => {
        $self.$opt.as_ref().unwrap().init().await?;
    };
    ($self:ident,$sys:ident,$opt:ident,$type:ty) => {
        $self.$opt.init().await?;
    };
}

macro_rules! start_module_opt {
    ($self:ident,$sys:ident,$opt:ident) => {
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        if let Some($opt) = $self.$opt.as_ref() {
            $sys.sub_tasks.lock().await.append(&mut $opt.start().await?);
        }
    };
}

macro_rules! start_module {
    ($self:ident,$sys:ident,$opt:ident) => {
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.start().await?);
    };
    ($self:ident,$sys:ident,$opt:ident,Option<$type:ty>) => {
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.as_ref().unwrap().start().await?);
    };
    ($self:ident,$sys:ident,$opt:ident,$type:ty) => {
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.start().await?);
    };
}

macro_rules! start_modules {
    ([$( $module:ident,$modulety:ty ),*], [$( $master_module:ident,$master_modulety:ty ),*],[$( $worker_module:ident,$worker_modulety:ty ),*]) => {
        pub struct LogicalModules {

            $( pub $module : $modulety, )*
            $( pub $master_module : Option<$master_modulety>, )*
            $( pub $worker_module : Option<$worker_modulety>, )*
        }

        impl LogicalModules {
            pub fn new(config: NodesConfig) -> Arc<Option<LogicalModules>> {
                let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<BroadcastMsg>(1);
                let arc = Arc::new(None);
                let args = LogicalModuleNewArgs {
                    btx: broadcast_tx,
                    logical_models: None,
                    parent_name: "".to_owned(),
                    nodes_config: config.clone(),
                    logical_modules_ref: LogicalModulesRef {
                        inner: Arc::downgrade(&arc),
                    },
                };
                wasm_host_funcs::set_singleton_modules(args.logical_modules_ref.clone());
                modules_global_bridge::set_singleton_modules(args.logical_modules_ref.clone());



                let mut logical_modules = LogicalModules {
                    $( $module : <$modulety>::new(args.clone()), )*
                    $( $master_module : None, )*
                    $( $worker_module : None, )*
                };
                let is_master = config.this.1.is_master();
                assert!(is_master || config.this.1.is_worker());
                if is_master {
                    $( logical_modules.$master_module = Some(<$master_modulety>::new(args.clone())); )*
                } else {
                    $( logical_modules.$worker_module = Some(<$worker_modulety>::new(args.clone())); )*
                }
                let _ = unsafe { util::unsafe_mut(&*arc) }.replace(logical_modules);
                arc
            }
            pub async fn start(&self, sys: &Sys) -> WSResult<()> {
                $(
                    init_module!(self, sys, $module);
                )*

                $(
                    init_module_opt!(self, sys, $master_module);
                )*

                $(
                    init_module_opt!(self, sys, $worker_module);
                )*

                $(
                    start_module!(self, sys, $module);
                )*

                $(
                    start_module_opt!(self, sys, $master_module);
                )*

                $(
                    start_module_opt!(self, sys, $worker_module);
                )*
                // assert!(self.start_cnt == ALL_MODULES_COUNT.add(0));
                Ok(())
            }
        }
    };
}

// impl LogicalModules {

// }

start_modules!(
    [
        p2p,
        P2PModule,
        metric_publisher,
        MetricPublisher,
        os,
        OperatingSystem,
        kv_store_engine,
        KvStoreEngine,
        appmeta_manager,
        AppMetaManager,
        data_general,
        DataGeneral,
        http_handler,
        HttpHandlerDispatch,
        dist_lock,
        DistLock,
        instance_manager,
        InstanceManager,
        executor,
        Executor,
        kv_user_client,
        KvUserClient
    ],
    [
        metric_observor,
        MetricObservor,
        // master_http,
        // MasterHttpHandler,
        master,
        Master,
        // master_kv,
        // MasterKv,
        data_master,
        DataMaster,
        app_master,
        MasterAppMgmt
    ],
    [worker, WorkerCore]
);
