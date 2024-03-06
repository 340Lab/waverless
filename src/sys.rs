use crate::{
    config::NodesConfig,
    general::{
        m_appmeta_manager::AppMetaManager,
        m_fs::Fs,
        m_kv_store_engine::KvStoreEngine,
        m_metric_publisher::MetricPublisher,
        network::{http_handler::HttpHandler, m_p2p::P2PModule},
    },
    master::{
        m_http_handler::MasterHttpHandler, m_master::Master, m_master_kv::MasterKv,
        m_metric_observor::MetricObservor,
    },
    util,
    worker::{
        m_executor::Executor, m_http_handler::WorkerHttpHandler,
        m_instance_manager::InstanceManager, m_kv_user_client::KvUserClient, m_worker::WorkerCore,
        wasm_host_funcs::set_singleton_modules,
    },
};
use crate::{
    // kv::{data_router::DataRouter, data_router_client::DataRouterClient, kv_client::KvClient},
    // module_iter::LogicalModuleParent,
    // network::p2p::P2PModule,
    result::WSResult,
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use lazy_static::lazy_static;
use std::{
    ops::Add,
    sync::{Arc, Weak},
};
use tokio::sync::Mutex;

pub struct Sys {
    logical_modules: Arc<Option<LogicalModules>>,
    sub_tasks: Mutex<Vec<JoinHandleWrapper>>,
}

impl Sys {
    pub fn new(config: NodesConfig) -> Sys {
        Sys {
            logical_modules: LogicalModules::new(config),
            sub_tasks: Vec::new().into(),
        }
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
    // async fn listen_async_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
    // fn listen_sync_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
}

#[derive(Clone, Debug)]
pub enum BroadcastMsg {
    SysEnd,
}

pub type BroadcastSender = tokio::sync::broadcast::Sender<BroadcastMsg>;

// #[derive(LogicalModuleParent)]

// 使用trait的目的是为了接口干净
// #[derive(ModuleView)]

// macro_rules! logical_modules_ref_impl {
//     ($module:ident,$type:ty) => {
//         impl LogicalModulesRef {
//             pub fn $module(&self) -> &$type {
//                 unsafe {
//                     (*self.inner.as_ref().unwrap().as_ptr())
//                         .$module
//                         .as_ref()
//                         .unwrap()
//                 }
//             }
//         }
//     };
// }

// macro_rules! logical_modules_refs {
//     ($module:ident,$t:ty) => {
//         logical_modules_ref_impl!($module,$t);
//     };
//     ($module:ident,$t:ty,$($modules:ident,$ts:ty),+) => {
//         // logical_modules_ref_impl!($module,$t);
//         logical_modules_refs!($module,$t);
//         logical_modules_refs!($($modules,$ts),+);
//     };
// }

macro_rules! count_modules {
    ($module:ident,$t:ty) => {1usize};
    ($module:ident,$t:ty,$($modules:ident,$ts:ty),+) => {1usize + count_modules!($($modules,$ts),+)};
}

macro_rules! logical_modules {
    // outter struct
    ($($modules:ident,$ts:ty),+)=>{

        pub struct LogicalModules {
            new_cnt:usize,
            start_cnt:usize,
            $(pub $modules: $ts),+
        }

        // logical_modules_refs!($($modules,$ts),+);
        lazy_static! {
            /// This is an example for using doc comment attributes
            static ref ALL_MODULES_COUNT: usize = count_modules!($($modules,$ts),+);
        }

        // impl LogicalModules{
        //     pub async fn start(&self, sys: &Sys) -> WSResult<()> {
        //         $(start_module!(self, sys, $modules, $ts);)+
        //         // start_module!(self, sys, p2p, );
        //         // start_module!(self, sys, http_handler);
        //         // start_module!(self, sys, metric_publisher);
        //         // start_module!(self, sys, fs);

        //         // start_module_opt!(self, sys, metric_observor);
        //         // start_module!(self, sys, kv_user_client);
        //         // start_module!(self, sys, instance_manager);
        //         // start_module!(self, sys, executor);

        //         assert!(self.start_cnt == ALL_MODULES_COUNT.add(0));
        //         Ok(())
        //     }
        // }
        // $(impl $modules(&self)->&$ts{
        //     self.$modules.as_ref().unwrap()
        // })*
    }
}
// lazy_static! {
//     static ref SETTED_MODULES_COUNT: AtomicUsize = AtomicUsize::new(0);
//     static ref STARTED_MODULES_COUNT: AtomicUsize = AtomicUsize::new(0);
// }

// pub struct LogicalModules {
//     // #[sub]
//     // pub kv_client: KvClient, // each module need a kv service
//     // #[sub]
//     // #[view()]
//     // pub data_router_client: DataRouterClient, // kv_client_need a data_router service
//     // #[sub]
//     // pub p2p_client: P2PClient, // modules need a client to call p2p service
//     // #[parent]
//     // pub p2p: P2PModule, // network basic service
//     // pub scheduler_node: Option<SchedulerNode>, // scheduler service
//     // pub general_kv_client: GKv::KvClient,
//     // pub general_kv: Option<GKv>,
//     // #[view(p2p, local_kv_client)]
//     // pub raft: Option<Box<dyn Raft>>,
//     pub meta_kv_client: Box<dyn KvClient>, // get set key range
//     // #[view(p2p, raft)]
//     // pub meta_kv: Option<Box<dyn KvNode>>, // run the raft or other consensus algorithm, handle meta_kv request
//     /// get set by metakv or generalkv directly
//     pub local_kv_client: Box<dyn KvClient>,
//     // handle request, local storage operations
//     pub local_kv: Option<Box<dyn KvNode>>,
//     // #[parent]
//     // pub data_router: Option<DataRouter>, // data_router service
//     // pub kv_node: Option<KvNode>,               // kv service
// }

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
                    &(*self.inner.inner.as_ptr())
                        .as_ref()
                        .unwrap()
                        .$module_name
                        .as_ref()
                        .unwrap()
                }
            }
        }
    };
    ($module:ident,$module_name:ident,$type:ty) => {
        impl $module {
            pub fn $module_name(&self) -> &$type {
                unsafe { &(*self.inner.inner.as_ptr()).as_ref().unwrap().$module_name }
            }
        }
    };
    ($module:ident) => {
        #[derive(Clone)]
        struct $module {
            inner: LogicalModulesRef,
        }
        impl $module {
            pub fn new(inner: LogicalModulesRef) -> Self {
                $module { inner }
            }
            // fn setup(&mut self, modules: Arc<LogicalModules>) {
            //     self.inner.setup(modules);
            // }
        }
    };
}

macro_rules! start_module_opt {
    ($self:ident,$sys:ident,$opt:ident) => {
        unsafe {
            let mu = ($self as *const LogicalModules) as *mut LogicalModules;
            (*mu).start_cnt += 1;
        }
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        if let Some($opt) = $self.$opt.as_ref() {
            $sys.sub_tasks.lock().await.append(&mut $opt.start().await?);
        }
    };
}

macro_rules! start_module {
    ($self:ident,$sys:ident,$opt:ident) => {
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe {
            let mu = ($self as *const LogicalModules) as *mut LogicalModules;
            (*mu).start_cnt += 1;
        }
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.start().await?);
    };
    ($self:ident,$sys:ident,$opt:ident,Option<$type:ty>) => {
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe {
            let mu = ($self as *const LogicalModules) as *mut LogicalModules;
            (*mu).start_cnt += 1;
        }
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.as_ref().unwrap().start().await?);
    };
    ($self:ident,$sys:ident,$opt:ident,$type:ty) => {
        // let _ = STARTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
        unsafe {
            let mu = ($self as *const LogicalModules) as *mut LogicalModules;
            (*mu).start_cnt += 1;
        }
        $sys.sub_tasks
            .lock()
            .await
            .append(&mut $self.$opt.start().await?);
    };
}

logical_modules!(
    // general
    p2p,
    P2PModule,
    http_handler,
    Box<dyn HttpHandler>,
    metric_publisher,
    MetricPublisher,
    fs,
    Fs,
    kv_store_engine,
    KvStoreEngine,
    appmeta_manager,
    AppMetaManager,
    ////////////////////////////
    // master
    metric_observor,
    Option<MetricObservor>,
    master,
    Option<Master>,
    master_kv,
    Option<MasterKv>,
    ////////////////////////////
    // worker
    worker,
    Option<WorkerCore>,
    kv_user_client,
    Option<KvUserClient>,
    instance_manager,
    Option<InstanceManager>,
    // kv_storage,
    // KvStorage,
    executor,
    Option<Executor>
);

impl LogicalModules {
    // pub fn iter<'a(&'a self) -> LogicalModuleIter<'a> {
    //     LogicalModuleIter {
    //         logical_modules: self,
    //         index: 0,
    //     }
    // }

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
        set_singleton_modules(args.logical_modules_ref.clone());

        let is_master = config.this.1.is_master();
        assert!(is_master || config.this.1.is_worker());

        let mut logical_modules = LogicalModules {
            p2p: P2PModule::new(args.clone()),
            metric_publisher: MetricPublisher::new(args.clone()),
            fs: Fs::new(args.clone()),
            kv_store_engine: KvStoreEngine::new(args.clone()),
            http_handler: if is_master {
                Box::new(MasterHttpHandler::new(args.clone()))
            } else {
                Box::new(WorkerHttpHandler::new(args.clone()))
            },
            appmeta_manager: AppMetaManager::new(args.clone()),
            metric_observor: None,
            master: None,
            master_kv: None,
            worker: None,
            kv_user_client: None,
            instance_manager: None,
            executor: None,
            new_cnt: 0,
            start_cnt: 0,
        };

        if is_master {
            logical_modules.metric_observor = Some(MetricObservor::new(args.clone()));
            logical_modules.master = Some(Master::new(args.clone()));
            logical_modules.master_kv = Some(MasterKv::new(args.clone()));
        } else {
            logical_modules.kv_user_client = Some(KvUserClient::new(args.clone()));
            logical_modules.instance_manager = Some(InstanceManager::new(args.clone()));
            logical_modules.executor = Some(Executor::new(args.clone()));
            logical_modules.worker = Some(WorkerCore::new(args.clone()));
        }
        let _ = unsafe { util::unsafe_mut(&*arc) }.replace(logical_modules);
        arc
    }

    pub async fn start(&self, sys: &Sys) -> WSResult<()> {
        //general
        start_module!(self, sys, p2p);
        start_module!(self, sys, http_handler);
        start_module!(self, sys, metric_publisher);
        start_module!(self, sys, fs);
        start_module!(self, sys, kv_store_engine);
        start_module!(self, sys, appmeta_manager);

        // master
        start_module_opt!(self, sys, metric_observor);
        start_module_opt!(self, sys, master);
        start_module_opt!(self, sys, master_kv);
        //worker
        start_module_opt!(self, sys, worker);
        start_module_opt!(self, sys, kv_user_client);
        start_module_opt!(self, sys, instance_manager);
        start_module_opt!(self, sys, executor);

        assert!(self.start_cnt == ALL_MODULES_COUNT.add(0));
        Ok(())
    }
}
