use crate::{
    config::NodesConfig,
    fs::Fs,
    kv::dyn_kv::client::DynKvClient,
    metric::{observor::MetricObservor, publisher::MetricPublisher},
    // module_iter::*,
    // module_view::setup_views,
    network::p2p::P2PModule,
    schedule::{
        container_manager::ContainerManager, executor::Executor, http_handler::ScheNode,
        master::ScheMaster, worker::ScheWorker,
    },
};
use crate::{
    // kv::{data_router::DataRouter, data_router_client::DataRouterClient, kv_client::KVClient},
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
    pub logical_modules: Arc<LogicalModules>,
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
        if let Err(err) = self.logical_modules.start(self).await {
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

macro_rules! logical_modules_ref_impl {
    ($module:ident,$type:ty) => {
        impl LogicalModulesRef {
            pub fn $module(&self) -> &$type {
                unsafe {
                    (*self.inner.as_ref().unwrap().as_ptr())
                        .$module
                        .as_ref()
                        .unwrap()
                }
            }
        }
    };
}

macro_rules! logical_modules_refs {
    ($module:ident,$t:ty) => {
        logical_modules_ref_impl!($module,$t);
    };
    ($module:ident,$t:ty,$($modules:ident,$ts:ty),+) => {
        // logical_modules_ref_impl!($module,$t);
        logical_modules_refs!($module,$t);
        logical_modules_refs!($($modules,$ts),+);
    };
}

macro_rules! count_modules {
    ($module:ident,$t:ty) => {1usize};
    ($module:ident,$t:ty,$($modules:ident,$ts:ty),+) => {1usize + count_modules!($($modules,$ts),+)};
}

macro_rules! logical_modules {
    // outter struct
    ($($modules:ident,$ts:ty),+)=>{
        #[derive(Default)]
        pub struct LogicalModules {
            new_cnt:usize,
            start_cnt:usize,
            $(pub $modules: Option<$ts>),+
        }

        logical_modules_refs!($($modules,$ts),+);
        lazy_static! {
            /// This is an example for using doc comment attributes
            static ref ALL_MODULES_COUNT: usize = count_modules!($($modules,$ts),+);
        }
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
//     // pub kv_client: KVClient, // each module need a kv service
//     // #[sub]
//     // #[view()]
//     // pub data_router_client: DataRouterClient, // kv_client_need a data_router service
//     // #[sub]
//     // pub p2p_client: P2PClient, // modules need a client to call p2p service
//     // #[parent]
//     // pub p2p: P2PModule, // network basic service
//     // pub scheduler_node: Option<SchedulerNode>, // scheduler service
//     // pub general_kv_client: GKV::KVClient,
//     // pub general_kv: Option<GKV>,
//     // #[view(p2p, local_kv_client)]
//     // pub raft: Option<Box<dyn Raft>>,
//     pub meta_kv_client: Box<dyn KVClient>, // get set key range
//     // #[view(p2p, raft)]
//     // pub meta_kv: Option<Box<dyn KVNode>>, // run the raft or other consensus algorithm, handle meta_kv request
//     /// get set by metakv or generalkv directly
//     pub local_kv_client: Box<dyn KVClient>,
//     // handle request, local storage operations
//     pub local_kv: Option<Box<dyn KVNode>>,
//     // #[parent]
//     // pub data_router: Option<DataRouter>, // data_router service
//     // pub kv_node: Option<KVNode>,               // kv service
// }

#[derive(Clone)]
pub struct LogicalModulesRef {
    inner: Option<Weak<LogicalModules>>,
}

impl LogicalModulesRef {
    pub fn new(inner: Arc<LogicalModules>) -> LogicalModulesRef {
        let inner = Arc::downgrade(&inner);
        LogicalModulesRef { inner: Some(inner) }
    }
}
// impl LogicalModulesRef {
//     fn setup(&mut self, modules: Arc<LogicalModules>) {
//         self.inner = Some(Arc::downgrade(&modules));
//     }
// }

macro_rules! logical_module_view_impl {
    ($module:ident,$module_name:ident,$type:ty) => {
        impl $module {
            pub fn $module_name(&self) -> &$type {
                self.inner.$module_name()
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
        if let Some($opt) = $self.$opt.as_ref().unwrap() {
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
            .append(&mut $self.$opt.as_ref().unwrap().start().await?);
    };
}

logical_modules!(
    p2p,
    P2PModule,
    request_handler,
    Box<dyn ScheNode>,
    metric_publisher,
    MetricPublisher,
    metric_observor,
    Option<MetricObservor>,
    dyn_kv_client,
    DynKvClient,
    container_manager,
    ContainerManager,
    executor,
    Executor,
    fs,
    Fs
);

// logical_module_view_impl!(MetaKVClientView);
// logical_module_view_impl!(MetaKVClientView, meta_kv_client, Box<dyn KVClient>);
// logical_module_view_impl!(MetaKVClientView, meta_kv, Option<Box<dyn KVNode>>);
// logical_module_view_impl!(MetaKVClientView, p2p, P2PModule);

logical_module_view_impl!(MetaKVView);
// logical_module_view_impl!(MetaKVView, p2p, P2PModule);
// logical_module_view_impl!(MetaKVView, meta_kv, Option<Box<dyn KVNode>>);
// logical_module_view_impl!(MetaKVView, local_kv, Option<Box<dyn KVNode>>);

logical_module_view_impl!(P2PView);
logical_module_view_impl!(P2PView, p2p, P2PModule);

logical_module_view_impl!(ScheMasterView);
logical_module_view_impl!(ScheMasterView, p2p, P2PModule);

logical_module_view_impl!(ScheWorkerView);
logical_module_view_impl!(ScheWorkerView, p2p, P2PModule);

logical_module_view_impl!(RequestHandlerView);
logical_module_view_impl!(RequestHandlerView, p2p, P2PModule);
logical_module_view_impl!(RequestHandlerView, request_handler, Box<dyn ScheNode>);
logical_module_view_impl!(RequestHandlerView, executor, Executor);

logical_module_view_impl!(MetricObservorView);
logical_module_view_impl!(MetricObservorView, p2p, P2PModule);
logical_module_view_impl!(MetricObservorView, metric_observor, Option<MetricObservor>);

logical_module_view_impl!(MetricPublisherView);
logical_module_view_impl!(MetricPublisherView, p2p, P2PModule);
logical_module_view_impl!(MetricPublisherView, metric_observor, Option<MetricObservor>);

logical_module_view_impl!(DynKvClientView);
logical_module_view_impl!(DynKvClientView, p2p, P2PModule);
logical_module_view_impl!(DynKvClientView, dyn_kv_client, DynKvClient);
logical_module_view_impl!(DynKvClientView, container_manager, ContainerManager);

logical_module_view_impl!(ExecutorView);
logical_module_view_impl!(ExecutorView, p2p, P2PModule);
logical_module_view_impl!(ExecutorView, container_manager, ContainerManager);
logical_module_view_impl!(ExecutorView, executor, Executor);
logical_module_view_impl!(ExecutorView, dyn_kv_client, DynKvClient);

logical_module_view_impl!(ContainerManagerView);
logical_module_view_impl!(ContainerManagerView, p2p, P2PModule);
logical_module_view_impl!(ContainerManagerView, container_manager, ContainerManager);

logical_module_view_impl!(FsView);
logical_module_view_impl!(FsView, fs, Fs);

fn modules_mut_ref(modules: &Arc<LogicalModules>) -> &mut LogicalModules {
    // let _ = SETTED_MODULES_COUNT.fetch_add(1, Ordering::SeqCst);
    let mu = unsafe { &mut *(Arc::downgrade(modules).as_ptr() as *mut LogicalModules) };
    mu.new_cnt += 1;
    mu
}

impl LogicalModules {
    // pub fn iter<'a(&'a self) -> LogicalModuleIter<'a> {
    //     LogicalModuleIter {
    //         logical_modules: self,
    //         index: 0,
    //     }
    // }

    pub fn new(config: NodesConfig) -> Arc<LogicalModules> {
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<BroadcastMsg>(1);
        let arc = Arc::new(LogicalModules::default());
        let args = LogicalModuleNewArgs {
            btx: broadcast_tx,
            logical_models: None,
            parent_name: "".to_owned(),
            nodes_config: config.clone(),
            logical_modules_ref: LogicalModulesRef {
                inner: Arc::downgrade(&arc).into(),
            },
        };
        let is_master = config.this.1.is_master();
        assert!(is_master || config.this.1.is_worker());

        modules_mut_ref(&arc).p2p = Some(P2PModule::new(args.clone()));
        modules_mut_ref(&arc).request_handler = Some(if is_master {
            Box::new(ScheMaster::new(args.clone()))
        } else {
            Box::new(ScheWorker::new(args.clone()))
        });
        modules_mut_ref(&arc).metric_publisher = Some(MetricPublisher::new(args.clone()));
        modules_mut_ref(&arc).metric_observor = if is_master {
            Some(Some(MetricObservor::new(args.clone())))
        } else {
            Some(None)
        };
        modules_mut_ref(&arc).dyn_kv_client = Some(DynKvClient::new(args.clone()));
        modules_mut_ref(&arc).container_manager = Some(ContainerManager::new(args.clone()));
        modules_mut_ref(&arc).executor = Some(Executor::new(args.clone()));
        modules_mut_ref(&arc).fs = Some(Fs::new(args.clone()));
        // modules_mut_ref(&arc).p2p_kernel = Some(Box::new(P2PQuicNode::new(args.clone())));
        // setup_views(&arc);
        arc
    }

    pub async fn start(&self, sys: &Sys) -> WSResult<()> {
        start_module!(self, sys, p2p);
        start_module!(self, sys, request_handler);
        start_module!(self, sys, metric_publisher);
        start_module_opt!(self, sys, metric_observor);
        start_module!(self, sys, dyn_kv_client);
        start_module!(self, sys, container_manager);
        start_module!(self, sys, executor);
        start_module!(self, sys, fs);

        assert!(self.start_cnt == ALL_MODULES_COUNT.add(0));
        assert!(self.start_cnt == self.new_cnt);
        Ok(())
    }
}
