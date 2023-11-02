use async_trait::async_trait;
use std::{
    cell::RefCell,
    sync::{Arc, Weak},
};
use tokio::task::JoinHandle;

use crate::{
    kv::{
        data_router::DataRouter, data_router_client::DataRouterClient,
        dist_kv_raft::tikvraft_proxy::RaftModule, kv_client::KVClient,
    },
    network::{
        p2p::{self, P2PModule, P2P},
        p2p_client::{self, P2PClient},
    },
    result::WSResult,
};

pub struct Sys {
    pub logical_modules: Arc<LogicalModules>,
    sub_tasks: RefCell<Vec<JoinHandle<()>>>,
}

impl Sys {
    pub fn new() -> Sys {
        Sys {
            logical_modules: LogicalModules::new(LogicalNodeConfig {
                as_scheduler: true,
                as_data_router: true,
                as_kv: true,
            }),
            sub_tasks: Vec::new().into(),
        }
    }
    pub async fn wait_for_end(&self) {
        if let Err(err) = self.logical_modules.start(self) {
            // log::error!("start logical nodes error: {:?}", err);
        }
        for task in self.sub_tasks.borrow_mut().iter_mut() {
            match task.await {
                Ok(_) => {}
                Err(err) => {
                    // log::error!("join task error: {:?}", err);
                }
            }
        }
    }
}

pub type NodeID = usize;

pub struct LogicalNodeConfig {
    as_scheduler: bool,
    as_data_router: bool,
    as_kv: bool,
}

pub struct LogicalModuleNewArgs {
    pub btx: BroadcastSender,
    pub logical_models: Option<Weak<LogicalModules>>,
}
impl Clone for LogicalModuleNewArgs {
    fn clone(&self) -> Self {
        Self {
            btx: self.btx.clone(),
            logical_models: self.logical_models.clone(),
        }
    }
}

pub trait LogicalModule {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized;
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>>;
}

#[derive(Clone)]
pub enum BroadcastMsg {
    SysEnd,
}

pub type BroadcastSender = tokio::sync::broadcast::Sender<BroadcastMsg>;
// pub type BroadcastReceiver = tokio::sync::broadcast::Receiver<BroadcastMsg>;

pub struct LogicalModules {
    kv_client: KVClient,                  // each module need a kv service
    data_router_client: DataRouterClient, // kv_client_need a data_router service
    p2p_client: P2PClient,                // modules need a client to call p2p service

    p2p: P2PModule, // network basic service
    // pub scheduler_node: Option<SchedulerNode>, // scheduler service
    data_router: Option<DataRouter>, // data_router service
                                     // pub kv_node: Option<KVNode>,               // kv service
}

macro_rules! make_path {
    ($name:ident) => {
        $name
    };
    ($p:ident, $($ps:ident),+) => {
        raft_kv.raft_module
        // concat_idents!($($ps).+,.,make_path!($ps))
    };
}

macro_rules! module_getter {
    ($name:ident, $type:ty) => {
        impl LogicalModules {
            pub fn $name<'a>(&'a self) -> &'a $type {
                &self.$name
            }
        }
    };
    ($name:ident, $type:ty, $p:ident) => {
        impl LogicalModules {
            pub fn $name<'a>(&'a self) -> &'a $type {
                &self.$p.$name
            }
        }
    };
}

module_getter!(kv_client, KVClient);
module_getter!(data_router_client, DataRouterClient);
module_getter!(p2p_client, P2PClient);
module_getter!(p2p, P2PModule);
module_getter!(data_router, Option<DataRouter>);

impl LogicalModules {
    pub fn new(config: LogicalNodeConfig) -> Arc<LogicalModules> {
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<BroadcastMsg>(1);
        let args = LogicalModuleNewArgs {
            btx: broadcast_tx,
            logical_models: todo!(),
        };
        let p2p = P2PModule::new(args.clone());
        let data_router = if config.as_data_router {
            Some(DataRouter::new(args.clone()))
        } else {
            None
        };
        let p2p_client = P2PClient::new(args.clone());
        let kv_client = KVClient::new(args.clone());
        let data_router_client = DataRouterClient::new(args.clone());

        // let scheduler_node =
        // if config.as_scheduler {
        //     Some(SchedulerNode::new())
        // } else
        // {
        //     None
        // };

        // let kv_node =
        // if config.as_kv {
        //     Some(KVNode::new())
        // } else
        // {
        //     None
        // };

        let arc = Arc::new(LogicalModules {
            kv_client,
            data_router_client,
            p2p_client,

            p2p,
            // scheduler_node,
            data_router,
            // kv_node,
        });
        arc.p2p.setup_logical_modules_view(Arc::downgrade(&arc));
        if let Some(v) = arc.data_router.as_ref() {
            v.raft_kv
                .raft_module
                .setup_logical_modules_view(Arc::downgrade(&arc));
        }
        arc
    }

    pub fn start(&self, sys: &Sys) -> WSResult<()> {
        sys.sub_tasks.borrow_mut().append(&mut self.p2p.start()?);
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.p2p_client.start()?);
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.kv_client.start()?);
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.data_router_client.start()?);
        if let Some(data_router) = &self.data_router {
            sys.sub_tasks.borrow_mut().append(&mut data_router.start()?);
        }
        Ok(())
    }
}
