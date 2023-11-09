use crate::{config::Config, module_iter::*, module_view::setup_views};
use async_trait::async_trait;
use std::{
    cell::RefCell,
    net::SocketAddr,
    sync::{Arc, Weak},
};

use crate::{
    kv::{data_router::DataRouter, data_router_client::DataRouterClient, kv_client::KVClient},
    module_iter::LogicalModuleParent,
    network::p2p::P2PModule,
    result::WSResult,
    util::JoinHandleWrapper,
};

pub struct Sys {
    pub logical_modules: Arc<LogicalModules>,
    sub_tasks: RefCell<Vec<JoinHandleWrapper>>,
}

impl Sys {
    pub fn new(config: Config) -> Sys {
        Sys {
            logical_modules: LogicalModules::new(LogicalNodeConfig {
                as_scheduler: true,
                as_data_router: true,
                as_kv: true,
                peers: config.peers.clone(),
                this: config.this,
            }),
            sub_tasks: Vec::new().into(),
        }
    }
    pub async fn wait_for_end(&self) {
        if let Err(err) = self.logical_modules.start(self).await {
            tracing::error!("start logical nodes error: {:?}", err);
        }
        for task in self.sub_tasks.borrow_mut().iter_mut() {
            task.join().await;
        }
    }
}

pub type NodeID = u32;

pub struct LogicalNodeConfig {
    as_scheduler: bool,
    as_data_router: bool,
    as_kv: bool,
    peers: Vec<(SocketAddr, NodeID)>,
    this: (SocketAddr, NodeID),
}

#[derive(Clone)]
pub struct LogicalModuleNewArgs {
    pub parent_name: String,
    pub btx: BroadcastSender,
    pub logical_models: Option<Weak<LogicalModules>>,
    pub peers_this: Option<(Vec<(SocketAddr, NodeID)>, (SocketAddr, NodeID))>,
}

impl LogicalModuleNewArgs {
    pub fn expand_parent_name(&mut self, this_name: &str) {
        let name = format!("{}::{}", self.parent_name, this_name);
        self.parent_name = name;
    }
}

#[async_trait]
pub trait LogicalModule {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized;
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>>;

    fn name(&self) -> &str;

    // async fn listen_async_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
    // fn listen_sync_signal(&self) -> tokio::sync::broadcast::Receiver<LogicalModuleState>;
}

#[derive(Clone, Debug)]
pub enum BroadcastMsg {
    SysEnd,
}

pub type BroadcastSender = tokio::sync::broadcast::Sender<BroadcastMsg>;

#[derive(LogicalModuleParent)]
pub struct LogicalModules {
    #[sub]
    pub kv_client: KVClient, // each module need a kv service
    #[sub]
    pub data_router_client: DataRouterClient, // kv_client_need a data_router service
    // #[sub]
    // pub p2p_client: P2PClient, // modules need a client to call p2p service
    #[parent]
    pub p2p: P2PModule, // network basic service
    // pub scheduler_node: Option<SchedulerNode>, // scheduler service
    #[parent]
    pub data_router: Option<DataRouter>, // data_router service
                                         // pub kv_node: Option<KVNode>,               // kv service
}

impl LogicalModules {
    // pub fn iter<'a(&'a self) -> LogicalModuleIter<'a> {
    //     LogicalModuleIter {
    //         logical_modules: self,
    //         index: 0,
    //     }
    // }

    pub fn new(config: LogicalNodeConfig) -> Arc<LogicalModules> {
        let (broadcast_tx, _broadcast_rx) = tokio::sync::broadcast::channel::<BroadcastMsg>(1);
        let args = LogicalModuleNewArgs {
            btx: broadcast_tx,
            logical_models: None,
            parent_name: "".to_owned(),
            peers_this: None,
        };

        let p2p = {
            let mut args = args.clone();
            args.peers_this = Some((config.peers.clone(), config.this));
            P2PModule::new(args)
        };
        let data_router = if config.as_data_router {
            Some(DataRouter::new(args.clone()))
        } else {
            None
        };

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

        let arc = LogicalModules {
            kv_client,
            data_router_client,
            // p2p_client,
            p2p,
            // scheduler_node,
            data_router,
            // kv_node,
        };

        let mut iter = arc.module_iter();
        while let Some(next) = iter.next_module() {
            tracing::info!("module itered {}", next.name())
        }
        drop(iter);

        let arc = Arc::new(arc);

        setup_views(&arc);
        arc
    }

    pub async fn start(&self, sys: &Sys) -> WSResult<()> {
        if let Some(data_router) = &self.data_router {
            sys.sub_tasks
                .borrow_mut()
                .append(&mut data_router.start().await?);
        }
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.data_router_client.start().await?);
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.p2p.start().await?);
        // sys.sub_tasks
        //     .borrow_mut()
        //     .append(&mut self.p2p_client.start().await?);
        sys.sub_tasks
            .borrow_mut()
            .append(&mut self.kv_client.start().await?);
        Ok(())
    }
}
