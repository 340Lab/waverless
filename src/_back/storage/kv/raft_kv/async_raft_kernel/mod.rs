use std::sync::Arc;

use crate::{
    result::{ErrCvt, WSResult},
    sys::{LogicalModuleNewArgs, MetaKvView, NodeID},
    util::JoinHandleWrapper,
};

pub mod network;
pub mod storage;

use self::storage::{ClientRequest, ClientResponse};
use crate::sys::LogicalModule;
use async_raft::{Config, Raft};
use async_trait::async_trait;
use network::RaftRouter;
use parking_lot::RwLock;
use storage::MemStore;
use ws_derive::LogicalModule;

/// A concrete Raft type used during testing.
pub type MemRaft = Raft<ClientRequest, ClientResponse, RaftRouter, MemStore>;

#[derive(LogicalModule)]
pub struct AsyncRaftModule {
    view: MetaKvView,
    raft_module: RwLock<Option<MemRaft>>,
}

impl AsyncRaftModule {
    pub fn raft(&self) -> MemRaft {
        self.raft_module.read().as_ref().unwrap().clone()
    }
}

#[async_trait]
impl LogicalModule for AsyncRaftModule {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: MetaKvView::new(args.logical_modules_ref.clone()),
            raft_module: RwLock::new(None),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        tracing::info!("start");
        let node_id: NodeID = self.view.p2p().nodes_config.this.0;
        // Build our Raft runtime config, then instantiate our
        // RaftNetwork & RaftStorage impls.
        let config = Arc::new(
            Config::build("primary-raft-group".into())
                .heartbeat_interval(100)
                .validate()
                .expect("failed to build Raft config"),
        );
        let network = Arc::new(RaftRouter::new(self.view.clone()));
        let storage: Arc<MemStore> = {
            let view = self.view.clone();
            Arc::new(MemStore::new(node_id as u64, view))
        };

        // Create a new Raft node, which spawns an async task which
        // runs the Raft core logic. Keep this Raft instance around
        // for calling API methods based on events in your app.
        let raft = Raft::new(node_id as u64, config, network, storage);
        assert!(self.raft_module.write().replace(raft).is_none());

        self.regist_rpc();
        self.raft()
            .initialize(
                self.view
                    .p2p()
                    .nodes_config
                    .get_meta_kv_nodes()
                    .iter()
                    .map(|&p| p as u64)
                    .collect(),
            )
            .await
            .map_err(|err| ErrCvt(err).to_ws_raft_err())?;

        // let (tx, rx) = std::sync::mpsc::channel();
        // self.logical_modules_view
        //     .p2p()
        //     .regist_dispatch(move |m: raft::prelude::Message| {
        //         tracing::info!("raft msg: {:?}", m);
        //         tx.send(RaftMsg::Raft(m)).unwrap_or_else(|e| {
        //             tracing::error!(
        //                 "send raft msg to thread channel error, raft thread may be dead, err:{e:?}"
        //             );
        //         });
        //         Ok(())
        //     });
        // let listen_for_net = self.logical_modules_view.p2p().listen();
        // let waiter = LogicalModuleWaiter::new(vec![listen_for_net]);
        // let view = self.logical_modules_view.clone();
        // let raft_thread = std::thread::spawn(move || {
        //     new_tick_thread(view, rx, waiter);
        // });
        Ok(vec![])
    }
}
