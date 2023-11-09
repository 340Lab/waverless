use std::sync::Arc;

use crate::{
    module_view::RaftModuleLMView,
    result::{ErrCvt, WSResult},
    sys::{LogicalModuleNewArgs, NodeID},
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
    name: String,
    pub logical_modules_view: RaftModuleLMView,
    raft_module: RwLock<Option<MemRaft>>,
}

impl AsyncRaftModule {
    pub fn raft(&self) -> MemRaft {
        self.raft_module.read().as_ref().unwrap().clone()
    }
}

#[async_trait]
impl LogicalModule for AsyncRaftModule {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            logical_modules_view: RaftModuleLMView::new(),
            name: args.parent_name,
            raft_module: RwLock::new(None),
        }
    }

    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let node_id: NodeID = self.logical_modules_view.p2p().this_node.1;
        // Build our Raft runtime config, then instantiate our
        // RaftNetwork & RaftStorage impls.
        let config = Arc::new(
            Config::build("primary-raft-group".into())
                .heartbeat_interval(100)
                .validate()
                .expect("failed to build Raft config"),
        );
        let network = Arc::new(RaftRouter::new(self.logical_modules_view.clone()));
        let storage = Arc::new(MemStore::new(node_id as u64));

        // Create a new Raft node, which spawns an async task which
        // runs the Raft core logic. Keep this Raft instance around
        // for calling API methods based on events in your app.
        let raft = Raft::new(node_id as u64, config, network, storage);
        assert!(self.raft_module.write().replace(raft).is_none());

        self.regist_rpc();
        self.raft()
            .initialize(
                self.logical_modules_view
                    .p2p()
                    .nodeid2addr
                    .iter()
                    .map(|(nid, _)| *nid as u64)
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

    fn name(&self) -> &str {
        &self.name
    }
}
