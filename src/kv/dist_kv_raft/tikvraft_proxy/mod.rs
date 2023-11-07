use prost::bytes::Bytes;
use raft::{
    eraftpb::{Entry, EntryType},
    prelude::{ConfChange, Message},
    raw_node::RawNode,
    storage::MemStorage,
    Config, Ready, StateRole,
};
use slog::{o, Drain};
use std::{
    cell::UnsafeCell,
    sync::{
        mpsc::{channel, RecvTimeoutError},
        Arc, Weak,
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

use crate::{
    module_iter::*,
    module_state_trans::LogicalModuleWaiter,
    module_view::TiKVRaftModuleLMView,
    network::p2p::P2PModule,
    network::serial::MsgPack,
    result::WSResult,
    sync_loop,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, Sys},
    util::JoinHandleWrapper,
};

pub enum RaftMsg {
    Propose {
        id: u8,
        callback: Box<dyn Fn() + Send>,
    },
    Raft(Message),
}

#[derive(LogicalModuleParent, LogicalModule)]
pub struct TiKVRaftModule {
    pub logical_modules_view: TiKVRaftModuleLMView,
    name: String,
}

impl LogicalModule for TiKVRaftModule {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            logical_modules_view: TiKVRaftModuleLMView::new(),
            name: args.parent_name,
        }
    }

    fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let (tx, rx) = std::sync::mpsc::channel();
        self.logical_modules_view
            .p2p()
            .regist_dispatch(move |m: raft::prelude::Message| {
                tracing::info!("raft msg: {:?}", m);
                tx.send(RaftMsg::Raft(m)).unwrap_or_else(|e| {
                    tracing::error!(
                        "send raft msg to thread channel error, raft thread may be dead, err:{e:?}"
                    );
                });
                Ok(())
            });
        let listen_for_net = self.logical_modules_view.p2p().listen();
        let waiter = LogicalModuleWaiter::new(vec![listen_for_net]);
        let raft_thread = std::thread::spawn(move || {
            new_tick_thread(rx, waiter);
        });
        Ok(vec![raft_thread.into()])
    }
    fn name(&self) -> &str {
        &self.name
    }
}

pub fn new_node() -> RawNode<MemStorage> {
    // Select some defaults, then change what we need.
    let config = Config {
        id: 1,
        ..Default::default()
    };
    // Initialize logger.
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    // ... Make any configuration changes.
    // After, make sure it's valid!
    config.validate().unwrap();
    // We'll use the built-in `MemStorage`, but you will likely want your own.
    // Finally, create our Raft node!
    let storage = MemStorage::new_with_conf_state((vec![1], vec![]));
    let mut node = RawNode::new(&config, storage, &logger).unwrap();
    node
}

struct RaftThreadState {
    node: RawNode<MemStorage>,
    remaining_timeout: Duration,
    timeout: Duration,
    rx: std::sync::mpsc::Receiver<RaftMsg>,
}

impl RaftThreadState {
    fn new(rx: std::sync::mpsc::Receiver<RaftMsg>) -> Self {
        let node = new_node();
        let timeout = Duration::from_millis(100);
        Self {
            node,
            remaining_timeout: timeout,
            timeout,
            rx,
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        if self.node.has_ready() {
            let mut ready = self.node.ready();
            handle_ready(&mut self.rx, self.remaining_timeout, &mut self.node, ready);
        }

        // Tick raft node
        let elapsed = now.elapsed();
        if elapsed >= self.remaining_timeout {
            self.remaining_timeout = self.timeout;
            // We drive Raft every 100ms.
            self.node.tick();
            // tracing::info!("raft thread tick");
        } else {
            self.remaining_timeout -= elapsed;
        }
    }
}

pub fn new_tick_thread(rx: std::sync::mpsc::Receiver<RaftMsg>, mut waiter: LogicalModuleWaiter) {
    let mut state = RaftThreadState::new(rx);

    sync_loop!("TiKVRaftModule::raft_tick", waiter, { state.tick() });
}

fn handle_ready(
    rx: &mut std::sync::mpsc::Receiver<RaftMsg>,
    remaining_timeout: Duration,
    node: &mut RawNode<MemStorage>,
    mut ready: Ready,
) {
    fn handle_messages(msgs: Vec<Message>) {}
    fn handle_committed_entries(node: &mut RawNode<MemStorage>, entries: Vec<Entry>) {
        let mut _last_apply_index = 0;
        for entry in entries {
            // TODO:
            // Mostly, you need to save the last apply index to resume applying
            // after restart. Here we just ignore this because we use a Memory storage.
            _last_apply_index = entry.index;

            if entry.data.is_empty() {
                // From new elected leaders.
                continue;
            }

            match entry.get_entry_type() {
                EntryType::EntryConfChange => {
                    // // For conf change messages, make them effective.
                    // let mut cc = ConfChange::default();
                    // cc.merge_from_bytes(&entry.data).unwrap();
                    // let cs = node.apply_conf_change(&cc).unwrap();
                    // store.wl().set_conf_state(cs);
                }
                EntryType::EntryNormal => {
                    // // For normal proposals, extract the key-value pair and then
                    // // insert them into the kv engine.
                    // let data = str::from_utf8(&entry.data).unwrap();
                    // let reg = Regex::new("put ([0-9]+) (.+)").unwrap();
                    // if let Some(caps) = reg.captures(data) {
                    //     kv_pairs.insert(caps[1].parse().unwrap(), caps[2].to_string());
                    // }
                }
                EntryType::EntryConfChangeV2 => {
                    panic!("unsupport EntryConfChangeV2")
                }
            }

            if node.raft.state == StateRole::Leader {
                // The leader should response to the clients, tell them if their proposals
                // succeeded or not.

                // TODO:
                // let proposal = proposals.lock().unwrap().pop_front().unwrap();
                // proposal.propose_success.send(true).unwrap();
            }

            // TODO: handle EntryConfChange
        }
    }

    // 1.Send messages to other peers.
    if !ready.messages().is_empty() {
        handle_messages(ready.take_messages())
    }

    // 2.select peers msgs
    match rx.recv_timeout(remaining_timeout) {
        Ok(RaftMsg::Propose { id, callback }) => {
            // TODO: figure out what propose is
            // cbs.insert(id, callback);
            // node.propose(vec![], vec![id]).unwrap();
        }
        Ok(RaftMsg::Raft(m)) => {
            if let Err(e) = node.step(m) {
                tracing::warn!("raft step error:{:?}", e);
            }
        }
        Err(RecvTimeoutError::Timeout) => (), // there's no message recently, just skip.
        Err(RecvTimeoutError::Disconnected) => {
            panic!("tx is held by network dispatcher, should not be disconnected")
        }
    }

    // 3.This is a snapshot, we need to apply the snapshot at first.
    if !ready.snapshot().is_empty() {
        node.mut_store()
            .wl()
            .apply_snapshot(ready.snapshot().clone())
            .unwrap();
    }

    // 4.There are some newly committed log entries which you must apply to the state machine
    handle_committed_entries(node, ready.take_committed_entries());

    // 5.Send persisted messages to other peers.
    if !ready.persisted_messages().is_empty() {
        for msg in ready.take_persisted_messages() {}
    }

    // 6.advance the Raft
    let mut light_rd = node.advance(ready);
    // Like step 1 and 3, you can use functions to make them behave the same.
    handle_messages(light_rd.take_messages());
    handle_committed_entries(node, light_rd.take_committed_entries());
    node.advance_apply();

    // 7.New entries not appended
    // TODO: make sure the logic here is correct
    let mut ready = node.ready();
    if !ready.entries().is_empty() {
        // Append entries to the Raft log
        node.mut_store().wl().append(ready.entries()).unwrap();
    }

    // The node may vote for a new leader,
    //  or the commit index has been increased.
    //  We must persist the changed HardState:
    if let Some(hs) = ready.hs() {
        // Raft HardState changed, and we need to persist it.
        node.mut_store().wl().set_hardstate(hs.clone());
    }
}
