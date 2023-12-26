use std::collections::{BTreeMap, HashMap};
use std::io::Cursor;

use anyhow::Result;
use async_raft::async_trait::async_trait;
use async_raft::raft::{Entry, EntryPayload, MembershipConfig};
use async_raft::storage::{CurrentSnapshotData, HardState, InitialState};
use async_raft::{AppData, AppDataResponse, NodeId, RaftStorage};
use prost::Message;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

use crate::kv::kv_interface::SetOptions;
use crate::network::proto;
use crate::network::proto::kv::kv_request::{KvDeleteRequest, KvPutRequest};
use crate::network::proto::kv::{KeyRange, KvPairs};
use crate::sys::MetaKVView;

const ERR_INCONSISTENT_LOG: &str =
    "a query was received which was expecting data to be in place which does not exist in the log";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum OpeType {
    Set,
    Delete,
}

/// The application data request type which the `MemStore` works with.
///
/// Conceptually, for demo purposes, this represents an update to a client's status info,
/// returning the previously recorded status.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientRequest {
    /// The ID of the client which has sent the request.
    pub client: String,
    /// The serial number of this request.
    pub serial: u64,
    /// A string describing the status of the client. For a real application, this should probably
    /// be an enum representing all of the various types of requests / operations which a client
    /// can perform.
    pub status: String,

    pub ope_type: OpeType,

    pub operation: Vec<u8>,
}

impl ClientRequest {
    pub fn proto_ope(&self) -> proto::kv::kv_request::Op {
        match self.ope_type {
            OpeType::Set => proto::kv::kv_request::Op::Set(KvPutRequest {
                kvs: KvPairs::decode(self.operation.as_slice()).unwrap().into(),
            }),
            OpeType::Delete => proto::kv::kv_request::Op::Delete(KvDeleteRequest {
                range: Some(KeyRange::decode(self.operation.as_slice()).unwrap()),
            }),
        }
    }
}

impl AppData for ClientRequest {}

/// The application data response type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ClientResponse(Option<String>);

impl AppDataResponse for ClientResponse {}

/// Error used to trigger Raft shutdown from storage.
#[derive(Clone, Debug, Error)]
pub enum ShutdownError {
    #[error("unsafe storage error")]
    UnsafeStorageError,
}

/// The application snapshot type which the `MemStore` works with.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MemStoreSnapshot {
    /// The last index covered by this snapshot.
    pub index: u64,
    /// The term of the last index covered by this snapshot.
    pub term: u64,
    /// The last memberhsip config included in this snapshot.
    pub membership: MembershipConfig,
    /// The data of the state machine at the time of this snapshot.
    pub data: Vec<u8>,
}

/// The state machine of the `MemStore`.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct MemStoreStateMachine {
    pub last_applied_log: u64,
    /// A mapping of client IDs to their state info.
    pub client_serial_responses: HashMap<String, (u64, Option<String>)>,
    /// The current status of a client by ID.
    pub client_status: HashMap<String, String>,
}

/// An in-memory storage system implementing the `async_raft::RaftStorage` trait.
pub struct MemStore {
    /// The ID of the Raft node for which this memory storage instances is configured.
    id: NodeId,
    /// The Raft log.
    log: RwLock<BTreeMap<u64, Entry<ClientRequest>>>,
    /// The Raft state machine.
    sm: RwLock<MemStoreStateMachine>,
    /// The current hard state.
    hs: RwLock<Option<HardState>>,
    /// The current snapshot.
    current_snapshot: RwLock<Option<MemStoreSnapshot>>,
    view: MetaKVView,
}

impl MemStore {
    /// Create a new `MemStore` instance.
    pub fn new(id: NodeId, view: MetaKVView) -> Self {
        let log = RwLock::new(BTreeMap::new());
        let sm = RwLock::new(MemStoreStateMachine::default());
        let hs = RwLock::new(None);
        let current_snapshot = RwLock::new(None);
        Self {
            id,
            log,
            sm,
            hs,
            current_snapshot,
            view,
        }
    }

    /// Create a new `MemStore` instance with some existing state (for testing).
    // #[cfg(test)]
    // pub fn new_with_state(
    //     id: NodeId,
    //     log: BTreeMap<u64, Entry<ClientRequest>>,
    //     sm: MemStoreStateMachine,
    //     hs: Option<HardState>,
    //     current_snapshot: Option<MemStoreSnapshot>,
    // ) -> Self {
    //     let log = RwLock::new(log);
    //     let sm = RwLock::new(sm);
    //     let hs = RwLock::new(hs);
    //     let current_snapshot = RwLock::new(current_snapshot);
    //     Self {
    //         id,
    //         log,
    //         sm,
    //         hs,
    //         current_snapshot,
    //     }
    // }

    /// Get a handle to the log for testing purposes.
    pub async fn get_log(&self) -> RwLockWriteGuard<'_, BTreeMap<u64, Entry<ClientRequest>>> {
        self.log.write().await
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, MemStoreStateMachine> {
        self.sm.write().await
    }

    /// Get a handle to the current hard state for testing purposes.
    pub async fn read_hard_state(&self) -> RwLockReadGuard<'_, Option<HardState>> {
        self.hs.read().await
    }
}

#[async_trait]
impl RaftStorage<ClientRequest, ClientResponse> for MemStore {
    type Snapshot = Cursor<Vec<u8>>;
    type ShutdownError = ShutdownError;

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_membership_config(&self) -> Result<MembershipConfig> {
        let log = self.log.read().await;
        let cfg_opt = log.values().rev().find_map(|entry| match &entry.payload {
            EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
            EntryPayload::SnapshotPointer(snap) => Some(snap.membership.clone()),
            _ => None,
        });
        Ok(match cfg_opt {
            Some(cfg) => cfg,
            None => MembershipConfig::new_initial(self.id),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_initial_state(&self) -> Result<InitialState> {
        let membership = self.get_membership_config().await?;
        let mut hs = self.hs.write().await;
        let log = self.log.read().await;
        let sm = self.sm.read().await;
        match &mut *hs {
            Some(inner) => {
                let (last_log_index, last_log_term) = match log.values().rev().next() {
                    Some(log) => (log.index, log.term),
                    None => (0, 0),
                };
                let last_applied_log = sm.last_applied_log;
                Ok(InitialState {
                    last_log_index,
                    last_log_term,
                    last_applied_log,
                    hard_state: inner.clone(),
                    membership,
                })
            }
            None => {
                let new = InitialState::new_initial(self.id);
                *hs = Some(new.hard_state.clone());
                Ok(new)
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, hs))]
    async fn save_hard_state(&self, hs: &HardState) -> Result<()> {
        *self.hs.write().await = Some(hs.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_log_entries(&self, start: u64, stop: u64) -> Result<Vec<Entry<ClientRequest>>> {
        // Invalid request, return empty vec.
        if start > stop {
            tracing::error!("invalid request, start > stop");
            return Ok(vec![]);
        }
        let log = self.log.read().await;
        Ok(log.range(start..stop).map(|(_, val)| val.clone()).collect())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn delete_logs_from(&self, start: u64, stop: Option<u64>) -> Result<()> {
        if stop.as_ref().map(|stop| &start > stop).unwrap_or(false) {
            tracing::error!("invalid request, start > stop");
            return Ok(());
        }
        let mut log = self.log.write().await;

        // If a stop point was specified, delete from start until the given stop point.
        if let Some(stop) = stop.as_ref() {
            for key in start..*stop {
                let _ = log.remove(&key);
            }
            return Ok(());
        }
        // Else, just split off the remainder.
        let _ = log.split_off(&start);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entry))]
    async fn append_entry_to_log(&self, entry: &Entry<ClientRequest>) -> Result<()> {
        let mut log = self.log.write().await;
        let _ = log.insert(entry.index, entry.clone());
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_log(&self, entries: &[Entry<ClientRequest>]) -> Result<()> {
        let mut log = self.log.write().await;
        for entry in entries {
            let _ = log.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    /// Apply the given log entry to the state machine.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a majority of the cluster, will be applied to the state machine.
    ///
    /// This is where the business logic of interacting with your application's state machine
    /// should live. This is 100% application specific. Perhaps this is where an application
    /// specific transaction is being started, or perhaps committed. This may be where a key/value
    /// is being stored. This may be where an entry is being appended to an immutable log.
    ///
    /// Error handling for this method is note worthy. If an error is returned from a call to this
    /// method, the error will be inspected, and if the error is an instance of
    /// `RaftStorage::ShutdownError`, then Raft will go into shutdown in order to preserve the
    /// safety of the data and avoid corruption. Any other errors will be propagated back up to the
    /// `Raft.client_write` call point.
    ///
    /// It is important to note that even in cases where an application specific error is returned,
    /// implementations should still record that the entry has been applied to the state machine.
    #[tracing::instrument(level = "trace", skip(self, data))]
    async fn apply_entry_to_state_machine(
        &self,
        index: &u64,
        data: &ClientRequest,
    ) -> Result<ClientResponse> {
        let mut sm = self.sm.write().await;
        // persist logs from [last_applied_log+1,index]
        // {
        //     let log = self.log.read().await;
        //     for (_log_idx, entry) in log.range(sm.last_applied_log + 1..*index + 1) {
        //         let kvs = entry.payload;
        match data.proto_ope() {
            proto::kv::kv_request::Op::Set(kvs) => {
                let _ = self
                    .view
                    .local_kv()
                    .as_ref()
                    .unwrap()
                    .set(
                        kvs.clone().kvs.unwrap().kvs,
                        SetOptions { consistent: true },
                    )
                    .await;

                tracing::info!(
                    "node {} set {:?}",
                    self.view.p2p().nodes_config.this.0,
                    kvs.clone().kvs.unwrap().kvs
                );
            }
            _ => {}
        }
        // }
        // }

        sm.last_applied_log = *index;
        // if let Some((serial, res)) = sm.client_serial_responses.get(&data.client) {
        //     if serial == &data.serial {
        //         return Ok(ClientResponse(res.clone()));
        //     }
        // }
        // let previous = sm
        //     .client_status
        //     .insert(data.client.clone(), data.status.clone());
        // let _ = sm
        //     .client_serial_responses
        //     .insert(data.client.clone(), (data.serial, previous.clone()));
        Ok(ClientResponse(None))
    }

    #[tracing::instrument(level = "trace", skip(self, entries))]
    async fn replicate_to_state_machine(&self, entries: &[(&u64, &ClientRequest)]) -> Result<()> {
        let mut sm = self.sm.write().await;
        for (index, data) in entries {
            sm.last_applied_log = **index;
            if let Some((serial, _)) = sm.client_serial_responses.get(&data.client) {
                if serial == &data.serial {
                    continue;
                }
            }
            let previous = sm
                .client_status
                .insert(data.client.clone(), data.status.clone());
            let _ = sm
                .client_serial_responses
                .insert(data.client.clone(), (data.serial, previous.clone()));
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn do_log_compaction(&self) -> Result<CurrentSnapshotData<Self::Snapshot>> {
        let (data, last_applied_log);
        {
            // Serialize the data of the state machine.
            let sm = self.sm.read().await;
            data = serde_json::to_vec(&*sm)?;
            last_applied_log = sm.last_applied_log;
        } // Release state machine read lock.

        let membership_config;
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let log = self.log.read().await;
            membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > last_applied_log)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));
        } // Release log read lock.

        let snapshot_bytes: Vec<u8>;
        let term;
        {
            let mut log = self.log.write().await;
            let mut current_snapshot = self.current_snapshot.write().await;
            term = log
                .get(&last_applied_log)
                .map(|entry| entry.term)
                .ok_or_else(|| anyhow::anyhow!(ERR_INCONSISTENT_LOG))?;
            *log = log.split_off(&last_applied_log);
            let _ = log.insert(
                last_applied_log,
                Entry::new_snapshot_pointer(
                    last_applied_log,
                    term,
                    "".into(),
                    membership_config.clone(),
                ),
            );

            let snapshot = MemStoreSnapshot {
                index: last_applied_log,
                term,
                membership: membership_config.clone(),
                data,
            };
            snapshot_bytes = serde_json::to_vec(&snapshot)?;
            *current_snapshot = Some(snapshot);
        } // Release log & snapshot write locks.

        tracing::trace!(
            { snapshot_size = snapshot_bytes.len() },
            "log compaction complete"
        );
        Ok(CurrentSnapshotData {
            term,
            index: last_applied_log,
            membership: membership_config.clone(),
            snapshot: Box::new(Cursor::new(snapshot_bytes)),
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn create_snapshot(&self) -> Result<(String, Box<Self::Snapshot>)> {
        Ok((String::from(""), Box::new(Cursor::new(Vec::new())))) // Snapshot IDs are insignificant to this storage engine.
    }

    #[tracing::instrument(level = "trace", skip(self, snapshot))]
    async fn finalize_snapshot_installation(
        &self,
        index: u64,
        term: u64,
        delete_through: Option<u64>,
        id: String,
        snapshot: Box<Self::Snapshot>,
    ) -> Result<()> {
        tracing::trace!(
            { snapshot_size = snapshot.get_ref().len() },
            "decoding snapshot for installation"
        );
        let raw = serde_json::to_string_pretty(snapshot.get_ref().as_slice())?;
        println!("JSON SNAP:\n{}", raw);
        let new_snapshot: MemStoreSnapshot = serde_json::from_slice(snapshot.get_ref().as_slice())?;
        // Update log.
        {
            // Go backwards through the log to find the most recent membership config <= the `through` index.
            let mut log = self.log.write().await;
            let membership_config = log
                .values()
                .rev()
                .skip_while(|entry| entry.index > index)
                .find_map(|entry| match &entry.payload {
                    EntryPayload::ConfigChange(cfg) => Some(cfg.membership.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| MembershipConfig::new_initial(self.id));

            match &delete_through {
                Some(through) => {
                    *log = log.split_off(&(through + 1));
                }
                None => log.clear(),
            }
            let _ = log.insert(
                index,
                Entry::new_snapshot_pointer(index, term, id, membership_config),
            );
        }

        // Update the state machine.
        {
            let new_sm: MemStoreStateMachine = serde_json::from_slice(&new_snapshot.data)?;
            let mut sm = self.sm.write().await;
            *sm = new_sm;
        }

        // Update current snapshot.
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = Some(new_snapshot);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn get_current_snapshot(&self) -> Result<Option<CurrentSnapshotData<Self::Snapshot>>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let reader = serde_json::to_vec(&snapshot)?;
                Ok(Some(CurrentSnapshotData {
                    index: snapshot.index,
                    term: snapshot.term,
                    membership: snapshot.membership.clone(),
                    snapshot: Box::new(Cursor::new(reader)),
                }))
            }
            None => Ok(None),
        }
    }
}
