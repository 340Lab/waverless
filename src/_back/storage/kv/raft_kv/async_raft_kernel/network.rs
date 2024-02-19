// We use anyhow::Result in our impl below.
use super::{
    storage::{ClientRequest, OpeType},
    AsyncRaftModule,
};
use crate::{
    kv::raft_kv::RaftKvNode,
    network::proto,
    result::WSError,
    sys::{MetaKvView, NodeID},
};
use anyhow::Result;
use async_raft::{
    raft::{
        AppendEntriesRequest, AppendEntriesResponse, ConflictOpt, Entry, EntryConfigChange,
        EntryNormal, EntryPayload, EntrySnapshotPointer, InstallSnapshotRequest,
        InstallSnapshotResponse, MembershipConfig, VoteRequest, VoteResponse,
    },
    RaftNetwork,
};
use async_trait::async_trait;

/// A type which emulates a network transport and implements the `RaftNetwork` trait.
pub struct RaftRouter {
    // ... some internal state ...
    view: MetaKvView,
}

impl RaftRouter {
    pub fn new(view: MetaKvView) -> Self {
        Self { view }
    }
}

macro_rules! transbetween {
    ($t1:ty,$t2:ty,$($fields:ident),+) => {
        impl From<$t1> for $t2 {
            fn from(v: $t1) -> Self {
                Self {
                    $($fields: v.$fields),+
                }
            }
        }
        impl From<$t2> for $t1 {
            fn from(v: $t2) -> Self {
                Self {
                    $($fields: v.$fields),+
                }
            }
        }
    };
}

transbetween!(
    proto::raft::VoteRequest,
    VoteRequest,
    term,
    candidate_id,
    last_log_index,
    last_log_term
);
transbetween!(proto::raft::VoteResponse, VoteResponse, term, vote_granted);

impl From<proto::raft::log_entry::EntryNormal> for EntryNormal<ClientRequest> {
    fn from(v: proto::raft::log_entry::EntryNormal) -> Self {
        Self {
            data: ClientRequest {
                client: v.client,
                serial: v.serial,
                status: v.status,
                ope_type: OpeType::Delete,
                operation: vec![],
            },
        }
    }
}
impl From<EntryNormal<ClientRequest>> for proto::raft::log_entry::EntryNormal {
    fn from(v: EntryNormal<ClientRequest>) -> Self {
        Self {
            client: v.data.client,
            serial: v.data.serial,
            status: v.data.status,
        }
    }
}

impl From<proto::raft::log_entry::MembershipConfig> for MembershipConfig {
    fn from(v: proto::raft::log_entry::MembershipConfig) -> Self {
        Self {
            members: v.members.into_iter().collect(),
            members_after_consensus: if v.members_after_consensus_exist {
                Some(v.members_after_consensus.into_iter().collect())
            } else {
                None
            },
        }
    }
}
impl From<MembershipConfig> for proto::raft::log_entry::MembershipConfig {
    fn from(v: MembershipConfig) -> Self {
        Self {
            members: v.members.into_iter().collect(),
            members_after_consensus_exist: v.members_after_consensus.is_some(),
            members_after_consensus: v
                .members_after_consensus
                .map(|v| v.into_iter().collect())
                .unwrap_or_default(),
        }
    }
}

impl From<proto::raft::log_entry::EntryConfigChange> for EntryConfigChange {
    fn from(v: proto::raft::log_entry::EntryConfigChange) -> Self {
        Self {
            membership: v.membership.unwrap().into(),
        }
    }
}
impl From<EntryConfigChange> for proto::raft::log_entry::EntryConfigChange {
    fn from(v: EntryConfigChange) -> Self {
        Self {
            membership: Some(v.membership.into()),
        }
    }
}

impl From<proto::raft::log_entry::EntrySnapshotPointer> for EntrySnapshotPointer {
    fn from(v: proto::raft::log_entry::EntrySnapshotPointer) -> Self {
        Self {
            id: v.id,
            membership: v.membership.unwrap().into(),
        }
    }
}
impl From<EntrySnapshotPointer> for proto::raft::log_entry::EntrySnapshotPointer {
    fn from(v: EntrySnapshotPointer) -> Self {
        Self {
            id: v.id,
            membership: Some(v.membership.into()),
        }
    }
}

impl From<proto::raft::log_entry::Payload> for EntryPayload<ClientRequest> {
    fn from(v: proto::raft::log_entry::Payload) -> Self {
        match v {
            proto::raft::log_entry::Payload::Blank(_) => EntryPayload::Blank,
            proto::raft::log_entry::Payload::Normal(v) => EntryPayload::Normal(v.into()),
            proto::raft::log_entry::Payload::ConfigChange(v) => {
                EntryPayload::ConfigChange(v.into())
            }
            proto::raft::log_entry::Payload::SnapshotPointer(v) => {
                EntryPayload::SnapshotPointer(v.into())
            }
        }
    }
}
impl From<EntryPayload<ClientRequest>> for proto::raft::log_entry::Payload {
    fn from(v: EntryPayload<ClientRequest>) -> Self {
        match v {
            EntryPayload::Blank => Self::Blank(false /*dummy bool*/),
            EntryPayload::Normal(v) => Self::Normal(v.into()),
            EntryPayload::ConfigChange(v) => Self::ConfigChange(v.into()),
            EntryPayload::SnapshotPointer(v) => Self::SnapshotPointer(v.into()),
        }
    }
}

impl From<proto::raft::LogEntry> for Entry<ClientRequest> {
    fn from(v: proto::raft::LogEntry) -> Self {
        Self {
            term: v.term,
            index: v.index,
            payload: v.payload.unwrap().into(),
        }
    }
}
impl From<Entry<ClientRequest>> for proto::raft::LogEntry {
    fn from(v: Entry<ClientRequest>) -> Self {
        Self {
            term: v.term,
            index: v.index,
            payload: Some(v.payload.into()),
        }
    }
}

impl From<proto::raft::AppendEntriesRequest> for AppendEntriesRequest<ClientRequest> {
    fn from(v: proto::raft::AppendEntriesRequest) -> Self {
        Self {
            term: v.term,
            leader_id: v.leader_id,
            prev_log_index: v.prev_log_index,
            prev_log_term: v.prev_log_term,
            entries: v.entries.into_iter().map(Into::into).collect(),
            leader_commit: v.leader_commit,
        }
    }
}

impl From<AppendEntriesRequest<ClientRequest>> for proto::raft::AppendEntriesRequest {
    fn from(v: AppendEntriesRequest<ClientRequest>) -> Self {
        Self {
            term: v.term,
            leader_id: v.leader_id,
            prev_log_index: v.prev_log_index,
            prev_log_term: v.prev_log_term,
            entries: v.entries.into_iter().map(Into::into).collect(),
            leader_commit: v.leader_commit,
        }
    }
}

impl From<proto::raft::AppendEntriesResponse> for AppendEntriesResponse {
    fn from(v: proto::raft::AppendEntriesResponse) -> Self {
        Self {
            term: v.term,
            success: v.success,
            conflict_opt: if v.success {
                None
            } else {
                Some(ConflictOpt {
                    term: v.conflict_term,
                    index: v.conflict_index,
                })
            },
        }
    }
}
impl From<AppendEntriesResponse> for proto::raft::AppendEntriesResponse {
    fn from(v: AppendEntriesResponse) -> Self {
        Self {
            term: v.term,
            success: v.success,
            conflict_term: if v.success {
                0
            } else {
                v.conflict_opt.as_ref().unwrap().term
            },
            conflict_index: if v.success {
                0
            } else {
                v.conflict_opt.as_ref().unwrap().index
            },
        }
    }
}

impl AsyncRaftModule {
    pub fn regist_rpc(&self) {
        let self_view = self.view.clone();
        self.view.p2p().regist_rpc::<proto::raft::VoteRequest, _>(
            move |nid: NodeID, _p2p, task_id, c| {
                let self_view = self_view.clone();
                let _ = tokio::spawn(async move {
                    // tracing::info!(
                    //     "handling vote request: node:{} task:{} req:{:?}",
                    //     nid,
                    //     task_id,
                    //     c
                    // );
                    if let Some(raft_meta) = self_view.meta_kv() {
                        let reft_meta = raft_meta.downcast_ref::<RaftKvNode>().unwrap();
                        match reft_meta.raft_inner.raft().vote(c.into()).await {
                            Ok(res) => {
                                // tracing::info!("handled vote request success: {:?}", res);
                                if let Err(err) = self_view
                                    .p2p()
                                    .send_resp(nid, task_id, proto::raft::VoteResponse::from(res))
                                    .await
                                {
                                    tracing::error!("send vote response error: {:?}", err);
                                }
                            }
                            Err(err) => {
                                tracing::error!("handle vote request error: {:?}", err);
                            }
                        }
                    } else {
                        tracing::error!(
                            "raft module is in data router, but data router is not set"
                        );
                    }
                });

                Ok(())
            },
        );
        let self_view = self.view.clone();
        self.view
            .p2p()
            .regist_rpc::<proto::raft::AppendEntriesRequest, _>(
                move |nid: NodeID, _p2p, task_id, req| {
                    let self_view = self_view.clone();
                    let _ = tokio::spawn(async move {
                        // tracing::info!(
                        //     "handling append entries request: node:{} task:{} req:{:?}",
                        //     nid,
                        //     task_id,
                        //     req
                        // );
                        if let Some(raft_meta) = self_view.meta_kv() {
                            let reft_meta = raft_meta.downcast_ref::<RaftKvNode>().unwrap();
                            match reft_meta.raft_inner.raft().append_entries(req.into()).await {
                                Ok(res) => {
                                    // tracing::info!(
                                    //     "handled append entries request success: {:?}",
                                    //     res
                                    // );
                                    if let Err(err) = self_view
                                        .p2p()
                                        .send_resp(
                                            nid,
                                            task_id,
                                            proto::raft::AppendEntriesResponse::from(res),
                                        )
                                        .await
                                    {
                                        tracing::error!(
                                            "send append entries response error: {:?}",
                                            err
                                        );
                                    }
                                }
                                Err(err) => {
                                    tracing::error!(
                                        "handle append entries request error: {:?}",
                                        err
                                    );
                                }
                            }
                        } else {
                            tracing::error!(
                                "raft module is in data router, but data router is not set"
                            );
                        }
                    });
                    Ok(())
                },
            );
    }
}

#[async_trait]
impl RaftNetwork<ClientRequest> for RaftRouter {
    /// Send an AppendEntries RPC to the target Raft node (§5).
    async fn append_entries(
        &self,
        target: u64,
        rpc: AppendEntriesRequest<ClientRequest>,
    ) -> Result<AppendEntriesResponse> {
        // tracing::info!("append_entries: target: {}, rpc: {:?}", target, rpc);
        let req = proto::raft::AppendEntriesRequest::from(rpc);
        let resp = self.view.p2p().call_rpc(target as NodeID, &req).await?;
        // ... snip ...
        Ok(resp.into())
    }

    /// Send an InstallSnapshot RPC to the target Raft node (§7).
    async fn install_snapshot(
        &self,
        _target: u64,
        _rpc: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse> {
        // tracing::info!("install_snapshot: target: {}, rpc: {:?}", target, rpc);
        // ... snip ...
        // Ok(InstallSnapshotResponse { term: 0 })
        Err(anyhow::Error::new(WSError::NotImplemented))
    }

    /// Send a RequestVote RPC to the target Raft node (§5).
    async fn vote(&self, target: u64, req: VoteRequest) -> Result<VoteResponse> {
        // tracing::info!("start vote");
        let req = proto::raft::VoteRequest::from(req);
        let resp = self.view.p2p().call_rpc(target as NodeID, &req).await?;
        // ... snip ...
        tracing::info!("vote: target: {}, req: {:?}", target, req);
        Ok(resp.into())
    }
}
