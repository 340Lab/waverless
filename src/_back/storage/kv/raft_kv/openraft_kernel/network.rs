use async_trait::async_trait;

use openraft::error::InstallSnapshotError;
use openraft::error::NetworkError;
use openraft::error::RPCError;
use openraft::error::RaftError;
use openraft::error::RemoteError;
use openraft::raft::AppendEntriesRequest;
use openraft::raft::AppendEntriesResponse;
use openraft::raft::InstallSnapshotRequest;
use openraft::raft::InstallSnapshotResponse;
use openraft::raft::VoteRequest;
use openraft::raft::VoteResponse;
use openraft::BasicNode;
use openraft::RaftNetwork;
use openraft::RaftNetworkFactory;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::ExampleTypeConfig;
use crate::sys::NodeID;

pub struct ExampleNetwork {}

impl ExampleNetwork {}

// NOTE: This could be implemented also on `Arc<ExampleNetwork>`, but since it's empty, implemented directly.
#[async_trait]
impl RaftNetworkFactory<ExampleTypeConfig> for ExampleNetwork {
    type Network = ExampleNetworkConnection;

    async fn new_client(&mut self, target: NodeID, node: &BasicNode) -> Self::Network {
        ExampleNetworkConnection {
            owner: ExampleNetwork {},
            target,
            target_node: node.clone(),
        }
    }
}

pub struct ExampleNetworkConnection {
    owner: ExampleNetwork,
    target: NodeID,
    target_node: BasicNode,
}

#[async_trait]
impl RaftNetwork<ExampleTypeConfig> for ExampleNetworkConnection {
    async fn send_append_entries(
        &mut self,
        req: AppendEntriesRequest<ExampleTypeConfig>,
    ) -> Result<AppendEntriesResponse<NodeID>, RPCError<NodeID, BasicNode, RaftError<NodeID>>> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-append", req)
            .await
    }

    async fn send_install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<ExampleTypeConfig>,
    ) -> Result<InstallSnapshotResponse<NodeID>, RPCError<NodeID, BasicNode, RaftError<NodeID>>>
    {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-snapshot", req)
            .await
    }

    async fn send_vote(
        &mut self,
        req: VoteRequest<NodeID>,
    ) -> Result<VoteResponse<NodeID>, RPCError<NodeID, BasicNode, RaftError<NodeID>>> {
        self.owner
            .send_rpc(self.target, &self.target_node, "raft-vote", req)
            .await
    }
}
