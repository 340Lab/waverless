// Follow the guide to use https://datafuselabs.github.io/openraft/
pub mod client;
pub mod network;
pub mod store;

use openraft::{BasicNode, Config, Raft};
use std::sync::Arc;

use crate::sys::NodeID;

use self::{
    network::ExampleNetwork,
    store::{ExampleRequest, ExampleResponse, ExampleStore},
};

openraft::declare_raft_types!(
    /// Declare the type configuration for example K/V store.
    pub ExampleTypeConfig: D = ExampleRequest, R = ExampleResponse, NodeId = NodeID, Node = BasicNode
);

pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, Arc<ExampleStore>>;

pub async fn start() {
    let node_id: NodeID = 0;
    // Create a configuration for the raft instance.
    let config = Arc::new(Config::default().validate().unwrap());

    // Create a instance of where the Raft data will be stored.
    let store = Arc::new(ExampleStore::default());

    // Create the network layer that will connect and communicate the raft instances and
    // will be used in conjunction with the store created above.
    let network = ExampleNetwork {};

    // Create a local raft instance.
    let raft = Raft::new(node_id, config.clone(), network, store.clone());
}
