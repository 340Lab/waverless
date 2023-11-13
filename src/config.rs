use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::Path,
};

use crate::sys::NodeID;

#[derive(Debug, Clone)]
pub struct NodesConfig {
    pub peers: HashMap<NodeID, NodeConfig>,
    pub this: (NodeID, NodeConfig),
}

impl NodesConfig {
    pub fn get_meta_kv_nodes(&self) -> HashSet<NodeID> {
        self.peers
            .iter()
            .filter(|(_, config)| config.spec.contains("meta"))
            .map(|(id, _)| *id)
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub addr: SocketAddr,
    pub spec: HashSet<String>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct YamlConfig {
    pub nodes: HashMap<NodeID, NodeConfig>,
    pub this: NodeID,
}

pub fn read_config(file_path: impl AsRef<Path>) -> NodesConfig {
    let file = std::fs::File::open(file_path).unwrap_or_else(|err| {
        panic!("open config file failed, err: {:?}", err);
    });
    let mut yaml_config: YamlConfig = serde_yaml::from_reader(file).unwrap_or_else(|e| {
        panic!("parse yaml config file failed, err: {:?}", e);
    });

    NodesConfig {
        this: (
            yaml_config.this,
            yaml_config.nodes.remove(&yaml_config.this).unwrap(),
        ),
        peers: yaml_config.nodes,
    }
}
