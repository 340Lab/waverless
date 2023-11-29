use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
};

use crate::sys::NodeID;

#[derive(Debug, Clone)]
pub struct NodesConfig {
    pub peers: HashMap<NodeID, NodeConfig>,
    pub this: (NodeID, NodeConfig),
    pub file_dir: PathBuf,
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

impl NodeConfig {
    pub fn is_master(&self) -> bool {
        self.spec.contains("master")
    }
    pub fn is_worker(&self) -> bool {
        self.spec.contains("worker")
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlConfig {
    pub nodes: HashMap<NodeID, NodeConfig>,
    // pub this: NodeID,
}

pub fn read_yaml_config(file_path: impl AsRef<Path>) -> YamlConfig {
    let file = std::fs::File::open(file_path).unwrap_or_else(|err| {
        panic!("open config file failed, err: {:?}", err);
    });
    serde_yaml::from_reader(file).unwrap_or_else(|e| {
        panic!("parse yaml config file failed, err: {:?}", e);
    })
}

pub fn read_config(this_id: NodeID, file_path: impl AsRef<Path>) -> NodesConfig {
    let config_path = file_path.as_ref().join("config/node_config.yaml");
    let mut yaml_config = read_yaml_config(config_path);

    NodesConfig {
        this: (this_id, yaml_config.nodes.remove(&this_id).unwrap()),
        peers: yaml_config.nodes,
        file_dir: file_path.as_ref().to_path_buf(),
    }
}
