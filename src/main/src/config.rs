use crate::sys::NodeID;
use core::panic;
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone)]
pub struct NodesConfig {
    pub peers: HashMap<NodeID, NodeConfig>,
    pub this: (NodeID, NodeConfig),
    pub file_dir: PathBuf,
}

impl NodesConfig {
    pub fn get_nodeconfig(&self, id: NodeID) -> &NodeConfig {
        if self.this.0 == id {
            &self.this.1
        } else {
            self.peers.get(&id).unwrap_or_else(|| {
                panic!("peers {:?}", self.peers);
            })
        }
    }
    pub fn node_cnt(&self) -> usize {
        self.peers.len() + 1
    }
    pub fn this_node(&self) -> NodeID {
        self.this.0
    }
    pub fn get_master_node(&self) -> NodeID {
        if self.this.1.is_master() {
            return self.this.0;
        }
        *self
            .peers
            .iter()
            .find(|(_, config)| config.is_master())
            .unwrap_or_else(|| {
                panic!("peers {:?}", self.peers);
            })
            .0
    }
    pub fn get_meta_kv_nodes(&self) -> HashSet<NodeID> {
        self.peers
            .iter()
            .filter(|(_, config)| config.spec.contains("meta"))
            .map(|(id, _)| *id)
            .collect()
    }
    pub fn get_worker_nodes(&self) -> HashSet<NodeID> {
        self.peers
            .iter()
            .filter(|(_, config)| config.is_worker())
            .map(|(id, _)| *id)
            .collect()
    }
    pub fn node_exist(&self, id: NodeID) -> bool {
        self.peers.contains_key(&id) || self.this.0 == id
    }
    pub fn all_nodes_iter<'a>(&'a self) -> impl Iterator<Item = (&'a NodeID, &'a NodeConfig)> {
        self.peers.iter().chain(Some((&self.this.0, &self.this.1)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfig {
    pub addr: SocketAddr,
    domain: Option<String>,
    pub spec: HashSet<String>,
}

impl NodeConfig {
    pub fn new(addr: SocketAddr, domain: Option<String>, spec: HashSet<String>) -> Self {
        Self { addr, domain, spec }
    }
    pub fn is_master(&self) -> bool {
        self.spec.contains("master")
    }
    pub fn is_worker(&self) -> bool {
        self.spec.contains("worker")
    }
    pub fn set_domain(&mut self, domain: Option<String>) {
        self.domain = domain;
    }
    fn get_http_domain<'a>(&'a self) -> Option<&'a str> {
        // check domain valid
        self.domain
            .as_ref()
            .filter(|d| {
                let ok = d.starts_with("http://") || d.starts_with("https://");
                if !ok {
                    tracing::warn!(
                        "Current domain is {}, domain should starts with http:// or https://",
                        d
                    );
                }
                ok
            })
            .map(|d| &**d)
    }

    pub fn http_url(&self) -> String {
        self.get_http_domain()
            .map(|d| d.to_string())
            .unwrap_or_else(|| format!("http://{}:{}", self.addr.ip(), self.addr.port() + 1))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlConfig {
    pub nodes: HashMap<NodeID, NodeConfig>,
    // pub this: NodeID,
}

fn read_yaml_config(file_path: impl AsRef<Path>) -> YamlConfig {
    tracing::info!("Running at dir: {:?}", std::env::current_dir());
    let path = file_path.as_ref().to_owned();
    let file = std::fs::File::open(file_path).unwrap_or_else(|err| {
        panic!("open config file {:?} failed, err: {:?}", path, err);
    });
    serde_yaml::from_reader(file).unwrap_or_else(|e| {
        panic!("parse yaml config file failed, err: {:?}", e);
    })
}

pub fn read_config(this_id: NodeID, file_path: impl AsRef<Path>) -> NodesConfig {
    let config_path = file_path.as_ref().join("files/node_config.yaml");
    let mut yaml_config = read_yaml_config(config_path);

    NodesConfig {
        this: (this_id, yaml_config.nodes.remove(&this_id).unwrap()),
        peers: yaml_config.nodes,
        file_dir: file_path.as_ref().to_path_buf(),
    }
}
