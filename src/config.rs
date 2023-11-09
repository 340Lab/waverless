use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path};

use crate::sys::NodeID;

#[derive(Debug)]
pub struct Config {
    pub peers: Vec<(SocketAddr, NodeID)>,
    pub this: (SocketAddr, NodeID),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct YamlConfig {
    nodes: Vec<SocketAddr>,
    this: SocketAddr,
}

pub fn read_config(file_path: impl AsRef<Path>) -> Config {
    let file = std::fs::File::open(file_path).unwrap_or_else(|err| {
        panic!("open config file failed, err: {:?}", err);
    });
    let yaml_config: YamlConfig = serde_yaml::from_reader(file).unwrap_or_else(|e| {
        panic!("parse yaml config file failed, err: {:?}", e);
    });

    let nodes_with_id_iter = || {
        yaml_config
            .nodes
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i as NodeID + 1))
    };
    Config {
        peers: nodes_with_id_iter()
            .filter(|(v, _i)| v != &yaml_config.this)
            .collect(),
        this: nodes_with_id_iter()
            .filter(|(v, _i)| v == &yaml_config.this)
            .next()
            .unwrap()
            .clone(),
    }
}
