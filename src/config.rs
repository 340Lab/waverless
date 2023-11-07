use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::Path};

#[derive(Debug)]
pub struct Config {
    pub peers: Vec<SocketAddr>,
    pub this: SocketAddr,
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
    Config {
        peers: yaml_config
            .nodes
            .iter()
            .filter(|v| **v != yaml_config.this)
            .map(|v| *v)
            .collect(),
        this: yaml_config.this,
    }
}
