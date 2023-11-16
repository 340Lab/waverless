#![allow(clippy::all)]
#![deny(
    unused_imports,
    unused_variables,
    unused_mut,
    clippy::unnecessary_mut_passed,
    unused_results
)]

use clap::Parser;
use cmd_arg::CmdArgs;

use sys::Sys;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

// #[macro_use]
// pub mod module_view;
pub mod cmd_arg;
pub mod config;
pub mod error_collector;
mod kv;
// pub mod module_iter;
// pub mod module_state_trans;
pub mod network;
pub mod request_router;
pub mod result;
mod sys;
pub mod util;

#[cfg(test)]
mod test;
// inlcude macro logical_module

// Include the `items` module, which is generated from items.proto.

#[tokio::main]
async fn main() {
    start_tracing();

    // tracing_subscriber::fmt::init();

    let args = CmdArgs::parse();
    let config = config::read_config(args.config_file);
    tracing::info!("config: {:?}", config);
    // dist_kv_raft::tikvraft_proxy::start();
    Sys::new(config).wait_for_end().await;
}

pub fn start_tracing() {
    let my_filter = tracing_subscriber::filter::filter_fn(|v| {
        // println!("{}", v.module_path().unwrap());
        // println!("{}", v.name());
        // if v.module_path().unwrap().contains("quinn_proto") {
        //     return false;
        // }

        // if v.module_path().unwrap().contains("qp2p::wire_msg") {
        //     return false;
        // }

        // println!("{}", v.target());
        if v.module_path().unwrap().contains("async_raft") {
            return false;
        }

        // if v.module_path().unwrap().contains("less::network::p2p") {
        //     return false;
        // }

        // v.level() == &tracing::Level::ERROR
        //     || v.level() == &tracing::Level::WARN
        //     || v.level() == &tracing::Level::INFO
        v.level() != &tracing::Level::TRACE
        // v.level() == &tracing::Level::INFO
        // true
    });
    let my_layer = tracing_subscriber::fmt::layer();
    tracing_subscriber::registry()
        .with(my_layer.with_filter(my_filter))
        .init();
}

pub fn new_test_systems() -> Vec<Sys> {
    let mut systems = vec![];
    for i in 1..4 {
        let config = config::read_config(format!("node{}_config.yaml", i));
        tracing::info!("config: {:?}", config);
        systems.push(Sys::new(config));
    }
    systems
}
