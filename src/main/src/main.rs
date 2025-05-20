#![allow(clippy::all)]
#![allow(invalid_reference_casting)] // allow unsafe cast
#![deny(
    unused_variables,
    unused_mut,
    unused_attributes,
    dead_code,
    clippy::unnecessary_mut_passed,
    unused_results,
    clippy::let_underscore_future,
    clippy::let_underscore_future,
    unused_must_use,
    unconditional_recursion
)]

use clap::Parser;
use cmd_arg::CmdArgs;

use sys::{LogicalModulesRef, Sys};
use tracing::Level;
use tracing_subscriber::{
    prelude::__tracing_subscriber_SubscriberExt, util::SubscriberInitExt, Layer,
};

pub mod general;
pub mod master;
pub mod worker;

pub mod apis;
pub mod cmd_arg;
pub mod config;
pub mod modules_global_bridge;
pub mod result;
pub mod sys;
pub mod util;
pub mod metrics;

#[tokio::main]
async fn main() {
    start_tracing();
    let args = CmdArgs::parse();
    let config = config::read_config(args.this_id, args.files_dir);
    tracing::info!("config: {:?}", config);
    // dist_kv_raft::tikvraft_proxy::start();
    let mut sys=Sys::new(config);
    let modules_ref=sys.new_logical_modules_ref();
    // modules_global_bridge::modules_ref_scope(modules_ref, async move{sys.wait_for_end().await;})   由于modules_ref_scope改为了异步函数，所以这里加上.await   曾俊
    modules_global_bridge::modules_ref_scope(modules_ref, async move{sys.wait_for_end().await;}).await;
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
        if let Some(mp) = v.module_path() {
            if mp.contains("async_raft") {
                return false;
            }
            if mp.contains("hyper") {
                return false;
            }
            if *v.level() == Level::DEBUG {
                // if mp.contains("wasm_serverless::worker::m_kv_user_client") {
                //     return false;
                // }
                // if mp.contains("wasm_serverless::general::m_data_general") {
                //     return false;
                // }
                // if mp.contains("wasm_serverless::master::m_data_master") {
                //     return false;
                // }
                if mp.contains("sled::pagecache") {
                    return false;
                }
                // return false;
            }
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
    let _ = tracing_subscriber::registry()
        .with(my_layer.with_filter(my_filter))
        .try_init();
}

pub fn new_test_systems() -> Vec<Sys> {
    let mut systems = vec![];
    for i in 1..4 {
        let config = config::read_config(i, "node_config.yaml");
        tracing::info!("config: {:?}", config);
        systems.push(Sys::new(config));
    }
    systems
}
