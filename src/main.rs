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

#[macro_use]
pub mod module_view;
pub mod cmd_arg;
pub mod config;
pub mod error_collector;
mod kv;
pub mod module_iter;
pub mod module_state_trans;
pub mod network;
pub mod result;
mod sys;
pub mod util;
// inlcude macro logical_module

// Include the `items` module, which is generated from items.proto.

#[tokio::main]
async fn main() {
    // let my_filter = tracing_subscriber::filter::filter_fn(|v| {
    //     // println!("{}", v.module_path().unwrap());
    //     // println!("{}", v.name());
    //     // if v.module_path().unwrap().contains("quinn_proto") {
    //     //     return false;
    //     // }

    //     // if v.module_path().unwrap().contains("qp2p::wire_msg") {
    //     //     return false;
    //     // }

    //     // println!("{}", v.target());
    //     if v.module_path().unwrap().contains("async_raft") {
    //         return false;
    //     }

    //     // if v.module_path().unwrap().contains("less::network::p2p") {
    //     //     return false;
    //     // }

    //     v.level() == &tracing::Level::ERROR
    //         || v.level() == &tracing::Level::WARN
    //         || v.level() == &tracing::Level::INFO
    //     // v.level() != &tracing::Level::TRACE //&& v.level() != &tracing::Level::INFO
    //     // true
    // });
    // let my_layer = tracing_subscriber::fmt::layer();
    // tracing_subscriber::registry()
    //     .with(my_layer.with_filter(my_filter))
    //     .init();

    tracing_subscriber::fmt::init();

    let args = CmdArgs::parse();
    let config = config::read_config(args.config_file);
    tracing::info!("config: {:?}", config);
    // dist_kv_raft::tikvraft_proxy::start();
    Sys::new(config).wait_for_end().await;
}
