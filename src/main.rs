use clap::Parser;
use cmd_arg::CmdArgs;
use kv::dist_kv_raft;
use sys::Sys;
use tokio::task::block_in_place;

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
    tracing_subscriber::fmt::init();
    let args = CmdArgs::parse();
    let config = config::read_config(args.config_file);
    tracing::info!("config: {:?}", config);
    // dist_kv_raft::tikvraft_proxy::start();
    Sys::new(config).wait_for_end().await;
}
