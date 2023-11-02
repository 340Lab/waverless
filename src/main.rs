#[macro_use]
pub mod logical_modules_view;

use kv::dist_kv_raft;
use sys::Sys;
use tokio::task::block_in_place;

mod kv;
pub mod network;
pub mod result;
mod sys;

// inlcude macro logical_module

// Include the `items` module, which is generated from items.proto.

#[tokio::main]
async fn main() {
    // dist_kv_raft::tikvraft_proxy::start();
    Sys::new().wait_for_end().await;
}
