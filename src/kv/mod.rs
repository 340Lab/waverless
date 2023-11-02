pub mod data_router;
pub mod data_router_client;
pub mod dist_kv;
pub mod dist_kv_raft;
pub mod dist_kv_rscode;
pub mod kv_client;
pub mod local_kv;
pub mod local_kv_sled;

pub struct KeyRange<'a> {
    start: &'a [u8],
    end: Option<&'a [u8]>,
}
